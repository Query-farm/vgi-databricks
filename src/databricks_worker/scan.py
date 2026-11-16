"""Shared machinery for the API-backed tables: pushdown, paging, row conversion.

Every table is a ``TableFunctionGenerator`` subclass of ``ApiScan``. A subclass
declares its ``FIXED_SCHEMA`` and implements:

* ``fanout(client, pd)`` (optional) resolves, once per scan, the ids a listing
  has to be repeated for, e.g. every cluster for ``cluster_events`` when no
  ``cluster_id`` was given. The ids are stored in the cursor, not recomputed.
* ``requests(pd, ids)`` turns the pushed-down SQL predicates into one or more
  API listings (path, query parameters or POST body, page parameter names).
* ``rows(client, items, wanted, listing)`` maps one page of API items to rows.

The cursor (``ScanState``) is ``(listing index, page token, fan-out ids)``. It
travels with each batch over the HTTP transport, holds no credentials, and lets
a scan resume on any worker process: Databricks page tokens are opaque and
stateless on the server side. ``auto_apply_filters`` re-applies every predicate
to each batch, so translating a predicate only ever has to *narrow* what the
API returns (a superset is fine), never be exact.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import dataclass, field
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Secret
from vgi.invocation import BindResponse
from vgi.table_filter_pushdown import PushdownFilters
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from .client import DATABRICKS_SECRET_TYPE, DatabricksClient, DatabricksConfig, NotFoundError

log = logging.getLogger(__name__)

TSTZ = pa.timestamp("us", tz="UTC")
STR_LIST = pa.list_(pa.string())
STR_MAP = pa.map_(pa.string(), pa.string())


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """Catalog tables take no arguments."""


@dataclass(kw_only=True)
class ScanState(ArrowSerializableDataclass):
    """Resumable cursor: which listing, the next page token, and the fan-out ids."""

    listing: int = 0
    token: str = ""
    resolved: bool = False
    ids: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class Listing:
    """One (possibly paginated) API collection to read.

    Attributes:
        path: Absolute API path.
        key: Response key holding the items; ``None`` means the response *is* one item.
        query: Query parameters (GET) or body fields (POST).
        size_param: Name of the page-size parameter, or ``None`` for unpaginated endpoints.
        max_size: The endpoint's maximum page size.
        token_param: Name of the request parameter that carries the page token.
        method: ``GET`` or ``POST``.
        missing_ok: Ignore "does not exist" (an equality filter named an id that is gone).
        context: Column values implied by the request (e.g. the pipeline id in the path).

    """

    path: str
    key: str | None
    query: tuple[tuple[str, Any], ...] = ()
    size_param: str | None = None
    max_size: int = 100
    token_param: str = "page_token"
    method: str = "GET"
    missing_ok: bool = False
    context: tuple[tuple[str, Any], ...] = ()


def secret_from(params: ProcessParams[Any]) -> dict[str, Any] | None:
    """Return the first resolved ``databricks`` secret, if any."""
    for secret in params.secrets.of_type(DATABRICKS_SECRET_TYPE):
        return dict(secret)
    return None


def client_from(params: ProcessParams[Any]) -> DatabricksClient:
    """Build a client from the secret DuckDB resolved for this call."""
    return DatabricksClient(DatabricksConfig.from_secret(secret_from(params)))


# --------------------------------------------------------------------- conversions


def ts(value: Any) -> dt.datetime | None:
    """Parse epoch milliseconds or an ISO-8601 string into an aware UTC datetime; 0 / empty is NULL."""
    if value is None or value == "" or value == 0:
        return None
    if isinstance(value, int | float):
        return dt.datetime.fromtimestamp(value / 1000.0, tz=dt.UTC)
    text = str(value)
    if text.isdigit():
        return dt.datetime.fromtimestamp(int(text) / 1000.0, tz=dt.UTC)
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)


def seconds(ms: Any) -> float | None:
    """Milliseconds to seconds; NULL stays NULL (0 is a real zero duration)."""
    if ms is None or ms == "":
        return None
    return float(ms) / 1000.0


def between(start: dt.datetime | None, end: dt.datetime | None) -> float | None:
    """Seconds from ``start`` to ``end`` when both are known."""
    if start is None or end is None:
        return None
    return (end - start).total_seconds()


def json_text(value: Any) -> str | None:
    """Serialize a JSON-able value for a JSON column (``None`` stays NULL)."""
    if value is None:
        return None
    return json.dumps(value, sort_keys=True)


def to_int(value: Any) -> int | None:
    """Coerce to int, keeping NULL."""
    return None if value is None or value == "" else int(value)


def str_map(value: Any) -> list[tuple[str, str]] | None:
    """Turn a JSON object of tags into MAP entries (sorted by key)."""
    if not isinstance(value, dict):
        return None
    return [(str(k), "" if v is None else str(v)) for k, v in sorted(value.items())]


def epoch_ms(value: Any) -> int | None:
    """Convert a timestamp / date filter constant to epoch milliseconds (UTC)."""
    if isinstance(value, dt.datetime):
        aware = value if value.tzinfo else value.replace(tzinfo=dt.UTC)
        return int(aware.timestamp() * 1000)
    if isinstance(value, dt.date):
        return int(dt.datetime(value.year, value.month, value.day, tzinfo=dt.UTC).timestamp() * 1000)
    return None


def iso(value: Any) -> str | None:
    """Format a timestamp filter constant as an ISO-8601 UTC string."""
    ms = epoch_ms(value)
    if ms is None:
        return None
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.UTC).isoformat().replace("+00:00", "Z")


# ------------------------------------------------------------------------ pushdown


class Pushdown:
    """Read-only helpers over the predicates DuckDB pushed into a scan."""

    def __init__(self, filters: PushdownFilters | None) -> None:
        """Wrap the (possibly absent) pushdown filters."""
        self.filters = filters

    def values(self, column: str) -> list[Any] | None:
        """Exact values a column is constrained to (``=`` or ``IN``), else None."""
        if not self.filters:
            return None
        arr = self.filters.get_column_values(column)
        if arr is None:
            return None
        return [v for v in arr.to_pylist() if v is not None]

    def one(self, column: str) -> Any | None:
        """Return the single value a column must equal, else None."""
        vals = self.values(column)
        return vals[0] if vals and len(vals) == 1 else None

    def bounds(self, column: str) -> tuple[Any, bool, Any, bool] | None:
        """``(low, low_inclusive, high, high_inclusive)`` for a column, else None."""
        if not self.filters:
            return None
        b = self.filters.get_column_bounds(column)
        if b is None:
            return None
        lo = b.min_value.as_py() if b.min_value is not None else None
        hi = b.max_value.as_py() if b.max_value is not None else None
        if lo is None and hi is None:
            return None
        return lo, b.min_inclusive, hi, b.max_inclusive

    def ms_range(self, column: str) -> tuple[int | None, int | None]:
        """Inclusive epoch-millisecond bounds for a timestamp column (strict bounds widen by nothing).

        A strict ``>`` / ``<`` is sent as inclusive; DuckDB's re-check trims the edge.
        """
        b = self.bounds(column)
        if b is None:
            return None, None
        lo, _, hi, _ = b
        return epoch_ms(lo), epoch_ms(hi)


class Remapped(Pushdown):
    """A ``Pushdown`` view whose columns were renamed / values translated for reuse."""

    def __init__(
        self, values: dict[str, list[Any]], bounds: dict[str, tuple[Any, bool, Any, bool] | None] | None = None
    ) -> None:
        """Hold precomputed constraints."""
        super().__init__(None)
        self._values = values
        self._bounds = {k: v for k, v in (bounds or {}).items() if v is not None}

    def values(self, column: str) -> list[Any] | None:
        """Return translated exact values for a native column."""
        return self._values.get(column)

    def bounds(self, column: str) -> tuple[Any, bool, Any, bool] | None:
        """Return the translated range for a native column."""
        return self._bounds.get(column)


MAX_ENUMERATED_RANGE = 100


def int_values(pd: Pushdown, column: str) -> list[int] | None:
    """``=`` / ``IN`` values of an id column as ints; None when absent or not numeric.

    DuckDB rewrites ``IN`` over consecutive integers (``job_id IN (103, 104)``) into a
    closed range, so a small closed integer range is enumerated too.
    """
    vals = pd.values(column)
    if not vals:
        b = pd.bounds(column)
        if b is None:
            return None
        lo, lo_inc, hi, hi_inc = b
        if not (isinstance(lo, int) and isinstance(hi, int)) or isinstance(lo, bool) or isinstance(hi, bool):
            return None
        lo, hi = (lo if lo_inc else lo + 1), (hi if hi_inc else hi - 1)
        if hi < lo or hi - lo >= MAX_ENUMERATED_RANGE:
            return None
        return list(range(lo, hi + 1))
    out = []
    for v in vals:
        try:
            out.append(int(v))
        except TypeError, ValueError:
            return None
    return sorted(set(out))


# --------------------------------------------------------------------------- base


@init_single_worker
class ApiScan(TableFunctionGenerator[NoArgs, ScanState]):
    """Base class for tables that page through one or more Databricks listings."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([])

    class Meta:
        """Overridden by every concrete table."""

        name = "api_scan"
        description = "Base class for Databricks API scans"

    @classmethod
    def on_bind(
        cls,
        params: BindParams[NoArgs],
        *,
        databricks: Annotated[dict[str, pa.Scalar[Any]] | None, Secret(DATABRICKS_SECRET_TYPE)] = None,
    ) -> BindResponse:
        """Declare the fixed schema and ask DuckDB for the ``databricks`` secret."""
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[NoArgs]) -> ScanState:
        """Start at the first listing's first page."""
        return ScanState()

    @classmethod
    def fanout(cls, client: DatabricksClient, pd: Pushdown) -> list[str] | None:
        """Ids to repeat the listing for (resolved once per scan); ``None`` when not needed."""
        return None

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str]) -> list[Listing]:
        """Return the listings to read for these pushed-down predicates."""
        raise NotImplementedError

    @classmethod
    def rows(
        cls, client: DatabricksClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map one page of API items to rows (dicts keyed by column name)."""
        raise NotImplementedError

    @classmethod
    def emit(cls, params: ProcessParams[Any], out: OutputCollector, rows: list[dict[str, Any]]) -> None:
        """Emit rows as one batch in the bound (projected) schema."""
        schema = params.output_schema
        columns = {f.name: [row.get(f.name) for row in rows] for f in schema}
        out.emit(pa.RecordBatch.from_pydict(columns, schema=schema))

    @classmethod
    def fetch(cls, client: DatabricksClient, listing: Listing, token: str) -> tuple[list[dict[str, Any]], str]:
        """Read one page of a listing; return its items and the next page token ('' when done)."""
        params: dict[str, Any] = {}
        for k, v in listing.query:
            if k in params:
                prev = params[k]
                params[k] = [*prev, v] if isinstance(prev, list) else [prev, v]
            else:
                params[k] = v
        if listing.size_param:
            params[listing.size_param] = min(client.config.page_size, listing.max_size)
        if token:
            params[listing.token_param] = token
        try:
            body = client.post(listing.path, params) if listing.method == "POST" else client.get(listing.path, params)
        except NotFoundError:
            if not listing.missing_ok:
                raise
            return [], ""
        if listing.key is None:
            return ([body] if isinstance(body, dict) and body else []), ""
        items = list(body.get(listing.key) or []) if isinstance(body, dict) else []
        nxt = str(body.get("next_page_token") or "") if isinstance(body, dict) else ""
        # Jobs signals the end with has_more, query history with has_next_page.
        for flag in ("has_more", "has_next_page"):
            if isinstance(body, dict) and body.get(flag) is False:
                nxt = ""
        if nxt == token:
            nxt = ""
        return items, nxt

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: ScanState, out: OutputCollector) -> None:
        """Read one page of the current listing, emit it, and advance the cursor."""
        client = client_from(params)
        pd = Pushdown(params.current_pushdown_filters)
        if not state.resolved:
            state.resolved = True
            ids = cls.fanout(client, pd)
            state.ids = list(ids) if ids is not None else []
        listings = cls.requests(pd, state.ids)
        wanted = set(params.output_schema.names)
        while state.listing < len(listings):
            listing = listings[state.listing]
            items, nxt = cls.fetch(client, listing, state.token)
            if nxt:
                state.token = nxt
            else:
                state.listing, state.token = state.listing + 1, ""
            if items:
                rows = cls.rows(client, items, wanted, listing)
                if rows:
                    cls.emit(params, out, rows)
                    return
        out.finish()


def drain(client: DatabricksClient, listing: Listing) -> list[dict[str, Any]]:
    """Read every page of a listing (used to resolve fan-out ids, which are small)."""
    out: list[dict[str, Any]] = []
    token = ""
    while True:
        items, token = ApiScan.fetch(client, listing, token)
        out.extend(items)
        if not token:
            return out
