"""HTTP client for the Databricks workspace REST API (2.0 / 2.1 / 2.2).

Everything that touches the network lives here:

* **Configuration** comes from a DuckDB secret of type ``databricks`` (see
  ``DATABRICKS_SECRET``). Nothing is read from the environment.
* **Authentication**, one of:

  - a personal access token (``TOKEN``), sent as ``Authorization: Bearer``;
  - OAuth machine-to-machine (``CLIENT_ID`` / ``CLIENT_SECRET`` of a service
    principal): the client-credentials grant against the workspace's own
    ``/oidc/v1/token`` endpoint, cached per process until shortly before it
    expires and refreshed on a 401;
  - an Azure AD (Entra ID) access token (``AZURE_TOKEN``) for Azure Databricks.

* **Politeness**: every request has a timeout; 429 and 502/503/504 are retried
  with backoff that honours ``Retry-After``.

Credentials are never written to table-function state or logs: each
``process`` call rebuilds the client from the secret the SDK hands it.
"""

from __future__ import annotations

import base64
import datetime as dt
import email.utils
import hashlib
import json
import logging
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
from vgi.catalog import SecretTypeSpec

log = logging.getLogger(__name__)

DATABRICKS_SECRET_TYPE = "databricks"
USER_AGENT = "vgi-databricks-worker/0.1"
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 1000
MAX_ATTEMPTS = 5
MAX_BACKOFF_SECONDS = 60.0
RETRY_STATUSES = frozenset({429, 502, 503, 504})
DOC_URL = "https://github.com/Query-farm/vgi-databricks#credentials"
AUTH_TYPES = ("auto", "pat", "oauth-m2m", "azure")


def _field(
    name: str, doc: str, type_: pa.DataType | None = None, *, redact: bool = False, required: bool = False
) -> Any:
    meta = {"vgi_doc": doc}
    if redact:
        meta["redact"] = "true"
    if required:
        meta["vgi_required"] = "true"
    return pa.field(name, type_ or pa.string(), metadata=meta)


DATABRICKS_SECRET = SecretTypeSpec(
    name=DATABRICKS_SECRET_TYPE,
    description=(
        "Connection to one Databricks workspace's REST API (AWS, Azure or GCP): the workspace URL plus a "
        "personal access token, an OAuth service principal (client id + secret), or an Azure AD token."
    ),
    schema=pa.schema(
        [
            _field(
                "host",
                "Workspace URL, e.g. 'https://dbc-a1b2c3d4-e5f6.cloud.databricks.com' or "
                "'https://adb-1234567890123456.7.azuredatabricks.net'.",
                required=True,
            ),
            _field("token", "Personal access token (dapi...), sent as a bearer token.", redact=True),
            _field("client_id", "OAuth M2M: the service principal's application (client) id."),
            _field(
                "client_secret",
                "OAuth M2M: the service principal's OAuth secret, exchanged at <host>/oidc/v1/token.",
                redact=True,
            ),
            _field(
                "azure_token", "An Azure AD (Entra ID) access token for the Azure Databricks resource.", redact=True
            ),
            _field(
                "auth_type",
                "'auto' (default: token, then client_id/client_secret, then azure_token), 'pat', 'oauth-m2m' "
                "or 'azure'.",
            ),
            _field("page_size", "Items requested per API page, capped at each endpoint's maximum.", pa.int64()),
            _field("timeout", "Per-request timeout in seconds (default 30).", pa.float64()),
        ],
        metadata={"vgi_doc_url": DOC_URL},
    ),
)


class DatabricksError(RuntimeError):
    """The Databricks API returned an error, or the secret is unusable."""


class NotFoundError(DatabricksError):
    """The API reported that the requested object does not exist."""


@dataclass(frozen=True, slots=True)
class DatabricksConfig:
    """Connection settings parsed from a ``databricks`` secret."""

    host: str
    token: str | None = field(default=None, repr=False)
    client_id: str | None = None
    client_secret: str | None = field(default=None, repr=False)
    azure_token: str | None = field(default=None, repr=False)
    auth_type: str = "pat"
    page_size: int = DEFAULT_PAGE_SIZE
    timeout: float = 30.0

    @classmethod
    def from_secret(cls, secret: Mapping[str, Any] | None) -> DatabricksConfig:
        """Build and validate a config from a resolved secret's key/value pairs.

        Args:
            secret: Secret fields; values may be ``pa.Scalar`` or plain Python.

        Returns:
            The validated configuration, with ``auth_type`` resolved from ``auto``.

        Raises:
            DatabricksError: No secret, or an invalid field value.

        """
        if not secret:
            raise DatabricksError(
                "no Databricks credentials: run CREATE SECRET (TYPE databricks, HOST 'https://<workspace>', "
                "TOKEN 'dapi...') or (TYPE databricks, HOST '...', CLIENT_ID '...', CLIENT_SECRET '...') "
                f"after ATTACH. See {DOC_URL}"
            )

        def get(name: str) -> Any:
            value = secret.get(name)
            if isinstance(value, pa.Scalar):
                value = value.as_py()
            return None if value == "" else value

        host = get("host")
        if not host:
            raise DatabricksError("databricks secret: HOST must be set to the workspace URL")
        host = str(host).strip().rstrip("/")
        if "://" not in host:
            host = f"https://{host}"
        if not host.startswith(("http://", "https://")):
            raise DatabricksError("databricks secret: HOST must be an http:// or https:// URL")
        token, client_id, client_secret, azure_token = (
            get("token"),
            get("client_id"),
            get("client_secret"),
            get("azure_token"),
        )
        auth = str(get("auth_type") or "auto").lower()
        if auth not in AUTH_TYPES:
            raise DatabricksError(f"databricks secret: AUTH_TYPE must be one of {', '.join(AUTH_TYPES)}, not {auth!r}")
        if auth == "auto":
            if token:
                auth = "pat"
            elif client_id and client_secret:
                auth = "oauth-m2m"
            elif azure_token:
                auth = "azure"
            else:
                raise DatabricksError("databricks secret: set TOKEN, or CLIENT_ID and CLIENT_SECRET, or AZURE_TOKEN")
        if auth == "pat" and not token:
            raise DatabricksError("databricks secret: AUTH_TYPE 'pat' needs TOKEN")
        if auth == "oauth-m2m" and not (client_id and client_secret):
            raise DatabricksError("databricks secret: AUTH_TYPE 'oauth-m2m' needs CLIENT_ID and CLIENT_SECRET")
        if auth == "azure" and not azure_token:
            raise DatabricksError("databricks secret: AUTH_TYPE 'azure' needs AZURE_TOKEN")
        page_size = int(get("page_size") or DEFAULT_PAGE_SIZE)
        if not 1 <= page_size <= MAX_PAGE_SIZE:
            raise DatabricksError(f"databricks secret: PAGE_SIZE must be between 1 and {MAX_PAGE_SIZE}")
        timeout = float(get("timeout") or 30.0)
        if timeout <= 0:
            raise DatabricksError("databricks secret: TIMEOUT must be positive")
        return cls(
            host=host,
            token=str(token) if token else None,
            client_id=str(client_id) if client_id else None,
            client_secret=str(client_secret) if client_secret else None,
            azure_token=str(azure_token) if azure_token else None,
            auth_type=auth,
            page_size=page_size,
            timeout=timeout,
        )


# Process-wide OAuth token cache. Keys hold a hash of the client secret, never the secret.
_LOCK = threading.Lock()
_TOKEN_CACHE: dict[tuple[str, str, str], tuple[str, float]] = {}


def clear_caches() -> None:
    """Forget exchanged OAuth tokens (used by tests)."""
    with _LOCK:
        _TOKEN_CACHE.clear()


def retry_delay(header: str | None, attempt: int) -> float:
    """Seconds to wait before retry ``attempt`` given a ``Retry-After`` header value."""
    if header:
        header = header.strip()
        if header.replace(".", "", 1).isdigit():
            return min(float(header), MAX_BACKOFF_SECONDS)
        try:
            when = email.utils.parsedate_to_datetime(header)
            return max(0.0, min((when - dt.datetime.now(dt.UTC)).total_seconds(), MAX_BACKOFF_SECONDS))
        except TypeError, ValueError:
            pass
    return min(0.5 * float(2**attempt), MAX_BACKOFF_SECONDS)


def _error_text(body: Any) -> str:
    """Databricks errors are ``{"error_code": ..., "message": ...}``."""
    if isinstance(body, dict):
        code, msg = body.get("error_code"), body.get("message")
        if code or msg:
            return f"{code or 'ERROR'}: {msg or ''}".strip()
    return str(body)[:800]


class DatabricksClient:
    """Authenticated JSON requests against one Databricks workspace."""

    def __init__(self, config: DatabricksConfig) -> None:
        """Bind to a configuration; OAuth tokens are fetched lazily."""
        self.config = config

    # ------------------------------------------------------------------ plumbing

    def _open(self, request: urllib.request.Request) -> tuple[int, Any]:
        """Send one request with retries; return (status, decoded JSON body or text)."""
        url = request.full_url.split("?", 1)[0]
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                with urllib.request.urlopen(request, timeout=self.config.timeout) as resp:
                    raw = resp.read()
                    return resp.status, json.loads(raw) if raw else {}
            except urllib.error.HTTPError as err:
                if err.code in RETRY_STATUSES and attempt < MAX_ATTEMPTS:
                    delay = retry_delay(err.headers.get("Retry-After"), attempt)
                    log.info("databricks: HTTP %s from %s; retrying in %.1fs", err.code, url, delay)
                    time.sleep(delay)
                    continue
                text = err.read(4000).decode(errors="replace")
                try:
                    return err.code, json.loads(text)
                except ValueError:
                    return err.code, text
            except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
                if attempt == MAX_ATTEMPTS:
                    reason = getattr(err, "reason", err)
                    raise DatabricksError(f"{request.get_method()} {url} failed: {reason}") from err
                time.sleep(min(0.5 * 2**attempt, MAX_BACKOFF_SECONDS))
        raise DatabricksError(f"{request.get_method()} {url}: gave up after {MAX_ATTEMPTS} attempts")

    def _oauth_token(self, *, refresh: bool) -> str:
        cfg = self.config
        assert cfg.client_id is not None and cfg.client_secret is not None
        key = (cfg.host, cfg.client_id, hashlib.sha256(cfg.client_secret.encode()).hexdigest())
        with _LOCK:
            hit = _TOKEN_CACHE.get(key)
        if hit and not refresh and hit[1] - 60 > time.time():
            return hit[0]
        basic = base64.b64encode(f"{cfg.client_id}:{cfg.client_secret}".encode()).decode()
        req = urllib.request.Request(
            f"{cfg.host}/oidc/v1/token",
            data=urllib.parse.urlencode({"grant_type": "client_credentials", "scope": "all-apis"}).encode(),
            headers={
                "Authorization": f"Basic {basic}",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
                "User-Agent": USER_AGENT,
            },
            method="POST",
        )
        status, body = self._open(req)
        if status != 200 or not isinstance(body, dict) or "access_token" not in body:
            detail = body.get("error_description") or body.get("error") if isinstance(body, dict) else body
            raise DatabricksError(
                f"OAuth token request to {cfg.host}/oidc/v1/token failed with HTTP {status}: {detail}"
            )
        token = str(body["access_token"])
        expires = time.time() + float(body.get("expires_in") or 3600)
        with _LOCK:
            _TOKEN_CACHE[key] = (token, expires)
        return token

    def _auth_header(self, *, refresh: bool = False) -> str:
        cfg = self.config
        if cfg.auth_type == "pat":
            return f"Bearer {cfg.token}"
        if cfg.auth_type == "azure":
            return f"Bearer {cfg.azure_token}"
        return f"Bearer {self._oauth_token(refresh=refresh)}"

    def _call(self, method: str, url: str, body: bytes | None) -> tuple[int, Any]:
        headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
        if body is not None:
            headers["Content-Type"] = "application/json"

        def send(refresh: bool) -> tuple[int, Any]:
            req = urllib.request.Request(
                url, data=body, headers={**headers, "Authorization": self._auth_header(refresh=refresh)}, method=method
            )
            return self._open(req)

        status, payload = send(False)
        if status == 401 and self.config.auth_type == "oauth-m2m":
            status, payload = send(True)
        return status, payload

    def _check(self, method: str, url: str, status: int, payload: Any) -> Any:
        if 200 <= status < 300:
            return payload
        text = _error_text(payload)
        shown = url.split("?", 1)[0]
        if status == 404 or "RESOURCE_DOES_NOT_EXIST" in text or "does not exist" in text.lower():
            raise NotFoundError(f"{method} {shown} failed with HTTP {status}: {text}")
        if status in (401, 403):
            raise DatabricksError(
                f"{method} {shown} failed with HTTP {status} (check the databricks secret's credentials and "
                f"the principal's permissions): {text}"
            )
        raise DatabricksError(f"{method} {shown} failed with HTTP {status}: {text}")

    # ------------------------------------------------------------------ public API

    def get(self, path: str, query: Mapping[str, Any] | None = None) -> Any:
        """GET ``<host><path>`` and return the decoded JSON body.

        Args:
            path: Absolute API path, e.g. ``/api/2.2/jobs/list``.
            query: Query parameters; list values repeat the parameter, ``None`` values are dropped,
                booleans are sent as ``true`` / ``false``.

        Returns:
            The decoded JSON response.

        """
        clean: dict[str, Any] = {}
        for k, v in (query or {}).items():
            if v is None:
                continue
            clean[k] = ("true" if v else "false") if isinstance(v, bool) else v
        qs = urllib.parse.urlencode(clean, doseq=True)
        url = f"{self.config.host}{path}" + (f"?{qs}" if qs else "")
        status, payload = self._call("GET", url, None)
        return self._check("GET", url, status, payload)

    def post(self, path: str, body: Mapping[str, Any]) -> Any:
        """POST a JSON body to a read-only query endpoint (e.g. ``/api/2.1/clusters/events``).

        Args:
            path: Absolute API path.
            body: Request body; ``None`` values are dropped.

        Returns:
            The decoded JSON response.

        """
        url = f"{self.config.host}{path}"
        data = json.dumps({k: v for k, v in body.items() if v is not None}).encode()
        status, payload = self._call("POST", url, data)
        return self._check("POST", url, status, payload)

    def ui_url(self, path: str) -> str:
        """Absolute URL of a page in the workspace UI."""
        return f"{self.config.host}{path}"


def quote(value: str) -> str:
    """Quote one path segment."""
    return urllib.parse.quote(value, safe="")
