"""Unit tests for the client, auth, paging, pushdown translation, masking and status mapping.

The SQL-level tests (through DuckDB and the real vgi extension) live in test/sql.
"""

from __future__ import annotations

import datetime as dt
import json
import urllib.request
from typing import Any

import pyarrow as pa
import pytest
from vgi.table_filter_pushdown import ComparisonOp, ConstantFilter, InFilter, PushdownFilters

from databricks_worker.client import DatabricksClient, DatabricksConfig, DatabricksError, NotFoundError, retry_delay
from databricks_worker.functions import OutputArgs, output_row
from databricks_worker.masking import MASK, mask_tree, mask_value
from databricks_worker.scan import ApiScan, Pushdown, int_values, ts
from databricks_worker.tables import (
    ClusterEvents,
    Clusters,
    JobRuns,
    Jobs,
    PipelineEvents,
    QueryHistory,
    run_listings,
    task_cluster,
    task_type,
)
from databricks_worker.unified import UnifiedRuns, UnifiedTaskRuns, trigger, unified_status

UTC = dt.UTC
TSTZ = pa.timestamp("us", tz="UTC")


def _client(host: str, **fields: Any) -> DatabricksClient:
    return DatabricksClient(DatabricksConfig.from_secret({"host": host, **fields}))


def _pd(*filters: Any) -> Pushdown:
    return Pushdown(PushdownFilters(filters=tuple(filters)))


def _eq(col: str, value: Any, typ: pa.DataType | None = None) -> ConstantFilter:
    return ConstantFilter(column_name=col, column_index=0, op=ComparisonOp.EQ, value=pa.scalar(value, typ))


def _cmp(col: str, op: ComparisonOp, value: Any, typ: pa.DataType = TSTZ) -> ConstantFilter:
    return ConstantFilter(column_name=col, column_index=0, op=op, value=pa.scalar(value, typ))


def _in(col: str, values: list[Any]) -> InFilter:
    return InFilter(column_name=col, column_index=0, values=pa.array(values))


def _requests(base: str) -> list[dict[str, Any]]:
    with urllib.request.urlopen(f"{base}/__requests") as resp:
        out: list[dict[str, Any]] = json.load(resp)
        return out


def _reset(base: str) -> None:
    urllib.request.urlopen(urllib.request.Request(f"{base}/__reset", data=b"", method="POST")).close()


# ------------------------------------------------------------------ configuration


def test_config_requires_secret() -> None:
    with pytest.raises(DatabricksError, match="no Databricks credentials"):
        DatabricksConfig.from_secret(None)


def test_config_resolves_auth_and_host() -> None:
    cfg = DatabricksConfig.from_secret({"host": pa.scalar("adb-1.7.azuredatabricks.net/"), "token": pa.scalar("x")})
    assert cfg.host == "https://adb-1.7.azuredatabricks.net"
    assert cfg.auth_type == "pat"
    assert DatabricksConfig.from_secret({"host": "h", "client_id": "a", "client_secret": "b"}).auth_type == "oauth-m2m"
    assert DatabricksConfig.from_secret({"host": "h", "azure_token": "t"}).auth_type == "azure"
    # A PAT wins over the other fields under 'auto'.
    assert DatabricksConfig.from_secret({"host": "h", "token": "t", "azure_token": "a"}).auth_type == "pat"


def test_config_rejects_bad_values() -> None:
    with pytest.raises(DatabricksError, match="HOST must be an http"):
        DatabricksConfig.from_secret({"host": "ftp://x", "token": "t"})
    with pytest.raises(DatabricksError, match="set TOKEN"):
        DatabricksConfig.from_secret({"host": "https://x"})
    with pytest.raises(DatabricksError, match="AUTH_TYPE must be one of"):
        DatabricksConfig.from_secret({"host": "https://x", "token": "t", "auth_type": "basic"})
    with pytest.raises(DatabricksError, match="'azure' needs AZURE_TOKEN"):
        DatabricksConfig.from_secret({"host": "https://x", "token": "t", "auth_type": "azure"})
    with pytest.raises(DatabricksError, match="PAGE_SIZE"):
        DatabricksConfig.from_secret({"host": "https://x", "token": "t", "page_size": 5000})


def test_config_repr_hides_secrets() -> None:
    cfg = DatabricksConfig.from_secret(
        {"host": "https://x", "token": "dapi123", "client_id": "c", "client_secret": "s3cr3t", "azure_token": "aad"}
    )
    assert "dapi123" not in repr(cfg) and "s3cr3t" not in repr(cfg) and "aad" not in repr(cfg)


def test_retry_delay_honours_retry_after() -> None:
    assert retry_delay("3", 1) == 3.0
    assert retry_delay("9999", 1) == 60.0
    assert retry_delay(None, 2) == 2.0


# ------------------------------------------------------------- client vs replay


def test_pat_and_429_retry(mock_api: str) -> None:
    # The flaky replay answers the first request to every path with 429; the client retries.
    body = _client(mock_api, token="dapi-fixture-token").get("/api/2.0/secrets/scopes/list")
    assert sorted(s["name"] for s in body["scopes"]) == ["kv-prod", "lake", "ml"]


def test_oauth_m2m_exchanges_and_caches_token(mock_plain: str) -> None:
    _reset(mock_plain)
    client = _client(mock_plain, client_id="fixture-sp-client-id", client_secret="fixture-sp-secret")
    client.get("/api/2.0/instance-pools/list")
    client.get("/api/2.0/secrets/scopes/list")
    paths = [r["path"] for r in _requests(mock_plain)]
    assert paths.count("/oidc/v1/token") == 1
    assert paths[-1] == "/api/2.0/secrets/scopes/list"


def test_oauth_bad_secret(mock_plain: str) -> None:
    with pytest.raises(DatabricksError, match="oidc/v1/token failed with HTTP 401"):
        _client(mock_plain, client_id="fixture-sp-client-id", client_secret="nope").get("/api/2.0/sql/warehouses")


def test_unauthorized_message_has_no_token(mock_plain: str) -> None:
    with pytest.raises(DatabricksError) as err:
        _client(mock_plain, token="dapi-wrong-but-secret").get("/api/2.0/sql/warehouses")
    assert "HTTP 401" in str(err.value) and "dapi-wrong-but-secret" not in str(err.value)


def test_unknown_parameter_is_rejected(mock_plain: str) -> None:
    with pytest.raises(DatabricksError, match="INVALID_PARAMETER_VALUE"):
        _client(mock_plain, token="dapi-fixture-token").get("/api/2.2/jobs/list", {"bogus": 1})


def test_not_found(mock_plain: str) -> None:
    with pytest.raises(NotFoundError):
        _client(mock_plain, token="dapi-fixture-token").get("/api/2.2/jobs/get", {"job_id": 999})


def test_fetch_pages_with_tokens(mock_plain: str) -> None:
    client = _client(mock_plain, token="dapi-fixture-token", page_size=3)
    (listing,) = JobRuns.requests(_pd(), [])
    seen, token, pages = [], "", 0
    while True:
        items, token = ApiScan.fetch(client, listing, token)
        seen += [r["run_id"] for r in items]
        pages += 1
        if not token:
            break
    assert pages == 3 and len(seen) == 8 and len(set(seen)) == 8


def test_pipeline_events_token_carries_filter(mock_plain: str) -> None:
    client = _client(mock_plain, token="dapi-fixture-token", page_size=1)
    pd = _pd(_eq("pipeline_id", "pl-orders-0001"), _in("level", ["ERROR", "WARN"]))
    (listing,) = PipelineEvents.requests(pd, [])
    ids, token = [], ""
    while True:
        items, token = PipelineEvents.fetch(client, listing, token)
        ids += [e["id"] for e in items]
        if not token:
            break
    assert sorted(ids) == ["ev-o-4", "ev-o-5", "ev-o-6"]


# ----------------------------------------------------------------- pushdown


def test_int_values_enumerates_small_ranges() -> None:
    lo = _cmp("job_id", ComparisonOp.GE, 103, pa.int64())
    hi = _cmp("job_id", ComparisonOp.LE, 104, pa.int64())
    assert int_values(_pd(lo, hi), "job_id") == [103, 104]
    wide = _cmp("job_id", ComparisonOp.LE, 10_000, pa.int64())
    assert int_values(_pd(lo, wide), "job_id") is None
    assert int_values(_pd(_in("job_id", [7, 3])), "job_id") == [3, 7]


def test_job_runs_translation() -> None:
    start = dt.datetime(2026, 11, 14, tzinfo=UTC)
    pd = _pd(
        _in("job_id", [101, 102]),
        _eq("life_cycle_state", "RUNNING"),
        _cmp("start_time", ComparisonOp.GE, start),
        _eq("run_type", "JOB_RUN"),
    )
    listings = run_listings(pd, expand_tasks=False)
    assert [dict(item.query)["job_id"] for item in listings] == [101, 102]
    q = dict(listings[0].query)
    assert q["active_only"] is True and q["start_time_from"] == 1794614400000 and q["run_type"] == "JOB_RUN"
    assert "completed_only" not in q
    done = dict(run_listings(_pd(_eq("result_state", "FAILED")), expand_tasks=True)[0].query)
    assert done["completed_only"] is True and done["expand_tasks"] is True
    (get,) = run_listings(_pd(_eq("run_id", 2002, pa.int64())), expand_tasks=False)
    assert get.path.endswith("/runs/get") and get.missing_ok


def test_mixed_states_do_not_push() -> None:
    q = dict(run_listings(_pd(_in("life_cycle_state", ["RUNNING", "TERMINATED"])), expand_tasks=False)[0].query)
    assert "active_only" not in q and "completed_only" not in q


def test_jobs_clusters_and_events_translation() -> None:
    (by_name,) = Jobs.requests(_pd(_eq("name", "nightly_etl")), [])
    assert dict(by_name.query)["name"] == "nightly_etl"
    (clusters,) = Clusters.requests(_pd(_in("state", ["RUNNING", "PENDING"]), _eq("cluster_source", "JOB")), [])
    assert ("filter_by.cluster_states", "RUNNING") in clusters.query
    assert ("filter_by.cluster_sources", "JOB") in clusters.query
    events = ClusterEvents.requests(_pd(_eq("type", "RESIZING")), ["c1", "c2"])
    assert [dict(e.query)["cluster_id"] for e in events] == ["c1", "c2"]
    assert all(e.method == "POST" and dict(e.query)["event_types"] == ["RESIZING"] for e in events)


def test_query_history_translation() -> None:
    pd = _pd(
        _cmp("query_start_time", ComparisonOp.GE, dt.datetime(2026, 11, 14, tzinfo=UTC)),
        _cmp("query_start_time", ComparisonOp.LT, dt.datetime(2026, 11, 15, tzinfo=UTC)),
        _in("status", ["FAILED"]),
        _eq("user_id", 1002, pa.int64()),
    )
    (listing,) = QueryHistory.requests(pd, [])
    q = listing.query
    assert ("filter_by.query_start_time_range.start_time_ms", 1794614400000) in q
    assert ("filter_by.query_start_time_range.end_time_ms", 1794700800001) in q
    assert ("filter_by.statuses", "FAILED") in q and ("filter_by.user_ids", 1002) in q


def test_pipeline_event_filter_expression() -> None:
    pd = _pd(
        _eq("pipeline_id", "p1"),
        _eq("level", "ERROR"),
        _cmp("timestamp", ComparisonOp.GT, dt.datetime(2026, 11, 14, tzinfo=UTC)),
    )
    (listing,) = PipelineEvents.requests(pd, [])
    assert listing.path == "/api/2.0/pipelines/p1/events"
    assert dict(listing.query)["filter"] == "level='ERROR' AND timestamp > '2026-11-14T00:00:00Z'"


def test_unified_translation() -> None:
    (runs,) = UnifiedRuns.requests(_pd(_in("status", ["running", "queued"])), [])
    assert dict(runs.query).get("active_only") is True
    (done,) = UnifiedRuns.requests(_pd(_eq("status", "failed")), [])
    assert dict(done.query).get("completed_only") is True
    assert UnifiedRuns.requests(_pd(_eq("status", "bogus")), []) == []
    (one,) = UnifiedTaskRuns.requests(_pd(_eq("run_id", "2002")), [])
    assert one.path.endswith("/runs/get")
    # A lower bound on a task's start does not bound its run's start; only the upper bound is sent.
    lo = _cmp("started_at", ComparisonOp.GE, dt.datetime(2026, 11, 14, tzinfo=UTC))
    hi = _cmp("started_at", ComparisonOp.LT, dt.datetime(2026, 11, 15, tzinfo=UTC))
    (tr,) = UnifiedTaskRuns.requests(_pd(lo, hi), [])
    q = dict(tr.query)
    assert "start_time_from" not in q and q["start_time_to"] == 1794700800000


# ---------------------------------------------------------------- conversions


def test_ts_parses_epoch_ms_and_iso() -> None:
    assert ts(0) is None and ts(None) is None
    assert ts(1794614400000) == dt.datetime(2026, 11, 14, tzinfo=UTC)
    assert ts("2026-11-14T00:30:40.000Z") == dt.datetime(2026, 11, 14, 0, 30, 40, tzinfo=UTC)


def test_task_shape_helpers() -> None:
    assert task_type({"task_key": "a", "notebook_task": {}}) == "notebook"
    assert task_type({"task_key": "a", "spark_python_task": {}}) == "spark_python"
    assert task_cluster({"existing_cluster_id": "c1"}) == "c1"
    assert task_cluster({"job_cluster_key": "k"}) == "job_cluster:k"
    assert task_cluster({"sql_task": {}}) == "serverless"


def test_masking() -> None:
    assert mask_value("spark.hadoop.fs.s3a.secret.key", "abc") == MASK
    assert mask_value("spark.sql.shuffle.partitions", "64") == "64"
    assert mask_value("fs.azure.account.key.x", "{{secrets/lake/key}}") == "{{secrets/lake/key}}"
    assert mask_value("note", "dapi0123456789abcdef0123456789") == MASK
    assert mask_value("jdbc", "postgres://user:pw@host/db") == MASK
    params = [{"name": "db_password", "default": "hunter2"}, {"name": "env", "default": "prod"}]
    assert mask_tree(params) == [{"name": "db_password", "default": MASK}, {"name": "env", "default": "prod"}]


def test_output_row_truncates_and_withholds_result() -> None:
    body = {
        "metadata": {"task_key": "t", "attempt_number": 1, "state": {"life_cycle_state": "TERMINATED"}},
        "logs": "x" * 50,
        "notebook_output": {"result": "secret-ish", "truncated": False},
    }
    row = output_row(body, 1, 2, OutputArgs(run_id=1, include_result=False, max_log_chars=10))
    assert row["logs"] == "x" * 10 and row["logs_truncated"] is True
    assert row["notebook_result"] is None and row["notebook_result_length"] == 10
    shown = output_row(body, 1, 2, OutputArgs(run_id=1, include_result=True, max_log_chars=100))
    assert shown["notebook_result"] == "secret-ish" and shown["logs_truncated"] is False


# ------------------------------------------------------------------- statuses


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("SUCCESS", "success"),
        ("SUCCESS_WITH_FAILURES", "success"),
        ("FAILED", "failed"),
        ("TIMEDOUT", "failed"),
        ("CANCELED", "cancelled"),
        ("UPSTREAM_FAILED", "skipped"),
        ("EXCLUDED", "skipped"),
        ("QUEUED", "queued"),
        ("WAITING_FOR_RETRY", "queued"),
        ("RUNNING", "running"),
        ("TERMINATING", "running"),
        ("INTERNAL_ERROR", "failed"),
        (None, "queued"),
        ("something_new", "queued"),
    ],
)
def test_unified_status(raw: str | None, expected: str) -> None:
    assert unified_status(raw) == expected


def test_trigger_mapping() -> None:
    assert trigger({"trigger": "PERIODIC"}) == "schedule"
    assert trigger({"trigger": "ONE_TIME"}) == "manual"
    assert trigger({"trigger": "ONE_TIME", "run_type": "SUBMIT_RUN"}) == "api"
    assert trigger({"trigger": "FILE_ARRIVAL"}) == "sensor"
    assert trigger({"trigger": "TABLE"}) == "event"
    assert trigger({"trigger": "RETRY"}) == "other"
    assert trigger({}) is None
