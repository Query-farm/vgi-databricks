#!/usr/bin/env python3
"""Generate ``workspace.json``: a small, fictitious Databricks workspace.

There is no local Databricks, so the replay data is hand-built from the
response shapes in the REST API reference
(https://docs.databricks.com/api/workspace/introduction): Jobs 2.2, Clusters
2.1, SQL warehouses / query history, Pipelines, Repos, Secrets and Workspace
2.0. Field names and value vocabularies follow the reference; ids, names and
times are invented. Regenerate with::

    python3 test/fixtures/build_workspace.py > test/fixtures/workspace.json
"""

from __future__ import annotations

import datetime as dt
import json
import sys
from typing import Any

HOST = "https://dbc-fixture.cloud.databricks.com"
ALICE, BOB = "alice@example.com", "bob@example.com"
BOT = "6f1e2d3c-0000-4000-8000-00000000e7b0"  # a service principal's application id


def ms(text: str) -> int:
    """Epoch milliseconds of an ISO time (UTC)."""
    return int(dt.datetime.fromisoformat(text).replace(tzinfo=dt.UTC).timestamp() * 1000)


def iso(text: str) -> str:
    """ISO-8601 with milliseconds and Z, the way the Pipelines API writes times."""
    return dt.datetime.fromisoformat(text).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def state(life: str, result: str | None = None, message: str = "", **extra: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"life_cycle_state": life, "state_message": message, "user_cancelled_or_timedout": False}
    if result:
        out["result_state"] = result
    out.update(extra)
    return out


def status(st: str, code: str | None = None, typ: str | None = None, message: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {"state": st}
    if code:
        out["termination_details"] = {"code": code, "type": typ or "SUCCESS", "message": message}
    return out


# ---------------------------------------------------------------------------- jobs

ETL_CLUSTER = {
    "spark_version": "15.4.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "autoscale": {"min_workers": 2, "max_workers": 8},
    "policy_id": "pol-jobs-001",
    "spark_conf": {
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "fs.azure.account.key.lake.dfs.core.windows.net": "{{secrets/lake/account-key}}",
        "spark.hadoop.fs.s3a.secret.key": "plaintext-fixture-secret",
    },
}

JOBS = [
    {
        "job_id": 101,
        "creator_user_name": ALICE,
        "run_as_user_name": BOT,
        "created_time": ms("2026-09-01T10:00:00"),
        "settings": {
            "name": "nightly_etl",
            "format": "MULTI_TASK",
            "schedule": {"quartz_cron_expression": "0 0 2 * * ?", "timezone_id": "UTC", "pause_status": "UNPAUSED"},
            "max_concurrent_runs": 1,
            "timeout_seconds": 7200,
            "tags": {"team": "data-eng", "env": "prod"},
            "parameters": [{"name": "env", "default": "prod"}, {"name": "db_password", "default": "hunter2-fixture"}],
            "job_clusters": [{"job_cluster_key": "etl_cluster", "new_cluster": ETL_CLUSTER}],
            "tasks": [
                {
                    "task_key": "ingest",
                    "description": "Land raw orders",
                    "job_cluster_key": "etl_cluster",
                    "notebook_task": {
                        "notebook_path": "/Repos/alice@example.com/etl/ingest",
                        "base_parameters": {"date": "{{job.start_time.iso_date}}"},
                        "source": "WORKSPACE",
                    },
                    "timeout_seconds": 1800,
                },
                {
                    "task_key": "transform",
                    "depends_on": [{"task_key": "ingest"}],
                    "job_cluster_key": "etl_cluster",
                    "spark_python_task": {"python_file": "/Repos/alice@example.com/etl/transform.py"},
                    "max_retries": 0,
                },
                {
                    "task_key": "publish",
                    "depends_on": [{"task_key": "transform"}],
                    "sql_task": {"warehouse_id": "wh-pro-02", "query": {"query_id": "q-publish"}},
                },
            ],
        },
    },
    {
        "job_id": 102,
        "creator_user_name": BOB,
        "run_as_user_name": BOB,
        "created_time": ms("2026-10-05T14:00:00"),
        "settings": {
            "name": "flaky_report",
            "format": "MULTI_TASK",
            "max_concurrent_runs": 1,
            "timeout_seconds": 0,
            "tags": {"team": "analytics"},
            "tasks": [
                {
                    "task_key": "extract",
                    "existing_cluster_id": "0915-120000-shared01",
                    "notebook_task": {"notebook_path": "/Users/bob@example.com/report", "source": "WORKSPACE"},
                },
                {
                    "task_key": "render",
                    "depends_on": [{"task_key": "extract"}],
                    "existing_cluster_id": "0915-120000-shared01",
                    "notebook_task": {"notebook_path": "/Users/bob@example.com/render", "source": "WORKSPACE"},
                    "max_retries": 2,
                    "min_retry_interval_millis": 60000,
                },
                {
                    "task_key": "email",
                    "depends_on": [{"task_key": "render"}],
                    "existing_cluster_id": "0915-120000-shared01",
                    "notebook_task": {"notebook_path": "/Users/bob@example.com/email", "source": "WORKSPACE"},
                },
            ],
        },
    },
    {
        "job_id": 103,
        "creator_user_name": ALICE,
        "run_as_user_name": BOT,
        "created_time": ms("2026-09-20T08:00:00"),
        "settings": {
            "name": "dlt_refresh",
            "format": "MULTI_TASK",
            "schedule": {
                "quartz_cron_expression": "0 30 0 * * ?",
                "timezone_id": "America/New_York",
                "pause_status": "PAUSED",
            },
            "max_concurrent_runs": 1,
            "timeout_seconds": 3600,
            "tags": {"team": "data-eng"},
            "tasks": [
                {"task_key": "refresh", "pipeline_task": {"pipeline_id": "pl-orders-0001", "full_refresh": False}}
            ],
        },
    },
    {
        "job_id": 104,
        "creator_user_name": ALICE,
        "run_as_user_name": ALICE,
        "created_time": ms("2026-11-01T09:00:00"),
        "settings": {
            "name": "table_watch",
            "format": "MULTI_TASK",
            "trigger": {
                "pause_status": "UNPAUSED",
                "table_update": {"table_names": ["main.sales.orders"], "condition": "ANY_UPDATED"},
            },
            "max_concurrent_runs": 1,
            "timeout_seconds": 0,
            "tags": {"team": "ml"},
            "environments": [{"environment_key": "default", "spec": {"client": "2"}}],
            "tasks": [
                {
                    "task_key": "score",
                    "environment_key": "default",
                    "notebook_task": {
                        "notebook_path": "/Users/alice@example.com/projects/churn",
                        "source": "WORKSPACE",
                    },
                }
            ],
        },
    },
]

# ---------------------------------------------------------------------------- runs


def url(job: int | None, run: int) -> str:
    return f"{HOST}/jobs/{job}/runs/{run}" if job else f"{HOST}/jobs/runs/{run}"


def task(
    job: int | None,
    run_id: int,
    key: str,
    start: str | None,
    end: str | None,
    st: dict[str, Any],
    sts: dict[str, Any],
    *,
    attempt: int = 0,
    cluster: str | None = None,
    spec: dict[str, Any] | None = None,
    setup: int = 0,
    execution: int = 0,
) -> dict[str, Any]:
    t: dict[str, Any] = {
        "run_id": run_id,
        "task_key": key,
        "state": st,
        "status": sts,
        "start_time": ms(start) if start else 0,
        "end_time": ms(end) if end else 0,
        "setup_duration": setup,
        "execution_duration": execution,
        "cleanup_duration": 0,
        "attempt_number": attempt,
        "run_page_url": url(job, run_id),
    }
    if cluster:
        t["cluster_instance"] = {"cluster_id": cluster, "spark_context_id": f"{run_id}0001"}
    if spec:
        t.update(spec)
    return t


def spec_of(job: int, key: str) -> dict[str, Any]:
    """The task's configuration copied from its job (depends_on, cluster, task type)."""
    for t in next(j for j in JOBS if j["job_id"] == job)["settings"]["tasks"]:
        if t["task_key"] == key:
            return {k: v for k, v in t.items() if k not in ("task_key", "max_retries", "min_retry_interval_millis")}
    raise KeyError(key)


OK = state("TERMINATED", "SUCCESS")
OK_S = status("TERMINATED", "SUCCESS", "SUCCESS")

JC1, JC2, JC3 = "1113-020100-jc2001ab", "1114-020100-jc2002cd", "1115-020100-jc2003ef"
SHARED = "0915-120000-shared01"


def run(
    run_id: int,
    job: int | None,
    name: str,
    number: int | None,
    trigger: str,
    start: str,
    end: str | None,
    st: dict[str, Any],
    sts: dict[str, Any],
    tasks: list[dict[str, Any]],
    *,
    run_type: str = "JOB_RUN",
    creator: str = BOT,
    queue: int = 0,
) -> dict[str, Any]:
    r: dict[str, Any] = {
        "run_id": run_id,
        "run_name": name,
        "number_in_job": number,
        "creator_user_name": creator,
        "original_attempt_run_id": run_id,
        "attempt_number": 0,
        "state": st,
        "status": sts,
        "start_time": ms(start),
        "end_time": ms(end) if end else 0,
        "setup_duration": 0,
        "execution_duration": 0,
        "cleanup_duration": 0,
        "queue_duration": queue,
        "run_duration": (ms(end) - ms(start)) if end else 0,
        "trigger": trigger,
        "run_type": run_type,
        "run_page_url": url(job, run_id),
        "format": "MULTI_TASK",
        "tasks": tasks,
    }
    if job is not None:
        r["job_id"] = job
        params = next(j for j in JOBS if j["job_id"] == job)["settings"].get("parameters") or []
        r["job_parameters"] = [{"name": p["name"], "default": p["default"], "value": p["default"]} for p in params]
    return r


UPSTREAM_FAILED = state("SKIPPED", "UPSTREAM_FAILED", "Upstream task(s) failed.")
UPSTREAM_CANCELED = state("SKIPPED", "UPSTREAM_CANCELED", "Upstream task(s) were canceled.")

RUNS = [
    run(
        2001,
        101,
        "nightly_etl",
        1,
        "PERIODIC",
        "2026-11-13T02:00:00",
        "2026-11-13T02:40:00",
        OK,
        OK_S,
        [
            task(
                101,
                3001,
                "ingest",
                "2026-11-13T02:00:30",
                "2026-11-13T02:12:00",
                OK,
                OK_S,
                cluster=JC1,
                spec=spec_of(101, "ingest"),
                setup=240000,
                execution=450000,
            ),
            task(
                101,
                3002,
                "transform",
                "2026-11-13T02:12:05",
                "2026-11-13T02:31:00",
                OK,
                OK_S,
                cluster=JC1,
                spec=spec_of(101, "transform"),
                execution=1135000,
            ),
            task(
                101,
                3003,
                "publish",
                "2026-11-13T02:31:05",
                "2026-11-13T02:40:00",
                OK,
                OK_S,
                spec=spec_of(101, "publish"),
                execution=535000,
            ),
        ],
    ),
    run(
        2002,
        101,
        "nightly_etl",
        2,
        "PERIODIC",
        "2026-11-14T02:00:00",
        "2026-11-14T02:25:00",
        state(
            "TERMINATED", "FAILED", "Task transform failed with message: Workload failed, see run output for details."
        ),
        status("TERMINATED", "RUN_EXECUTION_ERROR", "CLIENT_ERROR", "Task transform failed."),
        [
            task(
                101,
                3011,
                "ingest",
                "2026-11-14T02:00:30",
                "2026-11-14T02:13:00",
                OK,
                OK_S,
                cluster=JC2,
                spec=spec_of(101, "ingest"),
                setup=250000,
                execution=500000,
            ),
            task(
                101,
                3012,
                "transform",
                "2026-11-14T02:13:05",
                "2026-11-14T02:25:00",
                state("TERMINATED", "FAILED", "Workload failed, see run output for details"),
                status("TERMINATED", "RUN_EXECUTION_ERROR", "CLIENT_ERROR", "Workload failed"),
                cluster=JC2,
                spec=spec_of(101, "transform"),
                execution=715000,
            ),
            task(
                101,
                3013,
                "publish",
                None,
                None,
                UPSTREAM_FAILED,
                status("TERMINATED", "UPSTREAM_FAILED", "CLIENT_ERROR", "Upstream task(s) failed."),
                spec=spec_of(101, "publish"),
            ),
        ],
    ),
    run(
        2003,
        101,
        "nightly_etl",
        3,
        "PERIODIC",
        "2026-11-15T02:00:00",
        None,
        state("RUNNING"),
        status("RUNNING"),
        [
            task(
                101,
                3021,
                "ingest",
                "2026-11-15T02:00:30",
                "2026-11-15T02:12:30",
                OK,
                OK_S,
                cluster=JC3,
                spec=spec_of(101, "ingest"),
                setup=240000,
                execution=480000,
            ),
            task(
                101,
                3022,
                "transform",
                "2026-11-15T02:12:35",
                None,
                state("RUNNING"),
                status("RUNNING"),
                cluster=JC3,
                spec=spec_of(101, "transform"),
            ),
            task(
                101,
                3023,
                "publish",
                None,
                None,
                state("BLOCKED", queue_reason=None),
                status("BLOCKED"),
                spec=spec_of(101, "publish"),
            ),
        ],
    ),
    run(
        2101,
        102,
        "flaky_report",
        1,
        "ONE_TIME",
        "2026-11-14T09:00:00",
        "2026-11-14T09:12:00",
        OK,
        OK_S,
        [
            task(
                102,
                3101,
                "extract",
                "2026-11-14T09:00:10",
                "2026-11-14T09:03:00",
                OK,
                OK_S,
                cluster=SHARED,
                spec=spec_of(102, "extract"),
                execution=170000,
            ),
            task(
                102,
                3102,
                "render",
                "2026-11-14T09:03:05",
                "2026-11-14T09:05:00",
                state("TERMINATED", "FAILED", "Workload failed, see run output for details"),
                status("TERMINATED", "RUN_EXECUTION_ERROR", "CLIENT_ERROR", "Workload failed"),
                cluster=SHARED,
                spec=spec_of(102, "render"),
                execution=115000,
            ),
            task(
                102,
                3103,
                "render",
                "2026-11-14T09:06:05",
                "2026-11-14T09:09:00",
                OK,
                OK_S,
                attempt=1,
                cluster=SHARED,
                spec=spec_of(102, "render"),
                execution=175000,
            ),
            task(
                102,
                3104,
                "email",
                "2026-11-14T09:09:05",
                "2026-11-14T09:12:00",
                OK,
                OK_S,
                cluster=SHARED,
                spec=spec_of(102, "email"),
                execution=175000,
            ),
        ],
        creator=BOB,
    ),
    run(
        2102,
        102,
        "flaky_report",
        2,
        "ONE_TIME",
        "2026-11-14T15:00:00",
        "2026-11-14T15:03:00",
        state("TERMINATED", "CANCELED", "Run cancelled by bob@example.com.", user_cancelled_or_timedout=True),
        status("TERMINATED", "CANCELED", "SUCCESS", "Run cancelled by bob@example.com."),
        [
            task(
                102,
                3111,
                "extract",
                "2026-11-14T15:00:10",
                "2026-11-14T15:03:00",
                state("TERMINATED", "CANCELED", "Run cancelled.", user_cancelled_or_timedout=True),
                status("TERMINATED", "CANCELED", "SUCCESS"),
                cluster=SHARED,
                spec=spec_of(102, "extract"),
            ),
            task(
                102,
                3112,
                "render",
                None,
                None,
                UPSTREAM_CANCELED,
                status("TERMINATED", "UPSTREAM_CANCELED"),
                spec=spec_of(102, "render"),
            ),
            task(
                102,
                3113,
                "email",
                None,
                None,
                UPSTREAM_CANCELED,
                status("TERMINATED", "UPSTREAM_CANCELED"),
                spec=spec_of(102, "email"),
            ),
        ],
        creator=BOB,
    ),
    run(
        2201,
        103,
        "dlt_refresh",
        1,
        "PERIODIC",
        "2026-11-14T00:30:00",
        "2026-11-14T00:48:00",
        state("TERMINATED", "FAILED", "Task refresh failed with message: Pipeline update failed."),
        status("TERMINATED", "RUN_EXECUTION_ERROR", "CLIENT_ERROR", "Task refresh failed."),
        [
            task(
                103,
                3201,
                "refresh",
                "2026-11-14T00:30:20",
                "2026-11-14T00:48:00",
                state("TERMINATED", "FAILED", "Pipeline update failed."),
                status("TERMINATED", "RUN_EXECUTION_ERROR", "CLIENT_ERROR"),
                spec=spec_of(103, "refresh"),
                execution=1060000,
            ),
        ],
    ),
    run(
        2301,
        104,
        "table_watch",
        1,
        "TABLE",
        "2026-11-15T01:00:00",
        None,
        state("QUEUED", queue_reason="Queued due to reaching maximum concurrent runs on the warehouse."),
        status("QUEUED"),
        [
            task(104, 3301, "score", None, None, state("QUEUED"), status("QUEUED"), spec=spec_of(104, "score")),
        ],
        creator=ALICE,
        queue=1800000,
    ),
    run(
        2401,
        None,
        "adhoc_backfill",
        None,
        "ONE_TIME",
        "2026-11-13T12:00:00",
        "2026-11-13T12:05:00",
        OK,
        OK_S,
        [
            task(
                None,
                3401,
                "backfill",
                "2026-11-13T12:00:20",
                "2026-11-13T12:05:00",
                OK,
                OK_S,
                cluster="1113-120000-submit01",
                spec={"spark_python_task": {"python_file": "dbfs:/scripts/backfill.py"}, "new_cluster": ETL_CLUSTER},
                setup=150000,
                execution=130000,
            ),
        ],
        run_type="SUBMIT_RUN",
        creator=ALICE,
    ),
]

OUTPUTS: dict[str, dict[str, Any]] = {
    "3012": {
        "error": "AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `main`.`raw`.`orders_v2` cannot "
        "be found.",
        "error_trace": 'Traceback (most recent call last):\n  File "/Repos/alice@example.com/etl/transform.py", '
        'line 41, in <module>\n    orders = spark.table("main.raw.orders_v2")\n'
        + "pyspark.errors.exceptions.captured.AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] "
        "The table or view `main`.`raw`.`orders_v2` cannot be found.\n" * 3,
        "logs": "reading main.raw.orders_v2\n",
        "logs_truncated": False,
    },
    "3102": {
        "error": "Py4JJavaError: An error occurred while calling o412.save: java.net.SocketException: Connection "
        "reset by peer",
        "error_trace": "Py4JJavaError: An error occurred while calling o412.save.\n"
        ": java.net.SocketException: Connection reset by peer\n",
    },
    "3101": {"notebook_output": {"result": "extracted 42 rows", "truncated": False}},
    "3103": {"notebook_output": {"result": '{"rows": 42}', "truncated": False}},
    "3104": {"notebook_output": {"result": "sent", "truncated": False}},
    "3201": {"error": "Pipeline update upd-o-3 failed: Flow 'orders_clean' has FAILED."},
    "3401": {
        "logs": "Backfilling 2026-11-01..2026-11-12\n" + "backfilled 100 rows\n" * 12 + "done: 1200 rows\n",
        "logs_truncated": False,
    },
}

# ------------------------------------------------------------------------ clusters


def cluster(cid: str, name: str, st: str, source: str, creator: str, **extra: Any) -> dict[str, Any]:
    c: dict[str, Any] = {
        "cluster_id": cid,
        "cluster_name": name,
        "state": st,
        "state_message": "",
        "cluster_source": source,
        "creator_user_name": creator,
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "driver_node_type_id": "i3.xlarge",
        "runtime_engine": "STANDARD",
        "autotermination_minutes": 0,
        "data_security_mode": "USER_ISOLATION",
        "cluster_memory_mb": 30500,
        "cluster_cores": 4.0,
        "default_tags": {"Vendor": "Databricks", "Creator": creator, "ClusterName": name, "ClusterId": cid},
    }
    c.update(extra)
    return c


CLUSTERS = [
    cluster(
        SHARED,
        "shared-analytics",
        "RUNNING",
        "UI",
        ALICE,
        autoscale={"min_workers": 1, "max_workers": 4},
        num_workers=2,
        autotermination_minutes=60,
        policy_id="pol-allpurpose-01",
        start_time=ms("2026-11-14T08:00:00"),
        last_restarted_time=ms("2026-11-14T08:05:00"),
        custom_tags={"cost-center": "42"},
        spark_conf={
            "spark.sql.shuffle.partitions": "64",
            "spark.hadoop.javax.jdo.option.ConnectionPassword": "metastore-fixture-pass",
        },
        cluster_memory_mb=91500,
        cluster_cores=12.0,
    ),
    cluster(
        JC2,
        "job-101-run-2002",
        "TERMINATED",
        "JOB",
        BOT,
        autoscale={"min_workers": 2, "max_workers": 8},
        num_workers=2,
        policy_id="pol-jobs-001",
        start_time=ms("2026-11-14T02:00:30"),
        terminated_time=ms("2026-11-14T02:26:00"),
        termination_reason={"code": "JOB_FINISHED", "type": "SUCCESS"},
        data_security_mode="SINGLE_USER",
        single_user_name=BOT,
        default_tags={
            "Vendor": "Databricks",
            "Creator": BOT,
            "ClusterName": "job-101-run-2002",
            "ClusterId": JC2,
            "JobId": "101",
            "RunName": "nightly_etl",
        },
    ),
    cluster(
        "1110-090000-adhoc01",
        "alice-adhoc",
        "TERMINATED",
        "UI",
        ALICE,
        num_workers=0,
        autotermination_minutes=30,
        instance_pool_id="pool-general-01",
        data_security_mode="SINGLE_USER",
        single_user_name=ALICE,
        start_time=ms("2026-11-13T09:00:00"),
        terminated_time=ms("2026-11-13T11:30:00"),
        termination_reason={"code": "INACTIVITY", "type": "SUCCESS", "parameters": {"inactivity_duration_min": "30"}},
        cluster_memory_mb=30500,
        cluster_cores=4.0,
    ),
    cluster(
        "1101-100000-bobml01",
        "bob-ml-gpu",
        "TERMINATED",
        "API",
        BOB,
        node_type_id="g5.xlarge",
        driver_node_type_id="g5.xlarge",
        num_workers=1,
        autotermination_minutes=120,
        spark_version="15.4.x-gpu-ml-scala2.12",
        start_time=ms("2026-11-02T10:00:00"),
        terminated_time=ms("2026-11-02T16:00:00"),
        termination_reason={"code": "USER_REQUEST", "type": "SUCCESS", "parameters": {"username": BOB}},
    ),
]


def event(cid: str, when: str, typ: str, **details: Any) -> dict[str, Any]:
    return {"cluster_id": cid, "timestamp": ms(when), "type": typ, "details": details}


CLUSTER_EVENTS = [
    event(SHARED, "2026-09-15T12:00:00", "CREATING", user=ALICE),
    event(SHARED, "2026-11-14T08:00:00", "STARTING", user=ALICE),
    event(SHARED, "2026-11-14T08:05:00", "RUNNING", current_num_workers=1, target_num_workers=1),
    event(SHARED, "2026-11-14T10:00:00", "RESIZING", current_num_workers=1, target_num_workers=4, cause="AUTOSCALE"),
    event(SHARED, "2026-11-14T10:04:00", "UPSIZE_COMPLETED", current_num_workers=4, target_num_workers=4),
    event(SHARED, "2026-11-14T13:00:00", "RESIZING", current_num_workers=4, target_num_workers=2, cause="AUTOSCALE"),
    event(JC2, "2026-11-14T02:00:30", "CREATING", user=BOT),
    event(JC2, "2026-11-14T02:04:30", "RUNNING", current_num_workers=2, target_num_workers=2),
    event(JC2, "2026-11-14T02:26:00", "TERMINATING", reason={"code": "JOB_FINISHED", "type": "SUCCESS"}),
    event("1110-090000-adhoc01", "2026-11-13T09:00:00", "STARTING", user=ALICE),
    event("1110-090000-adhoc01", "2026-11-13T09:06:00", "RUNNING", current_num_workers=0, target_num_workers=0),
    event(
        "1110-090000-adhoc01",
        "2026-11-13T11:30:00",
        "TERMINATING",
        reason={"code": "INACTIVITY", "type": "SUCCESS", "parameters": {"inactivity_duration_min": "30"}},
    ),
    event(
        "1101-100000-bobml01",
        "2026-11-02T16:00:00",
        "TERMINATING",
        user=BOB,
        reason={"code": "USER_REQUEST", "type": "SUCCESS", "parameters": {"username": BOB}},
    ),
]

POLICIES = [
    {
        "policy_id": "pol-allpurpose-01",
        "name": "All-purpose (small)",
        "description": "Small interactive clusters with auto-termination.",
        "creator_user_name": ALICE,
        "created_at_timestamp": ms("2026-08-01T09:00:00"),
        "is_default": False,
        "max_clusters_per_user": 2,
        "definition": json.dumps(
            {
                "autotermination_minutes": {"type": "range", "maxValue": 120, "defaultValue": 60},
                "node_type_id": {"type": "allowlist", "values": ["i3.xlarge", "i3.2xlarge"]},
            }
        ),
    },
    {
        "policy_id": "pol-jobs-001",
        "name": "Job Compute",
        "description": "Job clusters for scheduled pipelines.",
        "creator_user_name": ALICE,
        "created_at_timestamp": ms("2026-08-01T09:30:00"),
        "is_default": False,
        "policy_family_id": "job-cluster",
        "definition": json.dumps({"cluster_type": {"type": "fixed", "value": "job"}}),
    },
    {
        "policy_id": "pol-personal-000",
        "name": "Personal Compute",
        "description": "Single-node personal compute.",
        "creator_user_name": None,
        "created_at_timestamp": ms("2026-07-01T00:00:00"),
        "is_default": True,
        "policy_family_id": "personal-vm",
        "definition": json.dumps(
            {"spark_conf.spark.databricks.cluster.profile": {"type": "fixed", "value": "singleNode"}}
        ),
    },
]

POOLS = [
    {
        "instance_pool_id": "pool-general-01",
        "instance_pool_name": "general-i3",
        "state": "ACTIVE",
        "node_type_id": "i3.xlarge",
        "min_idle_instances": 1,
        "max_capacity": 20,
        "idle_instance_autotermination_minutes": 15,
        "enable_elastic_disk": True,
        "preloaded_spark_versions": ["15.4.x-scala2.12"],
        "custom_tags": {"team": "platform"},
        "stats": {"used_count": 0, "idle_count": 1, "pending_used_count": 0, "pending_idle_count": 0},
    },
    {
        "instance_pool_id": "pool-gpu-02",
        "instance_pool_name": "gpu-g5",
        "state": "ACTIVE",
        "node_type_id": "g5.xlarge",
        "min_idle_instances": 0,
        "max_capacity": 4,
        "idle_instance_autotermination_minutes": 10,
        "stats": {"used_count": 0, "idle_count": 0, "pending_used_count": 0, "pending_idle_count": 0},
    },
]

# ----------------------------------------------------------------------------- SQL

WAREHOUSES = [
    {
        "id": "wh-serverless-01",
        "name": "Serverless Starter Warehouse",
        "state": "RUNNING",
        "cluster_size": "2X-Small",
        "warehouse_type": "PRO",
        "enable_serverless_compute": True,
        "enable_photon": True,
        "min_num_clusters": 1,
        "max_num_clusters": 1,
        "num_clusters": 1,
        "num_active_sessions": 2,
        "auto_stop_mins": 10,
        "spot_instance_policy": "COST_OPTIMIZED",
        "channel": {"name": "CHANNEL_NAME_CURRENT"},
        "creator_name": ALICE,
        "health": {"status": "HEALTHY"},
        "tags": {"custom_tags": [{"key": "team", "value": "analytics"}]},
        "jdbc_url": "jdbc:spark://dbc-fixture.cloud.databricks.com:443/default;transportMode=http;ssl=1;"
        "AuthMech=3;httpPath=/sql/1.0/warehouses/wh-serverless-01;",
    },
    {
        "id": "wh-pro-02",
        "name": "BI Pro",
        "state": "STOPPED",
        "cluster_size": "Medium",
        "warehouse_type": "PRO",
        "enable_serverless_compute": False,
        "enable_photon": True,
        "min_num_clusters": 1,
        "max_num_clusters": 3,
        "num_clusters": 0,
        "num_active_sessions": 0,
        "auto_stop_mins": 30,
        "spot_instance_policy": "RELIABILITY_OPTIMIZED",
        "channel": {"name": "CHANNEL_NAME_CURRENT"},
        "creator_name": ALICE,
        "tags": {},
        "jdbc_url": "jdbc:spark://dbc-fixture.cloud.databricks.com:443/default;transportMode=http;ssl=1;"
        "AuthMech=3;httpPath=/sql/1.0/warehouses/wh-pro-02;",
    },
]


def query(
    qid: str,
    wh: str,
    user: tuple[int, str],
    st: str,
    stype: str,
    text: str,
    start: str,
    dur_ms: int,
    rows: int,
    **extra: Any,
) -> dict[str, Any]:
    q: dict[str, Any] = {
        "query_id": qid,
        "status": st,
        "statement_type": stype,
        "query_text": text,
        "warehouse_id": wh,
        "endpoint_id": wh,
        "user_id": user[0],
        "user_name": user[1],
        "executed_as_user_id": user[0],
        "executed_as_user_name": user[1],
        "query_start_time_ms": ms(start),
        "rows_produced": rows,
        "is_final": st in ("FINISHED", "FAILED", "CANCELED"),
        "lookup_key": f"lk-{qid}",
        "spark_ui_url": f"{HOST}/sql/history?queryId={qid}",
        "metrics": {
            "total_time_ms": dur_ms,
            "compilation_time_ms": min(dur_ms, 150),
            "execution_time_ms": max(dur_ms - 150, 0),
            "read_bytes": extra.pop("read_bytes", 0),
            "rows_read_count": extra.pop("read_rows", 0),
            "spill_to_disk_bytes": extra.pop("spill", 0),
            "result_from_cache": extra.pop("cached", False),
        },
    }
    if st != "RUNNING":
        q["duration"] = dur_ms
        q["execution_end_time_ms"] = ms(start) + dur_ms - 10
        q["query_end_time_ms"] = ms(start) + dur_ms
    q.update(extra)
    return q


A, B, E = (1001, ALICE), (1002, BOB), (1003, BOT)
QUERIES = [
    query(
        "01ef-q1",
        "wh-pro-02",
        A,
        "FINISHED",
        "SELECT",
        "SELECT count(*) FROM main.sales.orders",
        "2026-11-14T09:00:00",
        1200,
        1,
        read_bytes=1048576,
        read_rows=125000,
    ),
    query(
        "01ef-q2",
        "wh-pro-02",
        B,
        "FAILED",
        "SELECT",
        "SELECT * FROM main.raw.orders_v2",
        "2026-11-14T09:05:00",
        300,
        0,
        error_message="[TABLE_OR_VIEW_NOT_FOUND] The table or view `main`.`raw`.`orders_v2` cannot be found.",
    ),
    query(
        "01ef-q3",
        "wh-serverless-01",
        B,
        "FAILED",
        "OTHER",
        "SELEC 1",
        "2026-11-14T10:00:00",
        40,
        0,
        error_message="[PARSE_SYNTAX_ERROR] Syntax error at or near 'SELEC'.",
    ),
    query(
        "01ef-q4",
        "wh-serverless-01",
        A,
        "FINISHED",
        "INSERT",
        "INSERT INTO main.sales.daily SELECT * FROM main.sales.orders_staging",
        "2026-11-14T11:00:00",
        35500,
        5000,
        read_bytes=73400320,
        read_rows=5000,
        spill=1048576,
    ),
    query(
        "01ef-q5",
        "wh-pro-02",
        E,
        "FINISHED",
        "MERGE",
        "MERGE INTO main.sales.orders t USING main.sales.orders_staging s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET *",
        "2026-11-14T02:20:00",
        120000,
        4800,
        read_bytes=524288000,
        read_rows=2500000,
    ),
    query(
        "01ef-q6",
        "wh-serverless-01",
        A,
        "CANCELED",
        "SELECT",
        "SELECT * FROM main.sales.orders ORDER BY amount",
        "2026-11-14T12:00:00",
        9000,
        0,
    ),
    query(
        "01ef-q7",
        "wh-serverless-01",
        B,
        "FINISHED",
        "SELECT",
        "SELECT region, sum(amount) FROM main.sales.orders GROUP BY 1",
        "2026-11-13T16:00:00",
        90,
        5,
        cached=True,
    ),
    query(
        "01ef-q8",
        "wh-pro-02",
        A,
        "RUNNING",
        "SELECT",
        "SELECT * FROM main.sales.orders_history",
        "2026-11-15T01:30:00",
        60000,
        0,
    ),
]

# ----------------------------------------------------------------------- pipelines

PIPELINES = [
    {
        "pipeline_id": "pl-orders-0001",
        "name": "orders_dlt",
        "state": "FAILED",
        "health": "UNHEALTHY",
        "creator_user_name": ALICE,
        "run_as_user_name": BOT,
        "spec": {
            "id": "pl-orders-0001",
            "name": "orders_dlt",
            "catalog": "main",
            "target": "sales",
            "serverless": True,
        },
        "latest_updates": [
            {"update_id": "upd-o-3", "state": "FAILED", "creation_time": iso("2026-11-14T00:30:40")},
            {"update_id": "upd-o-2", "state": "COMPLETED", "creation_time": iso("2026-11-13T00:30:40")},
            {"update_id": "upd-o-1", "state": "COMPLETED", "creation_time": iso("2026-11-12T00:30:40")},
        ],
        "last_modified": ms("2026-11-10T10:00:00"),
    },
    {
        "pipeline_id": "pl-events-0002",
        "name": "events_dlt",
        "state": "IDLE",
        "health": "HEALTHY",
        "creator_user_name": BOB,
        "run_as_user_name": BOB,
        "spec": {"id": "pl-events-0002", "name": "events_dlt", "catalog": "main", "target": "web", "serverless": True},
        "latest_updates": [
            {"update_id": "upd-e-2", "state": "COMPLETED", "creation_time": iso("2026-11-14T06:00:00")},
            {"update_id": "upd-e-1", "state": "COMPLETED", "creation_time": iso("2026-11-10T06:00:00")},
        ],
        "last_modified": ms("2026-11-01T10:00:00"),
    },
]


def update(pid: str, uid: str, st: str, cause: str, when: str, **extra: Any) -> dict[str, Any]:
    u: dict[str, Any] = {
        "pipeline_id": pid,
        "update_id": uid,
        "state": st,
        "cause": cause,
        "creation_time": ms(when),
        "full_refresh": False,
        "validate_only": False,
        "config": {"id": pid},
    }
    u.update(extra)
    return u


PIPELINE_UPDATES = {
    "pl-orders-0001": [
        update("pl-orders-0001", "upd-o-3", "FAILED", "JOB_TASK", "2026-11-14T00:30:40"),
        update("pl-orders-0001", "upd-o-2", "COMPLETED", "JOB_TASK", "2026-11-13T00:30:40"),
        update("pl-orders-0001", "upd-o-1", "COMPLETED", "USER_ACTION", "2026-11-12T00:30:40", full_refresh=True),
    ],
    "pl-events-0002": [
        update(
            "pl-events-0002", "upd-e-2", "COMPLETED", "API_CALL", "2026-11-14T06:00:00", refresh_selection=["sessions"]
        ),
        update("pl-events-0002", "upd-e-1", "COMPLETED", "USER_ACTION", "2026-11-10T06:00:00"),
    ],
}


def pevent(
    pid: str, eid: str, uid: str, when: str, level: str, etype: str, message: str, **extra: Any
) -> dict[str, Any]:
    origin: dict[str, Any] = {"pipeline_id": pid, "pipeline_name": pid, "update_id": uid}
    if "flow" in extra:
        origin["flow_name"] = origin["dataset_name"] = extra.pop("flow")
    e: dict[str, Any] = {
        "id": eid,
        "sequence": {"data_plane_id": {"seq_no": int(eid.rsplit("-", 1)[-1])}},
        "origin": origin,
        "timestamp": iso(when),
        "message": message,
        "level": level,
        "maturity_level": "STABLE",
        "event_type": etype,
        "details": extra.pop("details", {}),
    }
    e.update(extra)
    return e


PIPELINE_EVENTS = {
    "pl-orders-0001": [
        pevent(
            "pl-orders-0001",
            "ev-o-1",
            "upd-o-2",
            "2026-11-13T00:30:40",
            "INFO",
            "create_update",
            "Update upd-o-2 started by JOB_TASK.",
            details={"create_update": {"cause": "JOB_TASK"}},
        ),
        pevent(
            "pl-orders-0001",
            "ev-o-2",
            "upd-o-2",
            "2026-11-13T00:45:00",
            "INFO",
            "update_progress",
            "Update upd-o-2 is COMPLETED.",
            details={"update_progress": {"state": "COMPLETED"}},
        ),
        pevent(
            "pl-orders-0001",
            "ev-o-3",
            "upd-o-3",
            "2026-11-14T00:30:40",
            "INFO",
            "create_update",
            "Update upd-o-3 started by JOB_TASK.",
            details={"create_update": {"cause": "JOB_TASK"}},
        ),
        pevent(
            "pl-orders-0001",
            "ev-o-4",
            "upd-o-3",
            "2026-11-14T00:36:00",
            "WARN",
            "flow_progress",
            "Flow 'orders_clean' dropped 12 records failing expectation 'valid_amount'.",
            flow="orders_clean",
            details={"flow_progress": {"data_quality": {"dropped_records": 12}}},
        ),
        pevent(
            "pl-orders-0001",
            "ev-o-5",
            "upd-o-3",
            "2026-11-14T00:47:00",
            "ERROR",
            "flow_progress",
            "Flow 'orders_clean' has FAILED.",
            flow="orders_clean",
            error={
                "fatal": False,
                "exceptions": [
                    {
                        "class_name": "org.apache.spark.sql.AnalysisException",
                        "message": "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column `customer_idx` cannot be resolved.",
                    }
                ],
            },
            details={"flow_progress": {"status": "FAILED"}},
        ),
        pevent(
            "pl-orders-0001",
            "ev-o-6",
            "upd-o-3",
            "2026-11-14T00:47:30",
            "ERROR",
            "update_progress",
            "Update upd-o-3 is FAILED.",
            error={
                "fatal": True,
                "exceptions": [
                    {
                        "class_name": "com.databricks.pipelines.common.errors.UpdateFailedException",
                        "message": "Update failed because flow 'orders_clean' failed.",
                    }
                ],
            },
            details={"update_progress": {"state": "FAILED"}},
        ),
    ],
    "pl-events-0002": [
        pevent(
            "pl-events-0002",
            "ev-e-1",
            "upd-e-2",
            "2026-11-14T06:00:00",
            "INFO",
            "create_update",
            "Update upd-e-2 started by API_CALL.",
            details={"create_update": {"cause": "API_CALL"}},
        ),
        pevent(
            "pl-events-0002",
            "ev-e-2",
            "upd-e-2",
            "2026-11-14T06:09:00",
            "INFO",
            "update_progress",
            "Update upd-e-2 is COMPLETED.",
            details={"update_progress": {"state": "COMPLETED"}},
        ),
    ],
}

# ----------------------------------------------------------------------- workspace

REPOS = [
    {
        "id": 501,
        "path": "/Repos/alice@example.com/etl",
        "url": "https://github.com/example/etl.git",
        "provider": "gitHub",
        "branch": "main",
        "head_commit_id": "7e0847ede61f07adede22e2bcce6050216489171",
    },
    {
        "id": 502,
        "path": "/Repos/bob@example.com/reports",
        "url": "https://gitlab.com/example/reports.git",
        "provider": "gitLab",
        "branch": "feature/charts",
        "head_commit_id": "1d3b0d46a7b2d81e3de2a4a6a6a4d8fbc3b0a111",
        "sparse_checkout": {"patterns": ["reports/"]},
    },
]

SCOPES = [
    {"name": "lake", "backend_type": "DATABRICKS"},
    {
        "name": "kv-prod",
        "backend_type": "AZURE_KEYVAULT",
        "keyvault_metadata": {
            "resource_id": "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg/providers/"
            "Microsoft.KeyVault/vaults/kv-prod",
            "dns_name": "https://kv-prod.vault.azure.net/",
        },
    },
    {"name": "ml", "backend_type": "DATABRICKS"},
]

_OID = iter(range(9000, 9100))


def obj(path: str, typ: str, language: str | None = None, size: int | None = None) -> dict[str, Any]:
    o: dict[str, Any] = {
        "object_type": typ,
        "path": path,
        "object_id": next(_OID),
        "created_at": ms("2026-10-01T10:00:00"),
        "modified_at": ms("2026-11-10T10:00:00"),
        "resource_id": f"res-{path.strip('/').replace('/', '-') or 'root'}",
    }
    if language:
        o["language"] = language
    if size is not None:
        o["size"] = size
    return o


WORKSPACE = {
    "/": [obj("/Users", "DIRECTORY"), obj("/Shared", "DIRECTORY"), obj("/Repos", "DIRECTORY")],
    "/Users": [obj("/Users/alice@example.com", "DIRECTORY"), obj("/Users/bob@example.com", "DIRECTORY")],
    "/Users/alice@example.com": [
        obj("/Users/alice@example.com/ingest_dev", "NOTEBOOK", "PYTHON"),
        obj("/Users/alice@example.com/explore", "NOTEBOOK", "SQL"),
        obj("/Users/alice@example.com/projects", "DIRECTORY"),
        obj("/Users/alice@example.com/requirements.txt", "FILE", size=120),
    ],
    "/Users/alice@example.com/projects": [
        obj("/Users/alice@example.com/projects/churn", "NOTEBOOK", "PYTHON"),
        obj("/Users/alice@example.com/projects/helpers", "NOTEBOOK", "SCALA"),
    ],
    "/Users/bob@example.com": [obj("/Users/bob@example.com/report", "NOTEBOOK", "PYTHON")],
    "/Shared": [obj("/Shared/dashboards", "DIRECTORY"), obj("/Shared/common_udfs", "NOTEBOOK", "PYTHON")],
    "/Shared/dashboards": [obj("/Shared/dashboards/sales.lvdash.json", "DASHBOARD")],
    "/Repos": [],
}


def main() -> None:
    data = {
        "host": HOST,
        "jobs": JOBS,
        "runs": RUNS,
        "outputs": OUTPUTS,
        "clusters": CLUSTERS,
        "cluster_events": CLUSTER_EVENTS,
        "policies": POLICIES,
        "instance_pools": POOLS,
        "warehouses": WAREHOUSES,
        "queries": QUERIES,
        "pipelines": PIPELINES,
        "pipeline_updates": PIPELINE_UPDATES,
        "pipeline_events": PIPELINE_EVENTS,
        "repos": REPOS,
        "secret_scopes": SCOPES,
        "workspace": WORKSPACE,
    }
    json.dump(data, sys.stdout, indent=1, sort_keys=True)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
