"""The ``main`` schema: Databricks workspace resources as tables, with filter pushdown.

Each class below is the scan behind one catalog table. ``requests`` documents,
in code, exactly which SQL predicates reach the API; everything else is
filtered by DuckDB (``auto_apply_filters`` re-checks every pushed predicate).

All timestamps the API returns as epoch milliseconds (or ISO strings, for
pipelines) become ``TIMESTAMPTZ``; durations become seconds (``DOUBLE``).
"""

from __future__ import annotations

from typing import Any, ClassVar

import pyarrow as pa

from .client import DatabricksClient, quote
from .masking import mask_tree
from .meta import column
from .scan import (
    STR_LIST,
    STR_MAP,
    TSTZ,
    ApiScan,
    Listing,
    Pushdown,
    Remapped,
    drain,
    int_values,
    iso,
    json_text,
    seconds,
    str_map,
    to_int,
    ts,
)

JOBS_API = "/api/2.2/jobs"
CLUSTERS_API = "/api/2.1/clusters"

ACTIVE_LIFE_CYCLE = frozenset({"QUEUED", "PENDING", "RUNNING", "TERMINATING", "BLOCKED", "WAITING_FOR_RETRY"})
DONE_LIFE_CYCLE = frozenset({"TERMINATED", "SKIPPED", "INTERNAL_ERROR"})
ACTIVE_STATUS = frozenset({"BLOCKED", "PENDING", "QUEUED", "RUNNING", "TERMINATING", "WAITING"})


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def task_type(task: dict[str, Any]) -> str | None:
    """``notebook``, ``spark_python``, ``sql``, ``pipeline``, ``run_job``, ... from a task's ``*_task`` key."""
    for key in sorted(task):
        if key.endswith("_task") and isinstance(task[key], dict):
            return key.removesuffix("_task")
    return None


def task_cluster(task: dict[str, Any]) -> str | None:
    """Where a task runs: an existing cluster id, ``job_cluster:<key>``, or ``serverless``."""
    if task.get("existing_cluster_id"):
        return str(task["existing_cluster_id"])
    if task.get("job_cluster_key"):
        return f"job_cluster:{task['job_cluster_key']}"
    if task.get("new_cluster"):
        return "new_cluster"
    if task.get("environment_key") or "sql_task" in task or "pipeline_task" in task:
        return "serverless"
    return None


def _deps(task: dict[str, Any]) -> list[str]:
    return [str(d["task_key"]) for d in task.get("depends_on") or [] if isinstance(d, dict) and d.get("task_key")]


def _cluster_size(spec: dict[str, Any]) -> tuple[int | None, int | None, int | None]:
    auto = spec.get("autoscale") or {}
    return to_int(spec.get("num_workers")), to_int(auto.get("min_workers")), to_int(auto.get("max_workers"))


# ---------------------------------------------------------------------------- jobs

TASK_STRUCT = pa.struct(
    [
        pa.field("task_key", pa.string()),
        pa.field("task_type", pa.string()),
        pa.field("depends_on", STR_LIST),
        pa.field("cluster", pa.string()),
        pa.field("max_retries", pa.int64()),
        pa.field("timeout_seconds", pa.int64()),
        pa.field("description", pa.string()),
    ]
)
JOB_CLUSTER_STRUCT = pa.struct(
    [
        pa.field("job_cluster_key", pa.string()),
        pa.field("spark_version", pa.string()),
        pa.field("node_type_id", pa.string()),
        pa.field("num_workers", pa.int64()),
        pa.field("autoscale_min_workers", pa.int64()),
        pa.field("autoscale_max_workers", pa.int64()),
        pa.field("policy_id", pa.string()),
    ]
)


def all_job_tasks(client: DatabricksClient, job: dict[str, Any]) -> list[dict[str, Any]]:
    """Return a job's tasks; API 2.2 returns 100 per page, the rest come from jobs/get page tokens."""
    tasks = list((job.get("settings") or {}).get("tasks") or [])
    token = job.get("next_page_token") or ""
    if not job.get("has_more") and not token:
        return tasks
    tasks = []
    token = ""
    while True:
        query: dict[str, Any] = {"job_id": job["job_id"]}
        if token:
            query["page_token"] = token
        page = client.get(f"{JOBS_API}/get", query)
        tasks.extend((page.get("settings") or {}).get("tasks") or [])
        token = page.get("next_page_token") or ""
        if not token:
            return tasks


class Jobs(ApiScan):
    """Rows for ``databricks.main.jobs``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("job_id", pa.int64(), "Numeric identifier Databricks assigned to the job (unique).", nullable=False),
            column("name", pa.string(), "Job name."),
            column("creator_user_name", pa.string(), "User who created the job."),
            column("run_as_user_name", pa.string(), "User or service principal the job runs as."),
            column("created_time", TSTZ, "When the job was created."),
            column("schedule_cron", pa.string(), "Quartz cron expression of the job's schedule, if scheduled."),
            column("schedule_timezone", pa.string(), "Time zone id of the schedule, e.g. 'UTC'."),
            column("schedule_paused", pa.bool_(), "True when the schedule or trigger is paused."),
            column(
                "trigger_type",
                pa.string(),
                "How new runs start: periodic (cron), file_arrival, table_update, periodic_interval, "
                "continuous, or NULL (manual / API only).",
            ),
            column("max_concurrent_runs", pa.int64(), "Maximum number of concurrent runs (count of runs)."),
            column("timeout_seconds", pa.int64(), "Job timeout in seconds; 0 means none."),
            column("tags", STR_MAP, "Job tags as a MAP(VARCHAR, VARCHAR)."),
            column("task_count", pa.int64(), "Number of tasks in the job."),
            column(
                "tasks",
                pa.list_(TASK_STRUCT),
                "Task summary: task_key, task_type (notebook, spark_python, sql, pipeline, ...), depends_on, "
                "cluster (existing cluster id, job_cluster:<key> or serverless), max_retries, timeout_seconds, "
                "description.",
            ),
            column(
                "job_clusters",
                pa.list_(JOB_CLUSTER_STRUCT),
                "Shared job clusters: key, Spark version, node type, fixed size or autoscale bounds (count of "
                "workers), policy.",
            ),
            column(
                "settings",
                pa.json_(),
                "The full job settings (JSON); secret-looking parameter and Spark config values are masked '***'.",
            ),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "jobs_scan"
        description = "Scan behind the jobs table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str]) -> list[Listing]:
        """``job_id`` → jobs/get per id; ``name =`` → the list's exact (case-insensitive) name filter."""
        job_ids = int_values(pd, "job_id")
        if job_ids:
            return [Listing(f"{JOBS_API}/get", None, (("job_id", j),), missing_ok=True) for j in job_ids]
        q: list[tuple[str, Any]] = [("expand_tasks", True)]
        name = pd.one("name")
        if name is not None:
            q.append(("name", str(name)))
        return [Listing(f"{JOBS_API}/list", "jobs", tuple(q), size_param="limit", max_size=100)]

    @classmethod
    def rows(
        cls, client: DatabricksClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Flatten job settings into columns."""
        out = []
        for job in items:
            s = job.get("settings") or {}
            schedule = s.get("schedule") or {}
            trigger = s.get("trigger") or {}
            continuous = s.get("continuous") or {}
            ttype = None
            paused = None
            if schedule:
                ttype, paused = "periodic", schedule.get("pause_status") == "PAUSED"
            elif continuous:
                ttype, paused = "continuous", continuous.get("pause_status") == "PAUSED"
            elif trigger:
                for key in ("file_arrival", "table_update", "table", "periodic"):
                    if key in trigger:
                        ttype = {"table": "table_update", "periodic": "periodic_interval"}.get(key, key)
                paused = trigger.get("pause_status") == "PAUSED"
            tasks = all_job_tasks(client, job) if {"tasks", "task_count"} & wanted else []
            out.append(
                {
                    "job_id": job["job_id"],
                    "name": s.get("name"),
                    "creator_user_name": job.get("creator_user_name"),
                    "run_as_user_name": job.get("run_as_user_name"),
                    "created_time": ts(job.get("created_time")),
                    "schedule_cron": schedule.get("quartz_cron_expression"),
                    "schedule_timezone": schedule.get("timezone_id"),
                    "schedule_paused": paused,
                    "trigger_type": ttype,
                    "max_concurrent_runs": to_int(s.get("max_concurrent_runs")),
                    "timeout_seconds": to_int(s.get("timeout_seconds")),
                    "tags": str_map(s.get("tags") or {}),
                    "task_count": len(tasks),
                    "tasks": [
                        {
                            "task_key": t.get("task_key"),
                            "task_type": task_type(t),
                            "depends_on": _deps(t),
                            "cluster": task_cluster(t),
                            "max_retries": to_int(t.get("max_retries")),
                            "timeout_seconds": to_int(t.get("timeout_seconds")),
                            "description": t.get("description"),
                        }
                        for t in tasks
                    ],
                    "job_clusters": [
                        {
                            "job_cluster_key": jc.get("job_cluster_key"),
                            "spark_version": (jc.get("new_cluster") or {}).get("spark_version"),
                            "node_type_id": (jc.get("new_cluster") or {}).get("node_type_id"),
                            "num_workers": _cluster_size(jc.get("new_cluster") or {})[0],
                            "autoscale_min_workers": _cluster_size(jc.get("new_cluster") or {})[1],
                            "autoscale_max_workers": _cluster_size(jc.get("new_cluster") or {})[2],
                            "policy_id": (jc.get("new_cluster") or {}).get("policy_id"),
                        }
                        for jc in s.get("job_clusters") or []
                    ],
                    "settings": json_text(mask_tree({**s, "tasks": tasks} if tasks else s))
                    if "settings" in wanted
                    else None,
                }
            )
        return out


# ------------------------------------------------------------------------ job runs

STATE_FIELDS = [
    column(
        "life_cycle_state",
        pa.string(),
        "QUEUED, PENDING, RUNNING, TERMINATING, TERMINATED, SKIPPED, INTERNAL_ERROR, BLOCKED or WAITING_FOR_RETRY.",
    ),
    column(
        "result_state",
        pa.string(),
        "Outcome once terminated: SUCCESS, FAILED, TIMEDOUT, CANCELED, SUCCESS_WITH_FAILURES, UPSTREAM_FAILED, "
        "UPSTREAM_CANCELED, EXCLUDED, DISABLED, MAXIMUM_CONCURRENT_RUNS_REACHED; NULL while active.",
    ),
    column("state_message", pa.string(), "Human-readable state description, e.g. the failure reason."),
    column(
        "status",
        pa.string(),
        "Newer status.state: BLOCKED, PENDING, QUEUED, RUNNING, TERMINATING, TERMINATED, WAITING.",
    ),
    column("termination_code", pa.string(), "status.termination_details.code, e.g. SUCCESS, RUN_EXECUTION_ERROR."),
    column(
        "termination_type",
        pa.string(),
        "status.termination_details.type: SUCCESS, INTERNAL_ERROR, CLIENT_ERROR, CLOUD_FAILURE.",
    ),
]


def _state_cols(item: dict[str, Any]) -> dict[str, Any]:
    state = item.get("state") or {}
    status = item.get("status") or {}
    term = status.get("termination_details") or {}
    return {
        "life_cycle_state": state.get("life_cycle_state"),
        "result_state": state.get("result_state"),
        "state_message": state.get("state_message") or term.get("message") or None,
        "status": status.get("state"),
        "termination_code": term.get("code"),
        "termination_type": term.get("type"),
    }


JOB_RUN_FIELDS = [
    column("run_id", pa.int64(), "Run id (unique in the workspace).", nullable=False),
    column(
        "job_id", pa.int64(), "Unique numeric identifier of the job the run belongs to; NULL for one-time submit runs."
    ),
    column("run_name", pa.string(), "Run name (the job name for job runs)."),
    column("number_in_job", pa.int64(), "Sequence number of this run among the job's runs."),
    column(
        "original_attempt_run_id",
        pa.int64(),
        "Unique numeric run identifier of the first attempt, for a retried single-task run.",
    ),
    column(
        "attempt_number", pa.int64(), "Retry count before this attempt: 0 for the first attempt, 1 for the first retry."
    ),
    column("run_type", pa.string(), "JOB_RUN, WORKFLOW_RUN (dbutils.notebook.run) or SUBMIT_RUN (runs/submit)."),
    column(
        "trigger",
        pa.string(),
        "What started the run: PERIODIC, ONE_TIME, RETRY, RUN_JOB_TASK, FILE_ARRIVAL, TABLE, CONTINUOUS, ...",
    ),
    *STATE_FIELDS,
    column("termination_message", pa.string(), "status.termination_details.message."),
    column("user_cancelled_or_timedout", pa.bool_(), "True when a user cancelled the run or it timed out."),
    column("queue_reason", pa.string(), "Why the run is queued, while it is."),
    column("creator_user_name", pa.string(), "User who started the run."),
    column("start_time", TSTZ, "When the run was created (includes queueing and cluster setup)."),
    column("end_time", TSTZ, "When the run ended; NULL while active."),
    column("queue_duration_seconds", pa.float64(), "Seconds spent queued."),
    column("setup_duration_seconds", pa.float64(), "Seconds spent setting up the cluster (single-task runs)."),
    column("execution_duration_seconds", pa.float64(), "Seconds spent executing (single-task runs)."),
    column("cleanup_duration_seconds", pa.float64(), "Seconds spent cleaning up (single-task runs)."),
    column("run_duration_seconds", pa.float64(), "Total run duration in seconds (multi-task runs)."),
    column("cluster_id", pa.string(), "Cluster of a single-cluster run, when known."),
    column(
        "job_parameters",
        pa.list_(pa.struct([pa.field("name", pa.string()), pa.field("value", pa.string())])),
        "Job-level parameters of the run (secret-looking values masked).",
    ),
    column("run_page_url", pa.string(), "Link to the run in the workspace UI."),
]


def run_listings(pd: Pushdown, *, expand_tasks: bool, job_ids: list[int] | None = None) -> list[Listing]:
    """Build runs listings.

    * ``run_id = / IN`` → ``runs/get`` per run (exact).
    * ``job_id = / IN`` → one ``runs/list?job_id=`` per job.
    * an active life-cycle / status set → ``active_only``; a terminated one (or any ``result_state``)
      → ``completed_only``.
    * ``start_time`` range → ``start_time_from`` / ``start_time_to`` (epoch ms, inclusive).
    * ``run_type = x`` → ``run_type``.
    """
    run_ids = int_values(pd, "run_id")
    if run_ids:
        return [Listing(f"{JOBS_API}/runs/get", None, (("run_id", r),), missing_ok=True) for r in run_ids]
    q: list[tuple[str, Any]] = []
    if expand_tasks:
        q.append(("expand_tasks", True))
    lcs = {str(v) for v in pd.values("life_cycle_state") or []}
    sts = {str(v) for v in pd.values("status") or []}
    if (lcs and lcs <= ACTIVE_LIFE_CYCLE) or (sts and sts <= ACTIVE_STATUS):
        q.append(("active_only", True))
    elif (lcs and lcs <= DONE_LIFE_CYCLE) or (sts and sts == {"TERMINATED"}) or pd.values("result_state"):
        q.append(("completed_only", True))
    lo, hi = pd.ms_range("start_time")
    if lo is not None:
        q.append(("start_time_from", lo))
    if hi is not None:
        q.append(("start_time_to", hi))
    run_type = pd.one("run_type")
    if run_type is not None:
        q.append(("run_type", str(run_type)))
    jobs = job_ids if job_ids is not None else int_values(pd, "job_id")
    paging = {"size_param": "limit", "max_size": 25}
    if jobs:
        return [Listing(f"{JOBS_API}/runs/list", "runs", (("job_id", j), *q), **paging) for j in jobs]  # type: ignore[arg-type]
    return [Listing(f"{JOBS_API}/runs/list", "runs", tuple(q), **paging)]  # type: ignore[arg-type]


def job_run_row(r: dict[str, Any], wanted: set[str]) -> dict[str, Any]:
    """One ``job_runs`` row from a run object."""
    status = r.get("status") or {}
    term = status.get("termination_details") or {}
    state = r.get("state") or {}
    params = r.get("job_parameters") or []
    return {
        "run_id": r["run_id"],
        "job_id": to_int(r.get("job_id")),
        "run_name": r.get("run_name"),
        "number_in_job": to_int(r.get("number_in_job")),
        "original_attempt_run_id": to_int(r.get("original_attempt_run_id")),
        "attempt_number": to_int(r.get("attempt_number")),
        "run_type": r.get("run_type"),
        "trigger": r.get("trigger"),
        **_state_cols(r),
        "termination_message": term.get("message"),
        "user_cancelled_or_timedout": state.get("user_cancelled_or_timedout"),
        "queue_reason": state.get("queue_reason") or (status.get("queue_details") or {}).get("message"),
        "creator_user_name": r.get("creator_user_name"),
        "start_time": ts(r.get("start_time")),
        "end_time": ts(r.get("end_time")),
        "queue_duration_seconds": seconds(r.get("queue_duration")),
        "setup_duration_seconds": seconds(r.get("setup_duration")),
        "execution_duration_seconds": seconds(r.get("execution_duration")),
        "cleanup_duration_seconds": seconds(r.get("cleanup_duration")),
        "run_duration_seconds": seconds(r.get("run_duration")),
        "cluster_id": (r.get("cluster_instance") or {}).get("cluster_id"),
        "job_parameters": [
            {"name": p.get("name"), "value": p.get("value", p.get("default"))}
            for p in mask_tree(params)
            if isinstance(p, dict)
        ]
        if "job_parameters" in wanted
        else None,
        "run_page_url": r.get("run_page_url"),
    }


class JobRuns(ApiScan):
    """Rows for ``databricks.main.job_runs``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(JOB_RUN_FIELDS)

    class Meta:
        """Table metadata."""

        name = "job_runs_scan"
        description = "Scan behind the job_runs table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str]) -> list[Listing]:
        """See ``run_listings``."""
        return run_listings(pd, expand_tasks=False)

    @classmethod
    def rows(
        cls, client: DatabricksClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map run objects to rows."""
        return [job_run_row(r, wanted) for r in items]


# ------------------------------------------------------------------- job run tasks


def all_run_tasks(client: DatabricksClient, run: dict[str, Any]) -> list[dict[str, Any]]:
    """Every task (attempt) of a run; API 2.2 pages tasks 100 at a time via runs/get page tokens."""
    tasks = list(run.get("tasks") or [])
    if not run.get("has_more") and not run.get("next_page_token"):
        return tasks
    tasks, token = [], ""
    while True:
        query: dict[str, Any] = {"run_id": run["run_id"]}
        if token:
            query["page_token"] = token
        page = client.get(f"{JOBS_API}/runs/get", query)
        tasks.extend(page.get("tasks") or [])
        token = page.get("next_page_token") or ""
        if not token:
            return tasks


class JobRunTasks(ApiScan):
    """Rows for ``databricks.main.job_run_tasks``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("run_id", pa.int64(), "The parent job run (joins job_runs.run_id).", nullable=False),
            column("job_id", pa.int64(), "Unique numeric identifier of the parent run's job; NULL for submit runs."),
            column("task_run_id", pa.int64(), "This task attempt's own run id (use with run_output).", nullable=False),
            column("task_key", pa.string(), "Task key within the job.", nullable=False),
            column("attempt_number", pa.int64(), "Attempt number of the task, 0 for the first try; retries add rows."),
            column("task_type", pa.string(), "notebook, spark_python, sql, pipeline, run_job, dbt, condition, ..."),
            *STATE_FIELDS,
            column("start_time", TSTZ, "When the task attempt started."),
            column("end_time", TSTZ, "When the task attempt ended."),
            column("queue_duration_seconds", pa.float64(), "Seconds queued."),
            column("setup_duration_seconds", pa.float64(), "Seconds spent on cluster setup."),
            column("execution_duration_seconds", pa.float64(), "Seconds spent executing."),
            column("cleanup_duration_seconds", pa.float64(), "Seconds spent cleaning up."),
            column("cluster_id", pa.string(), "Cluster the attempt ran on (cluster_instance.cluster_id)."),
            column("existing_cluster_id", pa.string(), "All-purpose cluster the task is configured to use."),
            column("job_cluster_key", pa.string(), "Job cluster the task is configured to use."),
            column("depends_on", STR_LIST, "Task keys this task depends on."),
            column("description", pa.string(), "Task description."),
            column("run_page_url", pa.string(), "Link to the task run in the workspace UI."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "job_run_tasks_scan"
        description = "Scan behind the job_run_tasks table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str]) -> list[Listing]:
        """run_id → runs/get; job_id, start_time range → runs/list with expand_tasks (run-level filters only)."""
        # Task-level states do not narrow the parent runs, so only run/job/time filters reach the API.
        return run_listings(restrict(pd, ("run_id", "job_id", "start_time")), expand_tasks=True)

    @classmethod
    def rows(
        cls, client: DatabricksClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """One row per task attempt of every run on the page."""
        out = []
        for run in items:
            for t in all_run_tasks(client, run):
                out.append(
                    {
                        "run_id": run["run_id"],
                        "job_id": to_int(run.get("job_id")),
                        "task_run_id": t["run_id"],
                        "task_key": t.get("task_key"),
                        "attempt_number": to_int(t.get("attempt_number")),
                        "task_type": task_type(t),
                        **_state_cols(t),
                        "start_time": ts(t.get("start_time")),
                        "end_time": ts(t.get("end_time")),
                        "queue_duration_seconds": seconds(t.get("queue_duration")),
                        "setup_duration_seconds": seconds(t.get("setup_duration")),
                        "execution_duration_seconds": seconds(t.get("execution_duration")),
                        "cleanup_duration_seconds": seconds(t.get("cleanup_duration")),
                        "cluster_id": (t.get("cluster_instance") or {}).get("cluster_id"),
                        "existing_cluster_id": t.get("existing_cluster_id"),
                        "job_cluster_key": t.get("job_cluster_key"),
                        "depends_on": _deps(t),
                        "description": t.get("description"),
                        "run_page_url": t.get("run_page_url"),
                    }
                )
        return out


def restrict(pd: Pushdown, keep: tuple[str, ...]) -> Pushdown:
    """Return a view of ``pd`` limited to the given columns (values and bounds)."""
    return Remapped(
        {c: v for c in keep if (v := pd.values(c)) is not None},
        {c: pd.bounds(c) for c in keep},
    )


# ------------------------------------------------------------------------ clusters


class Clusters(ApiScan):
    """Rows for ``databricks.main.clusters``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column(
                "cluster_id", pa.string(), "Unique cluster identifier, e.g. '0915-120000-abcd1234'.", nullable=False
            ),
            column("cluster_name", pa.string(), "Display name given to the cluster by its creator."),
            column(
                "state", pa.string(), "PENDING, RUNNING, RESTARTING, RESIZING, TERMINATING, TERMINATED, ERROR, UNKNOWN."
            ),
            column("state_message", pa.string(), "Message about the current state."),
            column(
                "cluster_source",
                pa.string(),
                "Who created it: UI, API, JOB, PIPELINE, PIPELINE_MAINTENANCE, SQL, MODELS.",
            ),
            column("creator_user_name", pa.string(), "User who created the cluster."),
            column("single_user_name", pa.string(), "Assigned user of a single-user (dedicated) cluster."),
            column("data_security_mode", pa.string(), "Access mode, e.g. SINGLE_USER, USER_ISOLATION, NONE."),
            column("spark_version", pa.string(), "Databricks Runtime version key, e.g. '15.4.x-scala2.12'."),
            column("runtime_engine", pa.string(), "STANDARD or PHOTON."),
            column("node_type_id", pa.string(), "Worker node type."),
            column("driver_node_type_id", pa.string(), "Driver node type."),
            column("num_workers", pa.int64(), "Fixed worker count (or current count for autoscaling clusters)."),
            column(
                "autoscale_min_workers", pa.int64(), "Autoscaling lower bound (count of workers); NULL when fixed size."
            ),
            column(
                "autoscale_max_workers", pa.int64(), "Autoscaling upper bound (count of workers); NULL when fixed size."
            ),
            column("autotermination_minutes", pa.int64(), "Idle time before auto-termination, in minutes; 0 = never."),
            column("instance_pool_id", pa.string(), "Instance pool the workers come from."),
            column("policy_id", pa.string(), "Cluster policy applied (joins cluster_policies.policy_id)."),
            column("cluster_memory_mb", pa.int64(), "Total memory of the cluster in MB."),
            column("cluster_cores", pa.float64(), "Total CPU cores across driver and workers (count of cores)."),
            column("start_time", TSTZ, "When the cluster was created or last started."),
            column("terminated_time", TSTZ, "When the cluster terminated, if it is terminated."),
            column("last_restarted_time", TSTZ, "When the cluster was last restarted."),
            column(
                "termination_code", pa.string(), "termination_reason.code, e.g. INACTIVITY, JOB_FINISHED, USER_REQUEST."
            ),
            column(
                "termination_type",
                pa.string(),
                "termination_reason.type: SUCCESS, CLIENT_ERROR, SERVICE_FAULT, CLOUD_FAILURE.",
            ),
            column("custom_tags", STR_MAP, "User-defined tags."),
            column(
                "default_tags", STR_MAP, "Tags Databricks adds (Vendor, Creator, ClusterName, ClusterId, JobId, ...)."
            ),
            column("spark_conf", STR_MAP, "Spark configuration; secret-looking values masked '***'."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "clusters_scan"
        description = "Scan behind the clusters table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str]) -> list[Listing]:
        """cluster_id → clusters/get; state, cluster_source, policy_id → filter_by.* list filters."""
        cids = pd.values("cluster_id")
        if cids:
            return [Listing(f"{CLUSTERS_API}/get", None, (("cluster_id", str(c)),), missing_ok=True) for c in cids]
        q: list[tuple[str, Any]] = []
        q += [("filter_by.cluster_states", str(s)) for s in pd.values("state") or []]
        q += [("filter_by.cluster_sources", str(s)) for s in pd.values("cluster_source") or []]
        policy = pd.one("policy_id")
        if policy is not None:
            q.append(("filter_by.policy_id", str(policy)))
        return [Listing(f"{CLUSTERS_API}/list", "clusters", tuple(q), size_param="page_size", max_size=100)]

    @classmethod
    def rows(
        cls, client: DatabricksClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Flatten cluster details."""
        out = []
        for c in items:
            n, lo, hi = _cluster_size(c)
            reason = c.get("termination_reason") or {}
            conf = mask_tree(c.get("spark_conf") or {})
            out.append(
                {
                    "cluster_id": c["cluster_id"],
                    "cluster_name": c.get("cluster_name"),
                    "state": c.get("state"),
                    "state_message": c.get("state_message") or None,
                    "cluster_source": c.get("cluster_source"),
                    "creator_user_name": c.get("creator_user_name"),
                    "single_user_name": c.get("single_user_name"),
                    "data_security_mode": c.get("data_security_mode"),
                    "spark_version": c.get("spark_version"),
                    "runtime_engine": c.get("runtime_engine"),
                    "node_type_id": c.get("node_type_id"),
                    "driver_node_type_id": c.get("driver_node_type_id"),
                    "num_workers": n,
                    "autoscale_min_workers": lo,
                    "autoscale_max_workers": hi,
                    "autotermination_minutes": to_int(c.get("autotermination_minutes")),
                    "instance_pool_id": c.get("instance_pool_id"),
                    "policy_id": c.get("policy_id"),
                    "cluster_memory_mb": to_int(c.get("cluster_memory_mb")),
                    "cluster_cores": c.get("cluster_cores"),
                    "start_time": ts(c.get("start_time")),
                    "terminated_time": ts(c.get("terminated_time")),
                    "last_restarted_time": ts(c.get("last_restarted_time")),
                    "termination_code": reason.get("code"),
                    "termination_type": reason.get("type"),
                    "custom_tags": str_map(c.get("custom_tags") or {}),
                    "default_tags": str_map(c.get("default_tags") or {}),
                    "spark_conf": str_map(conf),
                }
            )
        return out


class ClusterEvents(ApiScan):
    """Rows for ``databricks.main.cluster_events``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("cluster_id", pa.string(), "Cluster the event belongs to.", nullable=False),
            column("timestamp", TSTZ, "When the event happened.", nullable=False),
            column(
                "type",
                pa.string(),
                "Event type: CREATING, STARTING, RUNNING, RESIZING, UPSIZE_COMPLETED, TERMINATING, RESTARTING, "
                "EDITED, DRIVER_HEALTHY, NODES_LOST, AUTOSCALING_STATS_REPORT, ...",
                nullable=False,
            ),
            column("user", pa.string(), "User who caused the event, when a user did."),
            column("current_num_workers", pa.int64(), "Workers in the cluster at the event."),
            column("target_num_workers", pa.int64(), "Target worker count at the event."),
            column("reason_code", pa.string(), "details.reason.code, e.g. INACTIVITY, USER_REQUEST."),
            column("reason_type", pa.string(), "details.reason.type, e.g. SUCCESS, CLIENT_ERROR."),
            column("cause", pa.string(), "details.cause, e.g. AUTOSCALE, USER_REQUEST, AUTORECOVERY."),
            column("details", pa.json_(), "The full event details (JSON)."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "cluster_events_scan"
        description = "Scan behind the cluster_events table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def fanout(cls, client: DatabricksClient, pd: Pushdown) -> list[str] | None:
        """Without a ``cluster_id`` filter, read events for every cluster the list returns."""
        if pd.values("cluster_id"):
            return None
        clusters = drain(client, Listing(f"{CLUSTERS_API}/list", "clusters", size_param="page_size", max_size=100))
        return sorted({str(c["cluster_id"]) for c in clusters})

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str]) -> list[Listing]:
        """cluster_id → body; timestamp range → start_time/end_time (ms); type = / IN → event_types."""
        cids = [str(c) for c in pd.values("cluster_id") or []] or ids
        lo, hi = pd.ms_range("timestamp")
        types = [str(t) for t in pd.values("type") or []]
        out = []
        for cid in cids:
            body: list[tuple[str, Any]] = [("cluster_id", cid), ("order", "DESC")]
            if lo is not None:
                body.append(("start_time", lo))
            if hi is not None:
                body.append(("end_time", hi))
            if types:
                body.append(("event_types", types))
            out.append(
                Listing(
                    f"{CLUSTERS_API}/events",
                    "events",
                    tuple(body),
                    size_param="page_size",
                    max_size=500,
                    method="POST",
                    missing_ok=True,
                )
            )
        return out

    @classmethod
    def rows(
        cls, client: DatabricksClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Flatten events."""
        out = []
        for e in items:
            d = e.get("details") or {}
            reason = d.get("reason") or {}
            out.append(
                {
                    "cluster_id": e["cluster_id"],
                    "timestamp": ts(e.get("timestamp")),
                    "type": e.get("type"),
                    "user": d.get("user"),
                    "current_num_workers": to_int(d.get("current_num_workers")),
                    "target_num_workers": to_int(d.get("target_num_workers")),
                    "reason_code": reason.get("code"),
                    "reason_type": reason.get("type"),
                    "cause": d.get("cause"),
                    "details": json_text(mask_tree(d)) if "details" in wanted else None,
                }
            )
        return out


class ClusterPolicies(ApiScan):
    """Rows for ``databricks.main.cluster_policies``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("policy_id", pa.string(), "Unique cluster policy identifier.", nullable=False),
            column("name", pa.string(), "Policy name."),
            column("description", pa.string(), "Policy description."),
            column("creator_user_name", pa.string(), "User who created the policy."),
            column("created_at", TSTZ, "When the policy was created."),
            column("is_default", pa.bool_(), "True for a default policy Databricks provides."),
            column(
                "max_clusters_per_user",
                pa.int64(),
                "Maximum number of clusters (count) one user may create with the policy.",
            ),
            column("policy_family_id", pa.string(), "Policy family it derives from, if any."),
            column("definition", pa.json_(), "Policy definition (JSON): attribute → rule."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "cluster_policies_scan"
        description = "Scan behind the cluster_policies table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str]) -> list[Listing]:
        """Unpaginated; nothing to push down (policy counts are small)."""
        return [Listing("/api/2.0/policies/clusters/list", "policies")]

    @classmethod
    def rows(
        cls, client: DatabricksClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Flatten policies; the definition arrives as a JSON string."""
        out = []
        for p in items:
            definition = p.get("definition")
            out.append(
                {
                    "policy_id": p["policy_id"],
                    "name": p.get("name"),
                    "description": p.get("description"),
                    "creator_user_name": p.get("creator_user_name"),
                    "created_at": ts(p.get("created_at_timestamp")),
                    "is_default": p.get("is_default"),
                    "max_clusters_per_user": to_int(p.get("max_clusters_per_user")),
                    "policy_family_id": p.get("policy_family_id"),
                    "definition": definition if isinstance(definition, str) else json_text(definition),
                }
            )
        return out


class InstancePools(ApiScan):
    """Rows for ``databricks.main.instance_pools``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("instance_pool_id", pa.string(), "Pool id.", nullable=False),
            column("instance_pool_name", pa.string(), "Pool name."),
            column("state", pa.string(), "ACTIVE, STOPPED or DELETED."),
            column("node_type_id", pa.string(), "Node type of the pool's instances."),
            column("min_idle_instances", pa.int64(), "Count of idle instances the pool keeps warm."),
            column("max_capacity", pa.int64(), "Maximum count of instances (idle + used)."),
            column(
                "idle_instance_autotermination_minutes",
                pa.int64(),
                "Idle time before extra instances are released, in minutes.",
            ),
            column("preloaded_spark_versions", STR_LIST, "Runtime versions preloaded on idle instances."),
            column("used_count", pa.int64(), "Count of instances in use by clusters."),
            column("idle_count", pa.int64(), "Count of idle instances ready for clusters."),
            column("pending_used_count", pa.int64(), "Count of instances being acquired for clusters."),
            column("pending_idle_count", pa.int64(), "Count of idle instances being acquired."),
            column("custom_tags", STR_MAP, "User-defined tags."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "instance_pools_scan"
        description = "Scan behind the instance_pools table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str]) -> list[Listing]:
        """Unpaginated; nothing to push down."""
        return [Listing("/api/2.0/instance-pools/list", "instance_pools")]

    @classmethod
    def rows(
        cls, client: DatabricksClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Flatten pools and their stats."""
        out = []
        for p in items:
            stats = p.get("stats") or {}
            out.append(
                {
                    "instance_pool_id": p["instance_pool_id"],
                    "instance_pool_name": p.get("instance_pool_name"),
                    "state": p.get("state"),
                    "node_type_id": p.get("node_type_id"),
                    "min_idle_instances": to_int(p.get("min_idle_instances")),
                    "max_capacity": to_int(p.get("max_capacity")),
                    "idle_instance_autotermination_minutes": to_int(p.get("idle_instance_autotermination_minutes")),
                    "preloaded_spark_versions": list(p.get("preloaded_spark_versions") or []),
                    "used_count": to_int(stats.get("used_count")),
                    "idle_count": to_int(stats.get("idle_count")),
                    "pending_used_count": to_int(stats.get("pending_used_count")),
                    "pending_idle_count": to_int(stats.get("pending_idle_count")),
                    "custom_tags": str_map(p.get("custom_tags") or {}),
                }
            )
        return out


# ---------------------------------------------------------------------------- SQL


class SqlWarehouses(ApiScan):
    """Rows for ``databricks.main.sql_warehouses``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column(
                "warehouse_id", pa.string(), "Unique SQL warehouse identifier (the httpPath suffix).", nullable=False
            ),
            column("name", pa.string(), "Warehouse name."),
            column("state", pa.string(), "STARTING, RUNNING, STOPPING, STOPPED, DELETING or DELETED."),
            column("cluster_size", pa.string(), "T-shirt size, e.g. '2X-Small', 'Medium'."),
            column("warehouse_type", pa.string(), "PRO or CLASSIC."),
            column("enable_serverless_compute", pa.bool_(), "True for a serverless warehouse."),
            column("enable_photon", pa.bool_(), "True when Photon is enabled."),
            column("min_num_clusters", pa.int64(), "Minimum clusters (scaling lower bound)."),
            column("max_num_clusters", pa.int64(), "Maximum clusters (scaling upper bound)."),
            column("num_clusters", pa.int64(), "Count of clusters currently running."),
            column("num_active_sessions", pa.int64(), "Count of open sessions."),
            column("auto_stop_mins", pa.int64(), "Idle time before the warehouse stops, in minutes; 0 = never."),
            column("spot_instance_policy", pa.string(), "COST_OPTIMIZED or RELIABILITY_OPTIMIZED."),
            column("channel", pa.string(), "Release channel: CHANNEL_NAME_CURRENT or CHANNEL_NAME_PREVIEW."),
            column("creator_name", pa.string(), "User who created the warehouse."),
            column("health_status", pa.string(), "health.status: HEALTHY, DEGRADED, FAILED."),
            column("health_summary", pa.string(), "health.summary text."),
            column("custom_tags", STR_MAP, "User-defined tags."),
            column("jdbc_url", pa.string(), "JDBC connection URL (contains no credentials)."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "sql_warehouses_scan"
        description = "Scan behind the sql_warehouses table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str]) -> list[Listing]:
        """warehouse_id → warehouses/{id}; otherwise the (unpaginated) list."""
        wids = pd.values("warehouse_id")
        if wids:
            return [Listing(f"/api/2.0/sql/warehouses/{quote(str(w))}", None, missing_ok=True) for w in wids]
        return [Listing("/api/2.0/sql/warehouses", "warehouses")]

    @classmethod
    def rows(
        cls, client: DatabricksClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Flatten warehouses."""
        out = []
        for w in items:
            health = w.get("health") or {}
            tags = {t.get("key"): t.get("value") for t in (w.get("tags") or {}).get("custom_tags") or []}
            out.append(
                {
                    "warehouse_id": w["id"],
                    "name": w.get("name"),
                    "state": w.get("state"),
                    "cluster_size": w.get("cluster_size"),
                    "warehouse_type": w.get("warehouse_type"),
                    "enable_serverless_compute": w.get("enable_serverless_compute"),
                    "enable_photon": w.get("enable_photon"),
                    "min_num_clusters": to_int(w.get("min_num_clusters")),
                    "max_num_clusters": to_int(w.get("max_num_clusters")),
                    "num_clusters": to_int(w.get("num_clusters")),
                    "num_active_sessions": to_int(w.get("num_active_sessions")),
                    "auto_stop_mins": to_int(w.get("auto_stop_mins")),
                    "spot_instance_policy": w.get("spot_instance_policy"),
                    "channel": (w.get("channel") or {}).get("name"),
                    "creator_name": w.get("creator_name"),
                    "health_status": health.get("status"),
                    "health_summary": health.get("summary"),
                    "custom_tags": str_map(tags),
                    "jdbc_url": w.get("jdbc_url"),
                }
            )
        return out


class QueryHistory(ApiScan):
    """Rows for ``databricks.main.query_history``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("query_id", pa.string(), "Query (statement) id.", nullable=False),
            column("status", pa.string(), "QUEUED, RUNNING, CANCELED, FAILED or FINISHED."),
            column("statement_type", pa.string(), "SELECT, INSERT, MERGE, CREATE, ... ."),
            column("query_text", pa.string(), "The SQL text."),
            column("warehouse_id", pa.string(), "Warehouse that ran it (joins sql_warehouses.warehouse_id)."),
            column("user_id", pa.int64(), "Unique numeric identifier of the workspace user who issued the query."),
            column("user_name", pa.string(), "Email / name of the user who issued it."),
            column("executed_as_user_name", pa.string(), "User the query ran as."),
            column("query_start_time", TSTZ, "When the query was received.", nullable=False),
            column("execution_end_time", TSTZ, "When execution finished."),
            column("query_end_time", TSTZ, "When the query ended (including result fetch)."),
            column("duration_seconds", pa.float64(), "Total time in seconds."),
            column("rows_produced", pa.int64(), "Count of rows returned or written."),
            column("error_message", pa.string(), "Error, when the query failed."),
            column("is_final", pa.bool_(), "True when the record will not change any more."),
            column("compilation_time_seconds", pa.float64(), "metrics.compilation_time_ms in seconds."),
            column("execution_time_seconds", pa.float64(), "metrics.execution_time_ms in seconds."),
            column("read_bytes", pa.int64(), "Data read, in bytes (metrics.read_bytes)."),
            column("read_rows", pa.int64(), "Count of rows read (metrics.rows_read_count)."),
            column("spill_to_disk_bytes", pa.int64(), "Data spilled to disk, in bytes."),
            column("result_from_cache", pa.bool_(), "metrics.result_from_cache."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "query_history_scan"
        description = "Scan behind the query_history table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str]) -> list[Listing]:
        """query_start_time range, warehouse_id, user_id, status and query_id → filter_by.* parameters."""
        q: list[tuple[str, Any]] = [("include_metrics", True)]
        lo, hi = pd.ms_range("query_start_time")
        if lo is not None:
            q.append(("filter_by.query_start_time_range.start_time_ms", lo))
        if hi is not None:
            # "started before end_time_ms": exclusive, so widen by 1 ms to keep an inclusive SQL bound whole.
            q.append(("filter_by.query_start_time_range.end_time_ms", hi + 1))
        q += [("filter_by.warehouse_ids", str(v)) for v in pd.values("warehouse_id") or []]
        q += [("filter_by.user_ids", int(v)) for v in int_values(pd, "user_id") or []]
        q += [("filter_by.statuses", str(v)) for v in pd.values("status") or []]
        q += [("filter_by.statement_ids", str(v)) for v in pd.values("query_id") or []]
        return [Listing("/api/2.0/sql/history/queries", "res", tuple(q), size_param="max_results", max_size=1000)]

    @classmethod
    def rows(
        cls, client: DatabricksClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Flatten query records and their metrics."""
        out = []
        for r in items:
            m = r.get("metrics") or {}
            out.append(
                {
                    "query_id": r["query_id"],
                    "status": r.get("status"),
                    "statement_type": r.get("statement_type"),
                    "query_text": r.get("query_text"),
                    "warehouse_id": r.get("warehouse_id") or r.get("endpoint_id"),
                    "user_id": to_int(r.get("user_id")),
                    "user_name": r.get("user_name"),
                    "executed_as_user_name": r.get("executed_as_user_name"),
                    "query_start_time": ts(r.get("query_start_time_ms")),
                    "execution_end_time": ts(r.get("execution_end_time_ms")),
                    "query_end_time": ts(r.get("query_end_time_ms")),
                    "duration_seconds": seconds(r.get("duration")),
                    "rows_produced": to_int(r.get("rows_produced")),
                    "error_message": r.get("error_message"),
                    "is_final": r.get("is_final"),
                    "compilation_time_seconds": seconds(m.get("compilation_time_ms")),
                    "execution_time_seconds": seconds(m.get("execution_time_ms")),
                    "read_bytes": to_int(m.get("read_bytes")),
                    "read_rows": to_int(m.get("rows_read_count")),
                    "spill_to_disk_bytes": to_int(m.get("spill_to_disk_bytes")),
                    "result_from_cache": m.get("result_from_cache"),
                }
            )
        return out


# ----------------------------------------------------------------------- pipelines


def _pipeline_ids(client: DatabricksClient) -> list[str]:
    items = drain(client, Listing("/api/2.0/pipelines", "statuses", size_param="max_results", max_size=100))
    return sorted({str(p["pipeline_id"]) for p in items})


class Pipelines(ApiScan):
    """Rows for ``databricks.main.pipelines``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("pipeline_id", pa.string(), "Unique pipeline identifier (a UUID).", nullable=False),
            column("name", pa.string(), "Pipeline name."),
            column(
                "state",
                pa.string(),
                "DEPLOYING, STARTING, RUNNING, STOPPING, DELETED, RECOVERING, FAILED, RESETTING, IDLE.",
            ),
            column("health", pa.string(), "HEALTHY or UNHEALTHY."),
            column("cluster_id", pa.string(), "Cluster currently running the pipeline, if any."),
            column("creator_user_name", pa.string(), "User who created the pipeline."),
            column("run_as_user_name", pa.string(), "User the pipeline runs as."),
            column("latest_update_id", pa.string(), "Most recent update id."),
            column("latest_update_state", pa.string(), "State of the most recent update."),
            column("latest_update_time", TSTZ, "When the most recent update was created."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "pipelines_scan"
        description = "Scan behind the pipelines table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str]) -> list[Listing]:
        """pipeline_id → pipelines/{id}; ``name =`` → ``filter=name LIKE '<name>'``."""
        pids = pd.values("pipeline_id")
        if pids:
            return [Listing(f"/api/2.0/pipelines/{quote(str(p))}", None, missing_ok=True) for p in pids]
        q: list[tuple[str, Any]] = []
        name = pd.one("name")
        if name is not None:
            q.append(("filter", f"name LIKE {_sql_str(str(name))}"))
        return [Listing("/api/2.0/pipelines", "statuses", tuple(q), size_param="max_results", max_size=100)]

    @classmethod
    def rows(
        cls, client: DatabricksClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Pipeline list entries and pipelines/get bodies share these fields."""
        out = []
        for p in items:
            latest = (p.get("latest_updates") or [{}])[0]
            out.append(
                {
                    "pipeline_id": p["pipeline_id"],
                    "name": p.get("name") or (p.get("spec") or {}).get("name"),
                    "state": p.get("state"),
                    "health": p.get("health"),
                    "cluster_id": p.get("cluster_id"),
                    "creator_user_name": p.get("creator_user_name"),
                    "run_as_user_name": p.get("run_as_user_name"),
                    "latest_update_id": latest.get("update_id"),
                    "latest_update_state": latest.get("state"),
                    "latest_update_time": ts(latest.get("creation_time")),
                }
            )
        return out


class PipelineUpdates(ApiScan):
    """Rows for ``databricks.main.pipeline_updates``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("pipeline_id", pa.string(), "Pipeline the update belongs to.", nullable=False),
            column("update_id", pa.string(), "Unique identifier of one pipeline update (run).", nullable=False),
            column(
                "state",
                pa.string(),
                "QUEUED, CREATED, WAITING_FOR_RESOURCES, INITIALIZING, RESETTING, SETTING_UP_TABLES, RUNNING, "
                "STOPPING, COMPLETED, FAILED or CANCELED.",
            ),
            column(
                "cause",
                pa.string(),
                "Why it started: API_CALL, JOB_TASK, RETRY_ON_FAILURE, SCHEMA_CHANGE, SERVICE_UPGRADE, USER_ACTION.",
            ),
            column("creation_time", TSTZ, "When the update was created."),
            column("full_refresh", pa.bool_(), "True for a full refresh of every table."),
            column("validate_only", pa.bool_(), "True for a validation-only update."),
            column("cluster_id", pa.string(), "Cluster that ran the update."),
            column("refresh_selection", STR_LIST, "Tables refreshed, for a partial refresh."),
            column("full_refresh_selection", STR_LIST, "Tables fully refreshed, for a partial full refresh."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "pipeline_updates_scan"
        description = "Scan behind the pipeline_updates table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def fanout(cls, client: DatabricksClient, pd: Pushdown) -> list[str] | None:
        """Without a ``pipeline_id`` filter, read updates of every pipeline."""
        return None if pd.values("pipeline_id") else _pipeline_ids(client)

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str]) -> list[Listing]:
        """pipeline_id → the URL path (one listing per pipeline)."""
        pids = [str(p) for p in pd.values("pipeline_id") or []] or ids
        return [
            Listing(
                f"/api/2.0/pipelines/{quote(p)}/updates",
                "updates",
                size_param="max_results",
                max_size=100,
                missing_ok=True,
                context=(("pipeline_id", p),),
            )
            for p in pids
        ]

    @classmethod
    def rows(
        cls, client: DatabricksClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Flatten updates."""
        pid = dict(listing.context)["pipeline_id"]
        return [
            {
                "pipeline_id": u.get("pipeline_id") or pid,
                "update_id": u["update_id"],
                "state": u.get("state"),
                "cause": u.get("cause"),
                "creation_time": ts(u.get("creation_time")),
                "full_refresh": u.get("full_refresh"),
                "validate_only": u.get("validate_only"),
                "cluster_id": u.get("cluster_id"),
                "refresh_selection": list(u.get("refresh_selection") or []),
                "full_refresh_selection": list(u.get("full_refresh_selection") or []),
            }
            for u in items
        ]


EXCEPTION_STRUCT = pa.struct([pa.field("class_name", pa.string()), pa.field("message", pa.string())])


class PipelineEvents(ApiScan):
    """Rows for ``databricks.main.pipeline_events``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("pipeline_id", pa.string(), "Pipeline the event belongs to.", nullable=False),
            column("event_id", pa.string(), "Unique identifier of the event-log entry.", nullable=False),
            column("update_id", pa.string(), "Update the event belongs to (joins pipeline_updates.update_id)."),
            column("timestamp", TSTZ, "When the event happened."),
            column("level", pa.string(), "INFO, WARN, ERROR or METRICS."),
            column("event_type", pa.string(), "e.g. create_update, update_progress, flow_progress, cluster_resources."),
            column("maturity_level", pa.string(), "STABLE, EVOLVING or DEPRECATED (schema stability of details)."),
            column("message", pa.string(), "Human-readable message."),
            column("flow_name", pa.string(), "Flow (table) the event concerns, if any."),
            column("dataset_name", pa.string(), "Dataset the event concerns, if any."),
            column("error_fatal", pa.bool_(), "error.fatal: true when the error stopped the update."),
            column("error_exceptions", pa.list_(EXCEPTION_STRUCT), "error.exceptions: class_name and message."),
            column("details", pa.json_(), "Event details (JSON; shape depends on event_type)."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "pipeline_events_scan"
        description = "Scan behind the pipeline_events table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def fanout(cls, client: DatabricksClient, pd: Pushdown) -> list[str] | None:
        """Without a ``pipeline_id`` filter, read events of every pipeline."""
        return None if pd.values("pipeline_id") else _pipeline_ids(client)

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str]) -> list[Listing]:
        """pipeline_id → URL path; level, event_id and timestamp range → the ``filter`` expression."""
        pids = [str(p) for p in pd.values("pipeline_id") or []] or ids
        conds: list[str] = []
        levels = [str(v) for v in pd.values("level") or []]
        if len(levels) == 1:
            conds.append(f"level={_sql_str(levels[0])}")
        elif levels:
            conds.append("level in (" + ", ".join(_sql_str(v) for v in levels) + ")")
        eid = pd.one("event_id")
        if eid is not None:
            conds.append(f"id={_sql_str(str(eid))}")
        b = pd.bounds("timestamp")
        if b is not None:
            lo, lo_inc, hi, hi_inc = b
            if iso(lo):
                conds.append(f"timestamp {'>=' if lo_inc else '>'} {_sql_str(iso(lo) or '')}")
            if iso(hi):
                conds.append(f"timestamp {'<=' if hi_inc else '<'} {_sql_str(iso(hi) or '')}")
        q: tuple[tuple[str, Any], ...] = (("filter", " AND ".join(conds)),) if conds else ()
        return [
            Listing(
                f"/api/2.0/pipelines/{quote(p)}/events",
                "events",
                q,
                size_param="max_results",
                max_size=100,
                missing_ok=True,
                context=(("pipeline_id", p),),
            )
            for p in pids
        ]

    @classmethod
    def fetch(cls, client: DatabricksClient, listing: Listing, token: str) -> tuple[list[dict[str, Any]], str]:
        """Read one page; with a page token send only max_results (the API rejects other fields then)."""
        if token:
            listing = Listing(
                listing.path,
                listing.key,
                (),
                size_param=listing.size_param,
                max_size=listing.max_size,
                missing_ok=listing.missing_ok,
                context=listing.context,
            )
        return super().fetch(client, listing, token)

    @classmethod
    def rows(
        cls, client: DatabricksClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Flatten events."""
        pid = dict(listing.context)["pipeline_id"]
        out = []
        for e in items:
            origin = e.get("origin") or {}
            err = e.get("error") or {}
            out.append(
                {
                    "pipeline_id": origin.get("pipeline_id") or pid,
                    "event_id": e["id"],
                    "update_id": origin.get("update_id"),
                    "timestamp": ts(e.get("timestamp")),
                    "level": e.get("level"),
                    "event_type": e.get("event_type"),
                    "maturity_level": e.get("maturity_level"),
                    "message": e.get("message"),
                    "flow_name": origin.get("flow_name"),
                    "dataset_name": origin.get("dataset_name"),
                    "error_fatal": err.get("fatal") if err else None,
                    "error_exceptions": [
                        {"class_name": x.get("class_name"), "message": x.get("message")}
                        for x in err.get("exceptions") or []
                    ]
                    if err
                    else None,
                    "details": json_text(mask_tree(e.get("details"))) if "details" in wanted else None,
                }
            )
        return out


# ----------------------------------------------------------------------- workspace


class Repos(ApiScan):
    """Rows for ``databricks.main.repos``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("repo_id", pa.int64(), "Repo (Git folder) id.", nullable=False),
            column("path", pa.string(), "Workspace path, e.g. '/Repos/alice@example.com/etl'."),
            column("url", pa.string(), "Remote Git URL."),
            column("provider", pa.string(), "gitHub, gitLab, azureDevOpsServices, bitbucketCloud, ..."),
            column("branch", pa.string(), "Checked-out branch."),
            column("head_commit_id", pa.string(), "SHA of the checked-out commit."),
            column("sparse_checkout_patterns", STR_LIST, "Sparse checkout cone patterns, if any."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "repos_scan"
        description = "Scan behind the repos table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str]) -> list[Listing]:
        """repo_id → repos/{id}; ``path =`` → ``path_prefix``. Pages with ``next_page_token``."""
        rids = int_values(pd, "repo_id")
        if rids:
            return [Listing(f"/api/2.0/repos/{r}", None, missing_ok=True) for r in rids]
        q: list[tuple[str, Any]] = []
        path = pd.one("path")
        if path is not None:
            q.append(("path_prefix", str(path)))
        return [Listing("/api/2.0/repos", "repos", tuple(q), token_param="next_page_token")]

    @classmethod
    def rows(
        cls, client: DatabricksClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Flatten repos."""
        return [
            {
                "repo_id": r["id"],
                "path": r.get("path"),
                "url": r.get("url"),
                "provider": r.get("provider"),
                "branch": r.get("branch"),
                "head_commit_id": r.get("head_commit_id"),
                "sparse_checkout_patterns": list((r.get("sparse_checkout") or {}).get("patterns") or []),
            }
            for r in items
        ]


class SecretScopes(ApiScan):
    """Rows for ``databricks.main.secret_scopes``: names and backends only, never secret values."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("scope", pa.string(), "Secret scope name.", nullable=False),
            column("backend_type", pa.string(), "DATABRICKS or AZURE_KEYVAULT."),
            column("keyvault_resource_id", pa.string(), "Azure Key Vault resource id, for Key Vault-backed scopes."),
            column("keyvault_dns_name", pa.string(), "Azure Key Vault DNS name, for Key Vault-backed scopes."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "secret_scopes_scan"
        description = "Scan behind the secret_scopes table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str]) -> list[Listing]:
        """Unpaginated; nothing to push down."""
        return [Listing("/api/2.0/secrets/scopes/list", "scopes")]

    @classmethod
    def rows(
        cls, client: DatabricksClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Scope metadata only."""
        out = []
        for s in items:
            kv = s.get("keyvault_metadata") or {}
            out.append(
                {
                    "scope": s["name"],
                    "backend_type": s.get("backend_type"),
                    "keyvault_resource_id": kv.get("resource_id"),
                    "keyvault_dns_name": kv.get("dns_name"),
                }
            )
        return out
