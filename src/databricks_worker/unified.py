"""The ``unified`` schema: runs and task runs in the cross-orchestrator shape.

The same two tables (same columns, types and status vocabulary) are served by
the Airflow, Dagster, Prefect, Temporal, Databricks, dbt and GitHub Actions
workers, so ``UNION ALL`` across them just works.

Databricks specifics:

* ``run_id`` is the Jobs run id as text (unique in the workspace);
  ``pipeline`` is the run name (the job's name when the run started).
* ``raw_status`` is the run's ``result_state`` once it terminated, else its
  ``life_cycle_state``; ``unified_status(raw_status)`` reproduces ``status``.
* Databricks records one creation time per run (``start_time``, which includes
  queueing and cluster setup), so ``queued_at`` and ``started_at`` are both that
  time and ``duration_seconds`` is ``end_time - start_time``.
* ``task_runs`` has one row per task *attempt* (a retried task appears once per
  try, each with its own ``task_run_id``); ``attempt`` is 1-based
  (Databricks ``attempt_number + 1``).
"""

from __future__ import annotations

from typing import Any, ClassVar

import pyarrow as pa

from .client import DatabricksClient
from .meta import column
from .scan import TSTZ, ApiScan, Listing, Pushdown, Remapped, between, to_int, ts
from .tables import ACTIVE_LIFE_CYCLE, DONE_LIFE_CYCLE, all_run_tasks, run_listings

ORCHESTRATOR = "databricks"
STATUSES = ("queued", "running", "success", "failed", "cancelled", "skipped")

# result_state (terminated) and life_cycle_state (otherwise) share one map: the vocabularies do not overlap.
STATUS_MAP = {
    # result_state
    "SUCCESS": "success",
    # Leaf tasks succeeded; some non-leaf task failed under an "all done" dependency.
    "SUCCESS_WITH_FAILURES": "success",
    "FAILED": "failed",
    "TIMEDOUT": "failed",
    "MAXIMUM_CONCURRENT_RUNS_REACHED": "failed",
    "CANCELED": "cancelled",
    "CANCELLED": "cancelled",
    # Never ran because something upstream failed / was cancelled: counted once, upstream.
    "UPSTREAM_FAILED": "skipped",
    "UPSTREAM_CANCELED": "skipped",
    "EXCLUDED": "skipped",
    "DISABLED": "skipped",
    # life_cycle_state
    "QUEUED": "queued",
    "PENDING": "queued",
    "BLOCKED": "queued",
    "WAITING_FOR_RETRY": "queued",
    "RUNNING": "running",
    "TERMINATING": "running",
    "SKIPPED": "skipped",
    "INTERNAL_ERROR": "failed",
    "TERMINATED": "failed",
}


def raw_status(item: dict[str, Any]) -> str | None:
    """``result_state`` when terminated, else ``life_cycle_state``."""
    state = item.get("state") or {}
    return state.get("result_state") or state.get("life_cycle_state")


def unified_status(raw: str | None) -> str:
    """Map a Databricks result / life-cycle state onto the unified vocabulary (unknown → queued)."""
    return STATUS_MAP.get((raw or "").upper(), "queued")


def trigger(run: dict[str, Any]) -> str | None:
    """Map the Jobs ``trigger`` / ``run_type`` onto schedule, manual, sensor, api, event or other."""
    if run.get("run_type") == "SUBMIT_RUN":
        return "api"
    t = run.get("trigger")
    if t is None:
        return None
    return {
        "PERIODIC": "schedule",
        "CONTINUOUS": "schedule",
        "CONTINUOUS_RESTART": "schedule",
        "ONE_TIME": "manual",
        "FILE_ARRIVAL": "sensor",
        "TABLE": "event",
        "MODEL": "event",
    }.get(str(t), "other")


def _run_pushdown(pd: Pushdown, *, run_col: str = "run_id") -> dict[str, list[Any]]:
    values: dict[str, list[Any]] = {}
    rids = pd.values(run_col)
    if rids:
        values["run_id"] = list(rids)
    return values


RUN_FIELDS = [
    column("orchestrator", pa.string(), "Always 'databricks'.", nullable=False),
    column("run_id", pa.string(), "Jobs run id as text (unique in the workspace).", nullable=False),
    column("pipeline", pa.string(), "Run name: the job's name when the run started (or the submit-run name)."),
    column("status", pa.string(), "queued, running, success, failed, cancelled or skipped.", nullable=False),
    column("raw_status", pa.string(), "Databricks result_state once terminated, else life_cycle_state."),
    column(
        "trigger",
        pa.string(),
        "schedule (cron / continuous), manual (run now), sensor (file arrival), api (runs/submit), "
        "event (table update), other (retry, run-job task).",
    ),
    column("queued_at", TSTZ, "When the run was created (Databricks start_time)."),
    column("started_at", TSTZ, "Databricks start_time (includes queueing and cluster setup)."),
    column("ended_at", TSTZ, "When the run ended; NULL while active."),
    column("duration_seconds", pa.float64(), "ended_at - started_at in seconds."),
    column("url", pa.string(), "Link to the run in the workspace UI."),
]

TASK_RUN_FIELDS = [
    column("orchestrator", pa.string(), "Always 'databricks'.", nullable=False),
    column("run_id", pa.string(), "Parent job run id as text; joins unified.runs.run_id.", nullable=False),
    column("task_run_id", pa.string(), "This task attempt's own run id as text.", nullable=False),
    column("task_name", pa.string(), "Task key."),
    column("status", pa.string(), "queued, running, success, failed, cancelled or skipped.", nullable=False),
    column("raw_status", pa.string(), "Databricks result_state once terminated, else life_cycle_state."),
    column("attempt", pa.int32(), "1-based attempt (Databricks attempt_number + 1); retries add rows."),
    column("started_at", TSTZ, "When the attempt started."),
    column("ended_at", TSTZ, "When the attempt ended."),
    column("duration_seconds", pa.float64(), "ended_at - started_at in seconds."),
    column("url", pa.string(), "Link to the task run in the workspace UI."),
]


def _inverse_life_cycle(statuses: list[Any]) -> list[str] | None:
    """Life-cycle states that can produce the wanted unified statuses (for active/completed pushdown)."""
    wanted = {str(s) for s in statuses}
    if wanted <= {"queued", "running"}:
        return sorted(ACTIVE_LIFE_CYCLE)
    if wanted <= {"success", "failed", "cancelled", "skipped"}:
        return sorted(DONE_LIFE_CYCLE)
    return None


class UnifiedRuns(ApiScan):
    """Rows for ``databricks.unified.runs``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(RUN_FIELDS)

    class Meta:
        """Table metadata."""

        name = "unified_runs_scan"
        description = "Scan behind the unified.runs table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str]) -> list[Listing]:
        """run_id → runs/get; status → active_only / completed_only; started_at range → start_time_from/to."""
        values = _run_pushdown(pd)
        statuses = pd.values("status")
        if statuses:
            if not {str(s) for s in statuses} & set(STATUSES):
                return []
            lcs = _inverse_life_cycle(statuses)
            if lcs:
                values["life_cycle_state"] = lcs
        return run_listings(Remapped(values, {"start_time": pd.bounds("started_at")}), expand_tasks=False)

    @classmethod
    def rows(
        cls, client: DatabricksClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map runs to unified rows."""
        out = []
        for r in items:
            raw = raw_status(r)
            start, end = ts(r.get("start_time")), ts(r.get("end_time"))
            out.append(
                {
                    "orchestrator": ORCHESTRATOR,
                    "run_id": str(r["run_id"]),
                    "pipeline": r.get("run_name"),
                    "status": unified_status(raw),
                    "raw_status": raw,
                    "trigger": trigger(r),
                    "queued_at": start,
                    "started_at": start,
                    "ended_at": end,
                    "duration_seconds": between(start, end),
                    "url": r.get("run_page_url"),
                }
            )
        return out


class UnifiedTaskRuns(ApiScan):
    """Rows for ``databricks.unified.task_runs``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(TASK_RUN_FIELDS)

    class Meta:
        """Table metadata."""

        name = "unified_task_runs_scan"
        description = "Scan behind the unified.task_runs table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, pd: Pushdown, ids: list[str]) -> list[Listing]:
        """run_id → runs/get; an upper bound on started_at bounds the parent's start_time_to."""
        values = _run_pushdown(pd)
        b = pd.bounds("started_at")
        # A task starts no earlier than its run, so only the upper bound carries over to the parent.
        upper = (None, True, b[2], True) if b is not None and b[2] is not None else None
        return run_listings(Remapped(values, {"start_time": upper}), expand_tasks=True)

    @classmethod
    def rows(
        cls, client: DatabricksClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """One unified row per task attempt."""
        out = []
        for run in items:
            for t in all_run_tasks(client, run):
                raw = raw_status(t)
                start, end = ts(t.get("start_time")), ts(t.get("end_time"))
                attempt = to_int(t.get("attempt_number"))
                out.append(
                    {
                        "orchestrator": ORCHESTRATOR,
                        "run_id": str(run["run_id"]),
                        "task_run_id": str(t["run_id"]),
                        "task_name": t.get("task_key"),
                        "status": unified_status(raw),
                        "raw_status": raw,
                        "attempt": (attempt or 0) + 1,
                        "started_at": start,
                        "ended_at": end,
                        "duration_seconds": between(start, end),
                        "url": t.get("run_page_url"),
                    }
                )
        return out
