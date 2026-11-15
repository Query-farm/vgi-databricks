"""Scalar ``unified_status(state)``: Databricks run / task state → cross-orchestrator status.

The same mapping the ``unified`` tables use, exposed so you can normalize states
you already have (``job_runs.result_state``, a system-table export, a webhook
feed) without calling the API.
"""

from __future__ import annotations

import json
from typing import Annotated, ClassVar

import pyarrow as pa
import pyarrow.compute as pc
from vgi.arguments import Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction

from .meta import object_tags
from .unified import unified_status

_STATE_DOC = (
    "A Databricks Jobs result_state (SUCCESS, FAILED, CANCELED, UPSTREAM_FAILED, ...) or, for a run that has "
    "not terminated, its life_cycle_state (QUEUED, PENDING, RUNNING, ...). Case-insensitive; NULL maps to queued."
)


class UnifiedStatus(ScalarFunction):
    """``unified_status(state)`` for every row."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "unified_status"
        description = "Map a Databricks Jobs run or task state onto queued/running/success/failed/cancelled/skipped"
        tags: ClassVar[dict[str, str]] = {
            **object_tags(
                title="Unified Status From Databricks State",
                category="runs",
                keywords="status mapping, normalize state, result_state, life_cycle_state, cross orchestrator",
                doc_llm=(
                    "unified_status(state) maps a Databricks Jobs result_state or life_cycle_state to the shared "
                    "orchestrator vocabulary used by the unified schema: SUCCESS and SUCCESS_WITH_FAILURES→success; "
                    "FAILED, TIMEDOUT, MAXIMUM_CONCURRENT_RUNS_REACHED, INTERNAL_ERROR→failed; CANCELED→cancelled; "
                    "UPSTREAM_FAILED, UPSTREAM_CANCELED, EXCLUDED, DISABLED, SKIPPED→skipped; QUEUED, PENDING, "
                    "BLOCKED, WAITING_FOR_RETRY→queued; RUNNING, TERMINATING→running. Case-insensitive; unknown and "
                    "NULL map to queued. Pass coalesce(result_state, life_cycle_state). No credentials or API calls."
                ),
                doc_md=(
                    "## unified_status(state)\n\n"
                    "Normalize a Databricks Jobs state to `queued`, `running`, `success`, `failed`, `cancelled` or "
                    "`skipped`, exactly as `unified.runs` and `unified.task_runs` do. Pass the `result_state` of a "
                    "terminated run, else its `life_cycle_state`.\n\n"
                    "`UPSTREAM_FAILED` becomes `skipped`: the failure is counted once, on the upstream task."
                ),
            ),
            "domain": "orchestration",
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "Normalize Databricks result and life-cycle states",
                        "sql": (
                            "SELECT s AS state, databricks.unified.unified_status(s) AS status FROM (VALUES "
                            "('SUCCESS'), ('FAILED'), ('UPSTREAM_FAILED'), ('CANCELED'), ('RUNNING'), "
                            "('WAITING_FOR_RETRY')) t(s) ORDER BY s"
                        ),
                        "expected_result": [
                            {"state": "CANCELED", "status": "cancelled"},
                            {"state": "FAILED", "status": "failed"},
                            {"state": "RUNNING", "status": "running"},
                            {"state": "SUCCESS", "status": "success"},
                            {"state": "UPSTREAM_FAILED", "status": "skipped"},
                            {"state": "WAITING_FOR_RETRY", "status": "queued"},
                        ],
                    },
                    {
                        "description": "A run with no state yet (NULL) counts as queued",
                        "sql": "SELECT databricks.unified.unified_status(NULL::VARCHAR) AS status",
                        "expected_result": [{"status": "queued"}],
                    },
                ]
            ),
        }
        examples: ClassVar[list[FunctionExample]] = [
            FunctionExample(
                sql=(
                    "SELECT databricks.unified.unified_status(coalesce(result_state, life_cycle_state)) AS status, "
                    "count(*) AS tasks FROM databricks.main.job_run_tasks GROUP BY ALL ORDER BY status"
                ),
                description="Task attempts per unified status",
            ),
        ]

    @classmethod
    def compute(
        cls,
        state: Annotated[pa.StringArray, Param(doc=_STATE_DOC)],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Map each state; NULL maps to queued."""
        # Map the handful of distinct states once, then gather: no per-row Python.
        distinct = pc.unique(state)
        mapped = pa.array([unified_status(s) for s in distinct.to_pylist()], type=pa.string())
        return pc.take(mapped, pc.index_in(state, value_set=distinct, skip_nulls=False))
