"""Assemble the ``databricks`` catalog and run the worker.

Run it:

    vgi-databricks-worker                      # stdio: what DuckDB launches by default
    vgi-databricks-worker --http --port 8000   # HTTP: for Docker / hosted deploys

Attach it from DuckDB, then give it credentials:

    ATTACH 'databricks' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-databricks-worker');
    CREATE SECRET (TYPE databricks, HOST 'https://<workspace>.cloud.databricks.com', TOKEN 'dapi...');
"""

from __future__ import annotations

import json
from typing import Any, cast

import pyarrow as pa
from vgi import Worker
from vgi.catalog import Catalog, Schema, Table
from vgi.catalog.constraints import ReferencedKey, constraint_record
from vgi.metadata import FunctionExample

from .client import DATABRICKS_SECRET
from .functions import RunOutput, WorkspaceObjects
from .meta import (
    CATALOG_DESCRIPTION,
    CATALOG_NAME,
    CATALOG_TAGS,
    MAIN_CATEGORIES,
    MAIN_DOC_LLM,
    MAIN_DOC_MD,
    SOURCE_URL,
    TABLE_DOCS,
    UNIFIED_CATEGORIES,
    UNIFIED_DOC_LLM,
    UNIFIED_DOC_MD,
    keywords_array,
    object_tags,
)
from .status import UnifiedStatus
from .tables import (
    ClusterEvents,
    ClusterPolicies,
    Clusters,
    InstancePools,
    JobRuns,
    JobRunTasks,
    Jobs,
    PipelineEvents,
    Pipelines,
    PipelineUpdates,
    QueryHistory,
    Repos,
    SecretScopes,
    SqlWarehouses,
)
from .unified import UnifiedRuns, UnifiedTaskRuns

Q = f"{CATALOG_NAME}.main"
U = f"{CATALOG_NAME}.unified"

EXAMPLES: dict[str, list[tuple[str, str]]] = {
    "jobs": [
        (
            "Scheduled jobs and their cron expressions",
            f"SELECT name, schedule_cron, schedule_timezone FROM {Q}.jobs "
            "WHERE trigger_type = 'periodic' AND NOT schedule_paused ORDER BY name",
        ),
        (
            "Jobs per owning team tag",
            f"SELECT tags['team'] AS team, count(*) AS jobs FROM {Q}.jobs GROUP BY team ORDER BY jobs DESC",
        ),
    ],
    "job_runs": [
        (
            "Failed runs of one job in November 2026 (job_id and the start_time range are sent to the API)",
            f"SELECT run_id, start_time, state_message FROM {Q}.job_runs WHERE job_id = 101 "
            "AND result_state = 'FAILED' AND start_time >= TIMESTAMPTZ '2026-11-01' ORDER BY start_time",
        ),
        (
            "Run outcomes and median duration per job",
            f"SELECT run_name, result_state, count(*) AS runs, median(run_duration_seconds) AS median_s "
            f"FROM {Q}.job_runs GROUP BY ALL ORDER BY run_name, result_state",
        ),
    ],
    "job_run_tasks": [
        (
            "Tasks that needed retries",
            f"SELECT run_id, task_key, count(*) AS attempts FROM {Q}.job_run_tasks "
            "GROUP BY ALL HAVING count(*) > 1 ORDER BY attempts DESC",
        ),
        (
            "Slowest task per job",
            f"SELECT job_id, task_key, max(execution_duration_seconds) AS worst_s FROM {Q}.job_run_tasks "
            "WHERE result_state = 'SUCCESS' GROUP BY ALL ORDER BY worst_s DESC LIMIT 10",
        ),
    ],
    "clusters": [
        (
            "Running clusters and their owners (state is sent to the API)",
            f"SELECT cluster_name, creator_user_name, spark_version, num_workers FROM {Q}.clusters "
            "WHERE state = 'RUNNING' ORDER BY cluster_name",
        ),
        (
            "Why clusters last terminated",
            f"SELECT termination_code, count(*) AS clusters FROM {Q}.clusters WHERE state = 'TERMINATED' "
            "GROUP BY ALL ORDER BY clusters DESC",
        ),
    ],
    "cluster_events": [
        (
            "Resize and termination events of one cluster",
            f"SELECT timestamp, type, current_num_workers, target_num_workers, reason_code FROM {Q}.cluster_events "
            "WHERE cluster_id = '0915-120000-shared01' AND type IN ('RESIZING', 'TERMINATING') ORDER BY timestamp",
        )
    ],
    "cluster_policies": [
        (
            "Policies and how many clusters use each",
            f"SELECT p.name, count(c.cluster_id) AS clusters FROM {Q}.cluster_policies p "
            f"LEFT JOIN {Q}.clusters c USING (policy_id) GROUP BY p.name ORDER BY p.name",
        )
    ],
    "instance_pools": [
        (
            "Pool utilisation",
            f"SELECT instance_pool_name, used_count, idle_count, max_capacity FROM {Q}.instance_pools "
            "ORDER BY instance_pool_name",
        )
    ],
    "sql_warehouses": [
        (
            "Warehouses by size and type",
            f"SELECT name, cluster_size, warehouse_type, enable_serverless_compute, state FROM {Q}.sql_warehouses "
            "ORDER BY name",
        )
    ],
    "query_history": [
        (
            "Failed queries per warehouse in one day (time range and status are sent to the API)",
            f"SELECT w.name, count(*) AS failed FROM {Q}.query_history q JOIN {Q}.sql_warehouses w USING "
            "(warehouse_id) WHERE q.status = 'FAILED' AND q.query_start_time >= TIMESTAMPTZ '2026-11-14' "
            "AND q.query_start_time < TIMESTAMPTZ '2026-11-15' GROUP BY w.name ORDER BY failed DESC",
        ),
        (
            "Slowest statements per user",
            f"SELECT user_name, max(duration_seconds) AS slowest_s, sum(read_bytes) AS bytes_read "
            f"FROM {Q}.query_history WHERE status = 'FINISHED' GROUP BY user_name ORDER BY slowest_s DESC",
        ),
    ],
    "pipelines": [
        (
            "Unhealthy pipelines",
            f"SELECT name, state, latest_update_state FROM {Q}.pipelines WHERE health = 'UNHEALTHY' ORDER BY name",
        )
    ],
    "pipeline_updates": [
        (
            "Update outcomes per pipeline",
            f"SELECT p.name, u.state, count(*) AS updates FROM {Q}.pipeline_updates u JOIN {Q}.pipelines p "
            "USING (pipeline_id) GROUP BY ALL ORDER BY p.name, u.state",
        )
    ],
    "pipeline_events": [
        (
            "Error events of one pipeline (pipeline_id and level are sent to the API)",
            f"SELECT timestamp, flow_name, message FROM {Q}.pipeline_events "
            "WHERE pipeline_id = 'pl-orders-0001' AND level = 'ERROR' ORDER BY timestamp",
        )
    ],
    "repos": [
        (
            "Git folders not on main",
            f"SELECT path, url, branch FROM {Q}.repos WHERE branch <> 'main' ORDER BY path",
        )
    ],
    "secret_scopes": [
        (
            "Secret scopes per backend",
            f"SELECT backend_type, list(scope ORDER BY scope) AS scopes FROM {Q}.secret_scopes GROUP BY ALL "
            "ORDER BY backend_type",
        )
    ],
    "runs": [
        ("Runs per status", f"SELECT status, count(*) AS runs FROM {U}.runs GROUP BY status ORDER BY status"),
        (
            "Failure rate per pipeline since November",
            f"SELECT pipeline, avg((status = 'failed')::INT) AS failure_rate FROM {U}.runs "
            "WHERE started_at >= TIMESTAMPTZ '2026-11-01' GROUP BY pipeline ORDER BY failure_rate DESC",
        ),
    ],
    "task_runs": [
        (
            "Task outcomes joined to their runs",
            f"SELECT r.pipeline, t.status, count(*) AS tasks FROM {U}.task_runs t JOIN {U}.runs r USING (run_id) "
            "GROUP BY ALL ORDER BY r.pipeline, t.status",
        )
    ],
}


def duckdb_type(t: pa.DataType) -> str:
    """Spell an Arrow type the way DuckDB names it (for ``vgi.result_columns_schema``)."""
    if getattr(t, "extension_name", None) == "arrow.json":
        return "JSON"
    if pa.types.is_timestamp(t):
        return "TIMESTAMPTZ" if t.tz else "TIMESTAMP"
    if pa.types.is_map(t):
        return f"MAP({duckdb_type(t.key_type)}, {duckdb_type(t.item_type)})"
    if pa.types.is_list(t):
        return f"{duckdb_type(t.value_type)}[]"
    if pa.types.is_struct(t):
        return "STRUCT(" + ", ".join(f"{f.name} {duckdb_type(f.type)}" for f in t) + ")"
    simple = {pa.string(): "VARCHAR", pa.int64(): "BIGINT", pa.int32(): "INTEGER", pa.bool_(): "BOOLEAN"}
    return simple.get(t, "DOUBLE" if pa.types.is_floating(t) else str(t).upper())


def _result_columns(schema: pa.Schema) -> str:
    return json.dumps(
        [
            {"name": f.name, "type": duckdb_type(f.type), "description": (f.metadata or {})[b"comment"].decode()}
            for f in schema
        ]
    )


def _examples_json(name: str) -> str:
    return json.dumps([{"description": d, "sql": s} for d, s in EXAMPLES[name]])


def _table(
    name: str,
    scan: type[Any],
    schema: list[str],
    key: list[str],
    fks: tuple[tuple[list[str], str, list[str]], ...] = (),
) -> Table:
    """Build a catalog table over ``scan`` with NOT NULL, primary key and foreign key records.

    Each foreign key is ``(columns, referenced table, referenced primary key)`` in the
    same schema. Only references every row satisfies by construction are declared
    (child rows are read from their parent's listing).
    """
    doc = TABLE_DOCS[name]
    not_null = [f.name for f in scan.FIXED_SCHEMA if not f.nullable]
    constraints = [
        constraint_record(
            schema, name, f"{name}_{c}_not_null", "NOT_NULL", [[c]], enforcement="ENFORCED", validation="ALL_ROWS"
        )
        for c in not_null
    ]
    if key:
        constraints.append(
            constraint_record(
                schema,
                name,
                f"{name}_pk",
                "PRIMARY_KEY",
                [[c] for c in key],
                enforcement="ENFORCED",
                validation="ALL_ROWS",
            )
        )
    for cols, ref_table, ref_cols in fks:
        constraints.append(
            constraint_record(
                schema,
                name,
                f"{name}_{ref_table}_fk",
                "FOREIGN_KEY",
                [[c] for c in cols],
                enforcement="ENFORCED",
                validation="ALL_ROWS",
                references=ReferencedKey(
                    schema_path=schema,
                    table=ref_table,
                    columns=[[c] for c in ref_cols],
                    constraint_id=f"{ref_table}_pk",
                ),
            )
        )
    return Table(
        name=name,
        function=scan,
        comment=doc["comment"],
        constraints=tuple(constraints),
        column_comments={f.name: (f.metadata or {})[b"comment"].decode() for f in scan.FIXED_SCHEMA},
        tags={
            **object_tags(
                title=doc["title"],
                category=doc["category"],
                keywords=doc["keywords"],
                doc_llm=doc["doc_llm"],
                doc_md=doc["doc_md"],
            ),
            "domain": "orchestration",
            "vgi.example_queries": _examples_json(name),
        },
    )


MAIN_TABLES = [
    _table("jobs", Jobs, ["main"], ["job_id"]),
    _table("job_runs", JobRuns, ["main"], ["run_id"]),
    _table("job_run_tasks", JobRunTasks, ["main"], ["task_run_id"], ((["run_id"], "job_runs", ["run_id"]),)),
    _table("clusters", Clusters, ["main"], ["cluster_id"]),
    _table("cluster_events", ClusterEvents, ["main"], []),
    _table("cluster_policies", ClusterPolicies, ["main"], ["policy_id"]),
    _table("instance_pools", InstancePools, ["main"], ["instance_pool_id"]),
    _table("sql_warehouses", SqlWarehouses, ["main"], ["warehouse_id"]),
    _table("query_history", QueryHistory, ["main"], ["query_id"]),
    _table("pipelines", Pipelines, ["main"], ["pipeline_id"]),
    _table(
        "pipeline_updates", PipelineUpdates, ["main"], ["update_id"], ((["pipeline_id"], "pipelines", ["pipeline_id"]),)
    ),
    _table(
        "pipeline_events", PipelineEvents, ["main"], ["event_id"], ((["pipeline_id"], "pipelines", ["pipeline_id"]),)
    ),
    _table("repos", Repos, ["main"], ["repo_id"]),
    _table("secret_scopes", SecretScopes, ["main"], ["scope"]),
]
UNIFIED_TABLES = [
    _table("runs", UnifiedRuns, ["unified"], ["run_id"]),
    _table("task_runs", UnifiedTaskRuns, ["unified"], ["task_run_id"], ((["run_id"], "runs", ["run_id"]),)),
]

RunOutput.Meta.tags = {
    **object_tags(
        title="Job Run Output",
        category="jobs",
        keywords="run output, error message, stack trace, logs, notebook exit value, debugging",
        doc_llm=(
            "run_output(run_id, include_result := false, max_log_chars := 65536) returns the output of a job run: "
            "one row per task run (every attempt) when run_id is a job run, or one row for a task run id "
            "(job_run_tasks.task_run_id). Columns: run_id, task_run_id, task_key, attempt_number, "
            "life_cycle_state, result_state, error, error_trace and logs (cut to max_log_chars), logs_truncated, "
            "notebook_result (the dbutils.notebook.exit value, only with include_result := true), "
            "notebook_result_length, notebook_result_truncated. Use it to find why a run failed."
        ),
        doc_md=(
            "## run_output(run_id, include_result := false, max_log_chars := 65536)\n\n"
            "Why did the run fail? Error message, stack trace and stdout of every task of a job run. The notebook "
            "exit value is returned only when you ask for it with `include_result := true`."
        ),
    ),
    "domain": "orchestration",
    "vgi.result_columns_schema": _result_columns(RunOutput.FIXED_SCHEMA),
}
RunOutput.Meta.examples = [
    FunctionExample(
        sql=(
            f"SELECT task_key, attempt_number, error FROM {Q}.run_output(2002) "
            "WHERE error IS NOT NULL ORDER BY task_key"
        ),
        description="Error messages of the failed tasks of one job run",
    ),
    FunctionExample(
        sql=(
            f"SELECT task_key, notebook_result FROM {Q}.run_output(2101, include_result := true) "
            "WHERE result_state = 'SUCCESS' ORDER BY task_key"
        ),
        description="Notebook exit values of the successful tasks of a run",
    ),
]

WorkspaceObjects.Meta.tags = {
    **object_tags(
        title="Workspace Objects",
        category="workspace",
        keywords="workspace, notebooks, files, folders, directory listing, workspace browser",
        doc_llm=(
            "workspace_objects(path, recursive := false) lists the notebooks, files, directories, Git folders "
            "(REPO) and dashboards under an absolute workspace path such as '/Users/alice@example.com' or '/'. "
            "Columns: path, parent_path, object_type, object_id, language (notebooks), created_at, modified_at, "
            "size (files), resource_id. recursive := true walks sub-directories and Git folders breadth first; "
            "on large workspaces start below '/'."
        ),
        doc_md=(
            "## workspace_objects(path, recursive := false)\n\n"
            "Browse the workspace tree: notebooks, files and folders under a path, optionally recursively."
        ),
    ),
    "domain": "orchestration",
    "vgi.result_columns_schema": _result_columns(WorkspaceObjects.FIXED_SCHEMA),
}
WorkspaceObjects.Meta.examples = [
    FunctionExample(
        sql=(
            f"SELECT language, count(*) AS notebooks FROM {Q}.workspace_objects('/Users/alice@example.com', "
            "recursive := true) WHERE object_type = 'NOTEBOOK' GROUP BY language ORDER BY notebooks DESC"
        ),
        description="Notebooks per language under a user's home folder",
    ),
]

FUNCTIONS: list[type[Any]] = [RunOutput, WorkspaceObjects]
for fn in [*FUNCTIONS, cast("type[Any]", UnifiedStatus)]:
    fn.Meta.tags = {
        **fn.Meta.tags,
        "vgi.example_queries": json.dumps([{"description": e.description, "sql": e.sql} for e in fn.Meta.examples]),
    }


def _schema_examples(names: list[str], extra: list[type[Any]]) -> str:
    out = [{"description": d, "sql": s} for n in names for d, s in EXAMPLES[n][:1]]
    out += [{"description": e.description, "sql": e.sql} for fn in extra for e in fn.Meta.examples[:1]]
    return json.dumps(out)


class DatabricksWorker(Worker):
    """The worker process: the ``databricks`` catalog with ``main`` and ``unified`` schemas."""

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_DESCRIPTION,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment="Databricks workspace resources as tables: jobs and runs, compute, SQL, pipelines, workspace.",
                tables=MAIN_TABLES,
                functions=FUNCTIONS,
                tags={
                    "vgi.title": "Databricks Workspace Resources",
                    "domain": "orchestration",
                    "vgi.keywords": keywords_array(
                        "jobs, job runs, task runs, clusters, sql warehouses, query history, pipelines, repos"
                    ),
                    "vgi.doc_llm": MAIN_DOC_LLM,
                    "vgi.doc_md": MAIN_DOC_MD,
                    "vgi.categories": json.dumps(MAIN_CATEGORIES),
                    "vgi.example_queries": _schema_examples([t.name for t in MAIN_TABLES], FUNCTIONS),
                },
            ),
            Schema(
                path=["unified"],
                comment="Job runs and task attempts in the cross-orchestrator shape shared by Query.Farm workers.",
                tables=UNIFIED_TABLES,
                functions=[UnifiedStatus],
                tags={
                    "vgi.title": "Unified Orchestrator Runs",
                    "domain": "orchestration",
                    "vgi.keywords": keywords_array("unified, runs, task runs, cross orchestrator, union all"),
                    "vgi.doc_llm": UNIFIED_DOC_LLM,
                    "vgi.doc_md": UNIFIED_DOC_MD,
                    "vgi.categories": json.dumps(UNIFIED_CATEGORIES),
                    "vgi.example_queries": _schema_examples([t.name for t in UNIFIED_TABLES], [UnifiedStatus]),
                },
            ),
        ],
    )

    secret_types = [DATABRICKS_SECRET]


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    DatabricksWorker.main()


if __name__ == "__main__":
    main()
