"""Discovery metadata: catalog, schemas, tables and functions.

DuckDB, coding agents and the ``vgi-lint`` metadata checker all read these
tags. Every table and function carries ``object_tags(...)``: a title, a
category from its schema's category list, keywords, and LLM- and human-facing
docs.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "databricks"

SOURCE_URL = "https://github.com/Query-farm/vgi-databricks"
DATABRICKS_API_DOCS = "https://docs.databricks.com/api/workspace/introduction"

MAIN_CATEGORIES = [
    {"name": "jobs", "title": "Jobs and runs", "description": "Lakeflow Jobs, their runs, task attempts and output."},
    {"name": "compute", "title": "Compute", "description": "Clusters, cluster events, policies and instance pools."},
    {"name": "sql", "title": "Databricks SQL", "description": "SQL warehouses and the query history."},
    {
        "name": "pipelines",
        "title": "Pipelines",
        "description": "Lakeflow Declarative (DLT) pipelines, updates, events.",
    },
    {"name": "workspace", "title": "Workspace", "description": "Workspace objects, Git folders and secret scopes."},
]
UNIFIED_CATEGORIES = [
    {"name": "runs", "title": "Cross-orchestrator runs", "description": "Runs and task runs in a shared shape."},
]
_ALL_CATEGORIES = {c["name"] for c in MAIN_CATEGORIES + UNIFIED_CATEGORIES}


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in _ALL_CATEGORIES:
        raise ValueError(f"unknown category {category!r}")
    return {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


CREDENTIALS_MD = (
    "Credentials come from a DuckDB secret of type `databricks`, created **after** `ATTACH`:\n\n"
    "```sql\n"
    "-- Personal access token\n"
    "CREATE SECRET dbx (TYPE databricks, HOST 'https://dbc-a1b2c3d4-e5f6.cloud.databricks.com', TOKEN 'dapi...');\n"
    "-- OAuth machine-to-machine (service principal), exchanged at <host>/oidc/v1/token\n"
    "CREATE SECRET dbx (TYPE databricks, HOST 'https://adb-123.7.azuredatabricks.net',\n"
    "                   CLIENT_ID '...', CLIENT_SECRET '...');\n"
    "-- Azure AD (Entra ID) access token\n"
    "CREATE SECRET dbx (TYPE databricks, HOST 'https://adb-123.7.azuredatabricks.net', AZURE_TOKEN '...');\n"
    "```"
)

CATALOG_DESCRIPTION = (
    "The Databricks workspace control plane as SQL tables: Lakeflow Jobs and their runs, clusters, SQL "
    "warehouses and query history, declarative pipelines, Git folders and secret scopes. Read-only."
)

CATALOG_DOC_LLM = (
    "Query one Databricks workspace's control plane (AWS, Azure or GCP) through its REST API: what ran, when, "
    "on which compute, and why it failed. It reads operational metadata (jobs and their runs and task attempts, "
    "cluster state and events, SQL warehouse query history, declarative pipeline updates and event logs, Git "
    "folders, secret scope names), never table data. Requires a DuckDB secret created after ATTACH: CREATE "
    "SECRET (TYPE databricks, HOST 'https://<workspace>', TOKEN 'dapi...') or with CLIENT_ID/CLIENT_SECRET "
    "(OAuth M2M service principal) or AZURE_TOKEN. Schema `main` mirrors the REST resources; schema `unified` "
    "holds runs and task runs in a shape shared with other orchestrator workers. Filters on ids, run start "
    "times, states, warehouse, user and event level are sent to the API; always bound run and query-history "
    "scans by a time range on large workspaces. Timestamps are `TIMESTAMPTZ` (UTC); durations are seconds. "
    "Billable usage (system.billing) and table data are out of scope."
)

CATALOG_DOC_MD = (
    "# Databricks\n\n"
    "Query a Databricks workspace's jobs, runs, clusters, SQL warehouses, query history and pipelines with "
    "SQL: the operational metadata you would otherwise click through in the UI or script against the REST "
    "API. Works with AWS, Azure and GCP workspaces. Read-only.\n\n"
    f"{CREDENTIALS_MD}\n\n"
    "Filters on job and run ids, run start times, cluster state, query start time, warehouse, user and status "
    "are pushed into the API's own parameters, so a query for last night's failed runs reads only those runs.\n\n"
    "Not covered: table data (use a Databricks SQL connector), billable usage (`system.billing`), and "
    "anything that changes the workspace."
)

MAIN_DOC_LLM = (
    "Native Databricks resources, one table per REST collection, plus table functions for a run's output and "
    "for browsing the workspace tree. The model: a job has runs; a run has task attempts (a retried task is "
    "one row per try), each with its own task run id whose output (error, stack trace, logs) can be fetched. "
    "Compute and SQL tables describe where work ran. Join on the shared id columns (job_id, run_id, "
    "cluster_id, policy_id, warehouse_id, pipeline_id). States are Databricks' own upper-case values. Ids, "
    "run start-time ranges, active vs completed state, cluster state and source, query start time, warehouse, "
    "user and status, pipeline ids, event level and time ranges are pushed down to the API."
)

MAIN_DOC_MD = (
    "## main\n\n"
    "Databricks workspace resources as tables. A job has runs; a run has task attempts (a retried task appears "
    "once per try), each with its own task run id and output.\n\n"
    "Filter by job, run and time range wherever you can: those predicates become API parameters, so the "
    "worker reads only the matching runs instead of the whole history.\n\n"
    "Secrets stay in Databricks: secret scopes are listed by name only, and secret-looking Spark config and "
    "job parameter values are masked."
)

UNIFIED_DOC_LLM = (
    "Cross-orchestrator tables with columns identical across the Airflow, Dagster, Prefect, Temporal, "
    "Databricks, dbt and GitHub workers, for UNION ALL. runs(orchestrator, run_id, pipeline, status, raw_status, "
    "trigger, queued_at, started_at, ended_at, duration_seconds, url) and task_runs(orchestrator, run_id, "
    "task_run_id, task_name, status, raw_status, attempt, started_at, ended_at, duration_seconds, url). status is "
    "one of queued, running, success, failed, cancelled, skipped; Databricks UPSTREAM_FAILED maps to skipped. "
    "run_id is the Jobs run id as text; pipeline is the run name (job name). raw_status is result_state, else "
    "life_cycle_state. task_runs has one row per task attempt; attempt is 1-based. Pushed down: run_id, status "
    "(active vs completed) and started_at ranges."
)

UNIFIED_DOC_MD = (
    "## unified\n\n"
    "Job runs and task attempts in the shape every Query.Farm orchestrator worker shares, so you can "
    "`UNION ALL` Databricks with Airflow, Dagster, Prefect and others.\n\n"
    "Databricks' `UPSTREAM_FAILED` counts as `skipped` so each failure is counted once, and each retry of a "
    "task is its own task run with a 1-based `attempt`."
)

TABLE_DOCS: dict[str, dict[str, str]] = {
    "jobs": {
        "comment": "Databricks Lakeflow Jobs: schedule or trigger, tasks, job clusters and tags.",
        "title": "Databricks Jobs",
        "category": "jobs",
        "keywords": "jobs, workflows, lakeflow jobs, schedule, cron, tasks, job clusters, trigger",
        "doc_llm": (
            "One row per job: job_id, name, creator_user_name, run_as_user_name, created_time, schedule_cron "
            "(Quartz), schedule_timezone, schedule_paused, trigger_type (periodic, file_arrival, table_update, "
            "continuous or NULL for manual-only), max_concurrent_runs, tags (`MAP(VARCHAR, VARCHAR)`), task_count, "
            "tasks (`STRUCT(task_key, task_type, depends_on, cluster, max_retries, timeout_seconds, description)[]`), "
            "job_clusters, settings (JSON, secret-looking values masked). Pushed down: job_id (= or IN, jobs/get) "
            "and name = (the API's exact, case-insensitive name filter)."
        ),
        "doc_md": "Every job with its schedule or trigger, task graph summary and job clusters.",
    },
    "job_runs": {
        "comment": "Databricks job runs with state, trigger, timings and termination details.",
        "title": "Databricks Job Run History",
        "category": "jobs",
        "keywords": "job runs, run history, failed runs, run state, result state, trigger, duration",
        "doc_llm": (
            "One row per job run (also one-time submit runs), keyed by run_id. life_cycle_state is QUEUED, "
            "PENDING, RUNNING, TERMINATING, TERMINATED, SKIPPED, INTERNAL_ERROR, ...; result_state (once "
            "terminated) is SUCCESS, FAILED, TIMEDOUT, CANCELED, ... . trigger is PERIODIC, ONE_TIME, RETRY, "
            "FILE_ARRIVAL, TABLE, ... . Durations are seconds. Pushed down: run_id (runs/get), job_id (= or IN), "
            "start_time ranges (start_time_from/to), active vs completed (from life_cycle_state, status or any "
            "result_state filter) and run_type. Join job_run_tasks ON run_id; jobs ON job_id."
        ),
        "doc_md": "Run history for every job. Filter by `job_id` and a `start_time` range to keep reads small.",
    },
    "job_run_tasks": {
        "comment": "Every task attempt of every Databricks job run (retries are separate rows).",
        "title": "Job Run Task Attempts",
        "category": "jobs",
        "keywords": "task runs, task attempts, retries, failed tasks, upstream failed, task duration",
        "doc_llm": (
            "One row per task attempt: run_id (parent job run), job_id, task_run_id (the attempt's own run id, "
            "input to run_output), task_key, attempt_number (0 = first try; a retried task has more rows), "
            "task_type, life_cycle_state, result_state (UPSTREAM_FAILED when an upstream task failed), "
            "state_message, start/end times, durations in seconds, cluster_id, depends_on. Reads runs with "
            "expand_tasks; pushed down: run_id, job_id, start_time ranges of the parent run. Join to job_runs "
            "ON job_run_tasks.run_id = job_runs.run_id."
        ),
        "doc_md": "Task-by-task, attempt-by-attempt history of every run.",
    },
    "clusters": {
        "comment": "Databricks clusters (all-purpose and recent job clusters) with size, state and termination.",
        "title": "Databricks Clusters",
        "category": "compute",
        "keywords": "clusters, compute, all-purpose cluster, job cluster, autoscale, runtime version, termination",
        "doc_llm": (
            "One row per cluster the list API returns (all-purpose clusters, plus job clusters terminated in "
            "the last 30 days): cluster_id, cluster_name, state, cluster_source (UI, API, JOB, PIPELINE, ...), "
            "spark_version, node types, num_workers or autoscale bounds, autotermination_minutes, policy_id, "
            "start/terminated times, termination_code/type, custom_tags/default_tags (`MAP(VARCHAR, VARCHAR)`), "
            "spark_conf ("
            "same type, secret values masked). Pushed down: cluster_id (clusters/get), state (= or IN), cluster_source "
            "(= or IN), policy_id."
        ),
        "doc_md": "Cluster inventory: size, runtime, owner, state and why it last terminated.",
    },
    "cluster_events": {
        "comment": "Cluster lifecycle events: starts, resizes, terminations, node loss.",
        "title": "Databricks Cluster Events",
        "category": "compute",
        "keywords": "cluster events, resize, autoscaling, termination, cluster lifecycle, nodes lost",
        "doc_llm": (
            "One row per cluster event: cluster_id, timestamp, type (CREATING, STARTING, RUNNING, RESIZING, "
            "UPSIZE_COMPLETED, TERMINATING, ...), user, current/target_num_workers, reason_code, reason_type, "
            "cause, details (JSON). Pushed down: cluster_id (= or IN; otherwise every listed cluster is read), "
            "timestamp ranges (start_time/end_time) and type (= or IN, event_types)."
        ),
        "doc_md": "What happened to each cluster and when: starts, resizes, terminations.",
    },
    "cluster_policies": {
        "comment": "Cluster policies and their JSON definitions.",
        "title": "Databricks Cluster Policies",
        "category": "compute",
        "keywords": "cluster policy, policies, governance, compute policy, policy definition",
        "doc_llm": (
            "One row per cluster policy: policy_id, name, description, creator_user_name, created_at, "
            "is_default, max_clusters_per_user, policy_family_id, definition (JSON). Join clusters ON policy_id."
        ),
        "doc_md": "Cluster policies and the rules they enforce.",
    },
    "instance_pools": {
        "comment": "Instance pools with capacity settings and current usage.",
        "title": "Databricks Instance Pools",
        "category": "compute",
        "keywords": "instance pool, pools, idle instances, capacity, warm pool",
        "doc_llm": (
            "One row per instance pool: instance_pool_id, instance_pool_name, state, node_type_id, "
            "min_idle_instances, max_capacity, idle_instance_autotermination_minutes, preloaded_spark_versions, "
            "used_count, idle_count, pending_used_count, pending_idle_count, custom_tags."
        ),
        "doc_md": "Instance pools: how many instances are idle, in use and pending.",
    },
    "sql_warehouses": {
        "comment": "Databricks SQL warehouses: size, type, scaling, state and health.",
        "title": "Databricks SQL Warehouses",
        "category": "sql",
        "keywords": "sql warehouse, warehouses, serverless, endpoint, cluster size, auto stop",
        "doc_llm": (
            "One row per SQL warehouse: warehouse_id, name, state, cluster_size, warehouse_type (PRO, CLASSIC), "
            "enable_serverless_compute, enable_photon, min/max_num_clusters, num_clusters, num_active_sessions, "
            "auto_stop_mins, channel, creator_name, health_status, custom_tags. Pushed down: warehouse_id."
        ),
        "doc_md": "SQL warehouse inventory and current state.",
    },
    "query_history": {
        "comment": "Databricks SQL query history with status, timings, rows and I/O metrics.",
        "title": "Databricks SQL Query History",
        "category": "sql",
        "keywords": "query history, sql queries, slow queries, failed queries, warehouse usage, statement",
        "doc_llm": (
            "One row per SQL statement run on a warehouse (or serverless compute): query_id, status (QUEUED, "
            "RUNNING, CANCELED, FAILED, FINISHED), statement_type, query_text, warehouse_id, user_id, user_name, "
            "query_start_time, execution_end_time, query_end_time, duration_seconds, rows_produced, error_message, "
            "compilation/execution time, read_bytes, read_rows, spill_to_disk_bytes, result_from_cache. Pushed "
            "down: query_start_time ranges, warehouse_id, user_id, status and query_id (= or IN). Always give a "
            "query_start_time range: the API keeps about 30 days."
        ),
        "doc_md": "Who ran which SQL, on which warehouse, how long it took and why it failed.",
    },
    "pipelines": {
        "comment": "Lakeflow Declarative Pipelines (Delta Live Tables) with state, health and latest update.",
        "title": "Databricks Pipelines",
        "category": "pipelines",
        "keywords": "pipelines, delta live tables, dlt, lakeflow declarative pipelines, streaming tables",
        "doc_llm": (
            "One row per pipeline: pipeline_id, name, state (IDLE, RUNNING, FAILED, ...), health (HEALTHY, "
            "UNHEALTHY), cluster_id, creator_user_name, run_as_user_name, latest_update_id, latest_update_state, "
            "latest_update_time. Pushed down: pipeline_id (pipelines/get) and name = (filter name LIKE)."
        ),
        "doc_md": "Every declarative pipeline with its health and latest update.",
    },
    "pipeline_updates": {
        "comment": "Pipeline updates (runs) with state, cause and refresh selection.",
        "title": "Databricks Pipeline Updates",
        "category": "pipelines",
        "keywords": "pipeline updates, dlt runs, full refresh, pipeline history",
        "doc_llm": (
            "One row per pipeline update: pipeline_id, update_id, state (COMPLETED, FAILED, CANCELED, RUNNING, ...), "
            "cause (API_CALL, JOB_TASK, RETRY_ON_FAILURE, USER_ACTION, ...), creation_time, full_refresh, "
            "validate_only, cluster_id, refresh_selection. Pushed down: pipeline_id (URL path); without it every "
            "pipeline is read. Join to pipelines ON pipeline_updates.pipeline_id = pipelines.pipeline_id."
        ),
        "doc_md": "Update history of each pipeline.",
    },
    "pipeline_events": {
        "comment": "Pipeline event log: progress, data quality, errors with exceptions.",
        "title": "Databricks Pipeline Event Log",
        "category": "pipelines",
        "keywords": "pipeline events, event log, dlt errors, flow progress, data quality, expectations",
        "doc_llm": (
            "One row per pipeline event: pipeline_id, event_id, update_id, timestamp, level (INFO, WARN, ERROR, "
            "METRICS), event_type, message, flow_name, dataset_name, error_fatal, error_exceptions "
            "(`STRUCT(class_name VARCHAR, message VARCHAR)[]`), details (JSON). Pushed down: pipeline_id (URL path), "
            "level "
            "(= or IN), event_id and timestamp ranges (the API's filter expression). Join to pipelines ON "
            "pipeline_events.pipeline_id = pipelines.pipeline_id, and to pipeline_updates ON update_id."
        ),
        "doc_md": "A pipeline's event log: progress, failures and their exceptions.",
    },
    "repos": {
        "comment": "Git folders (Repos) in the workspace: remote URL, branch and commit.",
        "title": "Databricks Git Folders",
        "category": "workspace",
        "keywords": "repos, git folders, git, branch, commit, source control",
        "doc_llm": (
            "One row per Git folder: repo_id, path, url, provider, branch, head_commit_id, "
            "sparse_checkout_patterns. Pushed down: repo_id (repos/{id}) and path = (path_prefix)."
        ),
        "doc_md": "Git folders, their remotes and checked-out branches.",
    },
    "secret_scopes": {
        "comment": "Secret scope names and backends. Secret values are never read.",
        "title": "Databricks Secret Scopes",
        "category": "workspace",
        "keywords": "secret scopes, secrets, key vault, credentials inventory",
        "doc_llm": (
            "One row per secret scope: scope, backend_type (DATABRICKS or AZURE_KEYVAULT), keyvault_resource_id, "
            "keyvault_dns_name. Names only: the worker never calls an API that returns secret values."
        ),
        "doc_md": "Which secret scopes exist and where they are stored. Values never leave Databricks.",
    },
    "runs": {
        "comment": "Databricks job runs in the cross-orchestrator unified.runs shape.",
        "title": "Cross-Orchestrator Runs",
        "category": "runs",
        "keywords": "unified runs, cross orchestrator, pipeline runs, run status, union all",
        "doc_llm": (
            "Databricks job runs as unified.runs: orchestrator='databricks', run_id (Jobs run id as text), "
            "pipeline (run name = job name), status (queued/running/success/failed/cancelled/skipped), raw_status "
            "(result_state else life_cycle_state), trigger (schedule, manual, sensor, api, event, other), "
            "queued_at = started_at = start_time, ended_at, duration_seconds, url. Pushed down: run_id, status "
            "(active vs completed), started_at ranges."
        ),
        "doc_md": "Job runs in the shared orchestrator shape, ready to `UNION ALL` with other workers.",
    },
    "task_runs": {
        "comment": "Databricks task attempts in the cross-orchestrator unified.task_runs shape.",
        "title": "Cross-Orchestrator Task Runs",
        "category": "runs",
        "keywords": "unified task runs, cross orchestrator, task status, retries, union all",
        "doc_llm": (
            "Databricks task attempts as unified.task_runs: run_id (parent run, joins unified.runs), task_run_id "
            "(the attempt's run id), task_name (task_key), status (UPSTREAM_FAILED→skipped), raw_status, attempt "
            "(1-based), started_at, ended_at, duration_seconds, url. Pushed down: run_id and an upper bound on "
            "started_at."
        ),
        "doc_md": "Task attempts in the shared orchestrator shape.",
    },
}

AGENT_TEST_TASKS = [
    {
        "name": "failed_runs",
        "prompt": "Using the databricks catalog, which job runs failed? Give the run id and the run name.",
    },
    {
        "name": "retried_task",
        "prompt": (
            "Using the databricks catalog, which task needed more than one attempt, in which run, and how many "
            "attempts?"
        ),
    },
    {
        "name": "runs_per_status",
        "prompt": "Using the databricks catalog's unified schema, how many runs are there per status?",
    },
    {
        "name": "scheduled_jobs",
        "prompt": "Using the databricks catalog, which jobs run on a cron schedule, and what is each cron expression?",
    },
    {
        "name": "upstream_failed",
        "prompt": (
            "Using the databricks catalog, which tasks never ran because an upstream task failed? Give the run id "
            "and task key."
        ),
    },
    {
        "name": "failure_error",
        "prompt": (
            "Using the databricks catalog, what error message did the failed 'transform' task of run 2002 report?"
        ),
    },
    {
        "name": "running_clusters",
        "prompt": "Using the databricks catalog, which clusters are running right now, and who created them?",
    },
    {
        "name": "inactivity_terminations",
        "prompt": "Using the databricks catalog, which clusters were terminated for inactivity?",
    },
    {
        "name": "failed_queries",
        "prompt": "Using the databricks catalog, how many SQL queries failed on each warehouse (by warehouse name)?",
    },
    {
        "name": "pipeline_errors",
        "prompt": "Using the databricks catalog, which pipeline logged ERROR events, and what were the messages?",
    },
    {
        "name": "keyvault_scopes",
        "prompt": "Using the databricks catalog, which secret scopes are backed by Azure Key Vault?",
    },
    {
        "name": "autoscale_events",
        "prompt": (
            "Using the databricks catalog, how many times did the cluster 'shared-analytics' resize (RESIZING "
            "events), and to which target worker counts?"
        ),
    },
    {
        "name": "policy_usage",
        "prompt": "Using the databricks catalog, how many clusters use each cluster policy (by policy name)?",
    },
    {
        "name": "pool_capacity",
        "prompt": "Using the databricks catalog, what is the maximum capacity of each instance pool?",
    },
    {
        "name": "failed_pipeline_updates",
        "prompt": "Using the databricks catalog, which pipeline updates failed? Give the pipeline name and update id.",
    },
    {
        "name": "repo_branches",
        "prompt": (
            "Using the databricks catalog, which Git folders are not on the main branch, and which branch are they on?"
        ),
    },
    {
        "name": "notebook_languages",
        "prompt": (
            "Using the databricks catalog, how many notebooks of each language are under "
            "/Users/alice@example.com, including sub-folders?"
        ),
    },
    {
        "name": "retried_unified_tasks",
        "prompt": (
            "Using the databricks catalog's unified schema, which task runs were a second (or later) attempt? "
            "Give the run id, task name and attempt."
        ),
    },
    {
        "name": "normalize_state",
        "prompt": "What unified status does the databricks worker assign to the Databricks state 'UPSTREAM_CANCELED'?",
    },
]

CATALOG_TAGS = {
    "vgi.title": "Databricks Workspace",
    "vgi.subtitle": "Jobs, runs, clusters, SQL warehouses, query history and pipelines as SQL tables",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "databricks, lakeflow jobs, workflows, clusters, sql warehouse, query history, delta live tables, "
        "orchestration, rest api"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
