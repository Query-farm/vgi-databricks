<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query Databricks Jobs, Clusters and Query History in DuckDB

> **vgi-databricks** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-databricks/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-databricks/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

The Databricks workspace **control plane** as SQL tables: Lakeflow Jobs and every run and task
attempt, run output and errors, clusters and their events, cluster policies, instance pools, SQL
warehouses and the SQL query history, Lakeflow Declarative (DLT) pipelines with their updates and
event logs, Git folders, secret scope names and the workspace tree. It works with AWS, Azure and
GCP workspaces through the REST API (2.0 / 2.1 / 2.2), and it is read-only.

```sql
-- Which tasks failed in last night's job runs, and with what message?
SELECT r.run_name, r.run_id, t.task_key, t.state_message
FROM databricks.main.job_runs r
JOIN databricks.main.job_run_tasks t USING (run_id)
WHERE r.result_state = 'FAILED' AND r.start_time >= TIMESTAMPTZ '2026-11-14 00:00:00+00'
  AND t.result_state = 'FAILED';

-- The error and stack trace of every task of one run:
SELECT task_key, error, error_trace FROM databricks.main.run_output(2002) WHERE error IS NOT NULL;
```

The `start_time` range and the "terminated" state implied by `result_state` go to the Jobs API
as `start_time_from` and `completed_only`, so the worker reads only last night's finished runs.

This is not a connector for Databricks *table data*: use a Databricks SQL / Unity Catalog
connector for that. Billable usage (`system.billing`) is out of scope too: query it with SQL in
Databricks.

## Quick start

**DuckDB 2.0:**

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;

ATTACH 'databricks' (TYPE vgi,
  LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-databricks@v0.1.1 vgi-databricks-worker');

-- Credentials go in a DuckDB secret, created after ATTACH (which registers the secret type).
CREATE SECRET dbx (TYPE databricks, HOST 'https://dbc-a1b2c3d4-e5f6.cloud.databricks.com',
                   TOKEN 'dapi...');

SELECT name, schedule_cron, trigger_type, task_count FROM databricks.main.jobs ORDER BY name;
```

**Haybarn 2.0** (`uvx haybarn-cli`): use `INSTALL vgi FROM community; LOAD vgi;` and the same
`ATTACH`.

**From a checkout:** `make build`, then use
`LOCATION '/abs/path/to/vgi-databricks/.venv/bin/vgi-databricks-worker'`.

## Credentials

Every table reads the one `databricks` secret in the session:

| Field | Meaning |
|---|---|
| `HOST` (required) | Workspace URL, e.g. `https://dbc-….cloud.databricks.com`, `https://adb-….azuredatabricks.net`, `https://….gcp.databricks.com`. A bare host name gets `https://`. |
| `TOKEN` | Personal access token, sent as a bearer token. |
| `CLIENT_ID`, `CLIENT_SECRET` | OAuth machine-to-machine: a service principal's OAuth secret, exchanged at `<HOST>/oidc/v1/token` (client credentials, scope `all-apis`). The token is cached per process and refreshed on expiry or a 401. |
| `AZURE_TOKEN` | An Azure AD (Entra ID) access token for the Azure Databricks resource. |
| `AUTH_TYPE` | `auto` (default: `TOKEN`, then `CLIENT_ID`/`CLIENT_SECRET`, then `AZURE_TOKEN`), `pat`, `oauth-m2m` or `azure`. |
| `PAGE_SIZE` | Items per API page (capped at each endpoint's maximum). |
| `TIMEOUT` | Per-request timeout in seconds (default 30). |

```sql
-- OAuth M2M with a service principal
CREATE SECRET dbx (TYPE databricks, HOST 'https://adb-1234567890123456.7.azuredatabricks.net',
                   CLIENT_ID '...', CLIENT_SECRET '...');
-- Azure AD token (e.g. from `az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d`)
CREATE SECRET dbx (TYPE databricks, HOST 'https://adb-….azuredatabricks.net', AZURE_TOKEN '...');
```

Credentials are read from the secret on every call. They are never written to logs, error
messages or the scan cursor, and nothing is read from environment variables. The principal only
needs read permissions (`CAN VIEW` on jobs and clusters, `CAN MONITOR` on warehouses for other
users' query history, and so on): the worker calls GET endpoints and the read-only cluster events
query.

## Tables and functions

Schema `main`:

| Table / function | One row per | Pushed down to the API |
|---|---|---|
| `jobs` | job: schedule / trigger, tasks (`STRUCT[]`), job clusters, tags (`MAP`), settings (`JSON`) | `job_id` (jobs/get), `name =` |
| `job_runs` | job run (also one-time submit runs) | `run_id` (runs/get), `job_id`, `start_time` range, active vs completed (from `life_cycle_state`, `status` or any `result_state`), `run_type` |
| `job_run_tasks` | task **attempt** (retries are separate rows) | the parent run's `run_id`, `job_id`, `start_time` |
| `run_output(run_id, include_result := false, max_log_chars := 65536)` | task run of a job run (or one task run) | — |
| `clusters` | cluster (all-purpose, and job clusters from the last 30 days) | `cluster_id` (clusters/get), `state`, `cluster_source`, `policy_id` |
| `cluster_events` | cluster event | `cluster_id`, `timestamp` range, `type` |
| `cluster_policies` | cluster policy | — |
| `instance_pools` | instance pool with usage stats | — |
| `sql_warehouses` | SQL warehouse | `warehouse_id` |
| `query_history` | SQL statement | `query_start_time` range, `warehouse_id`, `user_id`, `status`, `query_id` |
| `pipelines` | declarative pipeline | `pipeline_id`, `name =` |
| `pipeline_updates` | pipeline update | `pipeline_id` |
| `pipeline_events` | pipeline event-log entry | `pipeline_id`, `level`, `event_id`, `timestamp` range |
| `repos` | Git folder | `repo_id`, `path =` |
| `secret_scopes` | secret scope (name and backend only) | — |
| `workspace_objects(path, recursive := false)` | notebook / file / folder under a path | — |

Schema `unified` holds `runs` and `task_runs` with exactly the columns every Query.Farm
orchestrator worker (Airflow, Dagster, Prefect, Temporal, dbt, GitHub Actions) uses, so you can
`UNION ALL` them, plus the scalar `unified_status(state)`:

```sql
SELECT orchestrator, pipeline, status, count(*) AS runs
FROM (SELECT * FROM databricks.unified.runs UNION ALL SELECT * FROM airflow.unified.runs)
WHERE started_at >= TIMESTAMPTZ '2026-11-01'
GROUP BY ALL ORDER BY ALL;
```

Databricks specifics in `unified`: `run_id` is the Jobs run id as text and `pipeline` the run
name; `raw_status` is `result_state` once terminated, else `life_cycle_state`; `UPSTREAM_FAILED`
and `UPSTREAM_CANCELED` map to `skipped` (each failure counts once, upstream), `CANCELED` to
`cancelled`, `PERIODIC`/`CONTINUOUS` triggers to `schedule`, `ONE_TIME` to `manual`,
`FILE_ARRIVAL` to `sensor`, `TABLE` to `event` and submit runs to `api`. Databricks keeps one
creation time per run, so `queued_at = started_at = start_time`. `task_runs` has one row per
attempt, with a 1-based `attempt` (`attempt_number + 1`).

### How pushdown and paging work

Each table translates DuckDB's pushed-down filters into the API's own parameters (see the table
above), and DuckDB re-checks every predicate, so a translation only ever narrows what is read.
A small closed integer range or `IN` list on an id (`job_id IN (101, 102)`) becomes one `get` per
id. Without a `cluster_id` / `pipeline_id`, `cluster_events`, `pipeline_updates` and
`pipeline_events` read every listed cluster or pipeline.

The scan cursor is `(listing, page token, fan-out ids)`. Databricks page tokens are opaque and
stateless, so a scan resumes on any worker process (the HTTP transport) and holds no credentials.
429 and 502/503/504 responses are retried with backoff that honours `Retry-After`.

### Secrets never leak

* `secret_scopes` lists names and backends; the worker never calls an API that returns secret
  values.
* Spark config (`clusters.spark_conf`), job settings (`jobs.settings`), run parameters
  (`job_runs.job_parameters`) and event details are masked: a value under a secret-looking key
  (password, token, secret, key, credential, …) or that looks like a credential (`dapi…` tokens,
  JWTs, private keys, URLs with passwords) becomes `***`. `{{secrets/scope/key}}` references are
  kept.
* `run_output` returns the notebook exit value only with `include_result := true`, and cuts logs
  and stack traces to `max_log_chars`.

## Testing

There is no local Databricks, so the tests run against a strict replay
(`test/mock_api/server.py`) of a fictitious workspace (`test/fixtures/workspace.json`, generated
by `test/fixtures/build_workspace.py` from the response shapes in the REST API reference). The
replay enforces auth (PAT, OAuth M2M token exchange, Azure AD token), rejects any undocumented
query parameter or page size, binds page tokens to their filters, rate-limits the first request
to every path with a 429, pages task arrays, and logs every request so the SQL tests can assert
what was pushed down. CI needs no network.

```sh
make check              # ruff, mypy, pydoclint, pytest, SQL tests (stdio, launch, http), vgi-lint
make test-sql TRANSPORT=stdio
```

## Deploying

The same entry point serves stdio (DuckDB starts it) or HTTP (`vgi-databricks-worker --http
--port 8000`, or the Dockerfile). With HTTP, credentials still come from the caller's DuckDB
secret, never from the server's environment.

## Support

Open an issue on [Query-farm/vgi-databricks](https://github.com/Query-farm/vgi-databricks/issues).
For VGI itself, see [query.farm](https://query.farm).

## Authorship & License

Written by [Query.Farm](https://query.farm). Released under the [BSD Zero Clause License](LICENSE) (0BSD).

Copyright 2026 Query Farm LLC - https://query.farm

Databricks, Delta Live Tables and Lakeflow are trademarks of Databricks, Inc. This project is not
affiliated with or endorsed by Databricks.
