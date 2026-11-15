# Changelog

## 0.1.0 (unreleased)

Initial release: the Databricks workspace control plane (REST 2.x, read-only) as SQL.

- `main`: jobs, job_runs, job_run_tasks, clusters, cluster_events, cluster_policies,
  instance_pools, sql_warehouses, query_history, pipelines, pipeline_updates, pipeline_events,
  repos, secret_scopes; `run_output(run_id)` and `workspace_objects(path, recursive := false)`.
- `unified`: runs and task_runs in the shared orchestrator shape, plus `unified_status(state)`.
- Auth via a `databricks` secret: PAT, OAuth M2M (service principal) or Azure AD token.
- Filter pushdown into API parameters; externalized page tokens; 429 / Retry-After backoff.

Targets: vgi-python 0.51 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
