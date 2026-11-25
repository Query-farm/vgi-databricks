# Changelog

## 0.2.0 (2026-11-25)

- Speaks VGI protocol 3.4: require vgi-python `>=0.55.1,<0.56`; uv.lock re-locked. This release
  needs Haybarn 2.0.1 (or stock DuckDB 2.0 with `INSTALL vgi FROM query_farm`): Haybarn 2.0.0
  loads a 3.3 `vgi` extension that cannot attach it.
- SQL tests run on haybarn-unittest 2.0.1rc3 (CI and Makefile).
- New `serve` extra (`vgi-python[http,oauth]`); the Docker image installs it, so the image runs in
  Orchard hosted mode (`VGI_ORCHARD_HOSTED=1` with VGI_JWT_ISSUER, VGI_JWT_AUDIENCE,
  VGI_JWT_JWKS_URI and VGI_SIGNING_KEY). The Docker HEALTHCHECK uses `HEAD /health`, which
  stays anonymous in hosted mode.

## 0.1.1 (2026-11-16)

- Require vgi-python `>=0.52.2,<0.53`; uv.lock re-locked.
- Move to Python 3.14: `requires-python >=3.14`, CI, the Docker image and lint tooling target 3.14.

## 0.1.0 (unreleased)

Initial release: the Databricks workspace control plane (REST 2.x, read-only) as SQL.

- `main`: jobs, job_runs, job_run_tasks, clusters, cluster_events, cluster_policies,
  instance_pools, sql_warehouses, query_history, pipelines, pipeline_updates, pipeline_events,
  repos, secret_scopes; `run_output(run_id)` and `workspace_objects(path, recursive := false)`.
- `unified`: runs and task_runs in the shared orchestrator shape, plus `unified_status(state)`.
- Auth via a `databricks` secret: PAT, OAuth M2M (service principal) or Azure AD token.
- Filter pushdown into API parameters; externalized page tokens; 429 / Retry-After backoff.

Targets: vgi-python 0.51 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
