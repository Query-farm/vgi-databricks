#!/usr/bin/env python3
"""Offline, strict replay of the Databricks workspace REST API.

There is no local Databricks, so this server serves the fictitious workspace in
``test/fixtures/workspace.json`` (built by ``test/fixtures/build_workspace.py``
from the documented response shapes) and re-implements the parts of the API
the worker relies on, *strictly*, so a wrong request fails loudly:

* **Auth**: every API call needs ``Authorization: Bearer <t>`` where ``t`` is
  the fixture PAT (``dapi-fixture-token``), the fixture Azure AD token
  (``aad-fixture-token``), or a token issued by ``POST /oidc/v1/token`` for the
  fixture service principal (client credentials over HTTP basic, form body
  ``grant_type=client_credentials&scope=all-apis``). Anything else is 401.
* **Parameters**: each endpoint accepts only its documented query parameters
  (or POST body fields); anything else is 400 ``INVALID_PARAMETER_VALUE``.
  Page sizes above the endpoint's documented maximum are 400 too.
* **Paging**: opaque page tokens bound to the request's filters (a token sent
  with different filters is 400). Pipeline events reject any field but
  ``max_results`` next to a ``page_token``, as documented. Jobs and runs page
  their task arrays (``MOCK_TASK_PAGE``, default 100) with ``has_more`` /
  ``next_page_token`` as API 2.2 does.
* **Rate limits**: ``MOCK_FLAKY=1`` answers the first request to every path
  with 429 + ``Retry-After: 0`` (``REQUEST_LIMIT_EXCEEDED``).
* **Request log**: ``GET /__requests`` returns every API request
  (method, path, query as lists, JSON body) so SQL tests can assert pushdown;
  ``POST /__reset`` clears it.

Usage::

    python3 test/mock_api/server.py            # prints PORT:<n> once listening
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import sys
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
FIXTURE = HERE.parent / "fixtures" / "workspace.json"
PAT = "dapi-fixture-token"
AAD = "aad-fixture-token"
SP_ID, SP_SECRET = "fixture-sp-client-id", "fixture-sp-secret"

ACTIVE_LC = {"QUEUED", "PENDING", "RUNNING", "TERMINATING", "BLOCKED", "WAITING_FOR_RETRY"}
QH_FILTERS = {
    "filter_by.query_start_time_range.start_time_ms",
    "filter_by.query_start_time_range.end_time_ms",
    "filter_by.warehouse_ids",
    "filter_by.user_ids",
    "filter_by.statuses",
    "filter_by.statement_ids",
}


class ApiError(Exception):
    """A Databricks-style error response."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


def bad(message: str) -> ApiError:
    return ApiError(400, "INVALID_PARAMETER_VALUE", message)


def missing(message: str) -> ApiError:
    return ApiError(404, "RESOURCE_DOES_NOT_EXIST", message)


class Request:
    """Parsed request parameters with strict validation helpers."""

    def __init__(self, params: dict[str, list[Any]], allowed: set[str]) -> None:
        unknown = sorted(set(params) - allowed)
        if unknown:
            raise bad(f"Unsupported parameter(s): {', '.join(unknown)}")
        self.params = params

    def one(self, name: str) -> Any:
        vals = self.params.get(name)
        if not vals:
            return None
        if len(vals) > 1:
            raise bad(f"Parameter {name} given more than once")
        return vals[0]

    def many(self, name: str) -> list[Any]:
        return list(self.params.get(name) or [])

    def boolean(self, name: str) -> bool:
        v = self.one(name)
        if v is None:
            return False
        if isinstance(v, bool):
            return v
        if str(v) not in ("true", "false"):
            raise bad(f"{name} must be true or false, got {v!r}")
        return str(v) == "true"

    def integer(self, name: str, *, lo: int | None = None, hi: int | None = None) -> int | None:
        v = self.one(name)
        if v is None:
            return None
        try:
            n = int(v)
        except (TypeError, ValueError) as err:
            raise bad(f"{name} must be an integer, got {v!r}") from err
        if (lo is not None and n < lo) or (hi is not None and n > hi):
            raise bad(f"{name} must be between {lo} and {hi}, got {n}")
        return n


def _fingerprint(params: dict[str, list[Any]], skip: set[str]) -> str:
    keep = {k: sorted(map(str, v)) for k, v in params.items() if k not in skip}
    return hashlib.sha256(json.dumps(keep, sort_keys=True).encode()).hexdigest()[:16]


def make_token(offset: int, fp: str, extra: dict[str, Any] | None = None) -> str:
    raw = json.dumps({"o": offset, "f": fp, **(extra or {})}).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def read_token(token: str | None) -> dict[str, Any] | None:
    if not token:
        return None
    try:
        raw = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
        out = json.loads(raw)
        assert isinstance(out, dict) and isinstance(out.get("o"), int)
        return out
    except Exception as err:
        raise bad(f"Invalid page token {token!r}") from err


def page(
    items: list[Any], req: Request, size: int, *, token_param: str = "page_token", skip: set[str] | None = None
) -> tuple[list[Any], str | None]:
    """Slice ``items`` for the request's page token; return (page, next token)."""
    skip = (skip or set()) | {token_param}
    fp = _fingerprint(req.params, skip)
    tok = read_token(req.one(token_param))
    offset = 0
    if tok is not None:
        if tok["f"] != fp:
            raise bad("page_token does not match the request's other parameters")
        offset = tok["o"]
    chunk = items[offset : offset + size]
    nxt = make_token(offset + size, fp) if offset + size < len(items) else None
    return chunk, nxt


def _like(value: str | None, pattern: str) -> bool:
    rx = "".join(".*" if c == "%" else "." if c == "_" else re.escape(c) for c in pattern)
    return value is not None and re.fullmatch(rx, value, re.IGNORECASE) is not None


class Workspace:
    """Fixture data plus Databricks-ish filtering."""

    def __init__(self, fixture: Path) -> None:
        self.data = json.loads(fixture.read_text())
        self.task_page = int(os.environ.get("MOCK_TASK_PAGE", "100"))
        self.flaky = os.environ.get("MOCK_FLAKY") == "1"
        self.tokens: set[str] = {PAT, AAD}
        self.seen_paths: set[str] = set()
        self.requests: list[dict[str, Any]] = []
        self.lock = threading.Lock()
        self.issued = 0
        self.task_runs: dict[int, tuple[dict[str, Any], dict[str, Any]]] = {}
        for run in self.data["runs"]:
            for t in run.get("tasks") or []:
                self.task_runs[int(t["run_id"])] = (run, t)

    # ----------------------------------------------------------------------- jobs

    def _job_view(self, job: dict[str, Any], *, tasks: bool, offset: int = 0) -> dict[str, Any]:
        out = json.loads(json.dumps(job))
        s = out["settings"]
        if not tasks:
            s.pop("tasks", None)
            s.pop("job_clusters", None)
            return out
        all_tasks = s.get("tasks") or []
        s["tasks"] = all_tasks[offset : offset + self.task_page]
        if offset + self.task_page < len(all_tasks):
            out["has_more"] = True
            out["next_page_token"] = make_token(offset + self.task_page, f"job{job['job_id']}")
        return out

    def jobs_list(self, req: Request) -> dict[str, Any]:
        size = req.integer("limit", lo=1, hi=100) or 20
        name = req.one("name")
        expand = req.boolean("expand_tasks")
        jobs = sorted(self.data["jobs"], key=lambda j: j["job_id"])
        if name is not None:
            jobs = [j for j in jobs if j["settings"]["name"].lower() == str(name).lower()]
        chunk, nxt = page(jobs, req, size)
        out: dict[str, Any] = {"has_more": nxt is not None, "jobs": [self._job_view(j, tasks=expand) for j in chunk]}
        if nxt:
            out["next_page_token"] = nxt
        return out

    def jobs_get(self, req: Request) -> dict[str, Any]:
        job_id = req.integer("job_id")
        if job_id is None:
            raise bad("Missing required field: job_id")
        tok = read_token(req.one("page_token"))
        for j in self.data["jobs"]:
            if j["job_id"] == job_id:
                return self._job_view(j, tasks=True, offset=tok["o"] if tok else 0)
        raise missing(f"Job {job_id} does not exist.")

    def _run_view(self, run: dict[str, Any], *, tasks: bool, offset: int = 0) -> dict[str, Any]:
        out = json.loads(json.dumps(run))
        if not tasks:
            out.pop("tasks", None)
            return out
        all_tasks = out.get("tasks") or []
        out["tasks"] = all_tasks[offset : offset + self.task_page]
        if offset + self.task_page < len(all_tasks):
            out["has_more"] = True
            out["next_page_token"] = make_token(offset + self.task_page, f"run{run['run_id']}")
        return out

    def runs_list(self, req: Request) -> dict[str, Any]:
        size = req.integer("limit", lo=1, hi=25) or 20
        active, completed = req.boolean("active_only"), req.boolean("completed_only")
        if active and completed:
            raise bad("active_only and completed_only cannot both be true")
        runs = list(self.data["runs"])
        job_id = req.integer("job_id")
        if job_id is not None:
            runs = [r for r in runs if r.get("job_id") == job_id]
        if active:
            runs = [r for r in runs if r["state"]["life_cycle_state"] in ACTIVE_LC]
        if completed:
            runs = [r for r in runs if r["state"]["life_cycle_state"] not in ACTIVE_LC]
        run_type = req.one("run_type")
        if run_type is not None:
            if run_type not in ("JOB_RUN", "WORKFLOW_RUN", "SUBMIT_RUN"):
                raise bad(f"Invalid run_type {run_type!r}")
            runs = [r for r in runs if r.get("run_type") == run_type]
        lo, hi = req.integer("start_time_from", lo=0), req.integer("start_time_to", lo=0)
        if lo is not None:
            runs = [r for r in runs if r["start_time"] >= lo]
        if hi is not None:
            runs = [r for r in runs if r["start_time"] <= hi]
        runs.sort(key=lambda r: (-r["start_time"], -r["run_id"]))
        chunk, nxt = page(runs, req, size)
        expand = req.boolean("expand_tasks")
        out: dict[str, Any] = {"has_more": nxt is not None, "runs": [self._run_view(r, tasks=expand) for r in chunk]}
        if nxt:
            out["next_page_token"] = nxt
        return out

    def runs_get(self, req: Request) -> dict[str, Any]:
        run_id = req.integer("run_id")
        if run_id is None:
            raise bad("Missing required field: run_id")
        tok = read_token(req.one("page_token"))
        for r in self.data["runs"]:
            if r["run_id"] == run_id:
                return self._run_view(r, tasks=True, offset=tok["o"] if tok else 0)
        if run_id in self.task_runs:
            parent, t = self.task_runs[run_id]
            return {**t, "job_id": parent.get("job_id"), "run_type": parent.get("run_type"), "run_name": t["task_key"]}
        raise missing(f"Run {run_id} does not exist.")

    def runs_output(self, req: Request) -> dict[str, Any]:
        run_id = req.integer("run_id")
        if run_id is None:
            raise bad("Missing required field: run_id")
        if any(r["run_id"] == run_id for r in self.data["runs"]):
            raise bad(
                "Retrieving the output of runs with multiple tasks is not supported. Please retrieve the output "
                "of each individual task run instead."
            )
        if run_id not in self.task_runs:
            raise missing(f"Run {run_id} does not exist.")
        parent, t = self.task_runs[run_id]
        meta = {**t, "job_id": parent.get("job_id"), "run_name": t["task_key"]}
        return {"metadata": meta, **self.data["outputs"].get(str(run_id), {})}

    # -------------------------------------------------------------------- compute

    def clusters_list(self, req: Request) -> dict[str, Any]:
        size = req.integer("page_size", lo=1, hi=100) or 100
        clusters = sorted(self.data["clusters"], key=lambda c: c["cluster_id"])
        states = req.many("filter_by.cluster_states")
        if states:
            clusters = [c for c in clusters if c["state"] in states]
        sources = req.many("filter_by.cluster_sources")
        if sources:
            clusters = [c for c in clusters if c["cluster_source"] in sources]
        policy = req.one("filter_by.policy_id")
        if policy is not None:
            clusters = [c for c in clusters if c.get("policy_id") == policy]
        req.boolean("filter_by.is_pinned")
        chunk, nxt = page(clusters, req, size)
        out: dict[str, Any] = {"clusters": chunk}
        if nxt:
            out["next_page_token"] = nxt
        return out

    def clusters_get(self, req: Request) -> dict[str, Any]:
        cid = req.one("cluster_id")
        if cid is None:
            raise bad("Missing required field: cluster_id")
        for c in self.data["clusters"]:
            if c["cluster_id"] == cid:
                return dict(c)
        raise bad(f"Cluster {cid} does not exist")

    def cluster_events(self, body: dict[str, Any]) -> dict[str, Any]:
        allowed = {
            "cluster_id",
            "start_time",
            "end_time",
            "order",
            "event_types",
            "offset",
            "limit",
            "page_size",
            "page_token",
        }
        unknown = sorted(set(body) - allowed)
        if unknown:
            raise bad(f"Unsupported field(s): {', '.join(unknown)}")
        cid = body.get("cluster_id")
        if not cid:
            raise bad("Missing required field: cluster_id")
        if not any(c["cluster_id"] == cid for c in self.data["clusters"]):
            raise bad(f"Cluster {cid} does not exist")
        size = body.get("page_size") or 50
        if not isinstance(size, int) or not 0 <= size <= 500:
            raise bad("page_size must be between 0 and 500")
        order = body.get("order", "DESC")
        if order not in ("ASC", "DESC"):
            raise bad(f"Invalid order {order!r}")
        events = [e for e in self.data["cluster_events"] if e["cluster_id"] == cid]
        if body.get("start_time") is not None:
            events = [e for e in events if e["timestamp"] >= int(body["start_time"])]
        if body.get("end_time") is not None:
            events = [e for e in events if e["timestamp"] <= int(body["end_time"])]
        types = body.get("event_types")
        if types is not None:
            if not isinstance(types, list):
                raise bad("event_types must be an array")
            events = [e for e in events if e["type"] in types]
        events.sort(key=lambda e: e["timestamp"], reverse=order == "DESC")
        req = Request({k: v if isinstance(v, list) else [v] for k, v in body.items()}, allowed)
        chunk, nxt = page(events, req, size or 50)
        out: dict[str, Any] = {"events": chunk, "total_count": len(events)}
        if nxt:
            out["next_page_token"] = nxt
        return out

    # ------------------------------------------------------------------------ SQL

    def query_history(self, req: Request) -> dict[str, Any]:
        size = req.integer("max_results", lo=1, hi=1000) or 100
        rows = list(self.data["queries"])
        lo = req.integer("filter_by.query_start_time_range.start_time_ms", lo=0)
        hi = req.integer("filter_by.query_start_time_range.end_time_ms", lo=0)
        if lo is not None:
            rows = [r for r in rows if r["query_start_time_ms"] >= lo]
        if hi is not None:
            rows = [r for r in rows if r["query_start_time_ms"] < hi]
        for param, key in (
            ("filter_by.warehouse_ids", "warehouse_id"),
            ("filter_by.statuses", "status"),
            ("filter_by.statement_ids", "query_id"),
        ):
            vals = req.many(param)
            if vals:
                rows = [r for r in rows if r[key] in vals]
        uids = req.many("filter_by.user_ids")
        if uids:
            wanted = {int(u) for u in uids}
            rows = [r for r in rows if r["user_id"] in wanted]
        statuses = {"QUEUED", "RUNNING", "CANCELED", "FAILED", "FINISHED", "STARTED", "COMPILING", "COMPILED"}
        bad_status = [s for s in req.many("filter_by.statuses") if s not in statuses]
        if bad_status:
            raise bad(f"Invalid status {bad_status[0]!r}")
        if not req.boolean("include_metrics"):
            rows = [{k: v for k, v in r.items() if k != "metrics"} for r in rows]
        rows.sort(key=lambda r: -r["query_start_time_ms"])
        chunk, nxt = page(rows, req, size)
        out: dict[str, Any] = {"res": chunk, "has_next_page": nxt is not None}
        if nxt:
            out["next_page_token"] = nxt
        return out

    # ------------------------------------------------------------------ pipelines

    def pipelines_list(self, req: Request) -> dict[str, Any]:
        size = req.integer("max_results", lo=1, hi=100) or 25
        items = sorted(self.data["pipelines"], key=lambda p: p["name"])
        flt = req.one("filter")
        if flt is not None:
            m = re.fullmatch(r"\s*(name|creator_user_name)\s+LIKE\s+'((?:[^']|'')*)'\s*", flt, re.IGNORECASE)
            if not m:
                raise bad(f"Unsupported filter {flt!r}")
            field, pat = m.group(1).lower(), m.group(2).replace("''", "'")
            items = [p for p in items if _like(p.get(field), pat)]
        chunk, nxt = page(items, req, size)
        statuses = [
            {
                k: p[k]
                for k in (
                    "pipeline_id",
                    "name",
                    "state",
                    "health",
                    "creator_user_name",
                    "run_as_user_name",
                    "latest_updates",
                )
                if k in p
            }
            for p in chunk
        ]
        out: dict[str, Any] = {"statuses": statuses}
        if nxt:
            out["next_page_token"] = nxt
        return out

    def pipeline(self, pid: str) -> dict[str, Any]:
        for p in self.data["pipelines"]:
            if p["pipeline_id"] == pid:
                return p
        raise missing(f"Pipeline {pid} does not exist.")

    def pipeline_updates(self, pid: str, req: Request) -> dict[str, Any]:
        self.pipeline(pid)
        size = req.integer("max_results", lo=1, hi=100) or 25
        updates = sorted(self.data["pipeline_updates"].get(pid, []), key=lambda u: -u["creation_time"])
        chunk, nxt = page(updates, req, size)
        out: dict[str, Any] = {"updates": chunk}
        if nxt:
            out["next_page_token"] = nxt
        return out

    @staticmethod
    def _event_filter(events: list[dict[str, Any]], flt: str) -> list[dict[str, Any]]:
        """Documented grammar: level='X', level in ('X', ...), id='x', timestamp <op> 'iso', joined by AND."""
        for cond in re.split(r"\s+AND\s+", flt.strip(), flags=re.IGNORECASE):
            if m := re.fullmatch(r"level\s*=\s*'(\w+)'", cond):
                events = [e for e in events if e["level"] == m.group(1)]
            elif m := re.fullmatch(r"level\s+in\s*\(([^)]*)\)", cond, re.IGNORECASE):
                levels = re.findall(r"'(\w+)'", m.group(1))
                events = [e for e in events if e["level"] in levels]
            elif m := re.fullmatch(r"id\s*=\s*'([^']*)'", cond):
                events = [e for e in events if e["id"] == m.group(1)]
            elif m := re.fullmatch(r"timestamp\s*(>=|<=|>|<|=)\s*'([^']+)'", cond):
                op, when = m.group(1), m.group(2)
                cmp = {
                    ">=": lambda a, b: a >= b,
                    "<=": lambda a, b: a <= b,
                    ">": lambda a, b: a > b,
                    "<": lambda a, b: a < b,
                    "=": lambda a, b: a == b,
                }[op]
                norm = when.replace("+00:00", "Z")
                if not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z", norm):
                    raise bad(f"Invalid timestamp {when!r}")
                key = norm[:19]
                events = [e for e in events if cmp(e["timestamp"][:19], key)]
            else:
                raise bad(f"Unsupported filter condition {cond!r}")
        return events

    def pipeline_events(self, pid: str, req: Request) -> dict[str, Any]:
        self.pipeline(pid)
        size = req.integer("max_results", lo=1, hi=250) or 25
        token = req.one("page_token")
        if token and set(req.params) - {"page_token", "max_results"}:
            raise bad("page_token is mutually exclusive with all fields in this request except max_results")
        tok = read_token(token)
        flt = tok.get("filter", "") if tok else (req.one("filter") or "")
        order = tok.get("order", "timestamp desc") if tok else (req.one("order_by") or "timestamp desc")
        events = list(self.data["pipeline_events"].get(pid, []))
        if flt:
            events = self._event_filter(events, flt)
        events.sort(key=lambda e: e["timestamp"], reverse=order.endswith("desc"))
        offset = tok["o"] if tok else 0
        chunk = events[offset : offset + size]
        out: dict[str, Any] = {"events": chunk}
        if offset + size < len(events):
            out["next_page_token"] = make_token(offset + size, pid, {"filter": flt, "order": order})
        return out

    # ------------------------------------------------------------------ workspace

    def repos(self, req: Request) -> dict[str, Any]:
        prefix = req.one("path_prefix")
        repos = sorted(self.data["repos"], key=lambda r: r["id"])
        if prefix is not None:
            repos = [r for r in repos if r["path"].startswith(prefix)]
        # The Repos API pages with a token of its own name; one repo per page exercises it.
        chunk, nxt = page(repos, req, 1, token_param="next_page_token")
        out: dict[str, Any] = {"repos": chunk}
        if nxt:
            out["next_page_token"] = nxt
        return out

    def workspace_list(self, req: Request) -> dict[str, Any]:
        path = req.one("path")
        if not path:
            raise bad("Missing required field: path")
        objects = self.data["workspace"].get(path.rstrip("/") or "/")
        if objects is None:
            raise missing(f"Path ({path}) doesn't exist.")
        return {"objects": objects} if objects else {}

    # ---------------------------------------------------------------------- router

    def route(self, method: str, path: str, params: dict[str, list[Any]], body: dict[str, Any]) -> Any:
        if method == "POST" and path == "/api/2.1/clusters/events":
            return self.cluster_events(body)
        if method != "GET":
            raise ApiError(405, "ENDPOINT_NOT_FOUND", f"No API found for '{method} {path}'")
        simple: dict[str, tuple[set[str], Any]] = {
            "/api/2.2/jobs/list": ({"limit", "offset", "page_token", "expand_tasks", "name"}, self.jobs_list),
            "/api/2.2/jobs/get": ({"job_id", "page_token"}, self.jobs_get),
            "/api/2.2/jobs/runs/list": (
                {
                    "job_id",
                    "active_only",
                    "completed_only",
                    "offset",
                    "limit",
                    "page_token",
                    "run_type",
                    "expand_tasks",
                    "start_time_from",
                    "start_time_to",
                },
                self.runs_list,
            ),
            "/api/2.2/jobs/runs/get": (
                {"run_id", "include_history", "include_resolved_values", "page_token"},
                self.runs_get,
            ),
            "/api/2.2/jobs/runs/get-output": ({"run_id"}, self.runs_output),
            "/api/2.1/clusters/list": (
                {
                    "page_size",
                    "page_token",
                    "filter_by.cluster_states",
                    "filter_by.cluster_sources",
                    "filter_by.is_pinned",
                    "filter_by.policy_id",
                    "sort_by.direction",
                    "sort_by.field",
                },
                self.clusters_list,
            ),
            "/api/2.1/clusters/get": ({"cluster_id"}, self.clusters_get),
            "/api/2.0/policies/clusters/list": (
                {"sort_order", "sort_column"},
                lambda r: {"policies": self.data["policies"], "total_count": len(self.data["policies"])},
            ),
            "/api/2.0/instance-pools/list": (set(), lambda r: {"instance_pools": self.data["instance_pools"]}),
            "/api/2.0/sql/warehouses": ({"run_as_user_id"}, lambda r: {"warehouses": self.data["warehouses"]}),
            "/api/2.0/sql/history/queries": (
                {"max_results", "page_token", "include_metrics"} | QH_FILTERS,
                self.query_history,
            ),
            "/api/2.0/pipelines": ({"max_results", "page_token", "filter", "order_by"}, self.pipelines_list),
            "/api/2.0/repos": ({"path_prefix", "next_page_token"}, self.repos),
            "/api/2.0/secrets/scopes/list": (set(), lambda r: {"scopes": self.data["secret_scopes"]}),
            "/api/2.0/workspace/list": ({"path", "notebooks_modified_after"}, self.workspace_list),
        }
        if path in simple:
            allowed, fn = simple[path]
            return fn(Request(params, allowed))
        if m := re.fullmatch(r"/api/2\.0/sql/warehouses/([^/]+)", path):
            Request(params, set())
            wid = urllib.parse.unquote(m.group(1))
            for w in self.data["warehouses"]:
                if w["id"] == wid:
                    return w
            raise missing(f"Warehouse {wid} does not exist.")
        if m := re.fullmatch(r"/api/2\.0/pipelines/([^/]+)", path):
            Request(params, set())
            return self.pipeline(urllib.parse.unquote(m.group(1)))
        if m := re.fullmatch(r"/api/2\.0/pipelines/([^/]+)/updates", path):
            return self.pipeline_updates(
                urllib.parse.unquote(m.group(1)), Request(params, {"max_results", "page_token", "until_update_id"})
            )
        if m := re.fullmatch(r"/api/2\.0/pipelines/([^/]+)/events", path):
            return self.pipeline_events(
                urllib.parse.unquote(m.group(1)), Request(params, {"max_results", "page_token", "order_by", "filter"})
            )
        if m := re.fullmatch(r"/api/2\.0/repos/(\d+)", path):
            Request(params, set())
            for r in self.data["repos"]:
                if r["id"] == int(m.group(1)):
                    return r
            raise missing(f"Repo {m.group(1)} does not exist.")
        raise ApiError(404, "ENDPOINT_NOT_FOUND", f"No API found for 'GET {path}'")


class Handler(BaseHTTPRequestHandler):
    """HTTP front end over ``Workspace``."""

    ws: Workspace

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("mock-databricks: " + fmt % args + "\n")

    def _send(self, status: int, body: Any, headers: dict[str, str] | None = None) -> None:
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(raw)

    def _error(self, err: ApiError) -> None:
        self._send(err.status, {"error_code": err.code, "message": err.message})

    def _oauth(self, raw: bytes) -> None:
        auth = self.headers.get("Authorization", "")
        form = urllib.parse.parse_qs(raw.decode())
        try:
            cid, secret = base64.b64decode(auth.removeprefix("Basic ")).decode().split(":", 1)
        except Exception:
            cid, secret = "", ""
        if form.get("grant_type") != ["client_credentials"] or form.get("scope") != ["all-apis"]:
            self._send(400, {"error": "invalid_request", "error_description": "grant_type/scope not supported"})
            return
        if (cid, secret) != (SP_ID, SP_SECRET):
            self._send(401, {"error": "invalid_client", "error_description": "Client authentication failed"})
            return
        with self.ws.lock:
            self.ws.issued += 1
            token = f"oauth-fixture-{self.ws.issued}"
            self.ws.tokens.add(token)
        self._send(200, {"access_token": token, "token_type": "Bearer", "expires_in": 3600, "scope": "all-apis"})

    def _handle(self, method: str) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        if path == "/__requests" and method == "GET":
            with self.ws.lock:
                self._send(200, list(self.ws.requests))
            return
        if path == "/__reset" and method == "POST":
            with self.ws.lock:
                self.ws.requests.clear()
                self.ws.seen_paths.clear()
            self._send(200, {})
            return
        params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        body: dict[str, Any] = {}
        if raw and method == "POST" and path != "/oidc/v1/token":
            try:
                body = json.loads(raw)
            except ValueError:
                self._error(ApiError(400, "MALFORMED_REQUEST", "Request body is not JSON"))
                return
        with self.ws.lock:
            self.ws.requests.append({"method": method, "path": path, "query": params, "body": body})
            first = path not in self.ws.seen_paths
            self.ws.seen_paths.add(path)
        if self.ws.flaky and first:
            self._send(
                429,
                {"error_code": "REQUEST_LIMIT_EXCEEDED", "message": "Too many requests. Please wait and retry."},
                {"Retry-After": "0"},
            )
            return
        if path == "/oidc/v1/token" and method == "POST":
            self._oauth(raw)
            return
        token = self.headers.get("Authorization", "").removeprefix("Bearer ")
        if token not in self.ws.tokens:
            self._send(401, {"error_code": "UNAUTHENTICATED", "message": "Credential was not sent or was invalid."})
            return
        try:
            self._send(200, self.ws.route(method, path, params, body))
        except ApiError as err:
            self._error(err)

    def do_GET(self) -> None:
        self._handle("GET")

    def do_POST(self) -> None:
        self._handle("POST")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--fixture", type=Path, default=FIXTURE)
    args = parser.parse_args()
    Handler.ws = Workspace(args.fixture)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
