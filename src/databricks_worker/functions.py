"""Table functions that take arguments: ``run_output`` and ``workspace_objects``."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg, Secret
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from .client import DATABRICKS_SECRET_TYPE, DatabricksError, NotFoundError
from .meta import column
from .scan import TSTZ, client_from, to_int, ts
from .tables import JOBS_API, all_run_tasks

# ----------------------------------------------------------------------- run_output

OUTPUT_SCHEMA = pa.schema(
    [
        column("run_id", pa.int64(), "The run id you passed.", nullable=False),
        column("task_run_id", pa.int64(), "Task run whose output this row holds.", nullable=False),
        column("task_key", pa.string(), "Task key, for a task of a multi-task run."),
        column("attempt_number", pa.int64(), "Attempt number of the task run, 0 for the first try."),
        column("life_cycle_state", pa.string(), "Life-cycle state of the task run."),
        column("result_state", pa.string(), "Result state of the task run, once terminated."),
        column("error", pa.string(), "Error message, when the task failed."),
        column("error_trace", pa.string(), "Error stack trace, cut to max_log_chars."),
        column("logs", pa.string(), "stdout of the task (spark_python / python_wheel tasks), cut to max_log_chars."),
        column("logs_truncated", pa.bool_(), "True when Databricks or max_log_chars cut the logs or trace."),
        column("notebook_result", pa.string(), "dbutils.notebook.exit() value; NULL unless include_result := true."),
        column("notebook_result_length", pa.int64(), "Length in characters of the notebook exit value."),
        column("notebook_result_truncated", pa.bool_(), "True when Databricks truncated the exit value."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class OutputArgs:
    """Arguments for ``run_output(run_id, include_result := false, max_log_chars := 65536)``."""

    run_id: Annotated[int, Arg(0, doc="A job run id (every task's output) or one task run id.", ge=1)]
    include_result: Annotated[
        bool,
        Arg(
            "include_result",
            default=False,
            doc="Return the notebook exit value text. Default false: only its length and truncation flag.",
        ),
    ]
    max_log_chars: Annotated[
        int,
        Arg("max_log_chars", default=65536, doc="Keep at most this many characters of logs and error_trace.", ge=0),
    ]


@dataclass(kw_only=True)
class OutputState(ArrowSerializableDataclass):
    """Cursor: task runs whose output is still to be read (filled on the first call)."""

    started: bool = False
    pending: list[int] = field(default_factory=list)


def _cut(text: Any, limit: int) -> tuple[str | None, bool]:
    if text is None:
        return None, False
    s = str(text)
    return (s[:limit], True) if len(s) > limit else (s, False)


def output_row(body: dict[str, Any], run_id: int, task_run_id: int, args: OutputArgs) -> dict[str, Any]:
    """One ``run_output`` row from a ``runs/get-output`` response."""
    meta = body.get("metadata") or {}
    state = meta.get("state") or {}
    nb = body.get("notebook_output") or {}
    result = nb.get("result")
    logs, logs_cut = _cut(body.get("logs"), args.max_log_chars)
    trace, trace_cut = _cut(body.get("error_trace"), args.max_log_chars)
    return {
        "run_id": run_id,
        "task_run_id": task_run_id,
        "task_key": meta.get("task_key"),
        "attempt_number": to_int(meta.get("attempt_number")),
        "life_cycle_state": state.get("life_cycle_state"),
        "result_state": state.get("result_state"),
        "error": body.get("error"),
        "error_trace": trace,
        "logs": logs,
        "logs_truncated": bool(body.get("logs_truncated")) or logs_cut or trace_cut,
        "notebook_result": result if args.include_result else None,
        "notebook_result_length": None if result is None else len(str(result)),
        "notebook_result_truncated": nb.get("truncated") if nb else None,
    }


@init_single_worker
class RunOutput(TableFunctionGenerator[OutputArgs, OutputState]):
    """Output of a job run: one row per task run (every attempt)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = OUTPUT_SCHEMA

    class Meta:
        """Function metadata; tags are filled in by ``worker.py``."""

        name = "run_output"
        tags: ClassVar[dict[str, str]] = {}
        examples: ClassVar[list[FunctionExample]] = []
        description = "Error, stack trace, logs and notebook exit metadata of every task of one Databricks job run"

    @classmethod
    def on_bind(
        cls,
        params: BindParams[OutputArgs],
        *,
        databricks: Annotated[dict[str, pa.Scalar[Any]] | None, Secret(DATABRICKS_SECRET_TYPE)] = None,
    ) -> BindResponse:
        """Return the fixed schema and ask DuckDB for the ``databricks`` secret."""
        return BindResponse(output_schema=OUTPUT_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[OutputArgs]) -> OutputState:
        """Start empty; the task runs are resolved on the first ``process`` call."""
        return OutputState()

    @classmethod
    def process(cls, params: ProcessParams[OutputArgs], state: OutputState, out: OutputCollector) -> None:
        """Emit one task run's output per call."""
        args = params.args
        client = client_from(params)
        if not state.started:
            state.started = True
            try:
                run = client.get(f"{JOBS_API}/runs/get", {"run_id": args.run_id})
            except NotFoundError as err:
                raise DatabricksError(f"run_output: run {args.run_id} does not exist") from err
            tasks = all_run_tasks(client, run)
            # Output is per task run; a job run's own id has none when it has tasks.
            state.pending = [int(t["run_id"]) for t in tasks] if tasks else [args.run_id]
        if not state.pending:
            out.finish()
            return
        task_run_id = state.pending.pop(0)
        body = client.get(f"{JOBS_API}/runs/get-output", {"run_id": task_run_id})
        row = output_row(body, args.run_id, task_run_id, args)
        schema = params.output_schema
        out.emit(pa.RecordBatch.from_pydict({f.name: [row[f.name]] for f in schema}, schema=schema))


# ---------------------------------------------------------------- workspace_objects

WORKSPACE_SCHEMA = pa.schema(
    [
        column("path", pa.string(), "Absolute workspace path of the object.", nullable=False),
        column("parent_path", pa.string(), "Directory that was listed to find it."),
        column(
            "object_type",
            pa.string(),
            "NOTEBOOK, DIRECTORY, FILE, LIBRARY, REPO or DASHBOARD.",
        ),
        column("object_id", pa.int64(), "Object id."),
        column("language", pa.string(), "Notebook language: PYTHON, SQL, SCALA or R."),
        column("created_at", TSTZ, "When the object was created."),
        column("modified_at", TSTZ, "When the object was last modified."),
        column("size", pa.int64(), "File size in bytes (files only)."),
        column("resource_id", pa.string(), "Unique resource id of the object."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class WorkspaceArgs:
    """Arguments for ``workspace_objects(path, recursive := false)``."""

    path: Annotated[
        str, Arg(0, doc="Absolute workspace directory, e.g. '/Users/alice@example.com' or '/'.", pattern="^/")
    ]
    recursive: Annotated[
        bool,
        Arg("recursive", default=False, doc="Descend into sub-directories and Git folders. Default false."),
    ]


@dataclass(kw_only=True)
class WorkspaceState(ArrowSerializableDataclass):
    """Cursor: directories still to list (breadth first)."""

    started: bool = False
    queue: list[str] = field(default_factory=list)


@init_single_worker
class WorkspaceObjects(TableFunctionGenerator[WorkspaceArgs, WorkspaceState]):
    """Notebooks, files and folders under a workspace path, one directory per batch."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = WORKSPACE_SCHEMA

    class Meta:
        """Function metadata; tags are filled in by ``worker.py``."""

        name = "workspace_objects"
        tags: ClassVar[dict[str, str]] = {}
        examples: ClassVar[list[FunctionExample]] = []
        description = "Notebooks, files, folders and Git folders under a Databricks workspace path"

    @classmethod
    def on_bind(
        cls,
        params: BindParams[WorkspaceArgs],
        *,
        databricks: Annotated[dict[str, pa.Scalar[Any]] | None, Secret(DATABRICKS_SECRET_TYPE)] = None,
    ) -> BindResponse:
        """Return the fixed schema and ask DuckDB for the ``databricks`` secret."""
        return BindResponse(output_schema=WORKSPACE_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[WorkspaceArgs]) -> WorkspaceState:
        """Start with the requested path."""
        return WorkspaceState()

    @classmethod
    def process(cls, params: ProcessParams[WorkspaceArgs], state: WorkspaceState, out: OutputCollector) -> None:
        """List one directory per call; queue sub-directories when recursive."""
        args = params.args
        if not state.started:
            state.started = True
            state.queue = [args.path.rstrip("/") or "/"]
        client = client_from(params)
        while state.queue:
            directory = state.queue.pop(0)
            try:
                body = client.get("/api/2.0/workspace/list", {"path": directory})
            except NotFoundError as err:
                if directory == (args.path.rstrip("/") or "/"):
                    raise DatabricksError(f"workspace_objects: path {directory!r} does not exist") from err
                continue
            objects = list(body.get("objects") or [])
            if args.recursive:
                state.queue.extend(
                    str(o["path"]) for o in objects if o.get("object_type") in ("DIRECTORY", "REPO") and o.get("path")
                )
            if not objects:
                continue
            rows = [
                {
                    "path": o["path"],
                    "parent_path": directory,
                    "object_type": o.get("object_type"),
                    "object_id": to_int(o.get("object_id")),
                    "language": o.get("language"),
                    "created_at": ts(o.get("created_at")),
                    "modified_at": ts(o.get("modified_at")),
                    "size": to_int(o.get("size")),
                    "resource_id": o.get("resource_id"),
                }
                for o in objects
            ]
            schema = params.output_schema
            out.emit(pa.RecordBatch.from_pydict({f.name: [r[f.name] for r in rows] for f in schema}, schema=schema))
            return
        out.finish()
