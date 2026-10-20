"""
Drop-in integration helpers built on top of batch_logger.SessionLogger.
This is the ONE file (alongside batch_logger.py + logging_config.py) that
every project imports, regardless of which mode it runs in:

  - PARALLEL projects (Azure Durable Functions, fan-out-per-table,
    e.g. sf-to-snowflake)         -> use LoggingPipeline, below.
  - SEQUENTIAL projects (single process, main.py runs one function after
    another start to finish, e.g. Waverly-style)
                                   -> use run_sequential_pipeline, below.

Both entry points reduce integration to the same shape: one import, one
or two calls. Nothing project-specific lives in this file — it only ever
takes a `build_logger` callable (from the project's own logging_config.py)
and the project's own work as plain functions.

===========================================================================
PARALLEL projects
===========================================================================
Problem this solves
--------------------
Per-table logging is already 1 line: build_logger().run_tracked(...).
What actually gets copy-pasted into every new project's function_app.py is
the SESSION lifecycle boilerplate:
  - three extra activity_trigger functions (start / end / summary)
  - a session_id generated in the orchestrator
  - 3-4 separate `yield context.call_activity(...)` calls threaded through
    the orchestrator at the right points, on every exit path

None of that boilerplate is project-specific — it only needs `context`,
`run_start_time`, and a `build_logger` callable. So it lives here once,
and each project imports it and makes exactly two calls in its
orchestrator (`pipeline.start(...)` / `pipeline.finish(...)`), plus one
line at module load to attach it to their DFApp.

Usage in a project's function_app.py
--------------------------------------
    from logging_config import build_logger
    from pipeline_logging import LoggingPipeline

    durable_app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

    # One line, at import time: registers the 3 boilerplate activities
    # (activity_log_session_start / _end / _print_summary) on durable_app.
    pipeline = LoggingPipeline(durable_app, build_logger, title="My Pipeline Summary")

    @durable_app.orchestration_trigger(context_name="context")
    def orchestrator_pipeline(context):
        payload = context.get_input()
        run_start_time = payload["run_start_time"]

        # Call #1: posts the "Running" session row, returns a session_id
        # that survives Durable Functions replay (built from context.new_uuid()).
        session_id = yield from pipeline.start(context, run_start_time)

        tasks = [
            context.call_activity("activity_process_table", {**t, "session_id": session_id})
            for t in build_table_payloads(payload)
        ]
        results = yield context.task_all(tasks)

        failed = [r["table"] for r in results if not r["success"]]

        # Call #2: prints the one-time summary + posts Completed/Failed.
        yield from pipeline.finish(context, results, session_id, run_start_time, failed=bool(failed))

        return {"status": "partial_failure" if failed else "success", "failed": failed}

Each table's own activity is unchanged and still just:
    result = build_logger().run_tracked(table, _run_table, payload["session_id"])
"""
from __future__ import annotations
from .batch_logger import SessionLogger


class LoggingPipeline:
    def __init__(self, durable_app, build_logger_fn, title: str = "Pipeline Summary"):
        """
        durable_app:      the project's df.DFApp instance.
        build_logger_fn:  the project's build_logger() from logging_config.py
                           (kept as a callable, not a single instance, since
                           each activity needs its own fresh logger — see
                           batch_logger.py's "parallel mode helpers" section).
        title:            label used on the printed summary table.
        """
        self._build_logger = build_logger_fn
        self._title = title
        self._register_activities(durable_app)

    def _register_activities(self, durable_app):
        # Registered here, once, instead of hand-written in every project's
        # function_app.py. Function names match what call_activity() below
        # references, so they must stay in sync with start()/finish().
        @durable_app.activity_trigger(input_name="payload")
        def activity_log_session_start(payload: dict) -> None:
            self._build_logger().log_session_start(payload["session_id"], payload["start_time"])

        @durable_app.activity_trigger(input_name="payload")
        def activity_log_session_end(payload: dict) -> None:
            self._build_logger().log_session_end(
                payload["session_id"], payload["start_time"], payload["status"]
            )

        @durable_app.activity_trigger(input_name="results")
        def activity_print_summary(results: list) -> None:
            SessionLogger.print_summary_safe(results, title=self._title)

    def start(self, context, run_start_time: str):
        """
        Call as: session_id = yield from pipeline.start(context, run_start_time)

        Generates the session_id via context.new_uuid() (not uuid.uuid4()),
        since orchestrator code is replayed by the Durable Functions
        framework and needs a deterministic id across replays. Posts the
        initial "Running" session row and returns the id so the caller can
        pass it into each table's task payload.
        """
        session_id = str(context.new_uuid())
        yield context.call_activity(
            "activity_log_session_start",
            {"session_id": session_id, "start_time": run_start_time},
        )
        return session_id

    def finish(self, context, results: list, session_id: str, run_start_time: str, failed: bool):
        """
        Call as: yield from pipeline.finish(context, results, session_id, run_start_time, failed=...)

        Prints the one-time summary (routed through its own activity, since
        only yielded call_activity calls are guaranteed to run exactly once
        under replay — see batch_logger.py) and posts the final
        Completed/Failed session row. Call this on EVERY exit path of the
        orchestrator, not just the happy path.
        """
        yield context.call_activity("activity_print_summary", results)
        yield context.call_activity(
            "activity_log_session_end",
            {
                "session_id": session_id,
                "start_time": run_start_time,
                "status": "Failed" if failed else "Completed",
            },
        )


# ===========================================================================
# SEQUENTIAL projects
# ===========================================================================
def run_sequential_pipeline(build_logger_fn, steps: list[tuple[str, callable]]):
    """
    Runs a list of (name, func) steps, one after another in this process,
    with full session + per-step tracking, in a single call.

    This replaces the current sequential pattern of:
        with build_logger().session() as logger:
            @logger.track
            def step_one(): ...
            @logger.track
            def step_two(): ...
            step_one()
            step_two()

    which needs a manual @logger.track edit on every function you add.
    Instead:
        from pipeline_logging import run_sequential_pipeline
        from logging_config import build_logger
        import my_steps

        run_sequential_pipeline(build_logger, steps=[
            ("extract_account", my_steps.extract_account),
            ("load_account",    my_steps.load_account),
            ("extract_contact", my_steps.extract_contact),
            ("load_contact",    my_steps.load_contact),
        ])

    That's the entire integration: one import, one call. Adding, removing,
    or reordering a step is a one-line edit to the `steps` list — no
    decorators to add or remove elsewhere in the file.

    Behavior, matching what session()/track() already do under the hood:
      - Opens one session ("Running" -> "Completed"/"Failed"), same as
        `with build_logger().session():` today.
      - Each step is timed and logged individually via track(), same as
        `@logger.track` today — including per-step error capture and the
        live console table (sequential mode prints after every step; see
        batch_logger.py's module docstring for why that's safe here but
        not in parallel mode).
      - If a step raises, the exception propagates after being logged
        (steps after it do not run), matching @logger.track's behavior of
        never swallowing the original error.
      - Final flush + session close happens automatically on the way out,
        success or failure, via session()'s own finally block.

    Returns the list of return values from each step, in order, so callers
    that need a step's result can still get it back.
    """
    logger = build_logger_fn()
    results = []
    with logger.session():
        for name, func in steps:
            func.__name__ = name
            tracked = logger.track(func)
            results.append(tracked())
    return results
