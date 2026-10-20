"""Drop-in logging client for integration projects.

This is the piece that stops each integration project from calling the
logging API once per function call. Instead, records are collected in
memory and sent to the API in batches, either once enough have piled up
or once a bit of time has passed, whichever comes first. This batching
behavior is the same regardless of mode below — mode only controls what,
if anything, gets printed to the console.

--------------------------------------------------------------------------
MODES: "sequential" vs "parallel"
--------------------------------------------------------------------------
Different integration projects run their work differently, and that
affects whether it's safe to print live progress to the console:

  "sequential" (default for projects like the Waverly project, where main.py runs one function after
  another in a single process, start to finish):
      Only one function is ever running at any given moment, so it's
      safe to clear the screen and print a running results table after
      every tracked call — this mirrors the old `timer` decorator's
      behavior from base_orchestrator.py, just routed through this
      shared module instead of being duplicated per project.

  "parallel" (default for projects like sf-to-snowflake, where many
  units of work run at the same time — e.g. Azure Durable Functions
  fanning out one activity per table):
      Multiple pieces of code may be tracking function calls at the
      exact same moment, possibly on different machines. If each one
      tried to clear the screen and print its own table, the output
      would come out garbled and interleaved, and in a cloud runtime
      like Azure Functions there's no real terminal to "clear" anyway.
      So this mode never prints per-call. Instead, call the static
      method `SessionLogger.print_summary(records)` ONCE, after all the
      parallel work has finished (e.g. right after
      `context.task_all(tasks)` in a Durable Functions orchestrator),
      to print one clean summary table.

Either mode still buffers records in memory and sends them to the
logging API in bulk — mode never changes the API-call behavior, only
the console-printing behavior.
"""
from __future__ import annotations
import json, logging, os, threading, time, traceback, uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("batch_logger")

# The two supported values for the `mode` parameter below.
VALID_MODES = ("sequential", "parallel")


def _now() -> str:
    # Timestamps are always generated in UTC and formatted consistently,
    # so the API and database never have to guess about time zones.
    return datetime.now(timezone.utc).isoformat()


class SessionLogger:
    def __init__(self, project_id: str, api_base_url: str, api_key: str,
                 mode: str = "parallel",
                 flush_threshold: int = 50, flush_interval_seconds: int = 30,
                 fallback_dir: str = "failed_batches"):
        if mode not in VALID_MODES:
            raise ValueError(f"mode must be one of {VALID_MODES}, got {mode!r}")
        self.mode = mode

        self.project_id = project_id
        self.api_base_url = api_base_url.rstrip("/")
        self.api_key = api_key
        self.flush_threshold = flush_threshold
        self.flush_interval_seconds = flush_interval_seconds
        # Where batches get written if sending them to the API fails outright,
        # so nothing is silently lost.
        self.fallback_dir = Path(fallback_dir)

        self._session_id: Optional[str] = None
        # Set by track()'s wrapper while a tracked function is running, so
        # log_note() can attach a non-critical error to the right
        # session_detail row without the caller having to pass an id
        # around. Only one tracked function runs at a time per instance
        # (sequential mode: one process; parallel mode: one activity per
        # instance via run_tracked), so a plain attribute is enough — no
        # thread-local needed.
        self._current_detail_id: Optional[str] = None
        self._detail_buffer: list[dict] = []
        self._error_buffer: list[dict] = []
        # Protects the two buffers above, since the background flush thread
        # and the calling code can both touch them at the same time.
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._flush_thread: Optional[threading.Thread] = None

        # Only used in "sequential" mode. Keeps a running list of every
        # record tracked so far in this process, purely so the console
        # summary can show cumulative progress (not just the latest call).
        # This is separate from _detail_buffer, which gets emptied on every
        # flush() — we don't want the on-screen table to lose earlier rows
        # just because they've already been sent to the API.
        self._display_records: list[dict] = []

        self._http = requests.Session()
        # Automatically retries a failed send a few times with backoff before
        # giving up and falling back to writing the batch to disk.
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self._http.mount("https://", HTTPAdapter(max_retries=retries))
        self._http.headers.update({"X-API-Key": self.api_key, "Content-Type": "application/json"})

    @contextmanager
    def session(self):
        # Wrap a job's main run in "with logger.session():" to get
        # automatic start/end tracking and a guaranteed final flush.
        # This is meant for "sequential" mode — a single process running
        # start to finish. It's not used by "parallel"-style integrations
        # (e.g. Azure Durable Functions), where each unit of work runs as
        # its own isolated activity and can't share this object's state;
        # those instead post session start/end directly via _post_session()
        # from small dedicated activities.
        self._session_id = str(uuid.uuid4())
        start = _now()
        self._post_session(start_datetime=start, end_datetime=start, status="Running")

        # Start the background thread that flushes on a timer, independent
        # of whether the buffer has filled up yet.
        self._stop_event.clear()
        self._flush_thread = threading.Thread(target=self._background_flush_loop, daemon=True)
        self._flush_thread.start()

        status = "Completed"
        try:
            yield self
        except Exception:
            # If the wrapped code raises, record the session as failed but
            # still let the original exception propagate normally afterward.
            status = "Failed"
            raise
        finally:
            self._stop_event.set()
            if self._flush_thread:
                self._flush_thread.join(timeout=self.flush_interval_seconds + 5)
            # One last flush to catch anything buffered since the background
            # thread's last run, so nothing gets left behind when the job exits.
            self.flush()
            end = _now()
            self._post_session(start_datetime=start, end_datetime=end, status=status)

    def _post_session(self, start_datetime, end_datetime, status):
        body = {"id": self._session_id, "project_id": self.project_id,
                "start_datetime": start_datetime, "end_datetime": end_datetime, "status": status}
        self._post("/api/v1/session", body)

    # ----------------------------------------------------------------
    # "parallel" mode helpers
    # ----------------------------------------------------------------
    # The three methods below (log_session_start, log_session_end,
    # run_tracked) exist so "parallel"-style projects (e.g. an Azure
    # Durable Functions orchestrator fanning out one activity per table)
    # don't each have to hand-write the same session-start/session-end/
    # track-wrap-flush boilerplate in their own function_app.py. Each
    # method builds/uses a fresh SessionLogger instance — that's still
    # required, since isolated activities can't share one instance's
    # memory — but the *logic* of what to do with that instance now lives
    # here once, instead of being copy-pasted into every project.
    #
    # Typical usage inside a project's function_app.py:
    #
    #   @app.activity_trigger(input_name="payload")
    #   def activity_log_session_start(payload):
    #       build_logger().log_session_start(payload["session_id"], payload["start_time"])
    #
    #   @app.activity_trigger(input_name="payload")
    #   def activity_log_session_end(payload):
    #       build_logger().log_session_end(payload["session_id"], payload["start_time"], payload["status"])
    #
    #   @app.activity_trigger(input_name="payload")
    #   def activity_process_table(payload):
    #       table = payload["table"]
    #       def _run_table():
    #           ... real fetch/upload/load logic for this table ...
    #       result = build_logger().run_tracked(table, _run_table, payload["session_id"])
    #       result["table"] = table
    #       return result
    #
    #   @app.activity_trigger(input_name="results")
    #   def activity_print_summary(results):
    #       SessionLogger.print_summary_safe(results, title="Pipeline Summary")
    # ----------------------------------------------------------------

    def log_session_start(self, session_id: str, start_time: str):
        """
        Posts the initial "Running" row for a pipeline run. Meant to be
        called from a small dedicated activity at the very start of a
        "parallel"-mode orchestrator, since the orchestrator itself can't
        make HTTP calls directly and there's no single logger instance
        shared across the whole run in this mode.

        Wrapped in try/except so that a logging problem (anything beyond
        what _post_session's own retry/fallback logic already covers)
        can never stop or fail the real data pipeline.
        """
        self._session_id = session_id
        try:
            self._post_session(start_datetime=start_time, end_datetime=start_time, status="Running")
        except Exception:
            logger.exception("Failed to log session start (session_id=%s)", session_id)

    def log_session_end(self, session_id: str, start_time: str, status: str):
        """
        Posts the final Completed/Failed row for a pipeline run. Meant to
        be called from a small dedicated activity, on EVERY exit path of
        a "parallel"-mode orchestrator (success and every failure branch)
        — not just the happy path, otherwise a failed run never gets
        marked Failed and just looks stuck at Running forever.
        """
        self._session_id = session_id
        try:
            self._post_session(start_datetime=start_time, end_datetime=_now(), status=status)
        except Exception:
            logger.exception("Failed to log session end (session_id=%s)", session_id)

    def run_tracked(self, name: str, func, session_id: str) -> dict:
        """
        Runs one unit of work (e.g. one table) for "parallel" mode, doing
        everything track()-based logging needs in one call:
          1. attaches session_id to this (freshly built) logger instance
          2. renames `func` to `name` so the logged row is labeled
             meaningfully (e.g. "Account") instead of a generic inner
             function name
          3. wraps it with track() and calls it, so a session_detail
             record (+ a session_error record, on failure) gets buffered
          4. catches any exception so the caller gets a plain result dict
             back instead of having to repeat the same try/except
          5. flushes in `finally`, since a short-lived activity has no
             background thread to send buffered records on its own

        Returns {"success": bool, "error": str | None}. The caller merges
        in whatever extra fields it needs (e.g. "table", "timestamp").
        """
        self._session_id = session_id
        func.__name__ = name
        tracked = self.track(func)
        try:
            tracked()
            return {"success": True, "error": None}
        except Exception as e:
            logger.exception("run_tracked failed for %s", name)
            return {"success": False, "error": str(e)}
        finally:
            try:
                self.flush()
            except Exception:
                logger.exception("Failed to flush logs for %s", name)

    def track(self, func):
        # Decorator: put this on any function you want timed and logged.
        # The function's own return value and exceptions behave exactly as
        # they would without the decorator; logging happens alongside, not
        # instead of, the normal call. Batching/API behavior is identical
        # in both modes — only the optional console print at the end
        # differs, based on self.mode.
        @wraps(func)
        def wrapper(*args, **kwargs):
            detail_id = str(uuid.uuid4())
            # Exposed for the duration of the call so log_note(), called
            # from inside func, knows which session_detail row to attach
            # a non-critical error to.
            self._current_detail_id = detail_id
            start = time.monotonic()
            start_iso = _now()
            success = True
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                success = False
                self._log_error(detail_id, exc)
                # Re-raise so the caller's own error handling still runs
                # as if this decorator wasn't there.
                raise
            finally:
                self._current_detail_id = None
                end_iso = _now()
                duration = time.monotonic() - start
                record = {
                    "id": detail_id, "session_id": self._session_id, "function_name": func.__name__,
                    "start_time": start_iso, "end_time": end_iso, "duration": round(duration, 4),
                    "success": success, "status": "Completed" if success else "Failed",
                }
                self._buffer_detail(record)

                # "sequential" mode only: print a live, cumulative results
                # table after every tracked call — safe here because only
                # one function is ever running at a time in this mode.
                # "parallel" mode deliberately skips this: see the module
                # docstring and print_summary() below for why, and for the
                # alternative (a single summary printed once, at the end).
                if self.mode == "sequential":
                    self._print_live_summary(record)
        return wrapper

    def _print_live_summary(self, record: dict):
        # Appends this call's record to the running on-screen list and
        # reprints the whole table so far. Only ever called in
        # "sequential" mode (see track() above).
        self._display_records.append(record)
        try:
            from tabulate import tabulate
        except ImportError:
            # tabulate is an optional dependency, only needed for the
            # console-printing feature. If it's missing, skip printing
            # rather than breaking the actual integration.
            logger.warning("tabulate is not installed; skipping console summary. "
                            "Add 'tabulate' to requirements.txt to enable it.")
            return

        display_cols = ["function_name", "start_time", "end_time", "duration", "status"]
        rows = [{k: r.get(k) for k in display_cols} for r in self._display_records]

        os.system("cls" if os.name == "nt" else "clear")
        print("Execution Results")
        print(tabulate(rows, headers="keys", tablefmt="outline"))

    @staticmethod
    def print_summary(records: list[dict], title: str = "Pipeline Summary"):
        """
        For "parallel" mode: call this ONCE, after all parallel work has
        finished, passing in whatever result info you collected from each
        unit of work (e.g. the `results` list returned by
        `context.task_all(tasks)` in a Durable Functions orchestrator).

        This is a staticmethod (not tied to one SessionLogger instance)
        because in "parallel" mode, each unit of work typically builds its
        own logger instance (they may run on different machines and can't
        share memory) — there's no single instance that has seen every
        record, so the caller collects results itself and hands them here
        to print one combined table at the end.

        Example (inside a Durable Functions orchestrator, after fan-in):
            results = yield context.task_all(tasks)
            SessionLogger.print_summary(results)
        """
        try:
            from tabulate import tabulate
        except ImportError:
            logger.warning("tabulate is not installed; skipping console summary. "
                            "Add 'tabulate' to requirements.txt to enable it.")
            return
        summary_text = f"\n{title}\n{tabulate(records, headers='keys', tablefmt='outline')}"
        # Both print() and logging.info(): print() covers local/terminal
        # runs (e.g. someone testing this outside Azure), while
        # logging.info() is what actually shows up reliably in the Azure
        # Functions log stream / Application Insights — plain print()
        # output isn't always captured there the way logging module output
        # is, so relying on print() alone risks the summary silently
        # never appearing even once tabulate is installed and this code runs.
        try:
            print(summary_text)
        except UnicodeEncodeError:
            # Some consoles (notably Windows' default cp1252 terminal, used
            # by `func start` for local runs) can't print certain Unicode
            # characters — e.g. arrows or special symbols in a title. Rather
            # than let a display-only problem crash the caller, fall back
            # to an ASCII-safe rendering that drops/replaces those characters.
            print(summary_text.encode("ascii", errors="replace").decode("ascii"))
        logger.info(summary_text)

    @staticmethod
    def print_summary_safe(records: list[dict], title: str = "Pipeline Summary"):
        """
        Same as print_summary(), wrapped in try/except. A display/logging
        problem should never fail the activity that calls this or take
        down the orchestration — the real data pipeline already succeeded
        or failed independently of whether this summary prints. Use this
        version from the dedicated "print the summary once" activity in a
        "parallel"-mode project, instead of repeating the try/except in
        every project's own function_app.py.
        """
        try:
            SessionLogger.print_summary(records, title=title)
        except Exception:
            logger.exception("Failed to print pipeline summary")

    def log_note(self, message: str, level: str = "Warning", session_detail_id: Optional[str] = None):
        """
        Record a non-critical event (level "Warning" or "Info") against
        the currently-running tracked function, WITHOUT raising an
        exception and WITHOUT marking that function's session_detail row
        as Failed. This is for "we noticed something worth logging, but
        the function still succeeded" cases — retried a request, fell
        back to a default, skipped a row, etc.

        Call it from inside a function wrapped by track() or passed to
        run_tracked(); it automatically attaches to whichever
        session_detail row is currently executing on this logger
        instance. Pass session_detail_id explicitly only if you're
        logging from outside that call (rare).

        For actual failures, don't call this — let the exception
        propagate and track() will log it as "Critical" automatically.
        """
        if level not in ("Warning", "Info", "Critical"):
            raise ValueError('level must be "Warning", "Info", or "Critical"')
        detail_id = session_detail_id or self._current_detail_id
        if detail_id is None:
            logger.warning(
                "log_note() called with no active session_detail_id; dropping note: %s", message
            )
            return
        self._buffer_error({
            "id": str(uuid.uuid4()), "session_detail_id": detail_id, "timestamp": _now(),
            "error_level": level, "error_message": message, "stacktrace": None,
        })

    def _log_error(self, session_detail_id, exc, level="Critical"):
        self._buffer_error({
            "id": str(uuid.uuid4()), "session_detail_id": session_detail_id, "timestamp": _now(),
            "error_level": level, "error_message": str(exc),
            "stacktrace": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        })

    def _buffer_detail(self, record):
        with self._lock:
            self._detail_buffer.append(record)
            # Check the threshold while still holding the lock, so the
            # decision is based on an accurate count.
            should_flush = len(self._detail_buffer) >= self.flush_threshold
        if should_flush:
            self.flush()

    def _buffer_error(self, record):
        with self._lock:
            self._error_buffer.append(record)

    def _background_flush_loop(self):
        # Wakes up periodically and flushes, so records don't sit in memory
        # too long even if the buffer never reaches the size threshold.
        # Stops as soon as the stop event is set during shutdown.
        # Only ever started from session() — i.e. only relevant to
        # "sequential"-style, single-process usage.
        while not self._stop_event.wait(self.flush_interval_seconds):
            self.flush()

    def flush(self):
        # Swap out the buffers under the lock, then send whatever was
        # collected without holding the lock during the network call.
        with self._lock:
            details, self._detail_buffer = self._detail_buffer, []
            errors, self._error_buffer = self._error_buffer, []
        if details:
            self._post_bulk("/api/v1/session-detail/bulk", details)
        if errors:
            self._post_bulk("/api/v1/session-error/bulk", errors)

    def _post(self, path, body):
        try:
            resp = self._http.post(f"{self.api_base_url}{path}", data=json.dumps(body), timeout=15)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error("Failed to POST %s: %s", path, e)
            self._write_fallback(path, {"records": [body]})

    def _post_bulk(self, path, records):
        try:
            resp = self._http.post(f"{self.api_base_url}{path}", data=json.dumps({"records": records}), timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error("Failed to flush %d records to %s: %s", len(records), path, e)
            self._write_fallback(path, {"records": records})

    def _write_fallback(self, path, body):
        # Last resort: if the API can't be reached even after retries,
        # write the batch to a local file instead of dropping it, so it
        # can be inspected or replayed later.
        self.fallback_dir.mkdir(parents=True, exist_ok=True)
        filename = self.fallback_dir / f"{path.strip('/').replace('/', '_')}_{uuid.uuid4()}.json"
        with open(filename, "w") as f:
            json.dump(body, f)
        logger.warning("Wrote failed batch to %s for later replay", filename)