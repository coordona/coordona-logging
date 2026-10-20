"""Builds a SessionLogger from environment variables.

Required environment variables (local.settings.json locally, Application
Settings in Azure):
    LOGGING_API_BASE_URL   e.g. https://your-logging-api.azurewebsites.net
    LOGGING_API_KEY        must match API_KEY on the logging API side
    PROJECT_ID             exact id of the row in dbo.project (e.g. "PRJ-001")

Optional:
    LOGGING_FALLBACK_DIR   default /tmp/failed_batches
"""
import os
from .batch_logger import SessionLogger


def build_logger(mode: str) -> SessionLogger:
    """mode must be "sequential" or "parallel" (see batch_logger.py)."""
    return SessionLogger(
        project_id=os.environ["PROJECT_ID"],
        api_base_url=os.environ["LOGGING_API_BASE_URL"],
        api_key=os.environ["LOGGING_API_KEY"],
        mode=mode,
        # /tmp is the only writable directory in Azure Functions' default
        # filesystem, so the fallback-to-disk safety net has somewhere to write.
        fallback_dir=os.environ.get("LOGGING_FALLBACK_DIR", "/tmp/failed_batches"),
    )