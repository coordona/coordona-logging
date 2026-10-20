from importlib.metadata import version, PackageNotFoundError

from .batch_logger import SessionLogger
from .logging_config import build_logger
from .pipeline_logging import LoggingPipeline, run_sequential_pipeline

try:
    __version__ = version("coordona-logging")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = ["SessionLogger", "build_logger", "LoggingPipeline", "run_sequential_pipeline"]