# coordona-logging

Batched session and function logging client for Coordona integration projects.

## Install

```
coordona-logging @ git+https://github.com/coordona/coordona-logging.git@v0.1.0
```

Add `[console]` for printed summary tables:

```
coordona-logging[console] @ git+https://github.com/coordona/coordona-logging.git@v0.1.0
```

## Environment variables

- `PROJECT_ID`
- `LOGGING_API_BASE_URL`
- `LOGGING_API_KEY`
- `LOGGING_FALLBACK_DIR` (optional)

## Sequential project

```python
from functools import partial
from coordona_logging import build_logger, run_sequential_pipeline

run_sequential_pipeline(partial(build_logger, "sequential"), steps=[
    ("step_one", step_one),
    ("step_two", step_two),
])
```

## Parallel project (Azure Durable Functions)

```python
from functools import partial
from coordona_logging import build_logger, LoggingPipeline

build_logger_parallel = partial(build_logger, "parallel")
pipeline = LoggingPipeline(durable_app, build_logger_parallel, title="My Pipeline Summary")
```