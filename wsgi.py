from __future__ import annotations

from app import app, initialize_runtime

initialize_runtime(start_scheduler=True)

application = app
