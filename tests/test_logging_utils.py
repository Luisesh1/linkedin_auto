from __future__ import annotations

import json
import logging

from src.logging_utils import JsonFormatter


def test_json_formatter_includes_operational_context():
    formatter = JsonFormatter()
    record = logging.makeLogRecord(
        {
            "name": "tests.logger",
            "levelno": logging.INFO,
            "levelname": "INFO",
            "msg": "pipeline ready",
            "args": (),
            "event": "pipeline.ready",
            "session_id": "abc123",
        }
    )

    payload = json.loads(formatter.format(record))

    assert payload["message"] == "pipeline ready"
    assert payload["event"] == "pipeline.ready"
    assert payload["session_id"] == "abc123"
    assert "timestamp" in payload
    assert "thread" in payload
