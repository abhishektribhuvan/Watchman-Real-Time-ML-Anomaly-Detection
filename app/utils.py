"""Shared log-parsing utilities for the AIOps RCA Engine."""

import re

# Pre-compiled regex for performance — matches the extended common log format:
#   IP - - [timestamp] "METHOD /path HTTP/1.x" STATUS BODY_SIZE "referer" "user-agent" LATENCY
_LOG_PATTERN = re.compile(
    r'HTTP/[\d.]+"\s+(\d{3})\s+\d+\s+"[^"]*"\s+"[^"]*"\s+(\d+)\s*$'
)


def parse_log_line(line: str) -> tuple[int | None, int | None]:
    """Extract (status_code, latency_ms) from a raw log string.

    Returns (None, None) if the line doesn't match the expected format.
    """
    match = _LOG_PATTERN.search(line.strip())
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None
