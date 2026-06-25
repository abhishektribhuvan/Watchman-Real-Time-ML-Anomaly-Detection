"""
Shared utilities for the AIOps RCA Engine.
Centralizes common parsing logic used across training, API, and Kafka pipelines.
"""

import re
from itertools import islice


def parse_log_line(line: str):
    """
    Extracts (status_code, latency_ms) from a raw log string.
    
    Actual log format (Extended Common Log Format):
      IP - - [timestamp] "METHOD /path HTTP/1.x" STATUS BODY_SIZE "referer" "user-agent" LATENCY
    
    Example:
      148.5.169.251 - - [...] "DELETE /usr/admin HTTP/1.0" 200 5044 "http://..." "Mozilla/..." 1409
      -> returns (200, 1409)
    
    Returns (None, None) if the line doesn't match.
    """
    match = re.search(
        r'HTTP/[\d.]+"\s+(\d{3})\s+\d+\s+"[^"]*"\s+"[^"]*"\s+(\d+)\s*$',
        line.strip()
    )
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None


def skip_lines(file_handle, count: int):
    """
    Safely skip `count` lines from an open file handle.
    Returns the number of lines actually skipped (may be less than `count`
    if the file has fewer lines).
    """
    skipped = 0
    for _ in islice(file_handle, count):
        skipped += 1
    return skipped
