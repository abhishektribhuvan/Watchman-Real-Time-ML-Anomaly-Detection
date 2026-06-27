import re

_LOG_PATTERN = re.compile(
    r'HTTP/[\d.]+"\s+(\d{3})\s+\d+\s+"[^"]*"\s+"[^"]*"\s+(\d+)\s*$'
)


def parse_log_line(line: str) -> tuple[int | None, int | None]:
    match = _LOG_PATTERN.search(line.strip())
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None
