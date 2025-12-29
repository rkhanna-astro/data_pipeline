from datetime import datetime
from typing import Optional

SUPPORTED_FORMATS = [
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%dT%H:%M:%S",
]

def parse_timestamp(value: Optional[str]) -> Optional[str]:
    if not value:
        return None

    value = value.strip()

    # Epoch seconds
    if value.isdigit():
        return datetime.fromtimestamp(int(value)).isoformat()

    # Try known formats
    for fmt in SUPPORTED_FORMATS:
        try:
            return datetime.strptime(value, fmt).isoformat()
        except ValueError:
            continue

    raise ValueError(f"Invalid timestamp format: {value}")
