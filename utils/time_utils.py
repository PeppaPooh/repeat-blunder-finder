from datetime import date, datetime, timezone
from typing import Optional

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def human_timestamp(dt: datetime) -> str:
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()

def yyyy_mm_dd_to_epoch_ms(date_input) -> int:
    """
    Accepts:
    - "YYYY-MM-DD" string
    - datetime.date
    - datetime.datetime
    """
    if isinstance(date_input, datetime):
        dt = date_input
    elif isinstance(date_input, date):
        dt = datetime.combine(date_input, datetime.min.time())
    elif isinstance(date_input, str):
        dt = datetime.strptime(date_input, "%Y-%m-%d")
    else:
        raise TypeError(f"Unsupported date type: {type(date_input)}")

    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def epoch_ms_to_iso_utc(epoch_ms: Optional[int]) -> Optional[str]:
    if epoch_ms is None:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).isoformat()

def parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)

def timestamp_affix() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
