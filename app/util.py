from datetime import datetime, timezone

def datetime_to_epoch_ms(dt):
    # Asume dt es datetime (pydantic ya parseó). Si no tiene tzinfo asumimos UTC.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() * 1000)


def epoch_ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)

