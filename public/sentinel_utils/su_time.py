import re

try:
    import su_constants as constants
except ImportError:
    constants = fused.load("su_constants")

_NOW = re.compile(r"^NOW(?:([+-])(\d+)(SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR)S?)?$")
_DAY = re.compile(r"^\d{4}-?\d{2}-?\d{2}$")
_FORMS = "'YYYY-MM-DD', 'YYYYMMDD', an ISO time, 'NOW', 'NOW-30DAYS', 'NOW+2HOURS', a date or a datetime"
_FREQS = {"day": "D", "week": "W-MON", "month": "MS", "quarter": "QS", "season": "QS-DEC", "year": "YS"}


@fused.udf
def udf():
    return api()


def api():
    """Returns a DataFrame of the public functions: name, signature, doc."""
    return constants.api_table(globals())


def parse_time(value):
    """Returns a UTC pandas Timestamp for a date, a datetime, an ISO string, 'YYYYMMDD' or 'NOW-30DAYS'."""
    import pandas as pd

    if isinstance(value, str):
        text = value.strip().upper()
        match = _NOW.match(text)
        if match:
            now = pd.Timestamp.now(tz="UTC")
            sign, number, unit = match.groups()
            if not sign:
                return now
            offset = pd.DateOffset(**{unit.lower() + "s": int(number)})
            return now + offset if sign == "+" else now - offset
        if _DAY.match(text):
            text = text.replace("-", "")
            text = f"{text[:4]}-{text[4:6]}-{text[6:]}"
        value = text
    if value is None or isinstance(value, bool):
        raise ValueError(f"time must be {_FORMS}, got {value!r}")
    try:
        ts = pd.Timestamp(value)
    except (TypeError, ValueError):
        raise ValueError(f"time must be {_FORMS}, got {value!r}") from None
    if pd.isna(ts):
        raise ValueError(f"time must be {_FORMS}, got {value!r}")
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def parse_time_interval(date):
    """Returns (start, end) UTC Timestamps; a day covers 00:00 to 23:59:59.999, None means the last 30 days."""
    import pandas as pd

    if date is None:
        today = pd.Timestamp.now(tz="UTC").normalize()
        return today - pd.Timedelta(days=30), _day_end(today)
    if isinstance(date, (tuple, list)):
        if len(date) != 2:
            raise ValueError(f"date must be one time or a (start, end) pair, got {date!r}")
        start, end = parse_time(date[0]), parse_time(date[1])
        if _is_day(date[1]):
            end = _day_end(end)
    else:
        start = parse_time(date)
        end = _day_end(start) if _is_day(date) else start
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    return start, end


def serialize_time(value):
    """Returns 'YYYY-MM-DD' for a day, an ISO 8601 UTC string ('...Z') for a time; a tuple gives a tuple."""
    if isinstance(value, (tuple, list)):
        return tuple(serialize_time(v) for v in value)
    ts = parse_time(value)
    if _is_day(value):
        return ts.strftime("%Y-%m-%d")
    return ts.isoformat().replace("+00:00", "Z")


def filter_times(timestamps, time_difference):
    """Returns the sorted UTC Timestamps, keeping the first of each group closer than time_difference."""
    import pandas as pd

    gap = pd.Timedelta(time_difference)
    kept = []
    for ts in sorted({parse_time(t) for t in timestamps}):
        if not kept or ts - kept[-1] > gap:
            kept.append(ts)
    return kept


def split_time_interval(date, *, n=None, freq=None):
    """Returns [(start, end)] as 'YYYY-MM-DD' strings: n equal parts, or calendar periods of freq."""
    import pandas as pd

    if (n is None) == (freq is None):
        raise ValueError("give exactly one of n or freq")
    start, end = parse_time_interval(date)
    d0, d1 = start.tz_localize(None).normalize(), end.tz_localize(None).normalize()
    days = (d1 - d0).days + 1
    if n is not None:
        if not 1 <= int(n) <= days:
            raise ValueError(f"n must be between 1 and {days} (the number of days), got {n!r}")
        edges = [d0 + pd.Timedelta(days=round(i * days / int(n))) for i in range(int(n) + 1)]
    else:
        try:
            marks = pd.date_range(d0, d1, freq=_FREQS.get(freq, freq))
        except (TypeError, ValueError):
            raise ValueError(f"freq must be one of {list(_FREQS)} or a pandas frequency like '10D', got {freq!r}") from None
        edges = sorted({d0, *marks, d1 + pd.Timedelta(days=1)})
    return [(f"{a:%Y-%m-%d}", f"{b - pd.Timedelta(days=1):%Y-%m-%d}") for a, b in zip(edges[:-1], edges[1:])]


def _is_day(value):
    import datetime as dt

    if isinstance(value, str):
        return bool(_DAY.match(value.strip()))
    return isinstance(value, dt.date) and not isinstance(value, dt.datetime)


def _day_end(ts):
    import pandas as pd

    return ts.normalize() + pd.Timedelta(days=1) - pd.Timedelta(milliseconds=1)
