# ---------------------------------------------------------------------------
# Marketplace shared helpers — install/view stats (utils folder, not an
# endpoint). Builds on market_common; market_index loads this module and
# the API UDFs reach it through market_index.
# ---------------------------------------------------------------------------
import json
import os
from datetime import date, timedelta

_common = fused.load("market_common")
STATS_KINDS = _common.STATS_KINDS
STATS_WINDOW_DAYS = _common.STATS_WINDOW_DAYS
_S3_ERROR = _common._S3_ERROR
_SLUG_RE = _common._SLUG_RE
_iso = _common._iso
_s3_list_strict = _common._s3_list_strict
_s3_put_json = _common._s3_put_json
_s3_read_json = _common._s3_read_json
_utc_now = _common._utc_now


# ---- stats: events → day files → current totals ----------------------------
# One object per event, the slug in its KEY (stats/<kind>/events/<day>/
# <HH>-<slot>--<slug>.json), so a day is counted from a listing alone. The
# body is {slug, commit, at}: nothing about who. Slots are capped
# (EVENT_SLOTS_PER_HOUR per slug per hour, a later event reuses a slot), so
# a scripted flood on one slug writes at most 24 x that many objects a day
# and cannot push the other apps' events out of a day's listing. A day file
# (stats/<kind>/days/<day>.json = {slug: n}) is a recount of that day's keys,
# idempotent, so two workers writing it at once cannot double-count.
# stats/<kind>/current.json = the day files summed (totals + the 30-day
# window). The rollup runs in the UDFs that count (install, listing) and in
# marketplace_stats, never in the catalog/search/detail path: those read
# current.json. Any S3 read that FAILS (not merely absent) aborts a rollup
# with no write, so a transient error can never rewrite the totals.

EVENT_SLOTS_PER_HOUR = 100
#: How far back a rollup recounts, at most (days before the last rollup).
RECOUNT_MAX_DAYS = 35
#: Per worker: {(kind, slug, day-hour): next slot}, started at random so two
#: workers rarely share a slot.
_EVENT_SLOTS = {}


def _record_event(kind, slug, commit=None):
    if kind not in STATS_KINDS or not _SLUG_RE.fullmatch(slug or ""):
        return False
    now = _utc_now()
    hour = f"{now:%Y-%m-%dT%H}"
    for k in [k for k in _EVENT_SLOTS if k[2] != hour]:
        del _EVENT_SLOTS[k]
    key = (kind, slug, hour)
    slot = _EVENT_SLOTS.get(key, int.from_bytes(os.urandom(2), "big")) % EVENT_SLOTS_PER_HOUR
    _EVENT_SLOTS[key] = slot + 1
    rel = f"stats/{kind}/events/{now:%Y-%m-%d}/{now:%H}-{slot:03d}--{slug}.json"
    return _s3_put_json(rel, {"slug": slug, "commit": commit, "at": _iso(now)})


def _count_day(kind, day):
    """{slug: n} for that day's event keys, or _S3_ERROR."""
    keys = _s3_list_strict(f"stats/{kind}/events/{day}/")
    if keys is _S3_ERROR:
        return _S3_ERROR
    counts = {}
    for key in keys:
        name = key.rsplit("/", 1)[-1]
        if not name.endswith(".json") or "--" not in name:
            continue
        slug = name[:-len(".json")].split("--", 1)[1]
        if _SLUG_RE.fullmatch(slug):
            counts[slug] = counts.get(slug, 0) + 1
    return counts


def _day_names(keys):
    """`YYYY-MM-DD` names out of listed keys (`…/2026-10-27.json` or the
    folder `…/2026-10-27/`)."""
    out = set()
    for key in keys:
        name = key.rstrip("/").rsplit("/", 1)[-1]
        name = name[:-len(".json")] if name.endswith(".json") else name
        try:
            date.fromisoformat(name)
        except ValueError:
            continue
        out.add(name)
    return out


@fused.cache(cache_max_age="5m")
def _stats_rollup(kind):
    """Fold every day file into current.json's days, recount every day since
    the last rollup (minus a day), write what changed, answer current.json
    as JSON text. Raises (nothing written) when any read fails."""
    today = _utc_now().date()
    cur = _s3_read_json(f"stats/{kind}/current.json")
    if cur is _S3_ERROR:
        raise RuntimeError(f"stats/{kind}/current.json unreadable")
    cur = cur if isinstance(cur, dict) else None
    days = {d: m for d, m in ((cur or {}).get("days") or {}).items() if isinstance(m, dict)}
    listed = _s3_list_strict(f"stats/{kind}/days/")
    if listed is _S3_ERROR:
        raise RuntimeError(f"stats/{kind}/days/ unlistable")
    for d in sorted(_day_names(listed) - set(days)):
        got = _s3_read_json(f"stats/{kind}/days/{d}.json")
        if got is _S3_ERROR:
            raise RuntimeError(f"stats/{kind}/days/{d}.json unreadable")
        if isinstance(got, dict):
            days[d] = got
    # Which days to recount: from the day before the last rollup to today
    # (a quiet UTC day with no rollup is then still recounted later); with
    # no current.json at all, every day that has events.
    try:
        since = date.fromisoformat(str(cur["generated_at"])[:10]) - timedelta(days=1)
    except Exception:
        since = None
    if since is None:
        ev = _s3_list_strict(f"stats/{kind}/events/")
        if ev is _S3_ERROR:
            raise RuntimeError(f"stats/{kind}/events/ unlistable")
        recount = sorted(_day_names(ev) | {str(today)})
    else:
        since = max(since, today - timedelta(days=RECOUNT_MAX_DAYS))
        recount = [str(since + timedelta(days=i)) for i in range((today - since).days + 1)]
    changed = cur is None
    for d in recount:
        counted = _count_day(kind, d)
        if counted is _S3_ERROR:
            raise RuntimeError(f"stats/{kind}/events/{d}/ unlistable")
        if (counted or d in days) and days.get(d) != counted:
            days[d] = counted
            _s3_put_json(f"stats/{kind}/days/{d}.json", counted)
            changed = True
    totals, window = {}, {}
    cutoff = today - timedelta(days=STATS_WINDOW_DAYS)
    for d, m in days.items():
        try:
            recent = date.fromisoformat(d) >= cutoff
        except ValueError:
            continue
        for slug, n in m.items():
            totals[slug] = totals.get(slug, 0) + int(n)
            if recent:
                window[slug] = window.get(slug, 0) + int(n)
    out = {"kind": kind, "generated_at": _iso(_utc_now()), "window_days": STATS_WINDOW_DAYS,
           "totals": totals, "window": window, "days": days}
    if changed or (cur or {}).get("window") != window or str(today) > str((cur or {}).get("generated_at"))[:10]:
        _s3_put_json(f"stats/{kind}/current.json", out)
    return json.dumps(out)


def _stats_refresh():
    """Run the rollups (each fused.cache'd 5 min). Called by the UDFs that
    count and by marketplace_stats; a failed rollup is logged and skipped."""
    for kind in STATS_KINDS:
        try:
            _stats_rollup(kind)
        except Exception as e:
            print(f"stats rollup {kind}: {e}")


_STATS_MEMO = {"at": 0.0, "value": None}


def _stats_all():
    """{kind: {totals, window}} for every kind, read from current.json (the
    rollup's output; no listing on this path). Memoised 60 s per worker; a
    failed read keeps the last value rather than showing zeros."""
    import time
    now = time.monotonic()
    if _STATS_MEMO["value"] is not None and now - _STATS_MEMO["at"] < 60:
        return _STATS_MEMO["value"]
    last = _STATS_MEMO["value"] or {}
    out = {}
    for kind in STATS_KINDS:
        try:
            got = json.loads(_stats_current(kind))
        except Exception as e:  # the read failed (never cached): keep the last value
            print(f"stats {kind}: {e}")
            out[kind] = last.get(kind) or {"totals": {}, "window": {}}
            continue
        out[kind] = {"totals": got.get("totals") or {}, "window": got.get("window") or {}}
    _STATS_MEMO.update(at=now, value=out)
    return out


@fused.cache(cache_max_age="2m")
def _stats_current(kind):
    """stats/<kind>/current.json as JSON bytes (`{}` when absent), shared
    across workers for 2 min: the per-worker memo above never survives a
    request (each request runs the UDF in a fresh namespace), so without
    this every catalog answer paid one S3 read per kind. Raises on a failed
    read, which fused.cache does not store."""
    got = _s3_read_json(f"stats/{kind}/current.json")
    if got is _S3_ERROR:
        raise ValueError(f"stats/{kind}/current.json could not be read")
    if got is not None and not isinstance(got, dict):
        raise ValueError(f"stats/{kind}/current.json is not an object")
    return json.dumps(got or {}).encode("utf-8")


def _stats_for(slug):
    st = _stats_all()
    return {"installs": int(st["installs"]["totals"].get(slug, 0)),
            "installs_30d": int(st["installs"]["window"].get(slug, 0)),
            "views": int(st["views"]["totals"].get(slug, 0)),
            "views_30d": int(st["views"]["window"].get(slug, 0))}
