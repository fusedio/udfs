# ---------------------------------------------------------------------------
# Marketplace shared helpers — base layer (utils folder, not an endpoint).
# Config, MarketError, the S3 store, time helpers and the row answer every
# marketplace UDF returns. Loaded by market_stats_store, which market_index
# loads; the API UDFs reach all three through market_index. Contract:
# fused-render docs/CONTRIB_LOOP.md §18.2 (index layout), §18.3 (rows) and
# §18.12 (the S3 store + stats).
#
# The canvas's database is S3 (`S3_PREFIX`). GitHub stays the source of
# truth for the index and the app archives; S3 keeps what was read from them
# (so a GitHub outage or a broken block list serves the last good index,
# flagged `stale`), what was built (the catalog,
# one listing per slug), and the stats (install + view events, one tiny
# object each, rolled up into day files and a current total). No row ever
# depends on an S3 write succeeding: every put is best-effort and logged.
# ---------------------------------------------------------------------------
import json
import os
import re
from datetime import datetime, timezone

#: Everything durable lives under here: index/, facts/, catalog/, listings/,
#: stats/. Overridable without a code change by the env var or Fused secret
#: MARKETPLACE_S3_PREFIX.
S3_PREFIX = "s3://fused-asset/fused-render-marketplace/"
#: The rolling window the `*_30d` counts cover.
STATS_WINDOW_DAYS = 30
#: Event kinds the stats keep. "installs": fused-render asked
#: `fused_app_install` (one per install started). "views": the public
#: listing page was served.
STATS_KINDS = ("installs", "views")

#: The public index repo (owner/name). Overridable without a code change by
#: the env var or Fused secret MARKETPLACE_INDEX_REPO.
INDEX_REPO = "fusedio/fused-apps-index"
RAW_BASE = "https://raw.githubusercontent.com"
MAX_ZIP_BYTES = 50_000_000
MAX_README_CHARS = 200_000
_SLUG_RE = re.compile(r"[a-z0-9][a-z0-9-]{1,63}")
_REPO_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
_SHA_RE = re.compile(r"[0-9a-f]{40}")


class MarketError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def _index_repo():
    v = os.environ.get("MARKETPLACE_INDEX_REPO") or ""
    if not v:
        try:
            v = str(fused.secrets["MARKETPLACE_INDEX_REPO"])
        except Exception:
            v = ""
    v = v.strip()
    return v if _REPO_RE.fullmatch(v) else INDEX_REPO


def _s3_prefix():
    v = os.environ.get("MARKETPLACE_S3_PREFIX") or ""
    if not v:
        try:
            v = str(fused.secrets["MARKETPLACE_S3_PREFIX"])
        except Exception:
            v = ""
    v = v.strip()
    return (v.rstrip("/") + "/") if v.startswith("s3://") else S3_PREFIX


def _s3_key(rel):
    return _s3_prefix() + rel.lstrip("/")


def _s3_put_json(rel, obj):
    """Best effort: the row never waits on, or fails for, a write."""
    try:
        fused.api.upload(json.dumps(obj, separators=(",", ":")).encode("utf-8"), _s3_key(rel))
        return True
    except Exception as e:
        print(f"s3 put {rel}: {e}")
        return False


#: What `_s3_read_json` / `_s3_list_strict` answer when S3 failed (as
#: opposed to "nothing there"): a caller that WRITES on the strength of a
#: read must never take a failed read for an empty one.
_S3_ERROR = object()


def _s3_read_json(rel):
    """The parsed object; None when the key is absent (fused.api.get raises
    an HTTP 404 then); `_S3_ERROR` when the read failed any other way or
    the body is not JSON."""
    try:
        raw = fused.api.get(_s3_key(rel))
    except Exception as e:
        if getattr(getattr(e, "response", None), "status_code", None) == 404:
            return None
        print(f"s3 get {rel}: {e}")
        return _S3_ERROR
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception as e:
        print(f"s3 get {rel}: not JSON: {e}")
        return _S3_ERROR


def _s3_get_json(rel):
    """The parsed object, or None (absent, unreadable, not JSON): for reads
    that only serve, never for one a write depends on."""
    got = _s3_read_json(rel)
    return None if got is _S3_ERROR else got


def _s3_list_strict(rel):
    """Keys (full s3:// paths) under `rel`; [] when absent (an absent
    folder lists as empty); `_S3_ERROR` when the listing failed."""
    try:
        return [str(x) for x in fused.api.list(_s3_key(rel))]
    except Exception as e:
        print(f"s3 list {rel}: {e}")
        return _S3_ERROR


def _utc_now():
    return datetime.now(timezone.utc)


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")




def _answer(rows, columns=None):
    """A list of row dicts → what `format=json` serves as a JSON list. An
    empty answer keeps its `columns` so it still serialises as `[]`."""
    import pandas as pd
    return pd.DataFrame(rows, dtype=object) if rows else pd.DataFrame(columns=list(columns or []), dtype=object)


def _error_rows(code, message):
    return _answer([{"error_code": code, "error_message": message}])
