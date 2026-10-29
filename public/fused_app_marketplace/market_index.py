# ---------------------------------------------------------------------------
# Marketplace shared helpers — the index and the catalog rows (utils folder,
# not an endpoint). The API UDFs load THIS module only and take
# market_stats_store / market_common from it (`_stats`, `_common`): each
# fused.load is a fresh module, and MarketError / the S3 sentinels are
# compared by identity, so every caller must share these instances.
# ---------------------------------------------------------------------------
import io
import json
import re
import zipfile
from urllib.parse import quote

_stats = fused.load("market_stats_store")
_common = _stats._common
MAX_README_CHARS = _common.MAX_README_CHARS
MAX_ZIP_BYTES = _common.MAX_ZIP_BYTES
MarketError = _common.MarketError
RAW_BASE = _common.RAW_BASE
_REPO_RE = _common._REPO_RE
_S3_ERROR = _common._S3_ERROR
_SHA_RE = _common._SHA_RE
_SLUG_RE = _common._SLUG_RE
_index_repo = _common._index_repo
_iso = _common._iso
_s3_get_json = _common._s3_get_json
_s3_put_json = _common._s3_put_json
_s3_read_json = _common._s3_read_json
_utc_now = _common._utc_now
_stats_for = _stats._stats_for


#: Worker memo of `_index_refs`: {repo: {at, ok, value}}. A success is
#: reused for 15 s, a failure for 60 s, so a GitHub outage costs one slow
#: request a minute per worker instead of one per request.
_REFS_MEMO = {}
REFS_OK_S = 15
REFS_FAIL_S = 60


def _index_refs(repo):
    """(main's sha, the `reviews` branch's sha or "") as GitHub has them NOW.
    Read from the smart-HTTP ref advertisement (`<repo>.git/info/refs`,
    served uncached, about a kilobyte), so a new index commit is seen
    within REFS_OK_S and every archive below is fetched BY SHA (immutable).
    This is what keeps the index fresh: no cache sits on top of another."""
    import time
    now = time.monotonic()
    m = _REFS_MEMO.get(repo)
    if m and now - m["at"] < (REFS_OK_S if m["ok"] else REFS_FAIL_S):
        if m["ok"]:
            return m["value"]
        raise ValueError(m["value"])
    try:
        value = tuple(json.loads(_index_refs_cached(repo)))
    except Exception as e:
        _REFS_MEMO[repo] = {"at": now, "ok": False, "value": str(e)}
        raise
    _REFS_MEMO[repo] = {"at": now, "ok": True, "value": value}
    return value


@fused.cache(cache_max_age="15s")
def _index_refs_cached(repo):
    """`_index_refs`'s GitHub read, shared across workers for REFS_OK_S:
    the memo above never survives a request, so without this every answer
    paid the round trip. A failure raises (never cached)."""
    import requests
    r = requests.get(f"https://github.com/{repo}.git/info/refs?service=git-upload-pack",
                     timeout=10, headers={"User-Agent": "fused-marketplace"})
    if r.status_code in (401, 404):
        raise ValueError(f"the index repo {repo} was not found or is not public")
    r.raise_for_status()
    refs = {name.decode(): sha.decode()
            for sha, name in re.findall(rb"([0-9a-f]{40}) ([^\s\x00]+)", r.content)}
    head = refs.get("HEAD") or refs.get("refs/heads/main")
    if not head:
        raise ValueError(f"the index repo {repo} has no commits")
    return json.dumps([head, refs.get("refs/heads/reviews", "")]).encode("utf-8")


#: Per worker: {sha: (monotonic time, reason)} of archive fetches that
#: failed; retried after REFS_FAIL_S, so codeload being down costs one slow
#: request a minute, not one per request.
_ZIP_FAILED = {}


def _fetch_index_zip(repo, sha):
    """The index archive at commit `sha`. Not cached on its own: only
    `_index_text` reads it, and that is cached by sha."""
    import time
    import requests
    failed = _ZIP_FAILED.get(sha)
    if failed and time.monotonic() - failed[0] < REFS_FAIL_S:
        raise ValueError(failed[1])
    try:
        r = requests.get(f"https://codeload.github.com/{repo}/zip/{sha}", timeout=30)
        r.raise_for_status()
        if len(r.content) > MAX_ZIP_BYTES:
            raise ValueError("the index archive is larger than 50 MB")
    except Exception as e:
        _ZIP_FAILED[sha] = (time.monotonic(), f"the index archive {sha[:7]} could not be read: {e}")
        raise
    _ZIP_FAILED.pop(sha, None)
    return r.content


def _facts_rel(repo, commit):
    return f"facts/{repo.replace('/', '__')}/{commit}.json"


@fused.cache(cache_max_age="24h")
def _fetch_app_facts(repo, commit):
    """Everything the rows need from the app repo at `commit` (immutable, so a
    day's cache is safe), as JSON bytes: the file list, metadata, readme.
    S3 first (facts/<owner>__<name>/<commit>.json, written once, kept for
    ever); GitHub only when S3 has no copy."""
    import requests
    kept = _s3_get_json(_facts_rel(repo, commit))
    if isinstance(kept, dict) and "files" in kept:
        return json.dumps(kept).encode("utf-8")
    r = requests.get(f"https://codeload.github.com/{repo}/zip/{commit}", timeout=60)
    if r.status_code == 404:
        raise ValueError(f"{repo}@{commit[:7]} is not available on GitHub")
    r.raise_for_status()
    if len(r.content) > MAX_ZIP_BYTES:
        raise ValueError("the app archive is larger than 50 MB")
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    members = [i for i in zf.infolist() if not i.is_dir()]
    files, names = [], {}
    for i in members:
        rel = i.filename.split("/", 1)[1] if "/" in i.filename else i.filename
        if not rel:
            continue
        files.append({"path": rel, "size": i.file_size})
        names[rel.lower()] = i
    files.sort(key=lambda f: f["path"])

    meta = {}
    m = names.get("metadata.json")
    if m is not None and m.file_size <= 1_000_000:
        try:
            meta = json.loads(zf.read(m.filename).decode("utf-8"))
        except Exception:
            meta = {}
    if not isinstance(meta, dict):
        meta = {}
    readme = None
    for c in ("readme.md", "readme.markdown", "readme"):
        if c in names:
            readme = zf.read(names[c].filename).decode("utf-8", "replace")[:MAX_README_CHARS]
            break
    author = meta.get("author")
    author_name = author.get("name") if isinstance(author, dict) else author

    def str_list(v):
        return [x for x in v if isinstance(x, str) and x] if isinstance(v, list) else []

    def opt_str(v):
        return v if isinstance(v, str) and v else None

    facts = {
        "files": files,
        "size_bytes": sum(f["size"] for f in files),
        "file_count": len(files),
        "author_name": opt_str(author_name),
        "requires_python": meta.get("requires_python") is True or "pyproject.toml" in names,
        "min_fused_render": opt_str(meta.get("min_fused_render")),
        "ai_capabilities": str_list(meta.get("ai_capabilities")),
        "ai_models": str_list(meta.get("ai_models")),
        "ai_model_param": opt_str(meta.get("ai_model_param")),
        "readme": readme,
        "entry": "index.html" if "index.html" in names else None,
    }
    _s3_put_json(_facts_rel(repo, commit), facts)
    return json.dumps(facts).encode("utf-8")


def _app_facts(repo, commit):
    """The facts dict, or None when the archive cannot be read (the row is
    still served from the index; only the zip-derived fields go empty)."""
    try:
        return json.loads(_fetch_app_facts(repo, commit))
    except Exception as e:
        print(f"app facts {repo}@{commit[:7]}: {e}")
        return None


def _archive_at(zf):
    """When the archived commit was made, as codeload stamps every member
    (`YYYY-MM-DDTHH:MM:SS` in GitHub's archive clock, not UTC): only ever
    compared with another archive's stamp, to tell which index is newer."""
    stamps = [i.date_time for i in zf.infolist()]
    return "%04d-%02d-%02dT%02d:%02d:%02d" % max(stamps) if stamps else ""


#: Index versions (main + reviews) this worker has already settled in S3.
_INDEX_WRITTEN = {}


class _NotYet(Exception):
    """`_s3_has`: the object is there but at another version (or absent)."""


@fused.cache(cache_max_age="24h")
def _s3_has(rel, version, reviews_version):
    """True once S3's `rel` carries exactly (version, reviews_version) — and
    only True is ever cached: a mismatch raises `_NotYet` and a failed read
    raises too, so the caller re-reads next request and a later write is
    seen. One S3 read per index version instead of one per request (the
    per-worker memos never survive a request)."""
    cur = _s3_read_json(rel)
    if cur is _S3_ERROR:
        raise ValueError(f"{rel} could not be read")
    if isinstance(cur, dict) and (cur.get("version"), cur.get("reviews_version") or "") == \
            (version, reviews_version):
        return True
    raise _NotYet(rel)


def _persist_index(idx):
    """index/current.json = the newest index read from GitHub; every new
    version also lands in index/history/<generated_at>-<version>.json.
    Never regresses: a worker still holding an older parse (or a read of
    current.json that failed) writes nothing, because current.json is what
    the next GitHub outage serves (blocks included)."""
    v = f"{idx['version']}+{idx.get('reviews_version') or '-'}"
    if _INDEX_WRITTEN.get(v):
        return
    try:
        if _s3_has("index/current.json", idx["version"], idx.get("reviews_version") or ""):
            _INDEX_WRITTEN[v] = True
            return
    except _NotYet:
        pass
    except Exception as e:
        print(f"persist index: {e}")
        return  # try again on the next request
    cur = _s3_read_json("index/current.json")
    if cur is _S3_ERROR:
        return  # try again on the next request
    mine = (idx.get("index_at") or "", idx.get("reviews_at") or "")
    if isinstance(cur, dict):
        theirs = (cur.get("index_at") or "", cur.get("reviews_at") or "")
        same = (cur.get("version"), cur.get("reviews_version")) == (idx["version"], idx.get("reviews_version"))
        if same or theirs[0] > mine[0] or (theirs[0] == mine[0] and theirs[1] > mine[1]):
            _INDEX_WRITTEN[v] = True
            return
    if _s3_put_json("index/current.json", idx):
        _s3_put_json(f"index/history/{idx['generated_at'].replace(':', '')}-{idx['version']}.json", idx)
        _INDEX_WRITTEN[v] = True


class IndexDamaged(Exception):
    """A file the index must have is missing or unreadable at this commit."""


class ReviewsUnreadable(Exception):
    """The `reviews` branch's archive could not be read (main's could)."""


#: Part of `_index_text`'s cache key. fused.cache hashes the cached
#: function's OWN source and its arguments, not the helpers it calls, so a
#: change to what the parse produces (`_index_blobs`, `_clean_row`,
#: `_clean_block`, …) must bump this, or the 24 h cache keeps serving the
#: old shape for an index commit it has already parsed.
INDEX_PARSE_VERSION = 2


#: Index commits found damaged (by sha, so immutable): {sha: reason}. Spares
#: a re-download per request while a broken blocklist.json sits on main.
_BAD_HEADS = {}


def _load_index():
    """The index at main's current commit → {version, generated_at, stale,
    apps, reviews, blocked, featured, shelves, categories, …}. GitHub first
    (`_index_refs` for the shas, `_index_text` parsed once per sha); when it
    cannot be read, OR main's blocklist.json is missing or unreadable, the
    copy S3 keeps (index/current.json, the last good index) with
    `stale: true` — a broken block list must never read as an empty one.
    The reviews branch failing alone keeps main's index (its review entries
    from S3, `reviews_stale: true`, not re-persisted). Raises
    MarketError("index_unavailable") only when neither GitHub nor S3 answers."""
    repo = _index_repo()
    try:
        head, rev = _index_refs(repo)
        if head in _BAD_HEADS:
            raise IndexDamaged(_BAD_HEADS[head])
        try:
            idx = json.loads(_index_text(repo, head, rev, INDEX_PARSE_VERSION))
        except IndexDamaged as e:
            _BAD_HEADS[head] = str(e)
            raise
        except ReviewsUnreadable as e:
            # Only the reviews branch failed (main's archive was read): keep
            # main's fresh index. Anything else falls to the S3 copy below.
            print(f"index: {e}; main's reviews/ + the S3 copy's")
            idx = json.loads(_index_text(repo, head, "", INDEX_PARSE_VERSION))
            kept = _s3_get_json("index/current.json")
            if isinstance(kept, dict) and isinstance(kept.get("reviews"), dict):
                idx["reviews"].update(kept["reviews"])
            idx["reviews_stale"] = True
    except Exception as e:
        kept = _s3_get_json("index/current.json")
        if isinstance(kept, dict) and isinstance(kept.get("apps"), dict) \
                and isinstance(kept.get("blocked"), list):
            print(f"index: GitHub index unusable ({e}); serving the S3 copy {kept.get('version')}")
            kept["stale"] = True
            return kept
        raise MarketError("index_unavailable", f"The marketplace index could not be read: {e}")
    if not idx.get("reviews_stale"):
        _persist_index(idx)
    return idx


@fused.cache(cache_max_age="24h")
def _index_text(repo, head, reviews, parse_version):
    """The parsed index at main = `head` with the `reviews` branch at
    `reviews` ("" = main's reviews/ only), as JSON text. Both are commit
    shas, so the answer never changes: a new index commit is a new key.
    `parse_version` = INDEX_PARSE_VERSION (only part of the key)."""
    data = _fetch_index_zip(repo, head)
    zf = zipfile.ZipFile(io.BytesIO(data))
    blobs = _index_blobs(zf)
    reviews_at = ""
    # Review entries: the bot's `reviews` branch wins per slug; main's
    # reviews/ (hand entries) is the fallback. A failed branch read raises
    # here (never cached); `_load_index` then reads main alone.
    if reviews:
        try:
            rz = zipfile.ZipFile(io.BytesIO(_fetch_index_zip(repo, reviews)))
        except Exception as e:
            raise ReviewsUnreadable(f"reviews branch {reviews[:7]} unreadable ({e})") from e
        reviews_at = _archive_at(rz)
        for rel, v in _index_blobs(rz).items():
            if rel.startswith("reviews/") and v is not _UNREADABLE:
                blobs[rel] = v

    apps = {}
    for rel, row in blobs.items():
        if not rel.startswith("apps/"):
            continue
        slug = rel[len("apps/"):-len(".json")]
        clean = _clean_row(slug, row) if row is not _UNREADABLE else None
        if clean:
            apps[slug] = clean
        else:
            print(f"index: skipping invalid row {rel}")

    reviews_map = {}
    for rel, entries in blobs.items():
        if rel.startswith("reviews/") and isinstance(entries, list):
            seen, out = set(), []
            for e in entries:
                if isinstance(e, dict) and isinstance(e.get("commit"), str) and e["commit"] not in seen:
                    seen.add(e["commit"])
                    out.append(e)
            reviews_map[rel[len("reviews/"):-len(".json")]] = out

    # The block list fails CLOSED: absent, not JSON, or not {"blocked": [...]}
    # → this commit is damaged and the last good index is served instead.
    bl = blobs.get("blocklist.json")
    if not (isinstance(bl, dict) and isinstance(bl.get("blocked"), list)):
        raise IndexDamaged("blocklist.json is missing or unreadable at "
                           f"{head[:7]}; serving the last good block list")
    blocked = [_clean_block(b, apps, reviews_map) for b in bl["blocked"]]

    cols = blobs.get("collections.json") if isinstance(blobs.get("collections.json"), dict) else {}
    featured = [s for s in cols.get("featured") or [] if isinstance(s, str)]
    shelves = [s for s in cols.get("shelves") or []
               if isinstance(s, dict) and isinstance(s.get("id"), str) and s["id"]]

    cats_raw = blobs.get("categories.json")
    cats = [c for c in (cats_raw if isinstance(cats_raw, list) else [])
            if isinstance(c, dict) and isinstance(c.get("id"), str) and c["id"]]
    idx = {"repo": repo, "version": head[:7], "head": head,
           "reviews_version": reviews[:7] if reviews else "",
           "index_at": _archive_at(zf), "reviews_at": reviews_at,
           "generated_at": _iso(_utc_now()),
           "apps": apps, "reviews": reviews_map, "blocked": blocked,
           "featured": featured, "shelves": shelves, "categories": cats, "stale": False}
    return json.dumps(idx)


def _clean_block(b, apps, reviews):
    """One blocklist.json entry, normalised so nothing downstream can trip
    on it. Fail closed: an entry is NEVER dropped here; a level other than
    exactly "soft" reads as "hard", and so does any entry whose slug or
    commit is unusable; a malformed entry keeps what it has and says
    `malformed: true` (a short commit that names exactly one known release
    of that slug is expanded)."""
    b = b if isinstance(b, dict) else {}

    def s(k):
        v = b.get(k)
        return v if isinstance(v, str) else None

    slug, commit = s("slug"), s("commit")
    if slug and commit and not _SHA_RE.fullmatch(commit) and re.fullmatch(r"[0-9a-f]{7,39}", commit):
        known = {e["commit"] for e in reviews.get(slug, [])} | (
            {apps[slug]["commit"]} if slug in apps else set())
        hits = [c for c in known if c.startswith(commit)]
        if len(hits) == 1:
            commit = hits[0]
    usable = bool(slug and _SLUG_RE.fullmatch(slug) and commit and _SHA_RE.fullmatch(commit))
    level = "soft" if usable and b.get("level") == "soft" else "hard"
    malformed = not (usable and b.get("level") in ("hard", "soft") and s("commit") == commit)
    return {"slug": slug, "commit": commit, "level": level, "reason": s("reason"),
            "what_to_do": s("what_to_do"), "at": s("at"), "malformed": malformed}


#: `_index_blobs`' value for a file that is there but is not JSON.
_UNREADABLE = "__unreadable__"


def _index_blobs(zf):
    """{rel path: parsed JSON, or _UNREADABLE} for the index files the rows
    read. An unreadable file is kept as a marker, not dropped, so the
    caller can tell "broken" from "absent" (blocklist.json fails closed)."""
    blobs = {}
    for i in zf.infolist():
        if i.is_dir() or "/" not in i.filename:
            continue
        rel = i.filename.split("/", 1)[1]
        parts = rel.split("/")
        if (len(parts) == 2 and parts[0] in ("apps", "reviews") and parts[1].endswith(".json")) or \
                rel in ("blocklist.json", "collections.json", "categories.json"):
            if i.file_size > 5_000_000:
                print(f"index: {rel} is larger than 5 MB; unreadable")
                blobs[rel] = _UNREADABLE
                continue
            try:
                blobs[rel] = json.loads(zf.read(i.filename).decode("utf-8"))
            except Exception as e:
                print(f"index: unreadable {rel}: {e}")
                blobs[rel] = _UNREADABLE
    return blobs


def _clean_row(slug, row):
    if not isinstance(row, dict) or row.get("slug") != slug or not _SLUG_RE.fullmatch(slug):
        return None
    repo, commit = row.get("repo"), row.get("commit")
    if not (isinstance(repo, str) and _REPO_RE.fullmatch(repo)
            and isinstance(commit, str) and _SHA_RE.fullmatch(commit)):
        return None

    def s(k, default=None):
        v = row.get(k)
        return v if isinstance(v, str) and v else default

    remix = s("remix_of")
    return {"slug": slug, "repo": repo, "commit": commit, "branch": s("branch", "main"),
            "name": s("name", slug), "description": s("description", ""),
            "category": s("category", "other"),
            "tags": [t for t in row.get("tags") or [] if isinstance(t, str) and t],
            "icon": s("icon"), "preview": s("preview"),
            "remix_of": remix if remix and _SLUG_RE.fullmatch(remix) else None,
            "version": s("version", "0.0.0")}


def _raw_url(repo, commit, path):
    if not isinstance(path, str) or not path:
        return None
    return f"{RAW_BASE}/{repo}/{commit}/{quote(path.lstrip('/'), safe='/')}"


def _block_level(idx, slug, commit):
    level = None
    for b in idx["blocked"]:
        if b.get("slug") == slug and b.get("commit") == commit:
            if b.get("level") != "soft":
                return "hard"
            level = "soft"
    return level


def _releases(idx, slug):
    """Newest first. The row's commit always has an entry: until the review
    bot writes it, a stub with `whats_new: null`. Each entry carries the
    §18.3 keys plus today's client names (release_id/published_at/changelog/
    revoked, revoked = a hard block) so either reader works. `claude` is
    the review entry's marker: "skipped" = only the fixed rules ran (no
    API key), null = not recorded (entries written before the marker)."""
    row = idx["apps"][slug]
    out = []
    for e in idx["reviews"].get(slug, []):
        out.append({"commit": e["commit"], "at": e.get("at") if isinstance(e.get("at"), str) else None,
                    "whats_new": e.get("whats_new") if isinstance(e.get("whats_new"), str) else None,
                    "version": row["version"] if e["commit"] == row["commit"]
                    else (e.get("version") if isinstance(e.get("version"), str) else None),
                    "claude": e.get("claude") if isinstance(e.get("claude"), str) else None})
    if not any(r["commit"] == row["commit"] for r in out):
        out.insert(0, {"commit": row["commit"], "at": None, "whats_new": None, "version": row["version"],
                       "claude": None})
    for r in out:
        r["blocked"] = _block_level(idx, slug, r["commit"])
        r.update({"release_id": r["commit"], "published_at": r["at"],
                  "changelog": r["whats_new"] or "", "revoked": r["blocked"] == "hard"})
    return out


def _catalog_row(idx, slug, facts):
    row = idx["apps"][slug]
    repo, commit = row["repo"], row["commit"]
    owner = repo.split("/")[0]
    f = facts or {}
    st = _stats_for(slug)
    releases = _releases(idx, slug)
    dates = sorted(r["at"] for r in releases if r["at"])
    credits = {c for e in idx["reviews"].get(slug, []) for c in e.get("credits") or []
               if isinstance(c, str) and c and c.lower() != owner.lower()}
    current = next(r for r in releases if r["commit"] == commit)
    return {
        # The index is keyed by the app's fused-app-id (its slug IS the id
        # since 2026-10-27; rows listed earlier carry an older name-based
        # slug here until republished).
        "slug": slug, "app_id": slug, "name": row["name"], "description": row["description"],
        "author": {"name": f.get("author_name") or owner, "github": owner},
        "tags": row["tags"], "category": row["category"], "version": row["version"],
        "requires_python": bool(f.get("requires_python")),
        "min_fused_render": f.get("min_fused_render"),
        "ai_capabilities": f.get("ai_capabilities") or [],
        "ai_models": f.get("ai_models") or [],
        "ai_model_param": f.get("ai_model_param"),
        "icon_url": _raw_url(repo, commit, row["icon"]),
        "preview_url": _raw_url(repo, commit, row["preview"]),
        "size_bytes": int(f.get("size_bytes") or 0),
        "file_count": int(f.get("file_count") or 0),
        "release_id": commit, "commit": commit, "repo": repo, "branch": row["branch"],
        "source_url": f"https://github.com/{repo}",
        "published_at": dates[0] if dates else None,
        "updated_at": dates[-1] if dates else None,
        "featured": slug in idx["featured"],
        "remix_of": row["remix_of"],
        "remix_count": sum(1 for r in idx["apps"].values() if r["remix_of"] == slug),
        "contributor_count": len(credits) + 1,
        "last_release_at": dates[-1] if dates else None,
        "whats_new": current["whats_new"],
        "blocked": current["blocked"],
        "claude_review": current["claude"],
        "releases": releases,
        "installs": st["installs"], "installs_30d": st["installs_30d"],
        "views": st["views"], "views_30d": st["views_30d"],
        **_index_stamp(idx),
    }


def _index_stamp(idx):
    """On every row: which index answered and whether it is the S3 copy
    (`stale: true` = GitHub was unreadable or main's block list was broken;
    a reader treats the answer as "unknown", never as the current index)."""
    return {"catalog_version": idx["version"], "generated_at": idx["generated_at"],
            "stale": bool(idx.get("stale"))}


#: Catalog versions this worker has already written (catalog/current.json +
#: listings/<slug>.json), one write per version.
_CATALOG_WRITTEN = {}


def _persist_catalog(idx, rows):
    """catalog/current.json = the rows as last built; listings/<slug>.json =
    one row each (what the public listing page and fused_app_install read
    back when GitHub is away is the index, not these — these are for anyone
    reading the store directly)."""
    v = f"{idx['version']}+{idx.get('reviews_version') or '-'}"
    if _CATALOG_WRITTEN.get(v) or idx.get("stale") or idx.get("reviews_stale"):
        return
    # The memo never survives a request (each one runs the UDF in a fresh
    # namespace), so the 18 writes repeated on EVERY request. `_s3_has`
    # (fused.cache, shared across workers) answers "already written" from
    # one S3 read per version instead; a failed read just tries again.
    try:
        if _s3_has("catalog/current.json", idx["version"], idx.get("reviews_version") or ""):
            _CATALOG_WRITTEN[v] = True
            return
    except _NotYet:
        pass
    except Exception as e:
        print(f"persist catalog: {e}")
        return
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(8) as pool:
        ok = all(list(pool.map(lambda r: _s3_put_json(f"listings/{r['slug']}.json", r), rows)))
    # current.json last: its version is the probe above, so it must only
    # say "written" once every listing is.
    if ok and _s3_put_json("catalog/current.json",
                           {"version": idx["version"], "reviews_version": idx.get("reviews_version"),
                            "generated_at": idx["generated_at"], "apps": rows}):
        _CATALOG_WRITTEN[v] = True


def _facts_for(idx, slugs):
    from concurrent.futures import ThreadPoolExecutor
    slugs = list(slugs)
    if not slugs:
        return {}
    with ThreadPoolExecutor(min(8, len(slugs))) as pool:
        got = pool.map(lambda s: _app_facts(idx["apps"][s]["repo"], idx["apps"][s]["commit"]), slugs)
        return dict(zip(slugs, got))


def _sorted_slugs(idx):
    return sorted(idx["apps"], key=lambda s: (idx["apps"][s]["name"].casefold(), s))
