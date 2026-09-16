import html
import json
import struct

import fused

MAX_README_CHARS = 200_000
MAX_LISTED_FILES = 500
MAX_ICON_BYTES = 2_000_000
MAX_SHOT_BYTES = 6_000_000

# An icon is the app's mark; a shot is a picture of the app running. They sit
# side by side in the file list and nothing else about them is alike — the mark
# belongs at 64px above the title, the shot belongs full width under it.
ICON_NAMES = ("icon.svg", "icon.png")
SHOT_NAMES = ("preview.png", "screenshot.png", "preview.jpg", "screenshot.jpg")

# v2 container header: magic, u16 version, u16 flags, u32 index size, u32
# compressed index size. Then zlib(JSON index), then one zlib stream per file,
# each at `offset` bytes into the data section.
MAGIC = b"FUSEDAPP"
V2_VERSION = 2
V2_HEADER = struct.Struct("<8sHHII")
MAX_INDEX_BYTES = 4 * 1024 * 1024


@fused.udf(cache_max_age="30m")
def udf(path: str, preview: bool = False):
    """Unpack a `.fused` app file and show what is inside it.

    Two physical formats carry the same thing. **v2** (`FUSEDAPP` magic) is an
    opaque container: a versioned header, a deflated JSON index and one deflate
    stream per file. **v1** was a zip with `manifest.json` plus a `root` folder
    (by convention `files/`), which mail scanners classified by its `PK` bytes
    and flagged; v2 exists to be unremarkable to a scanner. Both are read here,
    since every v1 file already sent still has to open.

    Either way the read goes through a seekable handle and pulls only the index
    (or central directory) plus the two or three members shown — a 25 MB app
    file costs about what a 100 KB one does.
    """
    import fsspec

    try:
        with fsspec.open(path, "rb") as f:
            app = _read_app_file(f)
    except Exception as e:  # noqa: BLE001 - surfaced to the viewer, not swallowed
        return _error(path, f"{type(e).__name__}: {e}")

    # Signed URLs are what makes the download link work; mount paths cannot be
    # signed, so the page drops the link rather than offering a broken one.
    download_url = None
    if not (path.startswith("/mount/") or path.startswith("gdrive://")):
        try:
            download_url = fused.api.sign_url(path)
        except Exception:  # noqa: BLE001 - a missing link is not a failed preview
            download_url = None

    return _page(path, app, download_url)


def _read_app_file(f):
    """Read whichever format the bytes say this is. The magic decides — not the
    extension, which is `.fused` for both."""
    import zipfile

    head = f.read(len(MAGIC))
    f.seek(0)
    if head == MAGIC:
        return _read_v2(f)
    with zipfile.ZipFile(f) as zf:
        return _read_zip(zf)


def _inflate(raw, cap):
    """Decompress at most `cap + 1` bytes — one past the cap, so the caller can
    tell "at the cap" from "over it" — and never the declared length, which the
    file itself supplies and could claim to be a gigabyte."""
    import zlib

    return zlib.decompressobj().decompress(raw, cap + 1)


def _read_v2(f):
    """Pull the index, README, icon and file listing out of a v2 container.

    Everything the header and index declare is file-supplied, so each number is
    bounded before it is acted on: the index is capped before it inflates, and
    a member is read only through its own `offset`/`csize` window.
    """
    head = f.read(V2_HEADER.size)
    if len(head) < V2_HEADER.size:
        raise ValueError("not a fused app file (truncated header)")
    _magic, version, _flags, isize, icsize = V2_HEADER.unpack(head)
    if version != V2_VERSION:
        raise ValueError(f"unsupported .fused format version {version}")
    if isize > MAX_INDEX_BYTES or icsize > MAX_INDEX_BYTES:
        raise ValueError(f"file index is too large (> {MAX_INDEX_BYTES} bytes)")
    cindex = f.read(icsize)
    if len(cindex) != icsize:
        raise ValueError("not a fused app file (truncated index)")
    raw = _inflate(cindex, MAX_INDEX_BYTES)
    if len(raw) != isize:
        raise ValueError("file index does not match its declared size")
    index = json.loads(raw)
    if not isinstance(index, dict) or index.get("fused_app_file") != V2_VERSION:
        raise ValueError("not a fused app file (index carries no fused_app_file: 2)")
    members = index.get("files")
    if not isinstance(members, list):
        raise ValueError("invalid index: files is not a list")
    for m in members:
        if not isinstance(m, dict) or not isinstance(m.get("path"), str) or not m["path"]:
            raise ValueError("invalid index: file entry has no path")
        for key in ("offset", "size", "csize"):
            v = m.get(key)
            if not isinstance(v, int) or isinstance(v, bool) or v < 0:
                raise ValueError(f"invalid index: {key} of {m['path']!r} is not a size")

    data_start = V2_HEADER.size + icsize
    readme = None
    icon = None
    shot = None
    for m in members:
        rel = m["path"].lower()
        if readme is None and rel in ("readme.md", "readme.markdown", "readme"):
            body = _v2_member(f, data_start, m, MAX_README_CHARS)
            if body is not None:
                readme = body.decode("utf-8", "replace")[:MAX_README_CHARS]
        if icon is None and rel in ICON_NAMES:
            body = _v2_member(f, data_start, m, MAX_ICON_BYTES)
            if body is not None:
                icon = (_image_mime(rel), body)
        if shot is None and rel in SHOT_NAMES:
            body = _v2_member(f, data_start, m, MAX_SHOT_BYTES)
            if body is not None:
                shot = (_image_mime(rel), body)

    files = sorted(
        ({"name": m["path"], "size": m["size"]} for m in members),
        key=lambda f: f["name"],
    )
    return {
        "name": index.get("name") or "",
        "entry": index.get("entry") or "",
        "exported_at": index.get("exported_at") or "",
        "is_app_file": True,
        "readme": readme,
        "icon": icon,
        "shot": shot,
        "files": files,
        "total_size": sum(f["size"] for f in files),
    }


def _v2_member(f, data_start, entry, cap):
    """One member's decompressed bytes, or None when it is larger than `cap` —
    too big to show is not an error, it just leaves that panel empty. Corrupt
    bytes are: a member that does not inflate to its declared size or hash is a
    broken file, and saying so beats rendering half a README."""
    import hashlib

    if entry["size"] > cap:
        return None
    f.seek(data_start + entry["offset"])
    raw = f.read(entry["csize"])
    if len(raw) != entry["csize"]:
        raise ValueError(f"{entry['path']!r} runs past the end of the file")
    body = _inflate(raw, cap)
    if len(body) != entry["size"]:
        raise ValueError(f"{entry['path']!r} does not match its declared size")
    if isinstance(entry.get("sha256"), str):
        if hashlib.sha256(body).hexdigest() != entry["sha256"]:
            raise ValueError(f"{entry['path']!r} does not match its recorded hash")
    return body


def _read_zip(zf):
    """Pull the manifest, README, icon and file listing out of a v1 zip."""
    members = [i for i in zf.infolist() if not i.is_dir()]

    manifest = {}
    if "manifest.json" in zf.namelist():
        try:
            manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
        except Exception:  # noqa: BLE001 - an unreadable manifest still lists files
            manifest = {}

    root = str(manifest.get("root") or "files").strip("/")
    prefix = f"{root}/"

    def in_app(name):
        """Path as the app sees it: `files/index.html` is really `index.html`."""
        return name[len(prefix):] if name.startswith(prefix) else name

    readme = None
    icon = None
    shot = None
    for info in members:
        rel = in_app(info.filename).lower()
        if readme is None and rel in ("readme.md", "readme.markdown", "readme"):
            readme = zf.read(info.filename).decode("utf-8", "replace")[:MAX_README_CHARS]
        if icon is None and rel in ICON_NAMES and info.file_size <= MAX_ICON_BYTES:
            icon = (_image_mime(rel), zf.read(info.filename))
        if shot is None and rel in SHOT_NAMES and info.file_size <= MAX_SHOT_BYTES:
            shot = (_image_mime(rel), zf.read(info.filename))

    files = sorted(
        ({"name": in_app(i.filename), "size": i.file_size} for i in members),
        key=lambda f: f["name"],
    )
    return {
        "name": manifest.get("name") or "",
        "entry": manifest.get("entry") or "",
        "exported_at": manifest.get("exported_at") or "",
        "is_app_file": bool(manifest.get("fused_app_file")),
        "readme": readme,
        "icon": icon,
        "shot": shot,
        "files": files,
        "total_size": sum(f["size"] for f in files),
    }


def _js(value):
    """Embed a value in a <script> without letting its text close the tag."""
    return json.dumps(value).replace("</", "<\\/")


def _image_mime(rel):
    """Mime from the name. Only the four extensions we accept reach here."""
    if rel.endswith(".svg"):
        return "image/svg+xml"
    return "image/jpeg" if rel.endswith((".jpg", ".jpeg")) else "image/png"


def _data_uri(image):
    """An <img> src, not inline markup: an <img> cannot run the file's scripts."""
    import base64

    mime, raw = image
    return f"data:{mime};base64,{base64.b64encode(raw).decode('ascii')}"


def _page(path, app, download_url):
    file_name = html.escape(path.rsplit("/", 1)[-1], quote=True)
    title = html.escape(app["name"] or path.rsplit("/", 1)[-1], quote=True)
    listed = app["files"][:MAX_LISTED_FILES]
    hidden = len(app["files"]) - len(listed)

    icon = (
        f'<img class="mark" src="{_data_uri(app["icon"])}" alt="">'
        if app.get("icon")
        else '<div class="mark ph">.fused</div>'
    )
    # The screenshot is the whole point of a share page: someone deciding whether
    # to download this wants to see it running, not read about it.
    shot = (
        f'<img class="shot" src="{_data_uri(app["shot"])}" alt="{title} screenshot">'
        if app.get("shot")
        else ""
    )

    # The stamp is ISO-8601 UTC; only the day is worth showing.
    stamp = app.get("exported_at") or ""
    bits = [f'{len(app["files"])} files', '<span id="total"></span>']
    if stamp[:4].isdigit():
        bits.append(html.escape(stamp[:10], quote=True))
    if not app["is_app_file"]:
        bits.append('<span class="warn">not marked as an app file</span>')
    meta = " · ".join(bits)

    action = (
        f'<a class="dl" href="{html.escape(download_url, quote=True)}" download="{file_name}">'
        f"Download <span class=\"fn\">{file_name}</span></a>"
        if download_url
        else '<span class="warn">Download unavailable — this path cannot be signed</span>'
    )

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/dompurify@3/dist/purify.min.js"></script>
<style>
  * {{ box-sizing: border-box; }}
  html, body {{ margin: 0; background: #141414; color: #c9c9c9; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
          font-size: 15px; line-height: 1.7; -webkit-font-smoothing: antialiased; }}
  .page {{ max-width: 720px; margin: 0 auto; padding: 56px 24px 96px; }}

  .hero {{ display: flex; flex-direction: column; align-items: center; text-align: center;
           gap: 14px; }}
  .mark {{ width: 64px; height: 64px; border-radius: 15px; object-fit: contain;
           background: #202020; padding: 7px; }}
  .mark.ph {{ display: flex; align-items: center; justify-content: center; padding: 0;
           color: #D1E550; font-family: ui-monospace, Menlo, monospace; font-size: 12px; }}
  h1 {{ font-size: 2.1em; font-weight: 650; letter-spacing: -.02em; color: #f2f2f2;
        margin: 0; line-height: 1.15; }}
  .meta {{ color: #7d7d7d; font-size: .88em; margin: -6px 0 4px; }}
  .warn {{ color: #ff6b6b; }}
  .dl {{ display: inline-block; background: #D1E550; color: #141414; text-decoration: none;
         font-weight: 600; font-size: .95em; padding: 11px 22px; border-radius: 8px;
         transition: background .15s ease; }}
  .dl:hover {{ background: #E3FA62; }}
  .dl .fn {{ font-family: ui-monospace, Menlo, monospace; font-size: .92em; opacity: .75; }}

  .shot {{ display: block; width: 100%; margin: 48px 0 8px; border-radius: 12px;
           border: 1px solid #2b2b2b; }}

  .doc {{ margin-top: 48px; }}
  .doc > :first-child {{ margin-top: 0; }}
  .doc h1, .doc h2, .doc h3, .doc h4 {{ color: #f2f2f2; font-weight: 600;
           letter-spacing: -.01em; margin: 34px 0 12px; line-height: 1.3; }}
  .doc h1 {{ font-size: 1.45em; }} .doc h2 {{ font-size: 1.2em; }}
  .doc h3 {{ font-size: 1.05em; }}
  .doc a {{ color: #D1E550; }}
  .doc strong {{ color: #e8e8e8; }}
  .doc code {{ background: #232323; color: #d8d8d8; padding: 2px 6px; border-radius: 4px;
          font-family: ui-monospace, "JetBrains Mono", Menlo, Consolas, monospace;
          font-size: .87em; }}
  .doc pre {{ background: #1c1c1c; border: 1px solid #292929; padding: 14px 16px;
          border-radius: 8px; overflow-x: auto; }}
  .doc pre code {{ background: none; padding: 0; }}
  .doc blockquote {{ border-left: 3px solid #3a3a3a; margin: 16px 0;
          padding: 2px 0 2px 16px; color: #8d8d8d; }}
  .doc ul, .doc ol {{ padding-left: 22px; }}
  .doc li {{ margin: 6px 0; }}
  .doc table {{ border-collapse: collapse; margin: 16px 0; display: block;
          overflow-x: auto; }}
  .doc th, .doc td {{ border: 1px solid #333; padding: 8px 12px; text-align: left; }}
  .doc th {{ background: #232323; color: #e8e8e8; }}
  .doc img {{ max-width: 100%; border-radius: 8px; }}
  .doc hr {{ border: 0; border-top: 1px solid #292929; margin: 32px 0; }}
  .empty {{ color: #6f6f6f; font-style: italic; }}

  .files {{ margin-top: 56px; border-top: 1px solid #262626; padding-top: 8px; }}
  .files summary {{ cursor: pointer; color: #7d7d7d; font-size: .88em; padding: 10px 0;
          list-style: none; user-select: none; }}
  .files summary::-webkit-details-marker {{ display: none; }}
  .files summary::before {{ content: "▸ "; color: #555; }}
  .files[open] summary::before {{ content: "▾ "; }}
  .files summary:hover {{ color: #b0b0b0; }}
  table.list {{ border-collapse: collapse; width: 100%; font-size: .87em;
          margin-bottom: 8px; }}
  table.list td {{ border-bottom: 1px solid #222; padding: 7px 2px; }}
  td.n {{ font-family: ui-monospace, "JetBrains Mono", Menlo, Consolas, monospace;
          color: #a8a8a8; word-break: break-all; }}
  td.n.entry {{ color: #D1E550; }}
  td.s {{ text-align: right; color: #6f6f6f; white-space: nowrap; width: 1%; }}
  .more {{ color: #6f6f6f; font-size: .85em; padding: 6px 2px; }}

  @media (max-width: 560px) {{
    .page {{ padding: 36px 18px 64px; }}
    h1 {{ font-size: 1.7em; }}
    .shot {{ margin-top: 36px; }}
  }}
</style>
</head>
<body>
<div class="page">
  <div class="hero">
    {icon}
    <h1>{title}</h1>
    <div class="meta">{meta}</div>
    {action}
  </div>

  {shot}

  <div class="doc" id="readme"></div>

  <details class="files">
    <summary id="filesum">Files</summary>
    <table class="list" id="files"></table>
    {f'<div class="more">{hidden} more files not listed.</div>' if hidden > 0 else ""}
  </details>
</div>
<script>
  var README = {_js(app["readme"])};
  var FILES = {_js(listed)};
  var ENTRY = {_js(app["entry"])};
  var NAME = {_js(app["name"])};
  var TOTAL = {app["total_size"]};

  function human(n) {{
    var u = ["B", "KB", "MB", "GB"], i = 0;
    while (n >= 1024 && i < u.length - 1) {{ n /= 1024; i++; }}
    return (i === 0 ? n : n.toFixed(1)) + " " + u[i];
  }}

  document.getElementById("total").textContent = human(TOTAL);
  document.getElementById("filesum").textContent =
    FILES.length + " files · " + human(TOTAL);

  // The README is untrusted text: marked passes raw HTML through, so sanitise.
  var doc = document.getElementById("readme");
  doc.innerHTML = README === null
    ? '<span class="empty">This app file has no README.</span>'
    : DOMPurify.sanitize(marked.parse(README));

  // Nearly every README opens by repeating the app's name. The hero already
  // said it, so a second copy two lines down reads as a mistake — drop it.
  var lead = doc.firstElementChild;
  if (lead && lead.tagName === "H1" && NAME &&
      lead.textContent.trim().toLowerCase() === NAME.trim().toLowerCase()) {{
    lead.remove();
  }}

  var table = document.getElementById("files");
  FILES.forEach(function (f) {{
    var tr = document.createElement("tr");
    var name = document.createElement("td");
    name.className = "n" + (f.name === ENTRY ? " entry" : "");
    name.textContent = f.name;
    var size = document.createElement("td");
    size.className = "s";
    size.textContent = human(f.size);
    tr.appendChild(name);
    tr.appendChild(size);
    table.appendChild(tr);
  }});
</script>
</body>
</html>"""


def _error(path, message):
    safe = html.escape(path, quote=True)
    return f"""<!DOCTYPE html>
<html>
<body style="margin:0; padding:24px; background:#1a1a1a; color:#cccccc;
             font-family: system-ui, -apple-system, sans-serif; line-height:1.6;">
  <h2 style="color:#ff6b6b;">Could not open this .fused file</h2>
  <p>A <code>.fused</code> app file is an exported Fused app — either a
     <code>FUSEDAPP</code> container or, for older exports, a zip. This one
     could not be read:</p>
  <p><code>{html.escape(message, quote=True)}</code></p>
  <p><strong>Path:</strong> <code>{safe}</code></p>
</body>
</html>"""
