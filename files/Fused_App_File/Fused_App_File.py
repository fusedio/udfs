import html
import json

import fused

MAX_README_CHARS = 200_000
MAX_LISTED_FILES = 500
MAX_ICON_BYTES = 2_000_000


@fused.udf(cache_max_age="30m")
def udf(path: str, preview: bool = False):
    """Unpack a `.fused` app file and show what is inside it.

    A `.fused` file is a zip holding `manifest.json` plus a `root` folder (by
    convention `files/`) with the app's entry point, code and assets. This UDF
    reads the archive through a seekable handle, so only the central directory
    and the few members it shows are ever pulled — a 25 MB app file costs the
    same as a 100 KB one.
    """
    import zipfile

    import fsspec

    try:
        with fsspec.open(path, "rb") as f:
            with zipfile.ZipFile(f) as zf:
                app = _read_app(zf)
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


def _read_app(zf):
    """Pull the manifest, README, icon and file listing out of an open zip."""
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
    for info in members:
        rel = in_app(info.filename).lower()
        if readme is None and rel in ("readme.md", "readme.markdown", "readme"):
            readme = zf.read(info.filename).decode("utf-8", "replace")[:MAX_README_CHARS]
        if icon is None and rel in ("icon.svg", "preview.png", "icon.png"):
            if info.file_size <= MAX_ICON_BYTES:
                mime = "image/svg+xml" if rel.endswith(".svg") else "image/png"
                icon = (mime, zf.read(info.filename))

    files = sorted(
        ({"name": in_app(i.filename), "size": i.file_size} for i in members),
        key=lambda f: f["name"],
    )
    return {
        "name": manifest.get("name") or "",
        "entry": manifest.get("entry") or "",
        "root": root,
        "is_app_file": bool(manifest.get("fused_app_file")),
        "readme": readme,
        "icon": icon,
        "files": files,
        "total_size": sum(f["size"] for f in files),
    }


def _js(value):
    """Embed a value in a <script> without letting its text close the tag."""
    return json.dumps(value).replace("</", "<\\/")


def _icon_markup(icon):
    """SVG goes in an <img>, not inline: an <img> cannot run the file's scripts."""
    if icon is None:
        return '<div class="icon placeholder">.fused</div>'
    import base64

    mime, raw = icon
    data = base64.b64encode(raw).decode("ascii")
    return f'<img class="icon" src="data:{mime};base64,{data}" alt="app icon">'


def _page(path, app, download_url):
    file_name = html.escape(path.rsplit("/", 1)[-1], quote=True)
    title = html.escape(app["name"] or path.rsplit("/", 1)[-1], quote=True)
    entry = html.escape(app["entry"] or "—", quote=True)
    listed = app["files"][:MAX_LISTED_FILES]
    hidden = len(app["files"]) - len(listed)

    badge = "" if app["is_app_file"] else (
        '<span class="warn" title="manifest.json has no fused_app_file marker">'
        "not marked as an app file</span>"
    )
    download = (
        f'<a class="dl" href="{html.escape(download_url, quote=True)}" download="{file_name}">'
        f"Download {file_name}</a>"
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
  html, body {{ margin: 0; background: #1a1a1a; color: #cccccc; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
          font-size: 15px; line-height: 1.65; }}
  .page {{ max-width: 900px; margin: 0 auto; padding: 28px 24px 64px; }}
  header {{ display: flex; gap: 18px; align-items: center; margin-bottom: 8px; }}
  .icon {{ width: 72px; height: 72px; flex: none; object-fit: contain;
           background: #2a2a2a; border-radius: 12px; padding: 6px; }}
  .icon.placeholder {{ display: flex; align-items: center; justify-content: center;
           color: #D1E550; font-family: ui-monospace, Menlo, monospace; font-size: 13px;
           padding: 0; }}
  h1 {{ color: #D1E550; font-size: 1.6em; margin: 0 0 4px; }}
  .sub {{ color: #888; font-size: .9em; }}
  .sub code {{ color: #cccccc; }}
  .bar {{ display: flex; flex-wrap: wrap; gap: 10px; align-items: center; margin: 18px 0 28px; }}
  .dl {{ background: #D1E550; color: #1a1a1a; text-decoration: none; font-weight: 600;
         padding: 8px 16px; border-radius: 6px; }}
  .dl:hover {{ background: #E8FF59; }}
  .warn {{ color: #ff6b6b; font-size: .9em; }}
  h2 {{ color: #D1E550; font-size: 1.1em; margin: 32px 0 10px;
        border-bottom: 1px solid #333; padding-bottom: 6px; }}
  table {{ border-collapse: collapse; width: 100%; font-size: .92em; }}
  td {{ border-bottom: 1px solid #2a2a2a; padding: 6px 10px; }}
  td.n {{ font-family: ui-monospace, "JetBrains Mono", Menlo, Consolas, monospace;
          word-break: break-all; }}
  td.s {{ text-align: right; color: #888; white-space: nowrap; width: 1%; }}
  .entry {{ color: #D1E550; }}
  .more {{ color: #888; font-size: .9em; padding: 8px 10px; }}
  .doc h1, .doc h2, .doc h3, .doc h4 {{ color: #D1E550; margin: 24px 0 10px; line-height: 1.3; }}
  .doc h1 {{ font-size: 1.5em; border: 0; }} .doc h2 {{ font-size: 1.25em; }}
  .doc a {{ color: #D1E550; }}
  .doc code {{ background: #2a2a2a; padding: 2px 6px; border-radius: 3px;
          font-family: ui-monospace, "JetBrains Mono", Menlo, Consolas, monospace;
          font-size: .9em; }}
  .doc pre {{ background: #2a2a2a; padding: 14px 16px; border-radius: 6px; overflow-x: auto; }}
  .doc pre code {{ background: none; padding: 0; }}
  .doc blockquote {{ border-left: 4px solid #D1E550; margin: 16px 0; padding: 2px 0 2px 16px;
          color: #999; }}
  .doc table {{ margin: 16px 0; display: block; overflow-x: auto; }}
  .doc th, .doc td {{ border: 1px solid #444; padding: 8px 12px; text-align: left; }}
  .doc th {{ background: #2a2a2a; color: #D1E550; }}
  .doc img {{ max-width: 100%; }}
  .empty {{ color: #888; font-style: italic; }}
</style>
</head>
<body>
<div class="page">
  <header>
    {_icon_markup(app["icon"])}
    <div>
      <h1>{title}</h1>
      <div class="sub">Entry <code class="entry">{entry}</code> ·
        {len(app["files"])} files · <span id="total"></span> {badge}</div>
    </div>
  </header>
  <div class="bar">{download}</div>

  <h2>README</h2>
  <div class="doc" id="readme"></div>

  <h2>Contents</h2>
  <table id="files"></table>
  {f'<div class="more">{hidden} more files not listed.</div>' if hidden > 0 else ""}
</div>
<script>
  var README = {_js(app["readme"])};
  var FILES = {_js(listed)};
  var ENTRY = {_js(app["entry"])};
  var TOTAL = {app["total_size"]};

  function human(n) {{
    var u = ["B", "KB", "MB", "GB"], i = 0;
    while (n >= 1024 && i < u.length - 1) {{ n /= 1024; i++; }}
    return (i === 0 ? n : n.toFixed(1)) + " " + u[i];
  }}

  document.getElementById("total").textContent = human(TOTAL) + " unpacked";

  // The README is untrusted text: marked passes raw HTML through, so sanitise.
  document.getElementById("readme").innerHTML = README === null
    ? '<span class="empty">This app file has no README.</span>'
    : DOMPurify.sanitize(marked.parse(README));

  var rows = FILES.map(function (f) {{
    var tr = document.createElement("tr");
    var name = document.createElement("td");
    name.className = "n" + (f.name === ENTRY ? " entry" : "");
    name.textContent = f.name;
    var size = document.createElement("td");
    size.className = "s";
    size.textContent = human(f.size);
    tr.appendChild(name);
    tr.appendChild(size);
    return tr;
  }});
  var table = document.getElementById("files");
  rows.forEach(function (r) {{ table.appendChild(r); }});
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
  <p>A <code>.fused</code> app file is a zip archive holding
     <code>manifest.json</code> and the app's files. This one could not be read:</p>
  <p><code>{html.escape(message, quote=True)}</code></p>
  <p><strong>Path:</strong> <code>{safe}</code></p>
</body>
</html>"""
