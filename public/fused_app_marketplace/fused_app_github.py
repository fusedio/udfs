import html, io, json, re, zipfile, base64

ICON_NAMES = ("icon.png", "icon.svg", "icon.jpg", "icon.jpeg", "logo.png", "logo.svg")
SHOT_NAMES = ("screenshot.png", "screenshot.jpg", "screenshot.jpeg", "preview.png")
MIME = {"png": "image/png", "svg": "image/svg+xml", "jpg": "image/jpeg", "jpeg": "image/jpeg"}

# fused-render deep link: fused-render://open?git=https://github.com/<owner>/<repo>
FUSED_RENDER_GIT_LINK = "fused-render://open?git=https://github.com/{repo}"
FUSED_RENDER_DOWNLOAD_PAGE = "https://www.fused.io/render"


@fused.udf(cache_max_age="10m")
def udf(repo: str = "vasu2001/sine-starter"):
    """Show a landing page for a public GitHub repo that *is* a Fused app.

    The repo source is read from GitHub's zip archive and rendered: README,
    icon, screenshot, files, plus a fused-render `git=` install deep link.
    """
    repo = _clean_repo(repo)
    if not repo:
        return _error(repo, "Pass repo as owner/name, e.g. vasu2001/sine-starter")
    try:
        data = _fetch_zip(repo)
    except Exception as e:
        return _error(repo, f"Could not fetch repo from GitHub: {e}")

    zf = zipfile.ZipFile(io.BytesIO(data))
    members = [i for i in zf.infolist() if not i.is_dir()]
    top = members[0].filename.split("/", 1)[0] + "/" if members else ""
    rel = lambda n: n[len(top):] if n.startswith(top) else n

    app = _read_repo(zf, members, rel)
    return _page(repo, app)


def _clean_repo(repo):
    repo = (repo or "").strip()
    repo = re.sub(r"^(https?://)?(www\.)?github\.com/", "", repo).strip("/")
    repo = re.sub(r"\.git$", "", repo)
    parts = repo.split("/")
    if len(parts) < 2 or not all(re.fullmatch(r"[A-Za-z0-9_.-]+", p) for p in parts[:2]):
        return ""
    return "/".join(parts[:2])


@fused.cache(cache_max_age="10m")
def _fetch_zip(repo):
    import requests
    r = requests.get(f"https://codeload.github.com/{repo}/zip/HEAD", timeout=30)
    if r.status_code == 404:
        raise ValueError("repo not found or not public")
    r.raise_for_status()
    if len(r.content) > 50_000_000:
        raise ValueError("repo archive is larger than 50 MB")
    return r.content


def _read_repo(zf, members, rel):
    names = {rel(i.filename).lower(): i for i in members}
    manifest = {}
    for m in ("manifest.json", "fused.json", "app.json"):
        if m in names:
            try:
                manifest = json.loads(zf.read(names[m].filename).decode("utf-8"))
                break
            except Exception:
                pass

    def find(cands, cap):
        for c in cands:
            for prefix in ("", "files/", "public/", "assets/"):
                i = names.get(prefix + c)
                if i and i.file_size <= cap:
                    return (MIME[c.rsplit(".", 1)[-1]], zf.read(i.filename))
        return None

    readme = None
    for c in ("readme.md", "readme.markdown", "readme"):
        if c in names:
            readme = zf.read(names[c].filename).decode("utf-8", "replace")[:200_000]
            break
    files = sorted(({"name": rel(i.filename), "size": i.file_size} for i in members), key=lambda f: f["name"])
    return {
        "name": manifest.get("name") or "",
        "entry": manifest.get("entry") or ("index.html" if "index.html" in names else ""),
        "readme": readme,
        "icon": find(ICON_NAMES, 512_000),
        "shot": find(SHOT_NAMES, 4_000_000),
        "files": files,
        "total_size": sum(f["size"] for f in files),
    }


def _js(v):
    # Every "<" escaped, not only "</": "<!--" inside a README would
    # otherwise put the inline script into the HTML parser's escaped state.
    return json.dumps(v).replace("<", "\\u003c")


def _img(image, cls, alt=""):
    mime, data = image
    return f'<img class="{cls}" alt="{html.escape(alt)}" src="data:{mime};base64,{base64.b64encode(data).decode()}">'


_html = html

# The store look (fused-render frontend/src/styles/marketplace.css + tokens.css)
# as plain CSS: dark by default, the light palette under prefers-color-scheme.
# A plain string (no f-string), so braces are braces.
_CSS = """
:root{--bg:#131417;--bg-alt:#1b1d21;--fg:#e8eaed;--fg-muted:#9aa0a6;--fg-faint:#7a7e83;--border:#2a2d33;--accent:#E5FF44;--accent-rgb:229,255,68;--on-accent:#10131a;--accent-soft:#c9d95e;--success-bright:#3fca6b;--error:#ff6b6b;--error-rgb:255,107,107;--warning:#d29922;--warning-rgb:210,153,34;--tint:255,255,255;--icon-shadow:0 12px 28px rgba(0,0,0,.5),0 2px 6px rgba(0,0,0,.35);--icon-ring:rgba(255,255,255,.09)}
@media (prefers-color-scheme:light){:root{--bg:#ffffff;--bg-alt:#f4f5f7;--fg:#1f2023;--fg-muted:#61656c;--fg-faint:#84878d;--border:#d8dade;--accent:#5f7300;--accent-rgb:95,115,0;--on-accent:#ffffff;--accent-soft:#5f7300;--success-bright:#1a8f45;--error:#c62828;--error-rgb:198,40,40;--warning:#8f6a10;--warning-rgb:143,106,16;--tint:0,0,0;--icon-shadow:0 12px 26px rgba(16,24,40,.16),0 2px 6px rgba(16,24,40,.08);--icon-ring:rgba(0,0,0,.07)}}
*{box-sizing:border-box}
html,body{margin:0;background:var(--bg);color:var(--fg)}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;font-size:15px;line-height:1.5;-webkit-font-smoothing:antialiased;overflow-x:hidden}
.page{max-width:1000px;margin:0 auto;padding:28px 16px 72px}
.top{display:flex;align-items:center;justify-content:space-between;gap:12px;min-height:24px;margin:0 0 20px}
.brand{font-size:13px;font-weight:550;color:var(--fg-muted)}
.toplink{display:inline-flex;align-items:center;gap:3px;font-size:12.5px;font-weight:550;color:var(--fg-muted);text-decoration:none;white-space:nowrap}
.toplink:hover{color:var(--fg)}
.hero{display:flex;align-items:center;gap:28px}
.icon{position:relative;flex:none;display:grid;place-items:center;width:112px;height:112px;overflow:hidden;border-radius:26px;background:var(--bg-alt);box-shadow:var(--icon-shadow);color:var(--accent-soft);font-family:ui-monospace,Menlo,monospace;font-size:14px;font-weight:700}
.icon img{display:block;width:100%;height:100%;object-fit:cover}
.icon::after{content:"";position:absolute;inset:0;border-radius:inherit;box-shadow:inset 0 0 0 1px var(--icon-ring);pointer-events:none}
.hero-text{flex:1;min-width:0}
.name{margin:0;font-size:28px;font-weight:700;line-height:1.15;letter-spacing:-.022em;color:var(--fg);overflow-wrap:anywhere}
.verified{display:inline-block;width:.82em;height:.82em;margin-left:.3em;vertical-align:-.06em;color:var(--success-bright)}
.sub{max-width:620px;margin:6px 0 0;font-size:15px;line-height:1.45;color:var(--fg-muted)}
.author{margin-top:8px;font-size:13px;color:var(--fg-muted)}
.author a{color:var(--fg-muted);text-decoration:none}
.author a:hover{color:var(--fg);text-decoration:underline}
.tags{display:flex;flex-wrap:wrap;gap:6px;margin:12px 0 0;padding:0;list-style:none}
.tag{padding:3px 9px;font-size:11.5px;font-weight:550;color:var(--fg-muted);background:rgba(var(--tint),.06);border-radius:999px}
.actions{display:flex;flex:none;flex-direction:column;align-items:flex-end;gap:8px}
.action-row{display:flex;align-items:center;gap:10px}
.pill{display:inline-flex;align-items:center;justify-content:center;min-width:112px;height:36px;padding:0 18px;font-size:14px;font-weight:650;white-space:nowrap;text-decoration:none;color:var(--on-accent);background:var(--accent);border-radius:999px;transition:filter 120ms ease-out,transform 120ms ease-out}
.pill:hover{filter:brightness(.93)}
.pill:active{transform:scale(.97)}
.pill.alt{color:var(--fg);background:rgba(var(--tint),.08)}
.note{max-width:240px;font-size:12px;line-height:1.4;color:var(--fg-muted);text-align:right}
.note b{font-weight:650;color:var(--fg)}
.note .warn{color:var(--warning)}
.note .err{color:var(--error)}
.callout{display:flex;align-items:flex-start;gap:12px;margin:24px 0 0;padding:13px 16px 13px 14px;background:var(--bg-alt);border:1px solid var(--border);border-radius:14px}
.callout.warn{background:rgba(var(--warning-rgb),.08);border-color:rgba(var(--warning-rgb),.32)}
.callout.hard{background:rgba(var(--error-rgb),.08);border-color:rgba(var(--error-rgb),.32)}
.callout-glyph{display:grid;flex:none;place-items:center;width:28px;height:28px;color:var(--fg-muted);background:rgba(var(--tint),.06);border-radius:8px}
.callout.warn .callout-glyph{color:var(--warning);background:rgba(var(--warning-rgb),.14)}
.callout.hard .callout-glyph{color:var(--error);background:rgba(var(--error-rgb),.14)}
.callout-copy{flex:1;min-width:0;padding-top:4px}
.callout-title{font-size:13.5px;font-weight:650;line-height:1.35;color:var(--fg)}
.callout-body{max-width:760px;margin-top:3px;font-size:13px;line-height:1.5;color:var(--fg-muted);white-space:pre-wrap}
.callout-body a{color:var(--fg)}
.inst{margin:20px 0 0;padding:13px 16px;font-size:13px;color:var(--fg-muted);background:var(--bg-alt);border:1px solid var(--border);border-radius:14px}
.inst[hidden]{display:none}
.inst p{margin:0 0 10px}
.inst b{color:var(--fg)}
.strip{display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));margin:28px 0;padding:14px 0;border-top:1px solid var(--border);border-bottom:1px solid var(--border)}
.cell{display:flex;flex-direction:column;align-items:center;gap:6px;min-width:0;padding:4px 12px;text-align:center}
.cell+.cell{border-left:1px solid var(--border)}
.cell-label{font-size:10.5px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--fg-faint)}
.cell-value{display:block;max-width:100%;min-height:22px;overflow:hidden;font-size:17px;font-weight:650;letter-spacing:-.01em;color:var(--fg);white-space:nowrap;text-overflow:ellipsis}
.cell-caption{max-width:100%;overflow:hidden;font-size:12px;color:var(--fg-muted);white-space:nowrap;text-overflow:ellipsis}
.stage{display:block;width:100%;height:auto;margin:0 0 36px;background:var(--bg-alt);border:1px solid var(--border);border-radius:12px}
.body{display:grid;grid-template-columns:minmax(0,1fr) 280px;gap:48px;align-items:start}
.main{min-width:0}
.aside{display:flex;flex-direction:column;gap:28px;min-width:0}
.side-title{margin:0 0 10px;font-size:13px;font-weight:650;color:var(--fg)}
.info{margin:0}
.row{display:flex;align-items:baseline;justify-content:space-between;gap:16px;padding:9px 0;font-size:13px;border-top:1px solid var(--border)}
.row:last-child{border-bottom:1px solid var(--border)}
.row dt{flex:none;color:var(--fg-muted)}
.row dd{min-width:0;margin:0;overflow:hidden;font-weight:550;color:var(--fg);text-align:right;white-space:nowrap;text-overflow:ellipsis}
.row dd a{color:var(--fg);text-decoration:none}
.row dd a:hover{text-decoration:underline}
.section{margin-top:40px}
.section:first-child{margin-top:0}
.section-head{display:flex;align-items:baseline;justify-content:space-between;gap:12px;margin-bottom:14px}
.section-title{margin:0;font-size:17px;font-weight:650;letter-spacing:-.012em;color:var(--fg)}
.section-count{font-size:12px;font-weight:600;color:var(--fg-muted)}
.new{padding:14px 16px;background:var(--bg-alt);border:1px solid var(--border);border-radius:12px}
.new h2{margin:0 0 6px;font-size:13.5px;font-weight:650;color:var(--fg)}
.new p{margin:0;font-size:13.5px;line-height:1.5;color:var(--fg-muted);white-space:pre-wrap}
.md{max-width:680px;font-size:15px;line-height:1.65;color:var(--fg);overflow-wrap:break-word}
.md>:first-child{margin-top:0}
.md p,.md ul,.md ol,.md blockquote,.md pre,.md table{margin:0 0 16px}
.md h1,.md h2,.md h3,.md h4{margin:32px 0 10px;font-weight:650;line-height:1.3;letter-spacing:-.012em;color:var(--fg)}
.md h1{font-size:24px}
.md h2{font-size:20px}
.md h3{font-size:17px}
.md h4{font-size:15px}
.md a{color:var(--fg);text-decoration:underline;text-decoration-color:rgba(var(--tint),.35);text-underline-offset:3px}
.md a:hover{text-decoration-color:var(--fg)}
.md ul,.md ol{padding-left:22px}
.md li+li{margin-top:5px}
.md li::marker{color:var(--fg-faint)}
.md img{max-width:100%;height:auto;border:1px solid var(--border);border-radius:10px}
.md code{padding:2px 5px;font-family:ui-monospace,Menlo,Consolas,monospace;font-size:.86em;background:rgba(var(--tint),.07);border-radius:5px}
.md pre{padding:14px 16px;overflow-x:auto;font-size:13px;line-height:1.55;background:var(--bg-alt);border:1px solid var(--border);border-radius:10px}
.md pre code{padding:0;font-size:inherit;background:none}
.md blockquote{padding:2px 0 2px 16px;color:var(--fg-muted);border-left:3px solid var(--border)}
.md hr{margin:28px 0;border:none;border-top:1px solid var(--border)}
.md table{display:block;width:100%;overflow-x:auto;font-size:13.5px;border-collapse:collapse}
.md th,.md td{padding:7px 12px 7px 0;text-align:left;border-bottom:1px solid var(--border)}
.md th{font-weight:650}
.empty{color:var(--fg-faint);font-style:italic}
.files{width:100%;font-size:13px;border-collapse:collapse}
.files th{padding:0 0 8px;font-size:10.5px;font-weight:700;letter-spacing:.08em;text-align:left;text-transform:uppercase;color:var(--fg-faint);border-bottom:1px solid var(--border)}
.files td{padding:8px 0;border-bottom:1px solid var(--border)}
.files td.n{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12.5px;word-break:break-all}
.files td.n .dir{color:var(--fg-faint)}
.files td.n.entry{color:var(--accent-soft)}
.files td.s{width:90px;text-align:right;color:var(--fg-muted);font-variant-numeric:tabular-nums;white-space:nowrap}
.files tfoot td{padding-top:10px;font-size:12px;font-weight:600;color:var(--fg-muted);border-bottom:none}
.releases{margin:0;padding:0;list-style:none;border-top:1px solid var(--border)}
.release{padding:12px 0;border-bottom:1px solid var(--border)}
.release-head{display:flex;flex-wrap:wrap;align-items:baseline;gap:8px}
.release-when{font-size:13px;font-weight:650;color:var(--fg)}
.release-v{font-size:12px;color:var(--fg-faint)}
.release-tag{padding:1px 7px;font-size:10.5px;font-weight:700;letter-spacing:.03em;text-transform:uppercase;color:var(--error);background:rgba(var(--error-rgb),.13);border-radius:999px}
.release-tag.current{color:var(--accent-soft);background:rgba(var(--accent-rgb),.14)}
.release-new{margin:4px 0 0;font-size:13px;line-height:1.5;color:var(--fg-muted);overflow-wrap:anywhere;white-space:pre-wrap}
.release.blocked .release-new{text-decoration:line-through}
.state{max-width:520px;margin:72px auto 0;text-align:center}
.state h1{margin:0 0 8px;font-size:22px;font-weight:700;letter-spacing:-.02em;color:var(--fg)}
.state p{margin:0 0 20px;font-size:14px;color:var(--fg-muted);overflow-wrap:anywhere}
.foot{margin-top:56px;padding-top:16px;font-size:12px;color:var(--fg-faint);border-top:1px solid var(--border)}
.foot a{color:var(--fg-muted);text-decoration:none}
.foot a:hover{color:var(--fg)}
@media (max-width:860px){.body{grid-template-columns:minmax(0,1fr);gap:32px}.strip{grid-template-columns:repeat(3,minmax(0,1fr));row-gap:16px}.cell:nth-child(3n+1){border-left:none}}
@media (max-width:640px){.hero{flex-direction:column;align-items:flex-start;gap:18px}.actions{align-items:flex-start;width:100%}.note{max-width:none;text-align:left}.icon{width:88px;height:88px;border-radius:20px}.name{font-size:24px}.strip{grid-template-columns:repeat(2,minmax(0,1fr))}.cell:nth-child(3n+1){border-left:1px solid var(--border)}.cell:nth-child(2n+1){border-left:none}}
"""

# The store's VerifiedMark (platform/ui/appstore/VerifiedMark.tsx): the green
# scalloped badge after a reviewed app's name.
_VERIFIED = (
    '<svg class="verified" viewBox="0 0 24 24" role="img" aria-label="Reviewed by Fused" focusable="false">'
    "<title>Reviewed by Fused</title>"
    '<path fill="currentColor" fill-opacity="0.16" stroke="currentColor" stroke-width="1.6" stroke-linejoin="round" '
    'd="M12 2.2l2.1 1.7 2.7-.5 1.1 2.5 2.5 1.1-.5 2.7L21.8 12l-1.7 2.1.5 2.7-2.5 1.1-1.1 2.5-2.7-.5L12 21.8l-2.1-1.7-2.7.5-1.1-2.5-2.5-1.1.5-2.7L2.2 12l1.7-2.1-.5-2.7 2.5-1.1 1.1-2.5 2.7.5z"/>'
    '<path fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" d="M8.2 12.3l2.6 2.6 5.2-5.4"/>'
    "</svg>"
)
_ARROW = '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M7 17 17 7M7 7h10v10"/></svg>'
_WARN_GLYPH = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/><path d="M12 9v4M12 17h.01"/></svg>'

# The page script, a raw string: no f-string braces, regex backslashes as is.
# Reads the globals the page sets just before it (README, FILES, ENTRY, NAME,
# TOTAL, RAW, EMPTY).
_JS = r"""
function human(n){var u=["B","KB","MB","GB"],i=0;while(n>=1024&&i<3){n/=1024;i++}return(i?n.toFixed(1):Math.round(n))+" "+u[i]}
function fill(id,t){var el=document.getElementById(id);if(el)el.textContent=t}
fill("total",human(TOTAL));
var doc=document.getElementById("readme");
if(README===null){doc.innerHTML='<span class="empty">'+EMPTY+'</span>'}else{
  doc.innerHTML=DOMPurify.sanitize(marked.parse(README),{FORBID_TAGS:["style","form","input","button","textarea","select"],FORBID_ATTR:["style"]});
  doc.querySelectorAll("img").forEach(function(im){var s=im.getAttribute("src");if(s&&!/^(https?:|data:)/.test(s))im.src=RAW+s.replace(/^\.?\//,"")});
  doc.querySelectorAll("a[href]").forEach(function(a){a.target="_blank";a.rel="noopener"});
  var l=doc.firstElementChild;if(l&&l.tagName==="H1"&&l.textContent.trim().toLowerCase()===NAME.trim().toLowerCase())l.remove();}
var btn=document.getElementById("install-btn");
if(btn)btn.addEventListener("click",function(){setTimeout(function(){document.getElementById("inst").hidden=false},1800)});
var t=document.getElementById("files-body");
if(t){FILES.forEach(function(f){var tr=document.createElement("tr"),a=document.createElement("td"),b=document.createElement("td");
  a.className="n"+(f.path===ENTRY?" entry":"");var i=f.path.lastIndexOf("/");
  if(i>0){var d=document.createElement("span");d.className="dir";d.textContent=f.path.slice(0,i+1);a.append(d,document.createTextNode(f.path.slice(i+1)))}else a.textContent=f.path;
  b.className="s";b.textContent=human(f.size);tr.append(a,b);t.append(tr)});
  fill("files-foot",FILES.length?FILES.length+" files · "+human(TOTAL):"Files unavailable")}
"""


def _compact(n):
    """1234 → 1.2k, 12000 → 12k, 1500000 → 1.5M (the store's compactCount)."""
    n = int(n or 0)
    if n < 1000:
        return str(n)
    if n < 10_000:
        return f"{n / 1000:.1f}".replace(".0", "") + "k"
    if n < 1_000_000:
        return f"{round(n / 1000)}k"
    return f"{n / 1_000_000:.1f}".replace(".0", "") + "M"


def _human(n):
    n = float(n or 0)
    for u in ("B", "KB", "MB", "GB"):
        if n < 1024 or u == "GB":
            return (f"{n:.1f}" if u != "B" else f"{int(n)}") + " " + u
        n /= 1024


def _cat_label(cat):
    return " ".join(w.capitalize() for w in str(cat or "other").replace("_", "-").split("-"))


def _cell(label, value, caption=None, title=None):
    e = _html.escape
    t = f' title="{e(title, quote=True)}"' if title else ""
    cap = f'<dd class="cell-caption">{caption}</dd>' if caption is not None else ""
    return f'<div class="cell"{t}><dt class="cell-label">{e(label)}</dt><dd class="cell-value">{value}</dd>{cap}</div>'


def _info_row(label, value):
    return f"<div class=\"row\"><dt>{_html.escape(label)}</dt><dd>{value}</dd></div>"


def _callout(kind, title, body, glyph=_WARN_GLYPH):
    return (f'<section class="callout {kind}" role="alert"><span class="callout-glyph" aria-hidden="true">{glyph}</span>'
            f'<div class="callout-copy"><div class="callout-title">{title}</div>'
            + (f'<div class="callout-body">{body}</div>' if body else "") + "</div></section>")


def _head(title):
    return (f'<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">'
            f'<meta name="viewport" content="width=device-width, initial-scale=1"><meta name="color-scheme" content="dark light">'
            f"<title>{title}</title>"
            '<script src="https://cdn.jsdelivr.net/npm/marked@12.0.2/marked.min.js" integrity="sha384-/TQbtLCAerC3jgaim+N78RZSDYV7ryeoBCVqTuzRrFec2akfBkHS7ACQ3PQhvMVi" crossorigin="anonymous"></script>'
            '<script src="https://cdn.jsdelivr.net/npm/dompurify@3.1.6/dist/purify.min.js" integrity="sha384-+VfUPEb0PdtChMwmBcBmykRMDd+v6D/oFmB3rZM/puCMDYcIvF968OimRh4KQY9a" crossorigin="anonymous"></script>'
            f"<style>{_CSS}</style></head><body>")


def _state_page(title, heading, message, link=None):
    e = _html.escape
    btn = (f'<a class="pill alt" href="{e(link[1], quote=True)}" target="_blank" rel="noopener">{e(link[0])}</a>'
           if link else "")
    return (f'<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">'
            f'<meta name="viewport" content="width=device-width, initial-scale=1"><meta name="color-scheme" content="dark light">'
            f"<title>{e(title)}</title><style>{_CSS}</style></head><body><div class=\"page\">"
            f'<div class="top"><span class="brand">{e(title)}</span></div>'
            f'<div class="state"><h1>{e(heading)}</h1><p>{e(message)}</p>{btn}</div></div></body></html>')


def _page(repo, app):
    """The store's detail layout for an UNREVIEWED app read straight from its
    GitHub repo's tip: no reviewed tick, a plain "not reviewed" callout, and
    the git= install deep link."""
    e = _html.escape

    def q(s):
        return e(str(s), quote=True)

    owner, name = repo.split("/", 1)
    title = e(app["name"] or name)
    gh = f"https://github.com/{repo}"
    zip_url = f"https://codeload.github.com/{repo}/zip/HEAD"
    deeplink = FUSED_RENDER_GIT_LINK.format(repo=repo)
    icon = _img(app["icon"], "", "") if app["icon"] else ".fused"
    shot = _img(app["shot"], "stage", f"{app['name'] or name} screenshot") if app["shot"] else ""
    files = [{"path": f["name"], "size": f["size"]} for f in app["files"][:500]]
    more = max(0, len(app["files"]) - len(files))
    entry = app["entry"] or ""
    size = _human(app["total_size"]) if app["total_size"] else "—"
    owner_link = f'<a href="https://github.com/{q(owner)}" target="_blank" rel="noopener">{e(owner)}</a>'
    repo_link = f'<a href="{q(gh)}" target="_blank" rel="noopener">{e(repo)}</a>'

    strip = "".join([
        _cell("Source", "GitHub", e(repo), repo),
        _cell("Branch", "Default", "Latest commit"),
        _cell("Files", str(len(app["files"])), size),
        _cell("Entry", e(entry) if entry else "—", "Opens first" if entry else "No index.html"),
        _cell("Review", "None", "Not reviewed by Fused"),
    ])
    notice = _callout(
        "warn", "Not reviewed by Fused",
        f"This installs the latest commit of {repo_link}'s default branch straight from GitHub. "
        "Fused hasn't checked this code — read it before you run it.")
    files_html = (
        '<section class="section"><div class="section-head"><h2 class="section-title">Files</h2>'
        '<span class="section-count" id="files-foot"></span></div>'
        '<table class="files"><thead><tr><th>Path</th><th class="s">Size</th></tr></thead><tbody id="files-body"></tbody>'
        + (f'<tfoot><tr><td colspan="2">… and {more} more</td></tr></tfoot>' if more else "")
        + "</table></section>"
    )
    info = "".join([
        _info_row("Source", "GitHub · unreviewed"),
        _info_row("Repository", repo_link),
        _info_row("Developer", owner_link),
        _info_row("Size", size),
        _info_row("Files", str(len(app["files"]))),
        _info_row("Entry", e(entry) if entry else "—"),
        _info_row("Archive", f'<a href="{q(zip_url)}">Download .zip {_ARROW}</a>'),
    ])
    script = (f"var README={_js(app['readme'])},FILES={_js(files)},ENTRY={_js(entry)},"
              f"NAME={_js(app['name'] or name)},TOTAL={int(app['total_size'] or 0)},"
              f"RAW={_js(f'https://raw.githubusercontent.com/{repo}/HEAD/')},EMPTY={_js('This repo has no README.')};")
    return (
        _head(title) + '<div class="page">'
        f'<div class="top"><span class="brand">Fused app on GitHub</span>'
        f'<a class="toplink" href="{q(gh)}" target="_blank" rel="noopener">View on GitHub {_ARROW}</a></div>'
        f'<div class="hero"><div class="icon">{icon}</div><div class="hero-text">'
        f'<h1 class="name">{title}</h1>'
        f'<p class="sub">A Fused app, installed straight from its GitHub repository.</p>'
        f'<div class="author">by {owner_link} · {repo_link}</div></div>'
        f'<div class="actions"><div class="action-row">'
        f'<a class="pill" id="install-btn" href="{q(deeplink)}">Install</a>'
        f'<a class="pill alt" href="{q(gh)}" target="_blank" rel="noopener">GitHub</a></div>'
        f'<div class="note"><span class="warn">Not reviewed by Fused</span> · from GitHub</div></div></div>'
        f"{notice}"
        f'<div class="inst" id="inst" hidden><p><b>Didn\'t open?</b> You may need fused-render first.</p>'
        f'<a class="pill alt" href="{FUSED_RENDER_DOWNLOAD_PAGE}" target="_blank" rel="noopener">Download fused-render</a></div>'
        f'<dl class="strip">{strip}</dl>'
        f"{shot}"
        f'<div class="body"><div class="main">'
        f'<section class="section"><div class="md" id="readme"></div></section>'
        f"{files_html}</div>"
        f'<aside class="aside"><section><h3 class="side-title">Information</h3><dl class="info">{info}</dl></section></aside></div>'
        f'<div class="foot">Tip of {repo_link} · not reviewed by Fused'
        f' · <a href="{FUSED_RENDER_DOWNLOAD_PAGE}" target="_blank" rel="noopener">Get fused-render</a></div>'
        f"</div><script>{script}{_JS}</script></body></html>"
    )


def _error(repo, message):
    return _state_page("Fused app on GitHub", repo or "Invalid repo", message,
                       ("Get fused-render", FUSED_RENDER_DOWNLOAD_PAGE))
