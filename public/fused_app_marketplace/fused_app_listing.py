import html as _html

# fused-render deep link that installs the reviewed copy of a listing.
FUSED_RENDER_MARKET_LINK = "fused-render://open?marketplace={slug}"
FUSED_RENDER_DOWNLOAD_PAGE = "https://www.fused.io/render"


@fused.udf(cache_max_age="0s")
def udf(slug: str = "sine-starter"):
    """The public landing page of a reviewed marketplace listing: the app at
    its APPROVED commit (readme, icon, preview, files), the review line, What's
    new, remix lineage, contributors, and the install deep link."""
    slug = str(slug or "").strip()
    try:
        idx = _load_index()
    except MarketError as e:
        return _listing_error(slug, str(e))
    if slug not in idx["apps"]:
        return _listing_error(slug, "This app isn't in the Fused Community (it may have been delisted).")
    row = idx["apps"][slug]
    facts = _app_facts(row["repo"], row["commit"])
    app = _catalog_row(idx, slug, facts)
    _record_event("views", slug, row["commit"])
    _stats_refresh()
    remix = idx["apps"].get(app["remix_of"]) if app["remix_of"] else None
    block = next((b for b in idx["blocked"] if b["slug"] == slug and b["commit"] == row["commit"]
                  and b["level"] == app["blocked"]), None) if app["blocked"] else None
    return _listing_page(app, facts or {}, remix, block)


def _fmt_date(iso):
    try:
        return datetime.strptime(iso[:10], "%Y-%m-%d").strftime("%b %-d, %Y")
    except Exception:
        return None


def _js(v):
    # Every "<" escaped, not only "</": "<!--" inside a README would
    # otherwise put the inline script into the HTML parser's escaped state.
    return json.dumps(v).replace("<", "\\u003c")


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


def _listing_page(app, facts, remix, block):
    """The store's detail page as standalone HTML: hero (icon, name + reviewed
    tick, description, author, tags, Install), the block notice, the info
    strip, the preview, then README / files / releases beside the Information
    list."""
    e = _html.escape

    def q(s):
        return e(str(s), quote=True)

    repo, commit, slug = app["repo"], app["commit"], app["slug"]
    title = e(app["name"])
    tree = f"https://github.com/{repo}/tree/{commit}"
    hard = block is not None and block["level"] == "hard"
    author = app.get("author") or {}
    owner = repo.split("/")[0]
    aname = e(author.get("name") or owner)
    agh = author.get("github") or owner
    author_html = f'<a href="https://github.com/{q(agh)}" target="_blank" rel="noopener">{aname}</a>'
    remix_link = f'<a href="?slug={q(remix["slug"])}">{e(remix["name"])}</a>' if remix else ""
    byline = f"by {author_html}" + (f" · Remix of {remix_link}" if remix_link else "")
    icon = (f'<img alt="" src="{q(app["icon_url"])}">' if app["icon_url"] else ".fused")
    tags = [t for t in app.get("tags") or [] if t and t != app["category"]]
    tags_html = ('<ul class="tags">' + "".join(f'<li class="tag">{e(t)}</li>' for t in tags) + "</ul>") if tags else ""
    released = _fmt_date(app["last_release_at"] or "")
    installs = int(app.get("installs") or 0)
    install_txt = f"{_compact(installs)} install{'s' if installs != 1 else ''}"
    n = int(app.get("contributor_count") or 0)
    remixes = int(app.get("remix_count") or 0)
    cat = e(_cat_label(app["category"]))
    size = _human(app["size_bytes"]) if app["size_bytes"] else "—"

    # One primary action, its note under it (the store's hero note).
    if hard:
        action = ""
        note = '<span class="err">Withdrawn by Fused</span> · no longer installable'
    else:
        action = (f'<a class="pill" id="install-btn" href="{q(FUSED_RENDER_MARKET_LINK.format(slug=slug))}">Install</a>')
        note = ("Reviewed by Fused" + (f" · release {e(released)}" if released else "") + f" · {install_txt}")
    notice = ""
    if block is not None:
        reason = e(block.get("reason") or "Fused flagged this release.")
        todo = e(block.get("what_to_do") or "")
        head = "Fused withdrew this release" if hard else "Fused flagged this release"
        notice = _callout("hard" if hard else "warn", head, reason + (f"\n{todo}" if todo else ""))
    shot = (f'<img class="stage" alt="{title} screenshot" src="{q(app["preview_url"])}">'
            if app["preview_url"] else "")

    strip = "".join([
        _cell("Category", cat, "Fused Community"),
        _cell("Version", f"v{e(app['version'])}", "Latest"),
        _cell("Released", e(released) if released else "—", "Reviewed by Fused"),
        _cell("Installs", _compact(installs), "Install" if installs == 1 else "Installs", f"{installs} installs"),
        _cell("Remixes", str(remixes), f"Remix of {e(remix['name'])}" if remix else ("Remix" if remixes == 1 else "Remixes")),
    ])

    whats_new = (f'<section class="section"><div class="new"><h2>What\'s new in v{e(app["version"])}</h2>'
                 f'<p>{e(app["whats_new"])}</p></div></section>' if app["whats_new"] else "")
    files = (facts.get("files") or [])[:500]
    more = max(0, int(app["file_count"] or 0) - len(files))
    files_html = (
        '<section class="section"><div class="section-head"><h2 class="section-title">Files</h2>'
        '<span class="section-count" id="files-foot"></span></div>'
        '<table class="files"><thead><tr><th>Path</th><th class="s">Size</th></tr></thead><tbody id="files-body"></tbody>'
        + (f'<tfoot><tr><td colspan="2">… and {more} more</td></tr></tfoot>' if more else "")
        + "</table></section>"
    )
    rel_items = []
    for r in app.get("releases") or []:
        when = e(_fmt_date(r.get("at") or "") or "Release")
        ver = f'<span class="release-v">v{e(r["version"])}</span>' if r.get("version") else ""
        cur = '<span class="release-tag current">Current</span>' if r.get("commit") == commit else ""
        tag = '<span class="release-tag">Blocked</span>' if r.get("blocked") else ""
        rel_items.append(f'<li class="release{" blocked" if r.get("blocked") else ""}"><div class="release-head">'
                         f'<span class="release-when">{when}</span>{ver}{cur}{tag}</div>'
                         f'<p class="release-new">{e(r.get("whats_new") or "No notes for this release yet.")}</p></li>')
    releases = (f'<section class="section"><div class="section-head"><h2 class="section-title">Releases</h2></div>'
                f'<ol class="releases">{"".join(rel_items)}</ol></section>' if rel_items else "")

    info = "".join([
        _info_row("Version", f"v{e(app['version'])}"),
        _info_row("Released", e(released)) if released else "",
        _info_row("Remix of", remix_link) if remix_link else "",
        _info_row("Category", cat),
        _info_row("Developer", author_html),
        _info_row("Size", size),
        _info_row("Files", str(app["file_count"] or len(files) or "—")),
        _info_row("Contributors", f"{n} credited" if n > 1 else "The author"),
        _info_row("Python", "Required (runs locally)" if app.get("requires_python") else "Not required"),
        _info_row("Source", f'<a href="{q(tree)}" target="_blank" rel="noopener">GitHub {_ARROW}</a>'),
    ])
    script = (f"var README={_js(facts.get('readme'))},FILES={_js(files)},ENTRY={_js(facts.get('entry'))},"
              f"NAME={_js(app['name'])},TOTAL={int(app['size_bytes'] or 0)},RAW={_js(f'{RAW_BASE}/{repo}/{commit}/')},"
              f"EMPTY={_js('This app has no README.')};")
    return (
        _head(title) + '<div class="page">'
        f'<div class="top"><span class="brand">Fused Community</span>'
        f'<a class="toplink" href="{q(tree)}" target="_blank" rel="noopener">View on GitHub {_ARROW}</a></div>'
        f'<div class="hero"><div class="icon">{icon}</div><div class="hero-text">'
        f'<h1 class="name">{title}{"" if hard else _VERIFIED}</h1>'
        f'<p class="sub">{e(app["description"])}</p><div class="author">{byline}</div>{tags_html}</div>'
        f'<div class="actions"><div class="action-row">{action}</div><div class="note">{note}</div></div></div>'
        f"{notice}"
        f'<div class="inst" id="inst" hidden><p><b>Didn\'t open?</b> You may need fused-render first.</p>'
        f'<a class="pill alt" href="{FUSED_RENDER_DOWNLOAD_PAGE}" target="_blank" rel="noopener">Download fused-render</a></div>'
        f'<dl class="strip">{strip}</dl>'
        f"{shot}"
        f'<div class="body"><div class="main">{whats_new}'
        f'<section class="section"><div class="md" id="readme"></div></section>'
        f"{files_html}{releases}</div>"
        f'<aside class="aside"><section><h3 class="side-title">Information</h3><dl class="info">{info}</dl></section></aside></div>'
        f'<div class="foot">Reviewed release of <a href="{q(tree)}" target="_blank" rel="noopener">{e(repo)}</a> @ {e(commit[:7])}'
        f' · <a href="{FUSED_RENDER_DOWNLOAD_PAGE}" target="_blank" rel="noopener">Get fused-render</a></div>'
        f"</div><script>{script}{_JS}</script></body></html>"
    )


def _listing_error(slug, message):
    return _state_page("Fused Community", slug or "No app named", message,
                       ("Get fused-render", FUSED_RENDER_DOWNLOAD_PAGE))


# ---------------------------------------------------------------------------
# Marketplace shared helpers. UDF files run standalone, so this block is
# duplicated BYTE-FOR-BYTE in every marketplace_* UDF and fused_app_listing:
# change it in one, copy it to all. Contract: fused-render docs/CONTRIB_LOOP.md
# §18.2 (index layout), §18.3 (rows) and §18.12 (the S3 store + stats).
#
# The canvas's database is S3 (`S3_PREFIX`). GitHub stays the source of
# truth for the index and the app archives; S3 keeps what was read from them
# (so a GitHub outage or a broken block list serves the last good index,
# flagged `stale`), what was built (the catalog,
# one listing per slug), and the stats (install + view events, one tiny
# object each, rolled up into day files and a current total). No row ever
# depends on an S3 write succeeding: every put is best-effort and logged.
# ---------------------------------------------------------------------------
import io
import json
import os
import re
import zipfile
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote

#: Everything durable lives under here: index/, facts/, catalog/, listings/,
#: stats/. Overridable without a code change by the env var or Fused secret
#: MARKETPLACE_S3_PREFIX.
S3_PREFIX = "s3://fused-users/fused/vasu/fused-render-marketplace/"
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
    import requests
    now = time.monotonic()
    m = _REFS_MEMO.get(repo)
    if m and now - m["at"] < (REFS_OK_S if m["ok"] else REFS_FAIL_S):
        if m["ok"]:
            return m["value"]
        raise ValueError(m["value"])
    try:
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
        value = (head, refs.get("refs/heads/reviews", ""))
    except Exception as e:
        _REFS_MEMO[repo] = {"at": now, "ok": False, "value": str(e)}
        raise
    _REFS_MEMO[repo] = {"at": now, "ok": True, "value": value}
    return value


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


def _persist_index(idx):
    """index/current.json = the newest index read from GitHub; every new
    version also lands in index/history/<generated_at>-<version>.json.
    Never regresses: a worker still holding an older parse (or a read of
    current.json that failed) writes nothing, because current.json is what
    the next GitHub outage serves (blocks included)."""
    v = f"{idx['version']}+{idx.get('reviews_version') or '-'}"
    if _INDEX_WRITTEN.get(v):
        return
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
    ok = _s3_put_json("catalog/current.json",
                      {"version": idx["version"], "reviews_version": idx.get("reviews_version"),
                       "generated_at": idx["generated_at"], "apps": rows})
    for r in rows:
        ok = _s3_put_json(f"listings/{r['slug']}.json", r) and ok
    if ok:
        _CATALOG_WRITTEN[v] = True


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
        got = _s3_read_json(f"stats/{kind}/current.json")
        if got is _S3_ERROR or (got is not None and not isinstance(got, dict)):
            out[kind] = last.get(kind) or {"totals": {}, "window": {}}
            continue
        got = got or {}
        out[kind] = {"totals": got.get("totals") or {}, "window": got.get("window") or {}}
    _STATS_MEMO.update(at=now, value=out)
    return out


def _stats_for(slug):
    st = _stats_all()
    return {"installs": int(st["installs"]["totals"].get(slug, 0)),
            "installs_30d": int(st["installs"]["window"].get(slug, 0)),
            "views": int(st["views"]["totals"].get(slug, 0)),
            "views_30d": int(st["views"]["window"].get(slug, 0))}


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


def _answer(rows, columns=None):
    """A list of row dicts → what `format=json` serves as a JSON list. An
    empty answer keeps its `columns` so it still serialises as `[]`."""
    import pandas as pd
    return pd.DataFrame(rows, dtype=object) if rows else pd.DataFrame(columns=list(columns or []), dtype=object)


def _error_rows(code, message):
    return _answer([{"error_code": code, "error_message": message}])
