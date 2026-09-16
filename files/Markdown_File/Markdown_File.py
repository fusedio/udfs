import html

import fused

MAX_CHARS = 400_000


@fused.udf(cache_max_age="30m")
def udf(path: str, preview: bool = False):
    """Render Markdown as formatted prose.

    Text_File guesses whether a file is Markdown from its content; this UDF is
    claimed by extension, so `.md` renders without the guess.
    """
    if path.startswith("/mount/") or path.startswith("gdrive://"):
        return _no_signed_url(path, "Markdown")

    url = html.escape(fused.api.sign_url(path), quote=True)
    name = html.escape(path.rsplit("/", 1)[-1], quote=True)
    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{name}</title>
<script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/dompurify@3/dist/purify.min.js"></script>
<style>
  * {{ box-sizing: border-box; }}
  html, body {{ margin: 0; background: #1a1a1a; color: #cccccc; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
          font-size: 15px; line-height: 1.65; }}
  .doc {{ max-width: 860px; margin: 0 auto; padding: 32px 24px 64px; }}
  .doc h1, .doc h2, .doc h3, .doc h4, .doc h5, .doc h6 {{ color: #D1E550; margin: 28px 0 12px;
          line-height: 1.3; }}
  .doc h1 {{ font-size: 2em; }} .doc h2 {{ font-size: 1.5em; }} .doc h3 {{ font-size: 1.25em; }}
  .doc h1, .doc h2 {{ border-bottom: 1px solid #333; padding-bottom: 6px; }}
  .doc a {{ color: #D1E550; }}
  .doc code {{ background: #2a2a2a; padding: 2px 6px; border-radius: 3px;
          font-family: ui-monospace, "JetBrains Mono", Menlo, Consolas, monospace;
          font-size: .9em; }}
  .doc pre {{ background: #2a2a2a; padding: 14px 16px; border-radius: 6px; overflow-x: auto; }}
  .doc pre code {{ background: none; padding: 0; }}
  .doc blockquote {{ border-left: 4px solid #D1E550; margin: 16px 0; padding: 2px 0 2px 16px;
          color: #999; }}
  .doc table {{ border-collapse: collapse; width: 100%; margin: 16px 0; display: block;
          overflow-x: auto; }}
  .doc th, .doc td {{ border: 1px solid #444; padding: 8px 12px; text-align: left; }}
  .doc th {{ background: #2a2a2a; color: #D1E550; }}
  .doc img {{ max-width: 100%; }}
  .doc hr {{ border: 0; border-top: 1px solid #333; margin: 28px 0; }}
  .note {{ color: #D1E550; background: #2a2a2a; border-radius: 5px; padding: 10px;
          text-align: center; margin: 32px 0; }}
  .err {{ color: #ff6b6b; }}
</style>
</head>
<body>
<div class="doc" id="doc">Loading {name}…</div>
<script>
  var MAX = {MAX_CHARS};
  fetch("{url}")
    .then(function (r) {{
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.text();
    }})
    .then(function (text) {{
      var note = "";
      if (text.length > MAX) {{
        note = '\\n\\n<div class="note">Truncated at ' + MAX.toLocaleString() +
               ' of ' + text.length.toLocaleString() + ' characters.</div>';
        text = text.slice(0, MAX);
      }}
      // The file is untrusted text: marked emits raw HTML verbatim, so sanitise.
      document.getElementById("doc").innerHTML =
        DOMPurify.sanitize(marked.parse(text) + note);
    }})
    .catch(function (e) {{
      document.getElementById("doc").innerHTML =
        '<h2 class="err">Could not load {name}</h2><p>' + e.message + '</p>';
    }});
</script>
</body>
</html>"""


def _no_signed_url(path, label):
    """Signed URLs are the only way the browser can reach the bytes."""
    safe = html.escape(path, quote=True)
    return f"""<!DOCTYPE html>
<html>
<body style="margin:0; padding:24px; background:#1a1a1a; color:#cccccc;
             font-family: system-ui, -apple-system, sans-serif; line-height:1.6;">
  <h2 style="color:#ff6b6b;">{label} preview not available</h2>
  <p>Signed URLs are not supported for <code>/mount/</code> or <code>gdrive://</code>
     paths, so this viewer cannot load the file from the browser.</p>
  <p><strong>Path:</strong> <code>{safe}</code></p>
</body>
</html>"""
