import html
import os

import fused

MAX_CHARS = 800_000

# highlight.js language ids, keyed by extension. Anything it does not know
# still renders — just without colour.
LANGUAGES = {
    "py": "python", "js": "javascript", "mjs": "javascript", "cjs": "javascript",
    "ts": "typescript", "tsx": "typescript", "jsx": "javascript",
    "css": "css", "scss": "scss", "less": "less",
    "go": "go", "rs": "rust", "java": "java", "c": "c", "h": "c",
    "cpp": "cpp", "hpp": "cpp", "cc": "cpp", "rb": "ruby", "php": "php",
    "swift": "swift", "kt": "kotlin", "sh": "bash", "bash": "bash",
    "zsh": "bash", "fish": "bash", "sql": "sql", "yaml": "yaml", "yml": "yaml",
    "toml": "ini", "ini": "ini", "cfg": "ini", "conf": "ini", "xml": "xml",
    "lua": "lua", "r": "r", "pl": "perl", "vim": "vim", "gradle": "groovy",
    "proto": "protobuf", "graphql": "graphql", "tf": "hcl",
}


@fused.udf(cache_max_age="30m")
def udf(path: str, preview: bool = False):
    """Show a source file with syntax highlighting and line numbers."""
    if path.startswith("/mount/") or path.startswith("gdrive://"):
        return _no_signed_url(path, "Code")

    url = html.escape(fused.api.sign_url(path), quote=True)
    name = html.escape(path.rsplit("/", 1)[-1], quote=True)
    ext = os.path.splitext(path)[1].lstrip(".").lower()
    lang = LANGUAGES.get(ext, "plaintext")
    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{name}</title>
<link rel="stylesheet"
      href="https://cdn.jsdelivr.net/npm/highlight.js@11/styles/github-dark.min.css">
<script src="https://cdn.jsdelivr.net/npm/highlight.js@11/lib/highlight.min.js"></script>
<style>
  * {{ box-sizing: border-box; }}
  html, body {{ margin: 0; background: #1a1a1a; color: #cccccc; }}
  body {{ font-family: ui-monospace, "JetBrains Mono", Menlo, Consolas, monospace;
          font-size: 13px; line-height: 1.55; }}
  header {{ padding: 10px 16px; border-bottom: 1px solid #333; color: #999;
            font-size: 12px; position: sticky; top: 0; background: #1a1a1a; }}
  header b {{ color: #D1E550; font-weight: 600; }}
  .wrap {{ display: flex; }}
  .nums {{ flex: none; padding: 16px 10px 32px; text-align: right; color: #555;
           -webkit-user-select: none; user-select: none; border-right: 1px solid #2a2a2a; }}
  pre {{ margin: 0; flex: 1 1 auto; min-width: 0; padding: 16px; overflow-x: auto; }}
  pre code.hljs {{ background: none; padding: 0; }}
  .note {{ color: #D1E550; padding: 12px 16px; }}
  .err {{ color: #ff6b6b; padding: 24px;
          font-family: system-ui, -apple-system, sans-serif; }}
</style>
</head>
<body>
<header><b>{name}</b> · <span id="meta">loading…</span></header>
<div class="wrap"><div class="nums" id="nums"></div><pre><code id="code"
  class="language-{lang}"></code></pre></div>
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
        text = text.slice(0, MAX);
        note = " · truncated";
      }}
      var lines = text.split("\\n");
      // textContent, never innerHTML — the file is untrusted, and highlight.js
      // escapes on its own only once it owns the node.
      var code = document.getElementById("code");
      code.textContent = text;
      hljs.highlightElement(code);
      document.getElementById("nums").textContent =
        lines.map(function (_, i) {{ return i + 1; }}).join("\\n");
      document.getElementById("meta").textContent =
        lines.length.toLocaleString() + " lines · {lang}" + note;
    }})
    .catch(function (e) {{
      document.body.innerHTML =
        '<div class="err"><h2>Could not load {name}</h2><p>' + e.message + '</p></div>';
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
