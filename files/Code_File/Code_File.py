import html
import json
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
    """Show a source file with syntax highlighting and line numbers.

    The bytes are read here rather than fetched by the page from a signed URL.
    A requester-pays bucket signs a URL that is only honoured alongside an
    `x-amz-request-payer` header, which a browser fetch cannot add, so those
    files came back 403. Reading server-side also means `/mount/` and
    `gdrive://` paths, which cannot be signed at all, render like any other.
    """
    try:
        raw = fused.api.get(path)
    except Exception as e:  # noqa: BLE001 - surfaced to the viewer, not swallowed
        return _error(path, "code", f"{type(e).__name__}: {e}")

    text = raw.decode("utf-8", "replace")
    truncated = len(text) > MAX_CHARS
    if truncated:
        text = text[:MAX_CHARS]

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
</style>
</head>
<body>
<header><b>{name}</b> · <span id="meta"></span></header>
<div class="wrap"><div class="nums" id="nums"></div><pre><code id="code"
  class="language-{lang}"></code></pre></div>
<script>
  var TEXT = {_js(text)};
  var TRUNCATED = {_js(truncated)};

  var lines = TEXT.split("\\n");
  // textContent, never innerHTML — the file is untrusted, and highlight.js
  // escapes on its own only once it owns the node.
  var code = document.getElementById("code");
  code.textContent = TEXT;
  hljs.highlightElement(code);
  document.getElementById("nums").textContent =
    lines.map(function (_, i) {{ return i + 1; }}).join("\\n");
  document.getElementById("meta").textContent =
    lines.length.toLocaleString() + " lines · {lang}" + (TRUNCATED ? " · truncated" : "");
</script>
</body>
</html>"""


def _js(value):
    """Embed a value in a <script> without letting its text close the tag."""
    return json.dumps(value).replace("</", "<\\/")


def _error(path, label, message):
    """The read failed, so there is nothing to show — say which path and why."""
    safe = html.escape(path, quote=True)
    return f"""<!DOCTYPE html>
<html>
<body style="margin:0; padding:24px; background:#1a1a1a; color:#cccccc;
             font-family: system-ui, -apple-system, sans-serif; line-height:1.6;">
  <h2 style="color:#ff6b6b;">Could not read this {label} file</h2>
  <p><code>{html.escape(message, quote=True)}</code></p>
  <p><strong>Path:</strong> <code>{safe}</code></p>
</body>
</html>"""
