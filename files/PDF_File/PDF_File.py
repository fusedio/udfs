import html

import fused


@fused.udf(cache_max_age="30m")
def udf(path: str, preview: bool = False):
    """Show a PDF in the browser's own viewer, straight from a signed URL."""
    if path.startswith("/mount/") or path.startswith("gdrive://"):
        return _no_signed_url(path, "PDF")

    url = html.escape(fused.api.sign_url(path), quote=True)
    name = html.escape(path.rsplit("/", 1)[-1], quote=True)
    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{name}</title>
<style>
  html, body {{ margin: 0; height: 100%; background: #1a1a1a; color: #cccccc;
                font-family: system-ui, -apple-system, "Segoe UI", sans-serif; }}
  object {{ width: 100%; height: 100%; border: 0; display: block; }}
  .fallback {{ padding: 24px; line-height: 1.6; }}
  a {{ color: #D1E550; }}
</style>
</head>
<body>
  <!-- <object> keeps a real fallback subtree; <embed> silently shows nothing
       in browsers without a built-in PDF viewer. -->
  <object data="{url}" type="application/pdf">
    <div class="fallback">
      <h2>This browser will not display the PDF inline.</h2>
      <p><a href="{url}">Open {name} in a new tab</a></p>
    </div>
  </object>
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
