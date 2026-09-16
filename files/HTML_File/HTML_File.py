import html

import fused


@fused.udf(cache_max_age="30m")
def udf(path: str, preview: bool = False):
    """Render an HTML file as a page, not as a listing of its source.

    Text_File also claims `.html`, but shows the markup. This one puts the
    document in a sandboxed frame so it renders while staying unable to reach
    the workbench around it.
    """
    if path.startswith("/mount/") or path.startswith("gdrive://"):
        return _no_signed_url(path, "HTML")

    url = html.escape(fused.api.sign_url(path), quote=True)
    name = html.escape(path.rsplit("/", 1)[-1], quote=True)
    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{name}</title>
<style>
  html, body {{ margin: 0; height: 100%; background: #ffffff; }}
  iframe {{ width: 100%; height: 100%; border: 0; display: block; }}
  .err {{ padding: 24px; background: #1a1a1a; color: #ff6b6b; line-height: 1.6;
          font-family: system-ui, -apple-system, sans-serif; }}
  .err a {{ color: #D1E550; }}
</style>
</head>
<body>
<!-- `srcdoc`, not `src`: a sandbox without allow-same-origin gives the frame an
     opaque origin, and a cross-origin signed URL will not paint in one. -->
<iframe id="doc" title="{name}" sandbox="allow-scripts"></iframe>
<script>
  fetch("{url}")
    .then(function (r) {{
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.text();
    }})
    .then(function (text) {{
      document.getElementById("doc").srcdoc = text;
    }})
    .catch(function (e) {{
      document.body.innerHTML =
        '<div class="err"><h2>Could not load {name}</h2><p>' + e.message +
        '</p><p><a href="{url}">Open it directly</a></p></div>';
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
