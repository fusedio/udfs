import html
import json

import fused

MAX_CHARS = 2_000_000


@fused.udf(cache_max_age="30m")
def udf(path: str, preview: bool = False):
    """Render an HTML file as a page, not as a listing of its source.

    Text_File also claims `.html`, but shows the markup. This one puts the
    document in a sandboxed frame so it renders while staying unable to reach
    the workbench around it.

    The markup is read here rather than fetched by the page from a signed URL.
    A requester-pays bucket signs a URL that is only honoured alongside an
    `x-amz-request-payer` header, which a browser fetch cannot add, so those
    files came back 403. Reading server-side also means `/mount/` and
    `gdrive://` paths, which cannot be signed at all, render like any other.
    """
    try:
        raw = fused.api.get(path)
    except Exception as e:  # noqa: BLE001 - surfaced to the viewer, not swallowed
        return _error(path, "HTML", f"{type(e).__name__}: {e}")

    text = raw.decode("utf-8", "replace")
    if len(text) > MAX_CHARS:
        return _error(
            path,
            "HTML",
            f"the document is {len(text):,} characters, over the "
            f"{MAX_CHARS:,} this viewer inlines",
        )

    name = html.escape(path.rsplit("/", 1)[-1], quote=True)
    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{name}</title>
<style>
  html, body {{ margin: 0; height: 100%; background: #ffffff; }}
  iframe {{ width: 100%; height: 100%; border: 0; display: block; }}
</style>
</head>
<body>
<!-- `srcdoc`, not `src`: a sandbox without allow-same-origin gives the frame an
     opaque origin, and a cross-origin URL will not paint in one. The markup is
     assigned from script rather than written into the attribute so the
     document's own quotes and entities reach the frame untouched. -->
<iframe id="doc" title="{name}" sandbox="allow-scripts"></iframe>
<script>
  document.getElementById("doc").srcdoc = {_js(text)};
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
