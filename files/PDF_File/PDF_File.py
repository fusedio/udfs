import base64
import html
import json

import fused

# Past this the base64 of the file is a bigger page than is worth returning, so
# the document is left to the browser to fetch from a signed URL instead.
MAX_INLINE_BYTES = 20_000_000


@fused.udf(cache_max_age="30m")
def udf(path: str, preview: bool = False):
    """Show a PDF in the browser's own viewer.

    The bytes are read here and handed to the viewer as a blob rather than
    fetched by the browser from a signed URL. A requester-pays bucket signs a
    URL that is only honoured alongside an `x-amz-request-payer` header, which
    the browser cannot add, so those files came back 403 and the `<object>`
    fell through to its fallback. Reading server-side also means `/mount/` and
    `gdrive://` paths, which cannot be signed at all, open like any other.

    A blob URL rather than a `data:` URI because Chrome declines to hand a
    `data:application/pdf` document to its built-in viewer.
    """
    file_name = path.rsplit("/", 1)[-1]
    name = html.escape(file_name, quote=True)

    try:
        raw = fused.api.get(path)
    except Exception as e:  # noqa: BLE001 - surfaced to the viewer, not swallowed
        return _error(path, f"{type(e).__name__}: {e}")

    if len(raw) > MAX_INLINE_BYTES:
        # Too large to inline. A signed URL still works for any bucket that is
        # not requester-pays, which is the only case that got this far anyway.
        try:
            src = _js(fused.api.sign_url(path))
        except Exception as e:  # noqa: BLE001 - no link left to offer
            return _error(path, f"{len(raw):,} bytes is too large to inline, and "
                                f"the path cannot be signed: {type(e).__name__}: {e}")
        b64 = "null"
    else:
        src = "null"
        b64 = _js(base64.b64encode(raw).decode("ascii"))

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
<script>
  var B64 = {b64};
  var URL_ = {src};

  var href = URL_;
  if (B64 !== null) {{
    var bin = atob(B64);
    var bytes = new Uint8Array(bin.length);
    for (var i = 0; i < bin.length; i++) {{ bytes[i] = bin.charCodeAt(i); }}
    href = URL.createObjectURL(new Blob([bytes], {{ type: "application/pdf" }}));
  }}

  // <object> keeps a real fallback subtree; <embed> silently shows nothing
  // in browsers without a built-in PDF viewer. Built by hand rather than
  // written as markup so the blob URL never has to survive HTML escaping.
  var obj = document.createElement("object");
  obj.data = href;
  obj.type = "application/pdf";

  var fallback = document.createElement("div");
  fallback.className = "fallback";
  var heading = document.createElement("h2");
  heading.textContent = "This browser will not display the PDF inline.";
  var para = document.createElement("p");
  var link = document.createElement("a");
  link.href = href;
  link.target = "_blank";
  link.rel = "noopener";
  link.textContent = "Open " + {_js(file_name)} + " in a new tab";
  para.appendChild(link);
  fallback.appendChild(heading);
  fallback.appendChild(para);
  obj.appendChild(fallback);

  document.body.appendChild(obj);
</script>
</body>
</html>"""


def _js(value):
    """Embed a value in a <script> without letting its text close the tag."""
    return json.dumps(value).replace("</", "<\\/")


def _error(path, message):
    """The read failed, so there is nothing to show — say which path and why."""
    safe = html.escape(path, quote=True)
    return f"""<!DOCTYPE html>
<html>
<body style="margin:0; padding:24px; background:#1a1a1a; color:#cccccc;
             font-family: system-ui, -apple-system, sans-serif; line-height:1.6;">
  <h2 style="color:#ff6b6b;">Could not read this PDF</h2>
  <p><code>{html.escape(message, quote=True)}</code></p>
  <p><strong>Path:</strong> <code>{safe}</code></p>
</body>
</html>"""
