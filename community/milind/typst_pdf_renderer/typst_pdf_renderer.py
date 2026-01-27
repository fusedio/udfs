@fused.udf
def udf(typst_content: str = """
#set page(width: 400pt, height: auto, margin: 20pt)
#set text(font: "Linux Libertine", size: 12pt)

= Hello from Typst!

This PDF was generated using *Typst* inside a Fused UDF.

== Features
- Fast compilation
- Modern syntax
- Beautiful typography

#table(
  columns: (auto, auto),
  [*Name*], [*Value*],
  [Alpha], [1],
  [Beta], [2],
  [Gamma], [3],
)

$ sum_(k=0)^n k = (n(n+1)) / 2 $
"""):

    html = typst_to_html(typst_content)
    return html


def typst_to_html(content: str) -> str:
    """Compile Typst content to PDF and return HTML to render it."""
    import sys
    sys.path.append("/mount/envs/milind/lib/python3.11/site-packages/")
    import typst
    import base64
    
    # Compile typst to PDF
    typ_path = fused.file_path("document.typ")
    pdf_path = fused.file_path("document.pdf")
    
    with open(typ_path, "w") as f:
        f.write(content)
    
    typst.compile(typ_path, output=pdf_path)
    
    with open(pdf_path, "rb") as f:
        pdf_bytes = f.read()
    
    print(f"PDF generated: {len(pdf_bytes)} bytes")
    
    # Convert to base64 and render as HTML
    pdf_base64 = base64.b64encode(pdf_bytes).decode('utf-8')
    
    return f"""
    <html>
    <head>
        <style>
            body {{
                margin: 0;
                padding: 0;
                background: white;
            }}
            iframe {{
                width: 100%;
                height: 100vh;
                border: none;
            }}
        </style>
    </head>
    <body>
        <iframe src="data:application/pdf;base64,{pdf_base64}"></iframe>
    </body>
    </html>
    """