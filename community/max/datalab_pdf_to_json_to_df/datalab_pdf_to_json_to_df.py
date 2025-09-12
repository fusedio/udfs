@fused.udf
def udf(
    pdf_url: str = "https://www2.census.gov/library/publications/2024/demo/p60-284.pdf",
    raw_table_idx: int = 0,          # index of the *raw* table to return (default first)
):
    """
    Downloads a PDF (unless it already resides on the Fused system or S3), sends it to
    the Datalab OCR API, polls for completion, extracts all tables from the returned JSON,
    prints a readable preview of each *raw* table, and returns the raw table specified by
    `raw_table_idx` (default is the first raw table).  This version skips the combine‑step
    and works directly with the raw tables.
    """
    import os
    import time
    import urllib.parse
    from collections import OrderedDict

    import pandas as pd
    import requests
    import fsspec

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------
    api_url = "https://www.datalab.to/api/v1/table_rec"
    api_key = fused.secrets["datalab"]

    # ------------------------------------------------------------------
    # Determine a local cache path for the PDF
    # ------------------------------------------------------------------
    parsed = urllib.parse.urlparse(pdf_url)

    if parsed.scheme == "s3":
        # PDF already lives on S3 – copy it locally for the upload step
        pdf_name = os.path.splitext(os.path.basename(pdf_url))[0]
        if not pdf_name.lower().endswith(".pdf"):
            pdf_name = f"{pdf_name}.pdf"
        local_path = fused.file_path(f"pdfs/{pdf_name}")

        @fused.cache
        def copy_from_s3(s3_path: str, dst: str) -> str:
            """Copy a file from S3 to the local mount (cached)."""
            if not os.path.isfile(dst):
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                with fsspec.open(s3_path, "rb") as src, open(dst, "wb") as dst_f:
                    for chunk in src:
                        dst_f.write(chunk)
            return dst

        copy_from_s3(pdf_url, local_path)
        print(f"Using PDF from S3, cached locally at: {local_path}")
    else:
        # Remote URL – derive a stable local filename and download if needed
        pdf_name = os.path.splitext(os.path.basename(pdf_url))[0]
        if not pdf_name.lower().endswith(".pdf"):
            pdf_name = f"{pdf_name}.pdf"
        local_path = fused.file_path(f"pdfs/{pdf_name}")
        print(f"Local cache path for remote PDF: {local_path}")

        @fused.cache
        def download_pdf(url: str, path: str) -> str:
            """Download the PDF to the given local path (cached)."""
            if not os.path.isfile(path):
                os.makedirs(os.path.dirname(path), exist_ok=True)
                headers = {
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
                }
                r = requests.get(url, stream=True, timeout=10, headers=headers)
                r.raise_for_status()
                with open(path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            return path

        download_pdf(pdf_url, local_path)

    # ------------------------------------------------------------------
    # Cached helper: call Datalab API (multipart/form‑data)
    # ------------------------------------------------------------------
    @fused.cache
    def call_datalab(pdf_url: str, path: str, api_key: str) -> dict:
        """
        Upload the PDF to Datalab and return the JSON response (cached).
        Non‑file parameters are sent via the `data` payload, not `files`.
        """
        with open(path, "rb") as f:
            # Only the actual file goes in `files`
            files = {
                "file": (os.path.basename(path), f, "application/pdf")
            }
            # All other parameters go in `data` (as strings)
            data = {
                "use_llm": "true",
                "force_ocr": "false",
                "paginate": "false",
                "output_format": "json",
            }
            headers = {"X-Api-Key": api_key}
            resp = requests.post(api_url, files=files, data=data, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.json()

    # ------------------------------------------------------------------
    # Execute the steps
    # ------------------------------------------------------------------
    result_json = call_datalab(pdf_url, local_path, api_key)

    # ------------------------------------------------------------------
    # Poll for completion if a check URL is provided
    # ------------------------------------------------------------------
    headers = {"X-Api-Key": api_key}
    max_polls = 300
    data = result_json
    check_url = result_json.get("request_check_url")
    if check_url:
        for _ in range(max_polls):
            time.sleep(2)
            resp = requests.get(check_url, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") == "complete":
                break

    final_json = data

    # ------------------------------------------------------------------
    # Extract all table HTML blocks from the JSON structure
    # ------------------------------------------------------------------
    table_htmls = []
    for page in final_json.get("json", {}).get("children", []):
        for child in page.get("children", []):
            if child.get("block_type") == "Table" and child.get("html"):
                table_htmls.append(child["html"])

    if not table_htmls:
        raise ValueError("No tables found in the JSON structure.")

    # ------------------------------------------------------------------
    # Parse each table HTML into a DataFrame (skip empty tables)
    # ------------------------------------------------------------------
    raw_tables = []
    for idx, html in enumerate(table_htmls):
        df_tbl = pd.read_html(html)[0]  # assume a single table per HTML block
        if df_tbl.shape[0] == 0:
            continue
        raw_tables.append(df_tbl)

        # Printable preview of each raw table
        print(f"\n--- Raw Table {idx} ------------------------------------------------")
        print("Shape:", df_tbl.shape)
        print(df_tbl.head().to_string(index=False))
        print("-" * 60)

    if not raw_tables:
        raise ValueError("All extracted tables are empty.")

    # ------------------------------------------------------------------
    # Select and return the requested raw table
    # ------------------------------------------------------------------
    if raw_table_idx < 0 or raw_table_idx >= len(raw_tables):
        raise IndexError(
            f"raw_table_idx {raw_table_idx} out of range (0-{len(raw_tables) - 1})"
        )
    df = raw_tables[raw_table_idx]

    print(f"\n--- Selected Raw Table {raw_table_idx} ------------------------------------------------")
    print("Shape:", df.shape)
    print(df.head().to_string(index=False))
    print("-" * 60)

    # ------------------------------------------------------------------
    # Fix rows where multiple categories are merged in a single cell.
    # Mirrors the row‑splitting logic from `datalab_json_to_df`.
    # ------------------------------------------------------------------
    def split_merged_row(row):
        """Split a row whose cells contain two whitespace‑separated values."""
        parts = [str(v).split() for v in row]
        row1 = [p[0] if len(p) > 0 else "" for p in parts]
        row2 = [p[1] if len(p) > 1 else "" for p in parts]
        return row1, row2

    rows = []
    for _, row in df.iterrows():
        first_cell = str(row.iloc[0])
        if " " in first_cell and any(ch.isdigit() for ch in first_cell):
            r1, r2 = split_merged_row(row)
            rows.append(r1)
            rows.append(r2)
        else:
            rows.append(list(row))

    df = pd.DataFrame(rows, columns=df.columns)

    # Inspect schema as requested
    print(df.T)

    return df