import fused
import fsspec


@fused.udf()
def udf(path: str):
    """
    File UDF that accepts paths with empty/missing extensions.
    Detects the MIME type from the actual file content (magic bytes),
    infers a plausible extension, and delegates to the matching File UDF.

    Routing table (extension -> UDF):
      .parquet/.pq              -> UDF_Preview_Parquet
      .xlsx/.xls                -> UDF_Pandas_Excel
      .tif/.tiff                -> UDF_GeoTIFF_File
      .nc/.nc4/.hdf5            -> UDF_NetCDF_File
      .png/.jpg/.jpeg/.bmp/etc  -> UDF_ImageIO_File
      .mp4/.avi/.mov/.mkv/.webm -> UDF_Video_File
      .docx/.doc                -> UDF_DOCX_File
      .csv/.tsv/.json/.txt/etc  -> UDF_Text_File
      .zip                      -> UDF_GeoPandas_ZIP
      .geojson/.shp/.gpkg/.kml  -> UDF_Preview_Parquet
    """
    import os

    # --- Step 1: Determine if the path already has a known extension ---
    _, ext = os.path.splitext(path)
    ext = ext.lower().strip()

    KNOWN_EXTENSIONS = {
        ".csv",
        ".tsv",
        ".parquet",
        ".pq",
        ".json",
        ".geojson",
        ".xlsx",
        ".xls",
        ".shp",
        ".gpkg",
        ".kml",
        ".tif",
        ".tiff",
        ".zip",
        ".docx",
        ".doc",
        ".nc",
        ".nc4",
        ".hdf5",
        ".png",
        ".jpg",
        ".jpeg",
        ".bmp",
        ".webp",
        ".gif",
        ".mp4",
        ".avi",
        ".mov",
        ".mkv",
        ".webm",
        ".txt",
        ".log",
        ".md",
        ".xml",
        ".yaml",
        ".yml",
    }

    if ext not in KNOWN_EXTENSIONS:
        # --- Step 2: Open the file with fsspec to sniff its type ---
        print(f"No recognized extension for: {path}")
        print("Opening file to detect MIME type from content...")

        # --- Step 3: Detect MIME type from magic bytes ---
        ext = _detect_extension(path)
        print(f"Detected extension: {ext}")
    else:
        print(f"Extension already known: {ext}")

    # --- Step 4: Map extension to the right File UDF ---
    udf_name = _route_to_udf(ext)
    print(f"Routing to: UDF_{udf_name}")

    # --- Step 5: Load and call the target File UDF — let it handle everything ---
    target = fused.load(f"UDF_{udf_name}")
    return target(path=path)


def _detect_extension(path: str) -> str:
    """Detect file extension by reading magic bytes from the file header via fsspec."""
    import struct

    with fsspec.open(path, "rb") as f:
        header = f.read(512)

    # Parquet: magic bytes "PAR1"
    if header[:4] == b"PAR1":
        return ".parquet"

    # ZIP (also .xlsx, .shp in zip, .gpkg sometimes)
    if header[:2] == b"PK":
        if b"xl/" in header or b"[Content_Types].xml" in header:
            return ".xlsx"
        if b"word/" in header:
            return ".docx"
        return ".zip"

    # GeoTIFF / TIFF
    if header[:2] in (b"II", b"MM") and len(header) >= 4:
        byte_order = "<" if header[:2] == b"II" else ">"
        magic = struct.unpack(f"{byte_order}H", header[2:4])[0]
        if magic == 42 or magic == 43:  # 42=TIFF, 43=BigTIFF
            return ".tiff"

    # HDF5 / NetCDF4
    if header[:4] == b"\x89HDF" or header[:8] == b"\x89HDF\r\n\x1a\n":
        return ".nc"
    # Classic NetCDF (CDF magic)
    if header[:3] == b"CDF":
        return ".nc"

    # PNG
    if header[:8] == b"\x89PNG\r\n\x1a\n":
        return ".png"
    # JPEG
    if header[:2] == b"\xff\xd8":
        return ".jpg"
    # GIF
    if header[:4] == b"GIF8":
        return ".gif"
    # WebP
    if header[:4] == b"RIFF" and header[8:12] == b"WEBP":
        return ".webp"
    # BMP
    if header[:2] == b"BM":
        return ".bmp"

    # Video: MP4/MOV (ftyp box)
    if header[4:8] == b"ftyp":
        return ".mp4"
    # AVI
    if header[:4] == b"RIFF" and header[8:12] == b"AVI ":
        return ".avi"
    # MKV/WebM (Matroska)
    if header[:4] == b"\x1a\x45\xdf\xa3":
        return ".mkv"

    # GeoJSON or JSON: starts with { or [
    text_start = header.lstrip()
    if text_start[:1] in (b"{", b"["):
        try:
            text_sample = header.decode("utf-8", errors="ignore")
            if '"Feature' in text_sample or '"geometry"' in text_sample:
                return ".geojson"
            return ".json"
        except Exception:
            return ".json"

    # XML-based formats
    try:
        text_sample = header.decode("utf-8", errors="ignore").lower()
        if "<kml" in text_sample:
            return ".kml"
        if "<?xml" in text_sample:
            return ".xml"
    except Exception:
        pass

    # CSV heuristic: text with commas / tabs and newlines
    try:
        text_sample = header.decode("utf-8", errors="strict")
        newline_count = text_sample.count("\n")
        comma_count = text_sample.count(",")
        tab_count = text_sample.count("\t")
        if newline_count >= 1 and (comma_count >= 2 or tab_count >= 2):
            return ".csv"
    except UnicodeDecodeError:
        pass

    # Excel .xls (legacy BIFF / OLE2)
    if header[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
        return ".xls"

    # GeoPackage (SQLite)
    if header[:16] == b"SQLite format 3\x00":
        return ".gpkg"

    # Fallback: treat as text
    try:
        header.decode("utf-8", errors="strict")
        print("Could not determine type from magic bytes, falling back to .txt")
        return ".txt"
    except UnicodeDecodeError:
        print("Binary file of unknown type, falling back to .txt")
        return ".txt"


def _route_to_udf(ext: str) -> str:
    """Map a file extension to the appropriate existing File UDF name."""
    ext = ext.lower()

    ROUTING = {
        ".parquet": "Preview_Parquet",
        ".pq": "Preview_Parquet",
        ".xlsx": "Pandas_Excel",
        ".xls": "Pandas_Excel",
        ".tif": "GeoTIFF_File",
        ".tiff": "GeoTIFF_File",
        ".nc": "NetCDF_File",
        ".nc4": "NetCDF_File",
        ".hdf5": "NetCDF_File",
        ".png": "ImageIO_File",
        ".jpg": "ImageIO_File",
        ".jpeg": "ImageIO_File",
        ".bmp": "ImageIO_File",
        ".gif": "ImageIO_File",
        ".webp": "ImageIO_File",
        ".mp4": "Video_File",
        ".avi": "Video_File",
        ".mov": "Video_File",
        ".mkv": "Video_File",
        ".webm": "Video_File",
        ".docx": "DOCX_File",
        ".doc": "DOCX_File",
        ".csv": "Text_File",
        ".tsv": "Text_File",
        ".json": "Text_File",
        ".txt": "Text_File",
        ".log": "Text_File",
        ".md": "Text_File",
        ".xml": "Text_File",
        ".yaml": "Text_File",
        ".yml": "Text_File",
        ".geojson": "Preview_Parquet",
        ".shp": "Preview_Parquet",
        ".gpkg": "Preview_Parquet",
        ".kml": "Preview_Parquet",
        ".zip": "GeoPandas_ZIP",
    }

    return ROUTING.get(ext, "Text_File")
