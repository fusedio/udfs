@fused.udf
def udf(save_to_s3: bool = True):
    import pandas as pd
    import requests
    import io
    import zipfile

    url = "https://cloud.openquake.org/public.php/dav/files/6SnFk2f92JEr76H"

    @fused.cache
    def fetch_zip(url):
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        return response.content

    content = fetch_zip(url)
    zf = zipfile.ZipFile(io.BytesIO(content))

    tif_name = "v2023_1_pga_475_rock_3min.tif"
    s3_path = f"s3://fused-asset/demos/seismic_data/{tif_name}"

    print(f"Extracting {tif_name} from ZIP...")
    tif_bytes = zf.read(tif_name)
    print(f"Extracted {len(tif_bytes):,} bytes")

    if save_to_s3:
        import s3fs
        fs = s3fs.S3FileSystem()
        with fs.open(s3_path, "wb") as f:
            f.write(tif_bytes)
        print(f"Saved to {s3_path}")

    return pd.DataFrame([{
        "filename": tif_name,
        "size_bytes": len(tif_bytes),
        "s3_path": s3_path,
        "status": "saved" if save_to_s3 else "dry_run"
    }])
