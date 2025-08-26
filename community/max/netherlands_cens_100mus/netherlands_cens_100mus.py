@fused.udf
def udf(
    bounds: fused.types.Bounds,                     # Bounding box for the tile request (provided by the client)
    path: str = "s3://fused-asset/data/netherlands_census/_sample",  # Default location of the CBS census dataset
    preview: bool = False                          # If True, only load geometry (useful for quick previews)
):
    """
    Load a vector tile from the CBS Netherlands census dataset.
    The function reads the underlying geo‑partitioned table, optionally limits the columns
    to just the geometry for faster preview rendering, filters for areas where
    `aantal_inwoners` > 200, and returns a GeoJSON‑compatible DataFrame that the Fused visualisation
    layer can display.
    """

    # ------------------------------------------------------------------
    # 1️⃣  Decide which columns to read.
    #    When preview mode is on we only need the geometry column – this speeds up the query.
    #    Otherwise we read all columns (None tells the loader to fetch everything).
    # ------------------------------------------------------------------
    use_columns = ["geometry"] if preview else None

    # ------------------------------------------------------------------
    # 2️⃣  Load shared utility functions from the public common UDF library.
    #    `table_to_tile` is a helper that converts a Fused table into a tile‑ready GeoDataFrame.
    # ------------------------------------------------------------------
    common = fused.load(
        "https://github.com/fusedio/udfs/tree/b41216d/public/common/"
    ).utils

    # ------------------------------------------------------------------
    # 3️⃣  Normalise the input path.
    #    The dataset may be referenced with a trailing “/_sample” or “/_metadata”.
    #    In those cases we strip the suffix to obtain the base table path.
    # ------------------------------------------------------------------
    base_path = (
        path.rsplit("/", maxsplit=1)[0]
        if path.endswith("/_sample") or path.endswith("/_metadata")
        else path
    )

    # ------------------------------------------------------------------
    # 4️⃣  Convert the table slice that falls inside the requested bounds into a tile.
    #    `min_zoom=7` ensures that tiles are only generated for zoom levels >= 7,
    #    which matches the resolution of the source data.
    # ------------------------------------------------------------------
    df = common.table_to_tile(
        bounds,
        table=base_path,
        use_columns=use_columns,
        min_zoom=7,
    )

    # ------------------------------------------------------------------
    # 5️⃣  Filter for areas with more than 200 inhabitants.
    # ------------------------------------------------------------------
    if not preview:  # preview mode only loads geometry, so no filter needed there
        df = df[df["aantal_inwoners"] > 200]

    # ------------------------------------------------------------------
    # 6️⃣  Return the resulting GeoDataFrame.
    #    The Fused runtime will automatically render it using the vector layer configuration.
    # ------------------------------------------------------------------
    return df