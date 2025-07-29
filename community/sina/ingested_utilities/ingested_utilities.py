@fused.udf
def udf(
    bounds: fused.types.Bounds, 
    path: str='s3://fused-asset/data/us_electric_utilities/ingested/_sample', 
    preview: bool=False,
    buffer_size_m: float = 150,
    min_zoom: int = 10,
):
    """
    Taking power lines only within a `bounds`
    Returns bounding boxes of partitions if above min_zoom
    """
    
    use_columns = ['geometry'] if preview else None
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

    base_path = path.rsplit('/', maxsplit=1)[0] if path.endswith('/_sample') or path.endswith('/_metadata') else path
    # This uses the closes tile with get_tiles(clip=False), so it doesn't clip to tile
    df = common.table_to_tile(bounds, table=base_path, use_columns=use_columns, min_zoom=min_zoom, clip=True)

    if df.shape == 0:
        print(f'No power lines found, skipping... (returning None)')
        return None

    # Clipping to only the areas that are within California
    aoi_extent_clipped = common.get_tiles(bounds, target_num_tiles=1, clip=True)
    df = df[df.intersects(aoi_extent_clipped.geometry.iloc[0])]

    if 'VOLTAGE' in df.columns:
        # Simple power line height calculator: voltage / 10
        df['pole_height_m'] = [int(voltage) / 10 if voltage > 0 else 15 for voltage in df["VOLTAGE"]]
    
        # Only buffer tiles with some lines in them, otherwise return None
        if df.shape[0] > 0 and buffer_size_m > 0:
            df.geometry = df.to_crs(df.estimate_utm_crs()).geometry.buffer(buffer_size_m).to_crs(4326) 
            return df
        else:
            return df
    else:
        # Showing the partitions
        return df
    