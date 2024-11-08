@fused.udf
def udf(bbox: fused.types.TileGDF):
    import palettable    
    import utils
    import rasterio
    import os, json

    # 1. Set secrets as Environment Variables
    os.environ['GS_ACCESS_KEY_ID'] = fused.secrets["GS_ACCESS_KEY_ID"]
    os.environ['GS_SECRET_ACCESS_KEY'] = fused.secrets["GS_SECRET_ACCESS_KEY"]

    # 2. Set TIFF path
    input_tiff_path = "gs://fused-tmp/DNI_LTAy_Avg_Daily_Totals_DNI.tif"

    # 3. Read TIFF
    data = fused.utils.common.read_tiff(
        bbox=bbox,
        input_tiff_path=input_tiff_path,
        output_shape=(256, 256)
    ).filled(fill_value=0)

    # 4. Set colormap
    return fused.utils.common.visualize(
        data=data,
        min=0,
        max=8,
        colormap=palettable.scientific.sequential.LaJolla_20_r,
    )









