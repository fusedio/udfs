@fused.udf
def udf(
    bbox: fused.types.TileGDF = None,
    census_variable: str = "Total Pop",
    scale_factor: float = 200,
    is_density: bool = True,
    year: int = 2022
):
    import numpy as np
    
    from utils import acs_5yr_bbox

    # different geometry details per zoom level
    if bbox.z[0] > 12:
        suffix = None
    elif bbox.z[0] > 10:
        suffix = "simplify_0001"
    elif bbox.z[0] > 8:
        suffix = "simplify_001"
    elif bbox.z[0] > 5:
        suffix = "simplify_01"
    else:
        suffix = "centroid"
    print(suffix)

    # read the variables
    gdf = acs_5yr_bbox(
        bbox.total_bounds, census_variable=census_variable, suffix=suffix, year=year
    )
    if len(gdf) == 0:
        return None

    # shorten the column name
    gdf.columns = gdf.columns.map(
        lambda x: (str(x.split("|")[0]) + str(x.split("|")[-1])) if "|" in x else x
    )
    print(gdf.columns)

    # create a metric columns for the visualization
    if suffix == "centroid" or is_density == False:
        gdf["metric"] = gdf.iloc[:, 2] * scale_factor / 1000
    else:
        gdf["metric"] = np.sqrt(gdf.iloc[:, 2] / gdf.area) * scale_factor / 1000
    return gdf
