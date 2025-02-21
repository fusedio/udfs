@fused.udf
def udf(
    bbox: fused.types.Tile,
    cmap_name: str = None,  # 'tab20c'
):
    import rasterio
    import numpy as np
    import geopandas as gpd
    import shapely
    from rasterio.features import shapes
    from shapely.geometry import shape
    import pandas as pd

    envelope = bbox.geometry.envelope

    cog_url = "https://storage.googleapis.com/fire-cog/fire-cog.tif"

    def win_rows_cols(file_path, ll, ur):
        with rasterio.open(file_path) as src:
            src_crs = src.crs
            src_transform = src.transform

        X, Y = rasterio.warp.transform(
            {"init": "EPSG:4326"}, src_crs, [ll[0], ur[0]], [ll[1], ur[1]]
        )

        (rows, cols) = rasterio.transform.rowcol(src_transform, X, Y)
        ncols = cols[1] - cols[0]
        nrows = rows[0] - rows[1]  # ! rows[0] > rows[1]

        return {"col_off": cols[0], "row_off": rows[1], "width": ncols, "height": nrows}

    win = win_rows_cols(cog_url, envelope[0].bounds[:2], envelope[0].bounds[2:])

    window = rasterio.windows.Window(
        win["col_off"], win["row_off"], win["width"], win["height"]
    )

    with rasterio.open(cog_url) as src:
        subset = src.read(1, window=window)
        win_transform = src.window_transform(window)
        shape_gen = ((shape(s), v) for s, v in shapes(subset, transform=win_transform))
        gdf = gpd.GeoDataFrame(
            dict(zip(["geometry", "band1"], zip(*shape_gen))), crs=src.crs
        )

        # Swap in text values from the LANDFIRE data dictionary: https://landfire.gov/documents/LF_Data_Dictionary.pdf

        valid_band1_values = [
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            91,
            92,
            93,
            98,
            99,
        ]
        if cmap_name:
            from matplotlib import colormaps

            cmap = colormaps[cmap_name]
            r_channel = [c[0][0] * 255 for c in zip(cmap.colors, valid_band1_values)]
            g_channel = [c[0][1] * 255 for c in zip(cmap.colors, valid_band1_values)]
            b_channel = [c[0][2] * 255 for c in zip(cmap.colors, valid_band1_values)]
        else:
            r_channel = [
                255 * (i / len(valid_band1_values))
                for i in range(len(valid_band1_values))
            ]
            g_channel = [
                128 * (i / len(valid_band1_values))
                for i in range(len(valid_band1_values))
            ]
            b_channel = [
                128 * (i / len(valid_band1_values))
                for i in range(len(valid_band1_values))
            ]
        names = pd.DataFrame(
            {
                "band1": valid_band1_values,
                "definitions": [
                    "Surface fires that burn fine herbaceous fuels, cured and curing fuels, little shrubor timber present, primarily grasslands and savanna.",
                    "Burns fine, herbaceous fuels, stand is curing or dead, may produce fire brands on oak or pine stands.",
                    "Most intense fire of grass group, spreads quickly with wind, one third of stand dead or cured, stands average 3 feet tall.",
                    "Fast spreading fire, continuous overstory, flammable foliage and dead woody material, deep litter layer can inhibit suppression.",
                    "Low intensity fires, young, green shrubs with little dead material, fuels consist of litter from understory.",
                    "Broad range of shrubs, fire requires moderate winds to maintain flame at shrub height, or will drop to the ground with low winds",
                    "Foliage highly flammable, allowing fire to reach shrub strata levels, shrubs generally 2 to 6 feet high.",
                    "Slow, ground burning fires, closed canopy stands with short needle conifers or hardwoods, litter consist mainly of needles and leaves, with little undergrowth, occasional flares with concentrated fuels.",
                    "Longer flames, quicker surface fires, closed canopy stands of long-needles or hardwoods, rolling leaves in fall can cause spotting, dead-down material can cause occasional crowning.",
                    "Surface and ground fire more intense, dead-down fuels more abundant, frequent crowning and spotting causing fire control to be more difficult.",
                    "Fairly active fire, fuels consist of slash and herbaceous materials, slash originates from light partial cuts or thinning projects, fire is limited by spacing of fuel load and shade from overstory.",
                    "Rapid spreading and high intensity fires, dominated by slash resulting from heavy thinning projects and clearcuts, slash is mostly 3 inches or less.",
                    "Fire spreads quickly through smaller material and intensity builds slowly as large material ignites, continuous layer of slash larger than 3 inches in diameter predominates, resulting from clearcuts and heavy partial cuts, active flames sustained for long periods of time, fire is susceptible to spotting and weather conditions.",
                    "Urban",
                    "Snow/Ice",
                    "Agriculture",
                    "Water",
                    "Barren",
                ],
                "r": r_channel,
                "g": g_channel,
                "b": b_channel,
            }
        )

        merged_df = gdf.merge(names, on="band1", how="left")
        final_df = merged_df.to_crs("EPSG:4326")

    return final_df
