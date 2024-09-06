def create_map(
    zoom_start=12,
    location=[37.7749, -122.4194],
    draw=True,
    gdf_layers=[],
    image_layers=[],
    tile_layers=[],
    show_layercontrol=True,
    tiles="cartodbdark_matter",
    draw_polyline=True,
    drap_polygon=True,
    draw_rectangle=False,
    draw_circle=False,
    draw_marker=False,
    draw_circlemarker=False,
):
    import folium
    from folium.plugins import Draw

    # initialize map
    m = folium.Map(location=location, zoom_start=zoom_start, max_zoom=17, tiles=tiles)
    if draw:
        Draw(
            draw_options={
                "polyline": draw_polyline,
                "polygon": drap_polygon,
                "circle": draw_circle,
                "marker": draw_marker,
                "circlemarker": draw_circlemarker,
                "rectangle": draw_rectangle,
            }
        ).add_to(m)
    return m


def get_folium_gdf(folium_output):
    import geopandas as gpd

    if folium_output["all_drawings"]:
        return gpd.GeoDataFrame.from_features(folium_output["all_drawings"]).set_crs(
            4326
        )
    else:
        return gpd.GeoDataFrame({})


def add_raster(m, image, bounds, name="name"):
    import folium

    folium.raster_layers.ImageOverlay(
        image=image,  # .transpose(1,0,2),
        name=name,
        bounds=[[bounds[1], bounds[0]], [bounds[3], bounds[2]]],
    ).add_to(m)


def add_group_layer(m, gdf_tiles, layer_name="DEM"):
    import folium

    m_sub1 = folium.FeatureGroup(name=layer_name)
    for i in range(len(gdf_tiles)):
        tile = gdf_tiles.iloc[i]
        add_raster(m_sub1, tile.arr, tile.bounds, name=f"image_{i}")
        m.add_child(m_sub1)


async def get_arr_async(gdf_tiles):
    from pyodide.http import pyfetch

    results = [pyfetch(url) for url in gdf_tiles.url]
    import asyncio
    import io

    import numpy as np
    from PIL import Image

    results = await asyncio.gather(*results)
    gdf_tiles["arr"] = [
        np.array(Image.open(io.BytesIO(await i.bytes()))) for i in results
    ]
    return gdf_tiles
