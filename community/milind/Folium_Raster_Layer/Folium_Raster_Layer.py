@fused.udf(cache_max_age=0)
def udf(
    # Fused raster tile params
    token: str = "UDF_CDLs_Tile_Example",
    host: str = "https://staging.fused.io",   # or https://www.fused.io
    var: str = "RGB",                          # e.g. "RGB", "NDVI"

    # Map view
    center_lat: float = 37.803972,
    center_lng: float = -122.421297,
    zoom: int = 8,
    minzoom: int = 6,
    maxzoom: int = 20,

    # UI / rendering
    height: str = "700px",
    overlay_name: str = "Fused raster",
    basemap_name: str = "OpenStreetMap"       # you can change to "OpenStreetMap" / "Stamen Terrain" / "CartoDB positron" etc.
):
    """
    Single Folium map with a basemap + ONE raster XYZ overlay from a Fused UDF token.

    Tiles URL:
      {host}/server/v1/realtime-shared/{token}/run/tiles/{z}/{x}/{y}?dtype_out_raster=png[&var=...]
    """
    import folium
    from urllib.parse import quote_plus

    # Build fused raster XYZ url (keep {z}/{x}/{y} intact)
    base = host.rstrip("/") + f"/server/v1/realtime-shared/{token}/run/tiles/{{z}}/{{x}}/{{y}}?dtype_out_raster=png"
    tiles_url = base + (f"&var={quote_plus(var)}" if var else "")

    # Create map with no default tiles, then add a basemap explicitly
    m = folium.Map(
        location=[center_lat, center_lng],
        zoom_start=zoom,
        min_zoom=minzoom,
        max_zoom=maxzoom,
        tiles=None,
        control_scale=True,
        prefer_canvas=True,
        width="100%",
        height=height,
    )

    # Basemap
    folium.TileLayer(
        tiles=basemap_name,
        name=basemap_name,
        control=True
    ).add_to(m)

    # Fused raster overlay
    folium.TileLayer(
        tiles=tiles_url,
        attr=overlay_name,
        name=overlay_name,
        overlay=True,
        control=True,
        min_zoom=minzoom,
        max_zoom=maxzoom,
    ).add_to(m)

    # Layer switcher so you can toggle overlay
    folium.LayerControl(collapsed=False, position="topright").add_to(m)

    html = m.get_root().render()
    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    return common.html_to_obj(html)
