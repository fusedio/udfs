@fused.udf
def udf():
    map_utils = fused.load("https://github.com/fusedio/udfs/tree/3eb82cb/community/milind/map_utils/")

    import geopandas as gpd

    hex_data = fused.load("era5_monthly_mean")
    data = hex_data()

    # We can't visualize hex 15 hexagons, so turning to small points to visualise
    gdf = data.copy()
    common = fused.load("https://github.com/fusedio/udfs/tree/9bad664/public/common/")
    con = common.duckdb_connect()
    gdf = con.sql("""
        SELECT *, 
            h3_cell_to_latlng(CAST(hex AS UBIGINT)) AS latlng
        FROM gdf
    """).df()
    gdf['geometry'] = gpd.points_from_xy(
        gdf['latlng'].apply(lambda x: x[1]),
        gdf['latlng'].apply(lambda x: x[0])
    )
    gdf = gpd.GeoDataFrame(gdf.drop(columns=['latlng']), geometry='geometry', crs='EPSG:4326')
    
    print(f"{data.T=}")

    val_min = data['monthly_mean_temp'].min()
    val_max = data['monthly_mean_temp'].max()

    # Layer config for Census population data
    config_census = {
        "style": {
            "fillColor": {
                "type": "continuous",
                "attr": "monthly_mean_temp",
                "domain": [val_min, val_max],
                "steps": 20,
                "palette": "DarkMint",
            },
            "filled": True,
            "stroked": False,
            "opacity": 0.9,
            "pointRadius": 1,
        },
        "tooltip": ["hex", "monthly_mean_temp"],
    }

    widgets = {
        "controls": "bottom-right",
        "scale": "bottom-left",
        "basemap": "bottom-right",
        "layers": {"position": "top-right", "expanded": False},
        "legend": {"position": "top-right", "expanded": True},
    }

    html = map_utils.deckgl_layers(
        layers=[
            {
                "type": "vector",
                "data": gdf,
                "config": config_census,
                "visible": True,
                "name": "Hex 15 Temperature",
            }
        ],
        basemap="dark",
        theme="dark",
        initialViewState=None,
        widgets=widgets,
    
    )
    return html