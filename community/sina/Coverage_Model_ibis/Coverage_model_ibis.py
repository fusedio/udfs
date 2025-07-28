@fused.udf
def udf(bounds: fused.types.Bounds = [-122.438,37.774,-122.434,37.777], tech: str = 'Tarana', site_count: int = 1, col_plot: str = 'Rx_dBm'):
    """
    Function to read the data from the fused coverage model
    :param bounds:
    :param tech:
    :param site_count:
    :param col_plot:
    :return:
    """
    import pandas as pd
    import geopandas as gpd
    import ibis

    from palettable.colorbrewer.sequential import YlOrRd_9  # as color_map_used
    import matplotlib as mpl
    from utils import h3_cell_to_parent, h3_cell_to_boundary_wkt, ST_GeomFromText

    # convert bounds to tile
    common = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    tile = common.get_tiles(bounds, clip=True)

    @fused.cache
    def read_data(bounds, url: str, tech: str, site_count: int, col_plot: str, con_ibis):
        import ibis
        from ibis import _
        ibis.set_backend(con_ibis)

        minx, miny, maxx, maxy = bounds.bounds.values[0]
        parent_res = 15 - (19 - bounds.z.values[0])

        # Read the data from the parquet file
        t_gdf = (con_ibis
             .read_parquet(url, table_name='t_cov_hex')
             .filter((_.tech == tech) & (_.site_count == site_count))
             .filter(_.lon.between(minx, maxx) & _.lat.between(miny, maxy))
             .mutate(h3_parent=h3_cell_to_parent(_.h3, parent_res))
             .agg(by=['h3_parent'], metrics={f'{col_plot}': _[col_plot].max()})
             .rename({'h3': 'h3_parent'})
             .mutate(geometry=ST_GeomFromText(h3_cell_to_boundary_wkt(_.h3))
                     .cast('geometry')
                     )
             .select([col_plot, 'geometry'])
             )


        gdf = (t_gdf
               .to_pandas()
               .pipe(gpd.GeoDataFrame)
               .set_crs('epsg:4326')
               )
        return gdf

    s3_url = r"s3://fused-asset/misc/digitaltwinsim/coverage_model/cov_hex_hive_parquet/"
    # Connect to duckdb backend using ibis
    con_ibis = ibis.duckdb.connect(temp_directory='/tmp', allow_unsigned_extensions=True, extensions=['spatial'])
    con_ibis.raw_sql("INSTALL h3 FROM community")
    con_ibis.raw_sql("LOAD h3")

    gdf = read_data(tile, s3_url, tech, site_count, col_plot, con_ibis)

    if len(gdf):
        # Normalize colors
        normalizer = mpl.colors.Normalize(vmin=gdf[col_plot].min(), vmax=gdf[col_plot].max())
        normalized_values = normalizer(gdf[col_plot])
        # Convert the normalized values to RGBA
        get_fill_color = (pd.DataFrame(YlOrRd_9.mpl_colormap(normalized_values, alpha=0.5), columns=['r', 'g', 'b', 'a']) * 255).round().astype('int32')
        gdf = (gdf.merge(get_fill_color, left_index=True, right_index=True)
               )

    return gdf
