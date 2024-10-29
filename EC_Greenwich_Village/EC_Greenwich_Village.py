lat, lng = 40.7336, -74.0027# greenwich
@fused.udf
def udf(h3_res: int = 11, lat:float=lat, lng:float=lng, threshold:float=0.8, target_h3_cell: str=None):
    import geopandas as gpd
    import h3
    import numpy as np
    import pandas as pd
    import shapely
    
    from shapely.geometry import Polygon
    from scipy.spatial import distance

    # Use `target_h3_cell` if passed, otherwise generate it from `lat` & `lng`
    target_h3_cell = target_h3_cell or h3.latlng_to_cell(lat, lng, h3_res)
    print("target_h3_cell: ", target_h3_cell)

    @fused.cache
    def run(target_h3_cell=target_h3_cell):
        # Load hourly data from a public UDF
        df = fused.run("UDF_NYC_TLC_Hourly_2010", hourly=True, h3_res=h3_res)
        
        # Create larger time bins
        df['daypart']=((df['hour']+3) / 6).astype('int32') # % 4
        df = df.groupby(['hex','daypart'])['cnt'].sum().reset_index()
    
        # Normalize
        dfg = df.groupby("hex").cnt.sum().to_frame("cnt_all").reset_index()
        df = df.merge(dfg)
        df["metric"] = df["cnt"] / df["cnt_all"]
        df=df[df.cnt_all >50]
        all_hex_pickups = df
        
        # Reference: the hex that every other one will get compared to
        REFERENCE_HEX = df[df["hex"] == target_h3_cell]
        REFERENCE_HEX_PICKUPS = REFERENCE_HEX.groupby(["daypart"]).sum(["metric"])
        REFERENCE_HEX_PICKUPS = (
            REFERENCE_HEX_PICKUPS / REFERENCE_HEX_PICKUPS.metric.sum()
        )
        print("REFERENCE_HEX_PICKUPS", REFERENCE_HEX_PICKUPS.metric.sum())
        
        # Calculate cosine similarit
        compare = all_hex_pickups.join(REFERENCE_HEX_PICKUPS, on=["daypart"], lsuffix="_REF", rsuffix="")
        compare["ab"] = compare["metric"] * compare["metric_REF"]
        compare["a2"] = compare["metric"] * compare["metric"]
        compare["b2"] = compare["metric_REF"] * compare["metric_REF"]
        cos = compare.groupby(["hex"]).sum(["ab", "a2", "b2"])
        cos["sim"] = cos["ab"] / (cos["a2"].apply(np.sqrt) * cos["b2"].apply(np.sqrt))
        cos["sim_norm"] = (cos["sim"]-0.9)*10
        
        # Calculate Jensen-shannon distance (note 0 = similar on this scale)
        df_pivot = all_hex_pickups.pivot(index=['hex'], columns='daypart', values='metric')
        REFERENCE_HEX_PICKUPS['hex'] = target_h3_cell
        REF_pivot = REFERENCE_HEX_PICKUPS.reset_index().pivot(index=['hex'], columns=['daypart'], values='metric')
        REF_pivot_big = pd.concat([REF_pivot]*df_pivot.shape[0], ignore_index=True)
        df_jsd = distance.jensenshannon(REF_pivot_big, df_pivot, axis=1)
        df_df_jsd = pd.DataFrame(df_jsd, columns=['jsd'])
        df_df_jsd.index = df_pivot.index

        # Kring 2 smoothing on the output
        @fused.cache
        def kring2(df, target_col='daypart', group_by='cnt'):
            df['h3_11'] = df['hex']
            df['hex_k'] = df['h3_11'].apply(lambda x: h3.grid_disk(x, 2))
            df = df.explode('hex_k')
            df['hex'] = df['hex_k'].apply(lambda x: h3.cell_to_parent(x))
            if target_col:
                df = df.groupby(['hex',target_col])[group_by].mean().reset_index()
            else:
                df = df.groupby(['hex'])[group_by].mean().reset_index()
            return df
        
        df_df_jsd = df_df_jsd.reset_index()
        df_df_jsd = kring2(df_df_jsd, target_col=None, group_by='jsd').reset_index()

        # Filter
        gdf = df_df_jsd.filter(items=["hex", "jsd"])
        gdf["sim_norm"] = ((1-gdf["jsd"])-threshold)/(1-threshold)
        gdf[gdf['sim_norm']<0] = 0
        return gdf.reset_index()

    # Run cached function
    gdf = run()
    gdf['hex'] = gdf['hex'].astype(str)
    return gdf[['hex', 'sim_norm', 'jsd']]
