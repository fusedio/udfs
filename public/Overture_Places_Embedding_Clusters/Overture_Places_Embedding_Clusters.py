@fused.udf
def udf_h3_embedding(h3_index="894509b022bffff", h3_size=8):
    import geopandas as gpd
    import h3
    import pandas as pd
    from shapely.geometry import Polygon
    from sklearn.feature_extraction.text import CountVectorizer
    from utils import global_categories

    # 1. Polygon from H3
    bounds = Polygon([coord[::-1] for coord in h3.cell_to_boundary(h3_index)])
    bbox = gpd.GeoDataFrame({"h3_index": [h3_index], "geometry": [bounds]})

    # 2. Load Overture Places
    gdf = fused.run("UDF_Overture_Maps_Example", bbox=bbox, overture_type="place")

    # 3. Normalize the 'categories' column into individual columns
    categories_df = pd.json_normalize(gdf["categories"]).reset_index(drop=True)
    categories_df.rename(columns={"primary": "categories_primary"}, inplace=True)
    names_df = pd.json_normalize(gdf["names"]).reset_index(drop=True)
    names_df.rename(columns={"primary": "names_primary"}, inplace=True)

    # 4. Concatenate the new columns back into the original GeoDataFrame
    gdf2 = pd.concat(
        [
            gdf.drop(columns=["categories", "names"]).reset_index(),
            categories_df,
            names_df,
        ],
        axis=1,
    )
    gdf2["h3_index"] = gdf2.geometry.apply(
        lambda p: h3.latlng_to_cell(p.y, p.x, h3_size)
    )

    # 5. Group by H3, create categories primary set
    gdf3 = gdf2.dissolve(
        by="h3_index",
        as_index=False,
        aggfunc={
            "categories_primary": lambda x: list([y for y in set(x) if pd.notna(y)])
        },
    )

    # 6. Convert matrix to a list of embeddings (dense vectors)
    vectorizer = CountVectorizer(
        vocabulary=global_categories
    )  # Align with the same feature space (for consistent vocabulary)
    gdf3["embedding"] = gdf3["categories_primary"].apply(
        lambda x: vectorizer.transform([" ".join(x)]).toarray()[0].tolist()
    )
    return gdf3


import geopandas as gpd

# CDMX
aoi = gpd.GeoDataFrame.from_features(
    {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {},
                "geometry": {
                    "coordinates": [
                        [
                            [-99.1384892195269, 19.59152906611483],
                            [-99.17550402024722, 19.555179600313224],
                            [-99.17758936113303, 19.52717510080535],
                            [-99.22398819583992, 19.52324425606041],
                            [-99.2417135933678, 19.425433760674025],
                            [-99.20730546875396, 19.381670285340377],
                            [-99.22711620716832, 19.310837062987176],
                            [-99.1989641052113, 19.28082206107021],
                            [-99.14787325351213, 19.27688525915849],
                            [-99.07801433384182, 19.275901043897136],
                            [-99.04412754444925, 19.339862725832745],
                            [-99.01806078337803, 19.452472714593014],
                            [-99.00346339717856, 19.567460742295196],
                            [-99.1384892195269, 19.59152906611483],
                        ]
                    ],
                    "type": "Polygon",
                },
            }
        ],
    }
)
# Merida
# aoi = gpd.GeoDataFrame.from_features({"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"coordinates":[[[-89.76893755755052,21.117823115480732],[-89.76893755755052,20.82720703395323],[-89.47067057006947,20.82720703395323],[-89.47067057006947,21.117823115480732],[-89.76893755755052,21.117823115480732]]],"type":"Polygon"}}]})
# Leon
# aoi = gpd.GeoDataFrame.from_features({"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"coordinates":[[[-101.77430478747662,21.203654701096255],[-101.77430478747662,21.058658735343457],[-101.57371795831008,21.058658735343457],[-101.57371795831008,21.203654701096255],[-101.77430478747662,21.203654701096255]]],"type":"Polygon"}}]})


@fused.udf
def udf(bbox: fused.types.TileGDF = aoi, h3_size=8):
    import duckdb
    import geopandas as gpd
    import h3
    import numpy as np
    import pandas as pd
    from sklearn.cluster import KMeans
    from sklearn.metrics.pairwise import cosine_similarity
    from utils import global_categories

    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils

    # 1. Polyfill AOI
    h3s = h3.polygon_to_cells(h3.geo_to_h3shape(bbox.geometry.iloc[0]), h3_size)

    @fused.cache
    def run_udfs(h3_index):
        try:
            gdf = fused.run(
                udf_h3_embedding, h3_index=h3_index, h3_size=h3_size, engine="local"
            )
            gdf["h3_index"] = h3_index
            gdf.insert(0, "h3_index", gdf.pop("h3_index"))
            return gdf
        except Exception as e:
            print(e)

    # 2. Run Embeddings UDF for each H3 cell
    gdfs = utils.run_pool(run_udfs, h3s, max_workers=100)
    gdf = pd.concat(gdfs)

    embeddings = gdf.embedding.tolist()
    similarity_matrix = cosine_similarity(embeddings)

    # 3. Cluster using KMeans
    kmeans = KMeans(n_clusters=6, random_state=42)
    gdf["cluster"] = kmeans.fit_predict(embeddings)

    # 4. Describe each cluster
    category_rows = []
    for idx, row in gdf.iterrows():
        # Filter categories with non-zero embedding indices
        selected_categories = np.array(global_categories)[
            np.array(row["embedding"]) == 1
        ]
        for category in selected_categories:
            category_rows.append({"cluster": row["cluster"], "category": category})

    # 5. Structure output table
    category_df = pd.DataFrame(category_rows)
    category_counts = (
        category_df.groupby(["cluster", "category"]).size().reset_index(name="count")
    )
    category_arrays = (
        category_counts.groupby("cluster")
        .apply(lambda x: x.nlargest(5, "count")["category"].tolist())
        .reset_index(name="top_categories")
    )
    gdf = gdf[["h3_index", "cluster"]].merge(category_arrays, on="cluster", how="left")

    print("Cluster descriptions:")
    print(category_counts)
    print(category_arrays)

    return gdf[["h3_index", "cluster", "top_categories"]]
