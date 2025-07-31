@fused.udf
def udf(
    bounds: fused.types.Bounds = [-122.43021262458805,37.784599909261026,-122.39510368982017,37.81320400189578],
    release: str = "2024-11-19",
    min_zoom: int = 6,
    use_columns: list = ["geometry", "name", "fsq_category_ids"],
):

    common = fused.load("https://github.com/fusedio/udfs/tree/3ac8eaf/public/common/")

    path = f"s3://us-west-2.opendata.source.coop/fused/fsq-os-places/{release}/places/"
    df = common.table_to_tile(
        bounds, table=path, min_zoom=min_zoom, use_columns=use_columns, clip = True
    )

    df = join_fsq_categories(df, release=release)
 
    return df


def join_fsq_categories(df, *, release):
    if "fsq_category_ids" in df.columns:
        df = df.explode("fsq_category_ids")
        df_cat = get_fsq_categories(release=release)
        # 4/5/6 exist for some POIs
        df = (
            df.set_index("fsq_category_ids")
            .join(
                df_cat.set_index("category_id")[
                    [
                        "level1_category_name",
                        "level2_category_name",
                        "level3_category_name",
                    ]
                ]
            )
            .reset_index()
        )

    if "fsq_category_labels" in df.columns:
        df["fsq_category_labels"] = df["fsq_category_labels"].apply(category_fixup)

    return df


@fused.cache
def get_fsq_categories(release):
    import pandas as pd

    url = f"s3://fsq-os-places-us-east-1/release/dt={release}/categories/parquet/categories.snappy.parquet"
    df_cat = pd.read_parquet(url)
    return df_cat


def category_fixup(x):
    import numpy as np

    if x is None:
        return np.array(["NA"])
    return x
