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

    filename = "categories.snappy.parquet" if release == "2024-11-19" else "categories.zstd.parquet"

    url = f"s3://fsq-os-places-us-east-1/release/dt={release}/categories/parquet/{filename}"
    df_cat = pd.read_parquet(url)
    return df_cat


def category_fixup(x):
    import numpy as np

    if x is None:
        return np.array(["NA"])
    return x
