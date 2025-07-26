@fused.udf
def udf(path: str = "s3://fused-sample/demo_data/housing/housing_2024.csv"):
    import pandas as pd

    housing = pd.read_csv(path)
    housing["price_per_area"] = housing["price"] / housing["area"]

    return housing[["price", "price_per_area"]]
