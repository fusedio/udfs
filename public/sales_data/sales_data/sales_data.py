@fused.udf
def udf():
    import pandas as pd

    df = pd.DataFrame({
        "category": ["Electronics", "Clothing", "Food", "Books", "Toys", "Sports"],
        "sales": [42000, 28500, 15300, 9800, 21200, 33700],
    })
    return df
