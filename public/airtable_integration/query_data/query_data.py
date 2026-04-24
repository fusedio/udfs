@fused.udf(cache_max_age="0s")
def udf(name: str = "world"):
    import pandas as pd

    bases = fused.load('list_bases')()
    base_id = bases["base_id"].iloc[0]

    if not base_id:
        return "No Airtable bases found."

    try:
        records = fused.api.airtable_list_records(
            table="Team members",
            base_id=base_id,
        )
        
        df = pd.DataFrame([r["fields"] for r in records])
        return df
    except Exception as e:
        return "Table is not found or is inaccessible. Please verify `table` argument"
