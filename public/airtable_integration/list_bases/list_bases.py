@fused.udf(cache_max_age="0s")
def udf(name: str = "world"):
    import pandas as pd

    at = fused.api.airtable_connect()
    bases = at.list_bases()

    base_data = []
    for base in bases:
        base_data.append({"base_id": base["id"], "base_name": base["name"]})

    bases_df = pd.DataFrame(base_data)
    return bases_df

    records = fused.api.airtable_list_records(
        # table="Bugs and issues",
        table="Team members",
        # table="Features",
        base_id="apptYjfbmqqlSAFnK",
    )
    
    df = pd.DataFrame([r["fields"] for r in records])
    return df
