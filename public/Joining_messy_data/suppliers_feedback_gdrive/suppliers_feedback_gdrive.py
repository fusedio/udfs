@fused.udf
def udf(
    sheet_id: str = "1_utccObv7uSk-Ew92Yu3tW3roYCaZ8Shn3xhMelk24A", 
    sheet_name: str = "supplier_feedback"
):
    # Demo Google Sheets, you can replace with your own
    import pandas as pd
    
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
    df = pd.read_csv(url)

    print(df.T)
    return df
