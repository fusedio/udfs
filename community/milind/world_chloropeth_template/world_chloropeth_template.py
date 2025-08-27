@fused.udf
def udf(
    url: str = "https://api.worldbank.org/v2/en/indicator/SP.DYN.LE00.IN?downloadformat=csv",
    year: str = "2023",
    color_scheme: str = "viridis",
    projection: str = "naturalEarth1",
):
    """
    World Bank life‑expectancy choropleth that guarantees every country
    (including Algeria, Angola, Botswana, Afghanistan and all South‑American nations)
    appears on the map. Missing data is shown in light‑gray.

    Update: make the TopoJSON id join robust for both numeric ids like 12 and
    zero‑padded ids like "012" so countries like Algeria/Angola/Botswana render correctly.
    """
    import pandas as pd
    import altair as alt
    from jinja2 import Template

    # ------------------------------------------------------------------
    # 1️⃣ Load World Bank data (cached)
    # ------------------------------------------------------------------
    @fused.cache
    def load_data(download_url: str) -> pd.DataFrame:
        import requests, zipfile, io

        resp = requests.get(download_url, timeout=10)
        resp.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            data_csv_name = next(
                (
                    name
                    for name in z.namelist()
                    if name.lower().endswith(".csv")
                    and name.split("/")[-1].startswith("API_")
                ),
                None,
            )
            if data_csv_name is None:
                raise FileNotFoundError(
                    "Data CSV not found inside the downloaded zip."
                )
            with z.open(data_csv_name) as csv_file:
                df = pd.read_csv(csv_file, skiprows=4)
        return df

    # ------------------------------------------------------------------
    # 2️⃣ Load ISO‑3 → numeric mapping (cached)
    #    Source: https://github.com/datasets/country-codes
    # ------------------------------------------------------------------
    @fused.cache
    def load_country_code_map() -> pd.DataFrame:
        map_url = (
            "https://raw.githubusercontent.com/datasets/country-codes/master/data/country-codes.csv"
        )
        try:
            m = pd.read_csv(map_url, encoding="utf-8-sig", low_memory=False)
        except UnicodeDecodeError:
            m = pd.read_csv(map_url, encoding="latin1", low_memory=False)

        # Keep only the columns we need and rename them
        m = m[["ISO3166-1-Alpha-3", "ISO3166-1-numeric"]].rename(
            columns={
                "ISO3166-1-Alpha-3": "iso3",
                "ISO3166-1-numeric": "iso_numeric",
            }
        )
        # Drop rows with missing values and coerce numeric codes to int
        m = m.dropna(subset=["iso3", "iso_numeric"])
        m["iso_numeric"] = pd.to_numeric(m["iso_numeric"], errors="coerce")
        m = m.dropna(subset=["iso_numeric"])
        m["iso_numeric"] = m["iso_numeric"].astype(int)
        return m[["iso3", "iso_numeric"]]

    # ------------------------------------------------------------------
    # Load the datasets
    # ------------------------------------------------------------------
    df = load_data(url)
    code_map = load_country_code_map()

    # ------------------------------------------------------------------
    # Diagnostics – print column data‑types
    # ------------------------------------------------------------------
    print("=== Column data types ===")
    print(df.dtypes)

    # ------------------------------------------------------------------
    # 3️⃣ Prepare data for the choropleth (selected year)
    # ------------------------------------------------------------------
    value_col = year
    cols_needed = ["Country Name", "Country Code", value_col]
    cols_needed = [c for c in cols_needed if c in df.columns]

    df_year = df[cols_needed].copy()
    df_year = df_year.rename(
        columns={
            "Country Name": "country",
            "Country Code": "iso3",
            value_col: "life_expectancy",
        }
    )
    # Keep rows that have a country code; life_expectancy may be missing
    df_year = df_year.dropna(subset=["iso3"])

    # Join ISO‑3 → numeric id used by the TopoJSON (left join to keep all)
    df_year = df_year.merge(code_map, on="iso3", how="left")
    df_year = df_year.rename(columns={"iso_numeric": "id"})

    # Assign temporary ids for any countries that still miss a numeric code
    missing_id = df_year["id"].isna()
    if missing_id.any():
        start = 900
        for i in df_year[missing_id].index:
            df_year.at[i, "id"] = start
            start += 1

    # Fill missing life‑expectancy with -1 (will be shown as gray)
    df_year["life_expectancy"] = df_year["life_expectancy"].fillna(-1)

    # One row per country (average if duplicates)
    df_year = (
        df_year.groupby(["id", "country"], as_index=False)
        .agg({"life_expectancy": "mean"})
    )

    # Build a robust lookup table that matches both "12" and "012" id variants
    df_lookup_a = df_year.copy()
    df_lookup_a["id_str"] = df_lookup_a["id"].astype(int).astype(str)     # e.g., "12"
    df_lookup_b = df_year.copy()
    df_lookup_b["id_str"] = df_lookup_b["id"].apply(lambda x: f"{int(x):03d}")  # e.g., "012"
    df_lookup = pd.concat(
        [
            df_lookup_a[["id_str", "country", "life_expectancy"]],
            df_lookup_b[["id_str", "country", "life_expectancy"]],
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["id_str"], keep="first")

    print(f"Final rows: {len(df_year)}")
    print(f"With data: {(df_year['life_expectancy'] > 0).sum()}")
    print(f"Missing: {(df_year['life_expectancy'] == -1).sum()}")

    # ------------------------------------------------------------------
    # 4️⃣ Build the Altair world choropleth
    # ------------------------------------------------------------------
    alt.data_transformers.enable("default", max_rows=None)

    # Higher‑resolution 50 m TopoJSON as requested
    world_topojson = "https://cdn.jsdelivr.net/npm/world-atlas@2/countries-50m.json"

    # Determine colour domain from real data (ignore the placeholder -1)
    valid = df_year[df_year["life_expectancy"] > 0]["life_expectancy"]
    min_val, max_val = valid.min(), valid.max()

    base = (
        alt.Chart(
            alt.Data(
                url=world_topojson,
                format=alt.DataFormat(type="topojson", feature="countries"),
            )
        )
        .mark_geoshape(stroke="white", strokeWidth=0.3)
        # Make 'id_str' from the TopoJSON id, so we can match both "12" and "012"
        .transform_calculate(id_str="toString(datum.id)")
        .transform_lookup(
            lookup="id_str",
            from_=alt.LookupData(
                data=df_lookup,
                key="id_str",
                fields=["country", "life_expectancy"],
            ),
        )
        .encode(
            # Colour: real values vs missing (gray)
            color=alt.condition(
                alt.datum.life_expectancy > 0,
                alt.Color(
                    "life_expectancy:Q",
                    scale=alt.Scale(scheme=color_scheme, domain=[min_val, max_val]),
                    legend=alt.Legend(title=f"Life Expectancy ({year})"),
                ),
                alt.value("#d3d3d3"),
            ),
            tooltip=[
                alt.Tooltip("country:N", title="Country"),
                alt.Tooltip(
                    "life_expectancy:Q",
                    title=f"Life Expectancy ({year})",
                    format=".1f",
                ),
            ],
        )
        .project(type=projection)
        .properties(
            width="container",
            height="container",
            title=alt.TitleParams(
                text=f"Life Expectancy – World Choropleth ({year})",
                anchor="start",
                fontSize=16,
            ),
        )
        .configure_view(strokeWidth=0)
        .configure_legend(labelFontSize=11, titleFontSize=12)
    )

    chart = base

    # ------------------------------------------------------------------
    # 5️⃣ Convert chart → HTML and wrap for Fused
    # ------------------------------------------------------------------
    chart_html = chart.to_html()

    html_template = Template(
        """
        <!doctype html>
        <html>
        <head>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width,initial-scale=1" />
          <title>{{ title }}</title>
          <style>
            :root { --padding: 16px; --bg: #f8f9fa; --card-bg: #ffffff; --radius: 10px; }
            html,body { height:100%; margin:0; font-family:-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background:var(--bg); }
            .wrap { box-sizing:border-box; padding:var(--padding); height:100%; display:flex; align-items:center; justify-content:center; }
            .card { width:100%; max-width:1400px; height:calc(100% - 2*var(--padding)); background:var(--card-bg); border-radius:var(--radius); box-shadow:0 4px 20px rgba(0,0,0,0.08); overflow:hidden; }
            .card .vega-embed, .card .vega-embed > .vega-visualization { width:100% !important; height:100% !important; }
            @media (max-width:640px) { :root { --padding:8px; } .card { border-radius:6px; } }
          </style>
        </head>
        <body>
          <div class="wrap">
            <div class="card">
              {{ chart_html | safe }}
            </div>
          </div>
        </body>
        </html>
        """
    )

    rendered = html_template.render(title=f"Life Expectancy – {year}", chart_html=chart_html)

    # Load common utilities and return the HTML object
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    return common.html_to_obj(rendered)