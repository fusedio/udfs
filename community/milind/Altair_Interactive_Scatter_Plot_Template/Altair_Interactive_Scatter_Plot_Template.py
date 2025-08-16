@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Interactive Penguin Dashboard with Linked Selections
    # WHEN TO USE: Multi-panel interactive visualization with species comparison
    # DATA REQUIREMENTS: Categorical species data with continuous measurements
    # INTERACTIVITY: Brush selection on scatter plot filters bar chart, click selection filters scatter plot
    # =============================================================================
    
    import pandas as pd
    import altair as alt
    from jinja2 import Template

    # REQUIRED for HTML UDFs
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")

    # ALTAIR REQUIREMENT: Enable processing of larger datasets
    alt.data_transformers.enable("default", max_rows=None)

    @fused.cache
    def load_data():
        """
        Load Palmer Penguins dataset
        Contains: species, island, bill_length_mm, bill_depth_mm, flipper_length_mm, body_mass_g, sex, year
        """
        url = "https://raw.githubusercontent.com/allisonhorst/palmerpenguins/master/inst/extdata/penguins.csv"
        return pd.read_csv(url)

    df = load_data()
    
    # Print column data types for exploration
    print("=== Column data types ===")
    print(df.dtypes)
    print("\n=== First 5 rows ===")
    print(df.head())

    # ALTAIR CONFIGURATION
    config = {
        # DATA FIELDS
        "x_field": "bill_length_mm",
        "y_field": "bill_depth_mm", 
        "category_field": "species",
        "size_field": "body_mass_g",
        
        # COLOR SCHEME
        "species_colors": {
            'Adelie': '#1f77b4',
            'Chinstrap': '#ff7f0e', 
            'Gentoo': '#2ca02c'
        },
        
        # INTERACTIVITY
        "interactive": True,
        "tooltip_enabled": True,
        
        # LABELS
        "title": "Palmer Penguins: Interactive Species Analysis",
        "scatter_title": "Bill Dimensions by Species",
        "bar_title": "Species Distribution"
    }

    # Create color scale for species
    color_scale = alt.Scale(
        domain=list(config["species_colors"].keys()),
        range=list(config["species_colors"].values())
    )

    # Create selections
    brush = alt.selection_interval(encodings=['x', 'y'])
    click = alt.selection_point(encodings=['color'])

    # Top panel: Scatter plot of bill dimensions
    points = alt.Chart(df).mark_point().encode(
        alt.X('bill_length_mm:Q')
            .title('Bill Length (mm)')
            .scale(domain=[30, 60]),
        alt.Y('bill_depth_mm:Q')
            .title('Bill Depth (mm)')
            .scale(domain=[12, 25]),
        alt.Size('body_mass_g:Q').scale(range=[50, 400]),
        alt.Color('species:N', scale=color_scale),
        opacity=alt.when(brush).then(alt.value(1)).otherwise(alt.value(0.3))
    ).properties(
        width=550,
        height=350
    ).add_params(
        brush
    ).transform_filter(
        click
    )

    # Bottom panel: Bar chart of species count
    bars = alt.Chart(df).mark_bar().encode(
        alt.X('count()'),
        alt.Y('species:N'),
        alt.Color('species:N', scale=color_scale),
        opacity=alt.when(click).then(alt.value(1)).otherwise(alt.value(0.3))
    ).transform_filter(
        brush
    ).properties(
        width=550,
        height=200
    ).add_params(
        click
    )

    # Combine charts vertically
    chart = alt.vconcat(
        points,
        bars,
        data=df,
        title=config["title"]
    )

    # ALTAIR HTML GENERATION
    chart_html = chart.to_html()

    # RESPONSIVE HTML WRAPPER
    html_template = Template("""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>{{ title }}</title>
  <style>
    :root { --padding: 16px; --bg: #f5f7fa; --card-bg: #ffffff; --radius: 12px; }
    html,body { height:100%; margin:0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background:var(--bg); }
    .wrap { box-sizing:border-box; padding:var(--padding); height:100%; display:flex; align-items:center; justify-content:center; }
    .card { width:100%; max-width:1200px; height:calc(100% - 2*var(--padding)); background:var(--card-bg); border-radius:var(--radius); box-shadow:0 6px 25px rgba(0,0,0,0.08); overflow:hidden; }
    .card .vega-embed, .card .vega-embed > .vega-visualization { width:100% !important; height:100% !important; }
    @media (max-width:640px) { :root { --padding:8px; } .card { border-radius:8px; } }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      {{ chart_html | safe }}
    </div>
  </div>
</body>
</html>""")

    rendered = html_template.render(title=config["title"], chart_html=chart_html)
    return common.html_to_obj(rendered)