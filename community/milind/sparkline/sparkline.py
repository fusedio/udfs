@fused.udf
def udf(url: str = "https://raw.githubusercontent.com/kkakey/dog_traits_AKC/refs/heads/main/data/breed_traits.csv"):
    """
    Generic column-based sparkline table UDF.
    - url: http(s) or s3 path to a CSV/Parquet dataset. (required)
    Behavior:
    - Automatically detects a time column (common names like 'year','yr','date', or picks a column
      with many unique values if none match).
    - Aggregates all numeric columns (except the detected time column) by the time column (sum).
    - Returns an HTML table with one sparkline per numeric column.
    """
    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")
    import pandas as pd
    import numpy as np
    from html import escape

    @fused.cache
    def load_data(path):
        if not path:
            raise ValueError("no_path")
        p = path.lower()
        if p.endswith(".parquet") or p.endswith(".pq"):
            return pd.read_parquet(path)
        else:
            return pd.read_csv(path)

    # Load dataset (cached)
    try:
        df = load_data(url)
    except ValueError:
        return common.html_to_obj("<p>No dataset URL provided. Pass a URL to a CSV or Parquet file in the `url` parameter.</p>")
    except Exception as e:
        return common.html_to_obj(f"<p>Error loading dataset: {escape(str(e))}</p>")

    # Attempt to auto-detect a time column
    col_lower = {c.lower(): c for c in df.columns}
    preferred_names = ["year", "yr", "time", "date", "month"]
    time_col = None
    for name in preferred_names:
        if name in col_lower:
            time_col = col_lower[name]
            break

    # If not found, pick a column that's not all-NaN and has many unique values
    if time_col is None:
        candidate = None
        best_unique = 0
        for c in df.columns:
            non_na = df[c].dropna()
            if non_na.empty:
                continue
            unique_count = non_na.nunique()
            # prefer columns with > 1 unique value
            if unique_count > best_unique:
                best_unique = unique_count
                candidate = c
        if best_unique > 1:
            time_col = candidate

    if time_col is None:
        return common.html_to_obj("<p>Could not auto-detect a time column. Provide a dataset with a time-like column (e.g., 'year').</p>")

    # Determine numeric columns to render (exclude time_col)
    num_cols = [c for c in df.select_dtypes(include=[np.number]).columns if c != time_col]
    if not num_cols:
        return common.html_to_obj("<p>No numeric columns found (excluding the detected time column). Nothing to plot.</p>")

    # Normalize time axis: get sorted unique values
    times = sorted(df[time_col].dropna().unique().tolist())

    # Check if time values are integer-like (e.g., years) => build continuous integer range
    is_int_like = False
    try:
        if all(float(t).is_integer() for t in times):
            is_int_like = True
    except Exception:
        is_int_like = False

    if is_int_like and len(times) > 0:
        tmin, tmax = int(min(times)), int(max(times))
        times_full = list(range(tmin, tmax + 1))
    else:
        times_full = times

    # Aggregate numeric columns by time_col (sum) and reindex to full times
    try:
        ag = df.groupby(time_col)[num_cols].sum()
    except Exception as e:
        return common.html_to_obj(f"<p>Error aggregating by time column '{escape(time_col)}': {escape(str(e))}</p>")

    ag = ag.reindex(times_full, fill_value=0)
    ag = ag.astype(float)

    # Global max across all series for consistent vertical scaling
    global_max = max(1.0, float(ag.values.max()))

    # Sparkline settings
    spark_w = 140
    spark_h = 36

    rows_html = []
    for col in sorted(num_cols):
        series = ag[col].tolist()
        total = int(round(sum(series)))

        n = len(series)
        if n <= 1:
            points = f"0,{spark_h/2}"
        else:
            pts = []
            for i, v in enumerate(series):
                x = round(i * (spark_w / (n - 1)), 2)
                # scale value to [0..spark_h], baseline at bottom
                y = round(spark_h - ((v / global_max) * spark_h), 2)
                pts.append(f"{x},{y}")
            points = " ".join(pts)

        area_points = points + f" {spark_w},{spark_h} 0,{spark_h}"

        esc_col = escape(str(col))
        esc_total = escape(str(total))

        svg = (
            f'<svg class="spark" width="{spark_w}" height="{spark_h}" viewBox="0 0 {spark_w} {spark_h}" '
            f' xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="none" role="img" aria-label="sparkline for {esc_col}">'
            f'<polyline fill="none" stroke="#1f77b4" stroke-width="1.6" points="{points}" />'
            f'<path d="M {area_points}" fill="rgba(31,119,180,0.10)" stroke="none" />'
            f'</svg>'
        )

        rows_html.append(
            f"<tr><td style='white-space:nowrap'>{esc_col}</td>"
            f"<td style='text-align:right;padding-right:12px'>{esc_total}</td>"
            f"<td style='width:{spark_w}px'>{svg}</td></tr>"
        )

    title = f"Column sparklines by '{time_col}' ({len(times_full)} points)"
    html_content = f"""
    <style>
      table.spark-table {{border-collapse:collapse;font-family:Arial,Helvetica,sans-serif;width:100%;}}
      table.spark-table th, table.spark-table td {{padding:6px 8px;border-bottom:1px solid #ececec;}}
      table.spark-table th {{text-align:left;color:#333;font-weight:600;}}
      table.spark-table td {{color:#222;font-size:13px;}}
      table.spark-table tbody tr:hover {{background:#fbfbfb;}}
      .spark svg {{display:block}}
      .meta {{color:#666;font-size:12px;margin-top:6px;}}
    </style>

    <h3>{escape(title)}</h3>
    <table class="spark-table" role="table" aria-label="Column sparklines">
      <thead>
        <tr><th>Name</th><th style="text-align:right">Total</th><th>Trend</th></tr>
      </thead>
      <tbody>
        {''.join(rows_html)}
      </tbody>
    </table>
    <div class="meta">Sparklines show aggregated values by <strong>{escape(time_col)}</strong>. Vertical scale is consistent across all columns (0 - {int(global_max)}).</div>
    """

    return common.html_to_obj(html_content)