@fused.udf
def udf(ticker: str = "AAPL"):
    """
    Return the last 30 days of daily OHLCV data for the given ticker.
    The data is fetched from Yahoo Finance’s public JSON endpoint,
    so no API key or extra finance package is required.
    The result is a Pandas DataFrame with columns:
    Date, Open, High, Low, Close, Volume.
    """
    import pandas as pd
    import datetime as dt
    import requests
    import fused

    # ------------------------------------------------------------------
    # Cached helper that performs the HTTP request only once per ticker.
    # ------------------------------------------------------------------
    @fused.cache
    def fetch_last_30_days(symbol: str) -> pd.DataFrame:
        """
        Download the last 30 days of daily price data from Yahoo Finance.
        A realistic User‑Agent header is added to avoid the 429
        “Too Many Requests” response that Yahoo returns for
        generic script requests.
        """
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/"
            f"{symbol}?range=30d&interval=1d"
        )
        # Add a browser‑like User‑Agent header (helps avoid 429)
        headers = {"User-Agent": "Mozilla/5.0 (compatible; FusedWorkBench/1.0)"}

        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()          # raise on HTTP error
        except requests.exceptions.HTTPError as e:
            # Return a friendly message instead of raising
            return pd.DataFrame(
                {"Message": [f"HTTP error while fetching {symbol}: {e}"]},
                index=[0],
            )
        except Exception as e:
            # Catch network‑related errors (timeout, DNS, etc.)
            return pd.DataFrame(
                {"Message": [f"Request error for {symbol}: {e}"]},
                index=[0],
            )

        data = resp.json()
        # The JSON structure is nested; extract the needed arrays.
        result = data["chart"]["result"][0]
        timestamps = result.get("timestamp", [])
        quote = result["indicators"]["quote"][0]

        # Build DataFrame – convert Unix timestamps to dates
        df = pd.DataFrame(
            {
                "Date": pd.to_datetime(timestamps, unit="s"),
                "Open": quote.get("open", []),
                "High": quote.get("high", []),
                "Low":  quote.get("low", []),
                "Close": quote.get("close", []),
                "Volume": quote.get("volume", []),
            }
        )
        # Remove rows where any price field is missing (e.g., market closed)
        df = df.dropna(subset=["Open", "High", "Low", "Close"])
        return df

    # ------------------------------------------------------------------
    # Execute the cached fetch and return the DataFrame.
    # ------------------------------------------------------------------
    df = fetch_last_30_days(ticker)

    # If the request failed or returned no rows, give a friendly message.
    if df.empty or "Message" in df.columns:
        return pd.DataFrame(
            {"Message": [f"No data returned for ticker '{ticker}'"]},
            index=[0],
        )

    return df