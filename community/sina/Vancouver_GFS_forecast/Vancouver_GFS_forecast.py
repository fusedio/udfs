@fused.udf
def udf(
    lat: float = 49.2827, 
    lon: float = -123.1208, 
    forecast_days: int = 7
):
    import pandas as pd
    import requests
    from datetime import datetime
    
    try:
        # Validate inputs
        if lat is None or lon is None:
            raise ValueError("Latitude and longitude must be provided")
        
        # Limit forecast days to valid range (1-14)
        forecast_days = max(1, min(14, forecast_days))
        
        print(f"Fetching weather for location ({lat}, {lon}), forecast days: {forecast_days}")
        
        base_url = "https://api.open-meteo.com/v1/forecast"
        
        # Build the request URL with just the basic parameters
        params = {
            "latitude": lat,
            "longitude": lon,
            "daily": [
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "precipitation_hours",
                "wind_speed_10m_max",
                "sunshine_duration",
                "weather_code"
            ],
            "timezone": "auto",
            "forecast_days": forecast_days
        }
        
        # Make the API request
        print("Sending request to Open-Meteo API...")
        response = requests.get(base_url, params=params)
        
        # Check for successful response
        if response.status_code != 200:
            print(f"Error: API request failed with status code {response.status_code}")
            return pd.DataFrame({"error": [f"API request failed: {response.text}"]})
        
        # Parse the JSON response
        data = response.json()
        
        # Process daily data
        daily_data = data.get("daily", {})
        daily_times = daily_data.get("time", [])

        weather_codes = {
            0: "Clear sky",
            1: "Mainly clear",
            2: "Partly cloudy",
            3: "Overcast",
            45: "Fog",
            48: "Depositing rime fog",
            51: "Light drizzle",
            53: "Moderate drizzle",
            55: "Dense drizzle",
            56: "Light freezing drizzle",
            57: "Dense freezing drizzle",
            61: "Slight rain",
            63: "Moderate rain",
            65: "Heavy rain",
            66: "Light freezing rain",
            67: "Heavy freezing rain",
            71: "Slight snow fall",
            73: "Moderate snow fall",
            75: "Heavy snow fall",
            77: "Snow grains",
            80: "Slight rain showers",
            81: "Moderate rain showers",
            82: "Violent rain showers",
            85: "Slight snow showers",
            86: "Heavy snow showers",
            95: "Thunderstorm",
            96: "Thunderstorm with slight hail",
            99: "Thunderstorm with heavy hail"
        }
        
        # Create daily DataFrame with just the basic parameters
        weather_df = pd.DataFrame({
            "date": [str(date) for date in pd.to_datetime(daily_times).date],
            "temp_max_c": daily_data.get("temperature_2m_max", []),
            "temp_min_c": daily_data.get("temperature_2m_min", []),
            "precipitation_mm": daily_data.get("precipitation_sum", []),
            "precipitation_hours": daily_data.get("precipitation_hours", []),
            "wind_speed_km_h": daily_data.get("wind_speed_10m_max", []),
            "sunshine_hours": [d/3600 if d is not None else None for d in daily_data.get("sunshine_duration", [])],
            "weather": [weather_codes.get(code, "Unknown") for code in daily_data.get("weather_code", [])]
        })
        
        print(f"Successfully retrieved weather for {len(weather_df)} days")
        print(f"{weather_df=}")
        return weather_df
        
    except Exception as e:
        print(f"Error fetching weather: {e}")
        import traceback
        print(traceback.format_exc())
        return pd.DataFrame({"error": [str(e)]})