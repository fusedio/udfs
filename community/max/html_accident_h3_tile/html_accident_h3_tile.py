@fused.udf
def udf(
    bounds: fused.types.TileGDF = None,
    year: int = 2021
):
    import geopandas as gpd
    import pandas as pd
    import requests
    import re
    import fused
    
    from shapely.geometry import Point

    def prepare_data(bounds, acc_csv_url):
        df = pd.read_csv(acc_csv_url, encoding="shift_jis")
        
        # Rename Japanese columns to English
        df.columns = [jp_en_column_mapping.get(col, col) for col in df.columns]
        
        # Add prefecture name column for later use
        df["prefecture_name"] = df["prefecture_code"].map(prefecture_mapping)

        # Remove rows with missing latitude/longitude
        df = df[df["latitude"].notnull() & df["longitude"].notnull()]
        df = df[(df["latitude"] != "") & (df["longitude"] != "")]

        # Convert the DMS-style lat/lon strings to decimal degrees using DuckDB
        df = con.sql(f"""
            SELECT
                prefecture_name,
                year,
                month,
                day,
                hour,
                CASE WHEN age_A = 0 OR age_A = 1 THEN 0 ELSE age_A END AS age_A,
                CASE WHEN age_B = 0 OR age_B = 1 THEN 0 ELSE age_B END AS age_B,
                injuries,
                TRY_CAST(LEFT(CAST(latitude AS VARCHAR), 2) AS INTEGER) +
                TRY_CAST(SUBSTRING(CAST(latitude AS VARCHAR), 3, 2) AS INTEGER) / 60.0 +
                TRY_CAST(SUBSTRING(CAST(latitude AS VARCHAR), 5) AS INTEGER) / 3600000.0 AS latitude,
                TRY_CAST(LEFT(CAST(longitude AS VARCHAR), 3) AS INTEGER) +
                TRY_CAST(SUBSTRING(CAST(longitude AS VARCHAR), 4, 2) AS INTEGER) / 60.0 +
                TRY_CAST(SUBSTRING(CAST(longitude AS VARCHAR), 6) AS INTEGER) / 3600000.0 AS longitude
            FROM df;
        """).df()

        return df
        
    # Load DuckDB connection helper (no caching)
    duckdb_connect = fused.load(
        "https://github.com/fusedio/udfs/tree/3569595/public/common/"
    ).utils.duckdb_connect

    con = duckdb_connect()
    # Load H3 extension
    con.sql('INSTALL h3 FROM community; LOAD h3;')

    url = "https://www.npa.go.jp/publications/statistics/koutsuu/opendata/index_opendata.html"
    acc_csv_url = get_csv_from_page(url, year)

    df = prepare_data(bounds, acc_csv_url)

    # Determine which coordinate columns to use
    lat_col = "center_lat" if "center_lat" in df.columns else "latitude"
    lng_col = "center_lng" if "center_lng" in df.columns else "longitude"

    # Convert to GeoDataFrame
    df = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[lng_col], df[lat_col]),
        crs="EPSG:4326",
    )

    # Clip to bounds if supplied
    if bounds is not None:
        df = gpd.clip(df, bounds)

    # Drop geometry column if it exists to avoid serialization issues
    if "geometry" in df.columns:
        df = df.drop(columns=["geometry"])
    
    # DuckDB can compute the H3 index and the centroid directly
    df = con.sql(f"""
        SELECT
            *,
            h3_latlng_to_cell(latitude, longitude, 12) AS hex,
        FROM df;
    """).df()
    
    return df



import requests
from bs4 import BeautifulSoup

@fused.cache()
def get_csv_from_page(page_url, year):
    resp = requests.get(page_url)
    resp.encoding = 'shift_jis'  # Use shift_jis for this page
    html = resp.text
    soup = BeautifulSoup(html, "html.parser")
    
    # find all <a> tags
    links = soup.find_all("a", href=True)
    print(f"Total links found: {len(links)}")
    
    # filter links with correct year in the href
    links = [link for link in links if str(year) in link['href']]
    print(f"Links with year {year}: {len(links)}")
    print(links)
    
    # get link address 
    link_address = requests.compat.urljoin(page_url, links[0]['href'])
    print(link_address)
    
    resp = requests.get(link_address)
    resp.encoding = 'shift_jis'  # Use shift_jis for this page
    html = resp.text
    soup = BeautifulSoup(html, "html.parser")
    
    # find all csv files in the page
    links = soup.find_all("a", href=True)
    
    # filter links without .csv in the href
    links = [link for link in links if link['href'].endswith('.csv')]
    
    # get the honhyo csv
    honhyo_link = [link for link in links if 'honhyo' in link['href']][0]
    honhyo_link_address = requests.compat.urljoin(link_address, honhyo_link['href'])
    return honhyo_link_address
jp_en_column_mapping = {
    "資料区分": "data_category",
    "都道府県コード": "prefecture_code",
    "警察署等コード": "police_station_code",
    "本票番号": "record_number",
    "事故内容": "accident_content",
    "死者数": "deaths",
    "負傷者数": "injuries",
    "路線コード": "route_code",
    "地点コード": "location_code",
    "市区町村コード": "municipality_code",
    "発生日時　　年": "year",
    "発生日時　　月": "month",
    "発生日時　　日": "day",
    "発生日時　　時": "hour",
    "発生日時　　分": "minute",
    "昼夜": "day_night",
    "日の出時刻　　時": "sunrise_hour",
    "日の出時刻　　分": "sunrise_minute",
    "日の入り時刻　　時": "sunset_hour",
    "日の入り時刻　　分": "sunset_minute",
    "天候": "weather",
    "地形": "terrain",
    "路面状態": "road_surface",
    "道路形状": "road_shape",
    "信号機": "traffic_signal",
    "一時停止規制　標識（当事者A）": "stop_sign_A",
    "一時停止規制　表示（当事者A）": "stop_display_A",
    "一時停止規制　標識（当事者B）": "stop_sign_B",
    "一時停止規制　表示（当事者B）": "stop_display_B",
    "車道幅員": "road_width",
    "道路線形": "road_alignment",
    "衝突地点": "collision_point",
    "ゾーン規制": "zone_regulation",
    "中央分離帯施設等": "median_facility",
    "歩車道区分": "roadside_division",
    "事故類型": "accident_type",
    "年齢（当事者A）": "age_A",
    "年齢（当事者B）": "age_B",
    "当事者種別（当事者A）": "party_type_A",
    "当事者種別（当事者B）": "party_type_B",
    "用途別（当事者A）": "vehicle_use_A",
    "用途別（当事者B）": "vehicle_use_B",
    "車両形状等（当事者A）": "vehicle_shape_A",
    "車両形状等（当事者B）": "vehicle_shape_B",
    "オートマチック車（当事者A）": "automatic_A",
    "オートマチック車（当事者B）": "automatic_B",
    "サポカー（当事者A）": "support_car_A",
    "サポカー（当事者B）": "support_car_B",
    "速度規制（指定のみ）（当事者A）": "speed_limit_A",
    "速度規制（指定のみ）（当事者B）": "speed_limit_B",
    "車両の衝突部位（当事者A）": "collision_part_A",
    "車両の衝突部位（当事者B）": "collision_part_B",
    "車両の損壊程度（当事者A）": "damage_degree_A",
    "車両の損壊程度（当事者B）": "damage_degree_B",
    "エアバッグの装備（当事者A）": "airbag_A",
    "エアバッグの装備（当事者B）": "airbag_B",
    "サイドエアバッグの装備（当事者A）": "side_airbag_A",
    "サイドエアバッグの装備（当事者B）": "side_airbag_B",
    "人身損傷程度（当事者A）": "injury_degree_A",
    "人身損傷程度（当事者B）": "injury_degree_B",
    "地点　緯度（北緯）": "latitude",
    "地点　経度（東経）": "longitude",
    "曜日(発生年月日)": "weekday",
    "祝日(発生年月日)": "holiday",
    "認知機能検査経過日数（当事者A）": "cognitive_days_A",
    "認知機能検査経過日数（当事者B）": "cognitive_days_B",
    "運転練習の方法（当事者A）": "practice_method_A",
    "運転練習の方法（当事者B）": "practice_method_B"
}
prefecture_mapping = {
    10: "Hokkaido (Sapporo)",
    11: "Hokkaido (Hakodate)",
    12: "Hokkaido (Asahikawa)",
    13: "Hokkaido (Kushiro)",
    14: "Hokkaido (Kitami)",
    20: "Aomori",
    21: "Iwate",
    22: "Miyagi",
    23: "Akita",
    24: "Yamagata",
    25: "Fukushima",
    30: "Tokyo",
    40: "Ibaraki",
    41: "Tochigi",
    42: "Gunma",
    43: "Saitama",
    44: "Chiba",
    45: "Kanagawa",
    46: "Niigata",
    47: "Yamanashi",
    48: "Nagano",
    49: "Shizuoka",
    50: "Toyama",
    51: "Ishikawa",
    52: "Fukui",
    53: "Gifu",
    54: "Aichi",
    55: "Mie",
    60: "Shiga",
    61: "Kyoto",
    62: "Osaka",
    63: "Hyogo",
    64: "Nara",
    65: "Wakayama",
    70: "Tottori",
    71: "Shimane",
    72: "Okayama",
    73: "Hiroshima",
    74: "Yamaguchi",
    80: "Tokushima",
    81: "Kagawa",
    82: "Ehime",
    83: "Kochi",
    90: "Fukuoka",
    91: "Saga",
    92: "Nagasaki",
    93: "Kumamoto",
    94: "Oita",
    95: "Miyazaki",
    96: "Kagoshima",
    97: "Okinawa"
}