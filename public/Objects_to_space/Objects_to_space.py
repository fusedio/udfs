import micropip
import pandas as pd
import streamlit as st

await micropip.install(["geopandas"])

st.title("🛰️ Objects Launched into Space")


@st.cache_data
def load_data():
    url = "https://ourworldindata.org/grapher/yearly-number-of-objects-launched-into-outer-space.csv?v=1&csvType=filtered&useColumnShortNames=true&tab=table&time=2003..latest"
    return pd.read_csv(url).rename(columns={'Entity': 'Country'})

data = load_data()
countries = sorted(data['Country'].unique())

# Create a multiselect to choose countries
selected_countries = st.multiselect(
    "Select Countries", 
    countries,
    default=["United States", "China", "Russia", "France"]
)

# Filter data based on selected countries
if selected_countries:
    filtered_data = data[data['Country'].isin(selected_countries)]
else:
    filtered_data = data  # Show all if none selected

# Create a year range slider
year_range = st.slider(
    "Years to Display", 
    min_value=int(filtered_data["Year"].min()), 
    max_value=int(filtered_data["Year"].max()), 
    value=(2007, 2024)
)

# Filter by selected years
year_filtered_data = filtered_data[
    (filtered_data["Year"] >= year_range[0]) & 
    (filtered_data["Year"] <= year_range[1])
]

st.line_chart(year_filtered_data, x="Year", y="annual_launches", color="Country")
st.caption("[Data source](https://ourworldindata.org/grapher/yearly-number-of-objects-launched-into-outer-space): Our World in Data - Yearly Number of Objects Launched into Outer Space")