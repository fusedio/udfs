import micropip
import pandas as pd
import streamlit as st

await micropip.install(["geopandas"])

st.title("🛰️ Objects Launched into Space")

# Load data from the URL
@st.cache_data
def load_data():
    url = "https://ourworldindata.org/grapher/yearly-number-of-objects-launched-into-outer-space.csv?v=1&csvType=filtered&useColumnShortNames=true&tab=table&time=2003..latest"
    df = pd.read_csv(url)
    df = df.rename(columns={'Entity': 'Country'})
    return df

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

# Get min and max years from the data
min_year = filtered_data["Year"].min()
max_year = filtered_data["Year"].max()

# Create a year range slider
year_range = st.slider(
    "Years to Display", 
    min_value=int(min_year), 
    max_value=int(max_year), 
    value=(2007, 2024)
)

# Filter by selected years
year_filtered_data = filtered_data[
    (filtered_data["Year"] >= year_range[0]) & 
    (filtered_data["Year"] <= year_range[1])
]

# Display the chart with the correct column name "annual_launches"
st.line_chart(year_filtered_data, x="Year", y="annual_launches", color="Country")