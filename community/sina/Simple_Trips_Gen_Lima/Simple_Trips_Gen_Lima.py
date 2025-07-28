
from shapely.geometry import box
aoi = gpd.GeoDataFrame({'geometry': [box(*[-77.0685986328125, -12.055065023002463, -77.04211914062499, -12.032977469134593])]}).set_crs('EPSG:4326', allow_override=True)

@fused.udf
def udf(
    import geopandas as gpd
    from datetime import datetime, timedelta
    aoi: gpd.GeoDataFrame = aoi,
    start_hour: str = datetime.now(), 
    end_hour: str = datetime.now() + timedelta(hours=15),
    n_trips: int = 10 
):
    import time
    import numpy as np
    import osmnx as ox
    import pandas as pd
    
    utils = fused.load("https://github.com/fusedio/udfs/blob/main/public/common/").utils

    # Process input parameters
    area_of_interest = aoi.total_bounds
    start_hour = pd.Timestamp(start_hour)
    end_hour = pd.Timestamp(end_hour)

    start = time.time()

    @fused.cache
    def get_graph():
        G = ox.graph_from_polygon(box(*area_of_interest), network_type="all")
        return G

    G = get_graph()
    print(f"Downloaded graph in {time.time() - start:.2f} seconds")

    # Prepare the graph for routing
    # impute speed on all edges missing data
    hwy_speeds = {"residential": 35, "secondary": 50, "tertiary": 60}
    G = ox.add_edge_speeds(G, hwy_speeds)

    # calculate travel time (seconds) for all edges
    G = ox.add_edge_travel_times(G)

    # Get the graph node to query coordinates after routing
    nodes = G.nodes()
    nodes = pd.DataFrame.from_dict(nodes, orient="index")
    nodes = nodes.rename(columns={0: "x", 1: "y"})
    nodes.index.name = "osmid"
    # Create a GeoDataFrame from the nodes
    nodes = gpd.GeoDataFrame(nodes, geometry=gpd.points_from_xy(nodes.x, nodes.y))

    DATA_URL = "https://raw.githubusercontent.com/Claudio9701/mediamap/separate-components/public/grid_data.geojson"
    data = gpd.read_file(DATA_URL)
    # Relevant cols: "denspob", "desc_zoni"

    # Set simulation parameters
    population_size = data["denspob"].sum().round().astype(int)
    trips_per_person = 2

    origins = data[data["desc_zoni"] == "RESIDENCIAL"].geometry.centroid  # Home
    destinations = data[data["desc_zoni"] == "COMERCIAL"].geometry.centroid  # Work

    total_trips = population_size * trips_per_person
    departure_hour = np.random.normal(7, 0.5, population_size).round(2)
    return_hour = np.random.normal(18, 0.5, population_size).round(2)

    print(
        f"""
    Simulation Parameters:
    ---------------------      
    Population size: {population_size}
    Origins: {origins.shape[0]}
    Destinations: {destinations.shape[0]}
    Total trips: {total_trips}
    Departure times: start {departure_hour.min()} end {departure_hour.max()}
    Return times:  start {return_hour.min()} end {return_hour.max()}
    """
    )

    # Calculate the nearest nodes to the origins and destinations
    start = time.time()
    origins_nearest_nodes = pd.Series(ox.nearest_nodes(G, origins.x, origins.y))
    destinations_nearest_nodes = pd.Series(
        ox.nearest_nodes(G, destinations.x, destinations.y)
    )
    print(f"Found nearest nodes in {time.time() - start:.2f} seconds")

    # Stratified sampling origins using the population density
    start = time.time()
    sampled_origins = origins_nearest_nodes.sample(
        weights=data[data["desc_zoni"] == "RESIDENCIAL"]["denspob"],
        n=population_size,
        replace=True,
    )
    # Random sampling destinations
    sampled_destinations = destinations_nearest_nodes.sample(
        n=population_size, replace=True
    )
    print(f"Origins and destinations sampled in {time.time() - start:.2f} seconds")

    # Create a dummy pd.Series to use pandarallel
    indexs_series = pd.Series(range(population_size))
    # Convert the departure and return times to datetime
    today = datetime.today()
    start_date = datetime(today.year, today.month, today.day)
    work_start_dts = pd.to_timedelta(departure_hour, unit="h") + start_date
    home_start_dts = pd.to_timedelta(return_hour, unit="h") + start_date

    batch_size = 50
    num_batches = (population_size + batch_size - 1) // batch_size
    print("# batches:", num_batches)

    # Run paralellized UDF
    @fused.cache
    def generate_single_trip_v2(x):
        return fused.run(
            udf_generate_single_trip_v2,
            origin=x[0],
            destination=x[1],
            start_timestamp=x[2],
            route_type=x[3],
            
        )

    # prepare inputs
    origins = pd.concat([sampled_origins, sampled_destinations])
    destinations = pd.concat([sampled_destinations, sampled_origins])
    route_type = ["home"]*len(sampled_origins) + ["work"]*len(sampled_destinations)
    
    start_dts = home_start_dts.append(work_start_dts)

    df = pd.DataFrame(
        {
            "origins": origins.values,
            "destinations": destinations.values,
            "start_dts": start_dts.values,
            "route_type": route_type
        }
    )

    # Filter the data to the time range
    print(f"{start_hour = }")
    print(f"{end_hour = }")
    print(f"{df['start_dts'].describe() = }")
    df = df[(df["start_dts"] >= start_hour) & (df["start_dts"] <= end_hour)]
    df["start_dts"] = df["start_dts"].apply(lambda x: x.timestamp())
    
    print(f"{df.shape = }")
    if df.shape[0] == 0:
        return gpd.GeoDataFrame(df, geometry=[])
        
    print(f"{n_trips = }")
    if df.shape[0] > n_trips:
        df_sample = df.sample(n=n_trips)
    else:
        df_sample = df
    
    param_list = list(df_sample.itertuples(index=False, name=None))

    # Generate the trips to work and to home
    start = time.time()
    result = utils.run_pool(generate_single_trip_v2, param_list)
    print(len(result))
    print(f"Trips generated in {time.time() - start:.2f} seconds")
    
    result_concat = pd.concat(result)
    result_gdf = gpd.GeoDataFrame(result_concat)

    print(result_gdf.T)
    return result_gdf


@fused.udf
def udf_generate_single_trip_v2(
    route_type: str,
    aoi: gpd.GeoDataFrame = aoi,
    origin: int = 310953861,
    destination: int = 915282395,
    start_timestamp: float = 1732125996.0,
):
    import time
    from datetime import datetime

    import geopandas as gpd
    import osmnx as ox
    import pandas as pd
    from shapely.geometry import LineString, box

    start_time = pd.Timestamp.fromtimestamp(start_timestamp)

    # Download the Graph for the data area + 1km buffer using OSMnx
    area_of_interest = aoi.total_bounds
    start = time.time()

    @fused.cache
    def get_graph():
        G = ox.graph_from_polygon(box(*area_of_interest), network_type="all")
        return G

    G = get_graph()
    print(f"Downloaded graph in {time.time() - start:.2f} seconds")

    # Prepare the graph for routing
    # impute speed on all edges missing data
    hwy_speeds = {"residential": 35, "secondary": 50, "tertiary": 60}
    G = ox.add_edge_speeds(G, hwy_speeds)

    # calculate travel time (seconds) for all edges
    G = ox.add_edge_travel_times(G)
    # Get the graph node to query coordinates after routing
    nodes = G.nodes()
    nodes = pd.DataFrame.from_dict(nodes, orient="index")
    nodes = nodes.rename(columns={0: "x", 1: "y"})
    nodes.index.name = "osmid"
    # Create a GeoDataFrame from the nodes
    nodes = gpd.GeoDataFrame(nodes, geometry=gpd.points_from_xy(nodes.x, nodes.y))

    # Get the graph node to query coordinates after routing

    route = ox.routing.shortest_path(
        G, origin, destination, weight="travel_time", cpus=1
    )

    if (route_len := len(route)) <= 1:
        return {}

    # Get route information

    route_gdf = ox.routing.route_to_gdf(G, route, "travel_time")
    # Build the timestamp for each route step
    time_deltas = pd.to_timedelta(route_gdf["travel_time"].cumsum(), unit="s")
    timestamps_series = (time_deltas + start_time).astype("int64") // 10**9
    timestamps = timestamps_series.values.tolist()
    timestamps.insert(
        0, int(datetime.timestamp(start_time))
    )  # Add origin to match route length

    assert route_len == len(
        timestamps
    ), "Route and timestamps must have the same length"

    # Get the coordinates of the nodes in the route
    route_nodes = nodes.loc[route]

    out = pd.DataFrame(
        {
            "start_time": timestamps[0],
            "path": route_nodes[["x", "y"]].values.tolist(),
            "timestamps": timestamps,
            "geometry": LineString(route_nodes[["x", "y"]].values.tolist()),
            "route_type": route_type
        }
    )
    out = gpd.GeoDataFrame(out)

    return out
