@fused.udf
def udf(bbox=None):
    import re
    import requests
    import shapely
    import pandas as pd

    data = requests.get(
        'https://raw.githubusercontent.com/MicrosoftDocs/azure-docs/88e1ce95875f10a8b634e2fc471d660963a12074/includes/expressroute-azure-regions-geopolitical-region.md'
    )
    locations = re.findall('North America.*', data.text)[0].split('|')[-2].split('<br/>')
    locations = set([l.replace('2', '') for l in locations])

    dc_df = pd.DataFrame(locations, columns=['city'])
    dc_df = add_lat_lon(dc_df)

    import h3
    dc_df['h3_3'] = dc_df.apply(
        lambda x: h3.latlng_to_cell(x.lat, x.lon, 3), axis=1
    )

    us_h3s = get_h3s()

    df = pd.DataFrame(
        [[c, False, 255, 0, 0] if c not in dc_df['h3_3'].tolist()
         else [c, True, 0, 0, 255] for c in us_h3s],
        columns=['h3_3', 'is_datacenter', 'r', 'g', 'b']
    )
    df['index'] = df.index

    edges = create_formulation_input(df)

    import networkx as nx
    d_graph = create_assignment_graph(edges)

    import highspy
    h = highspy.Highs()
    edge_assi, c_nodes, var_look_up = formulation(h, d_graph)

    h.run()
    solution = h.getSolution()
    info = h.getInfo()
    model_status = h.getModelStatus()
    print('Model status = ', h.modelStatusToString(model_status))
    print()
    print('Optimal objective = ', info.objective_function_value)

    solution_lines = []
    num_var = h.getNumCol()
    sol_hubs = set()
    for icol in range(num_var):
        if solution.col_value[icol] > 0.01 and var_look_up[icol][0] == 'x':
            s, e = (var_look_up[icol].split('_'))[1:]
            sol_hubs.add(s)
            lat_s, lon_s = h3.cell_to_latlng(df.loc[int(s)].h3_3)
            lat_e, lon_e = h3.cell_to_latlng(df.loc[int(e)].h3_3)
            solution_lines.append([lat_s, lon_s, lat_e, lon_e])

    solution_df = pd.DataFrame(
        solution_lines,
        columns=['hub_latitude', 'hub_longitude', 'demand_latitude', 'demand_longitude']
    )
    solution_df['weight'] = 1
    print("number of datacenters selected:", len(sol_hubs))
    return solution_df


@fused.cache
def add_lat_lon(df):
    from geopy.geocoders import Nominatim
    geolocator = Nominatim(user_agent="fused")
    df['location'] = df.apply(lambda x: geolocator.geocode(x.city), axis=1)
    df['lat'] = df['location'].apply(lambda x: x.latitude)
    df['lon'] = df['location'].apply(lambda x: x.longitude)
    return df


@fused.cache
def get_h3s():
    import geopandas as gpd
    import h3

    usa = gpd.read_file(
        'https://raw.githubusercontent.com/scdoshi/us-geojson/master/geojson/nation/US.geojson'
    )
    polygons = list(list(x.geoms) for x in usa.geometry)[0]
    polygons.sort(key=lambda x: x.area)
    coords = polygons[-1].exterior.coords[:]
    coords = list(c[:2][::-1] for c in coords)
    poly = h3.H3Poly(coords)
    return set(h3.h3shape_to_cells(poly, res=3))


def int_var_dict(h, var_str, name, variable_index_count=0, lb=0, ub=1):
    var_dict = {}
    for i, v_str in enumerate(var_str.values()):
        h.addVar(lb, ub)
        var_dict[f'{name}_{v_str}'] = i + variable_index_count
    return var_dict


def formulation(h, graph):
    import highspy
    import numpy as np

    inf = highspy.kHighsInf
    tasks = {j: f'{j}' for j in graph.nodes() if graph.out_degree[j] == 0}
    print(f'{len(tasks)} demand locations')
    cands = {i: f'{i}' for i in graph.nodes() if graph.in_degree[i] == 0}
    print(f'{len(cands)} hub locations')
    edges = {(s, e): f"{s}_{e}" for s, e in list(graph.edges())}
    edge_assi = int_var_dict(h, edges, 'x', lb=0, ub=1)
    c_nodes = int_var_dict(
        h, cands, 'z', variable_index_count=len(edge_assi), lb=0, ub=1
    )

    # Creating task assignment constraints
    for j in tasks:
        h.addRow(
            1, 1, len(graph.in_edges(j)),
            np.array([edge_assi[f'x_{i[0]}_{j}'] for i in graph.in_edges(j)]),
            np.array([1 for _ in graph.in_edges(j)])
        )
        for s, e, data in graph.in_edges(j, data=True):
            h.changeColCost(edge_assi[f'x_{s}_{e}'], data['cost'])

    # Creating indicator to push cost to use minimal amount of hubs
    for i in cands:
        h.addRow(
            -inf, 0, len(graph.out_edges(i)) + 1,
            [edge_assi[f'x_{i}_{j[1]}'] for j in graph.out_edges(i)] +
            [c_nodes[f'z_{i}']],
            [1 for _ in graph.out_edges(i)] + [-graph.nodes[i]['capacity']]
        )

    h.addRow(
        1, 10, len(c_nodes),
        [c_nodes[f'z_{c}'] for c in cands], [1 for _ in c_nodes]
    )

    for c in cands:
        h.changeColIntegrality(
            c_nodes[f'z_{c}'], highspy.HighsVarType.kInteger
        )
        h.changeColCost(c_nodes[f'z_{c}'], graph.nodes[c]['cost'])

    return edge_assi, c_nodes, {v: k for k, v in {**edge_assi, **c_nodes}.items()}


@fused.cache
def create_formulation_input(df):
    import h3

    i_j_hex = {h.h3_3: i for i, h in df.iterrows()}
    demand_locations = df[df.is_datacenter == False]
    data_center_locations = df[df.is_datacenter == True]

    edges = {}
    for h in data_center_locations['h3_3']:
        for d in demand_locations['h3_3']:
            edges[i_j_hex[h], i_j_hex[d]] = h3.great_circle_distance(
                h3.cell_to_latlng(d), h3.cell_to_latlng(h)
            )
    return edges


def create_assignment_graph(edges):
    import networkx as nx

    d_graph = nx.DiGraph()
    data_centers = set()
    for (s, e), cost in edges.items():
        d_graph.add_edge(s, e, cost=cost)
        data_centers.add(s)

    for one in data_centers:
        d_graph.nodes[one]['cost'] = 20_000
        d_graph.nodes[one]['capacity'] = 1000
    return d_graph