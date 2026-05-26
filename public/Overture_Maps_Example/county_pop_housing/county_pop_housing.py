@fused.udf
def udf(bounds: fused.types.Bounds=[-74.556, 40.4, -73.374, 41.029], res: int=8):
    joined = fused.run('elev_pop_merge', bounds=bounds, res=res)

    print(joined.T)

    # Aggregate per county
    county_agg = joined.groupby(['county']).agg({
        'POP20': 'sum',
        'HOUSING20': 'sum',
        'avg_elevation': 'mean',
        'hex': 'nunique'
    }).reset_index()
    
    # Calculate population density and housing units per person
    county_agg['pop_density'] = county_agg['POP20'] / county_agg['hex']
    county_agg['housing_per_person'] = county_agg['HOUSING20'] / county_agg['POP20']
    
    return county_agg