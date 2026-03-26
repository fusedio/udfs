@fused.udf
def udf(state_fips: str = None, county_fips: str = None, resolution: str = "5m"):
    """
    Return US county boundaries

    - If state_fips & county_fips are provided, return that single county.
    - Otherwise, return 5 small counties chosen for diverse climates & NDVI contrast:

    1. San Juan County, WA   (~450 km² — maritime Pacific NW islands)
    2. Santa Cruz County, AZ (~3,200 km² — Sonoran desert/grassland)
    3. Decatur County, IA    (~1,370 km² — Midwest corn-belt cropland)
    4. Macon County, AL      (~1,580 km² — SE Piedmont mixed forest)
    5. Knox County, ME       (~950 km² — coastal boreal/deciduous)
    """
    import requests
    import geopandas as gpd
    import pandas as pd

    AREAS = [
        {"state": "53", "county": "055", "name": "San Juan County, WA",   "biome": "Maritime Pacific NW"},
        {"state": "04", "county": "023", "name": "Santa Cruz County, AZ", "biome": "Desert/Grassland"},
        {"state": "19", "county": "053", "name": "Decatur County, IA",    "biome": "Cropland"},
        {"state": "01", "county": "087", "name": "Macon County, AL",      "biome": "SE Mixed Forest"},
        {"state": "23", "county": "013", "name": "Knox County, ME",       "biome": "Boreal/Deciduous"},
    ]

    url = f"https://eric.clst.org/assets/wiki/uploads/Stuff/gz_2010_us_050_00_{resolution}.json"

    @fused.cache
    def load_all_counties(u):
        import requests, geopandas as gpd
        r = requests.get(u, timeout=60)
        r.raise_for_status()
        return gpd.GeoDataFrame.from_features(r.json()["features"], crs="EPSG:4326")

    gdf_all = load_all_counties(url)
    print("All counties shape:", gdf_all.shape)
    print("Columns:", gdf_all.columns.tolist())

    # ── Single-county mode: filter by state_fips + county_fips ────────
    if state_fips is not None and county_fips is not None:
        mask = (gdf_all["STATE"].astype(str) == state_fips) & \
               (gdf_all["COUNTY"].astype(str) == county_fips)
        if mask.sum() != 1:
            raise ValueError(
                f"County {state_fips}/{county_fips} not found. "
                f"Matched {mask.sum()} rows."
            )
        out = gdf_all[mask].copy()
        cols = [c for c in out.columns if c != "geometry"] + ["geometry"]
        print(f"Returning single county: state={state_fips}, county={county_fips}")
        return out[cols]

    # ── Default mode: return all 5 hardcoded counties ─────────────────
    results = []
    for area in AREAS:
        geoid = area["state"] + area["county"]
        filtered = None

        for col in ["GEO_ID", "GEOID", "GEOID10", "FIPS"]:
            if col not in gdf_all.columns:
                continue
            s = gdf_all[col].astype(str)
            if col == "GEO_ID":
                mask = s.str.endswith(geoid)
            else:
                mask = s == geoid
            if mask.sum() == 1:
                filtered = gdf_all[mask].copy()
                break

        if filtered is None or len(filtered) == 0:
            # Fallback: scan all object columns
            for c in gdf_all.select_dtypes("object").columns:
                s = gdf_all[c].astype(str)
                mask = (s == geoid) | s.str.endswith(geoid)
                if mask.sum() == 1:
                    filtered = gdf_all[mask].copy()
                    break

        if filtered is not None and len(filtered) == 1:
            filtered["area_name"] = area["name"]
            filtered["biome"] = area["biome"]
            results.append(filtered)
        else:
            print(f"WARNING: could not find {area['name']} (GEOID {geoid})")

    out = gpd.GeoDataFrame(pd.concat(results, ignore_index=True), crs="EPSG:4326")
    print("Output shape:", out.shape)
    print(out[["area_name", "biome"]])
    cols = [c for c in out.columns if c != "geometry"] + ["geometry"]
    return out[cols]