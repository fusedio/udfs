def list_stac_collections(catalog):
    names = [x.id for x in catalog.get_collections()]
    for x in names: print(x)