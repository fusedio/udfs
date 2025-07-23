import ibis

@ibis.udf.scalar.builtin
def h3_cell_to_boundary_wkt(h3_cell: int) -> str:
    """ Function to convert h3 cell to boundary wkt """

@ibis.udf.scalar.builtin
def h3_cell_to_parent(h3_cell: int, parent_res: int) -> int:
    """ Function to convert h3 cell to parent """


@ibis.udf.scalar.builtin
def ST_GeomFromText(wkt: str) -> ibis.dtype('Polygon'):
    """ Function to convert wkt to geometry """
