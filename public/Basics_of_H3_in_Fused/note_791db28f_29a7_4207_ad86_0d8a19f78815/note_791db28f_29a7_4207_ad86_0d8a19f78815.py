H3 is a hexagonal grid system that divides the world into hex cells at different resolutions.Higher `res` = smaller hexagons = more detail.

**This UDF:**

1. **Loads point data** from `ny411_noise_points` (NYC 311 noise complaints with lat/lng)
2. **Converts points → H3 cells** using DuckDB's `h3_latlng_to_cell(lat, lng, res)` — each point gets assigned to the hex cell it falls in. In this example hex res 9
3. **Groups & counts** **→**`GROUP BY hex` + `COUNT(*)` tells us how many noise complaints fall in each hexagon
4. **Returns a DataFrame** with `hex` (the H3 cell ID) and `cnt` (number of complaints per cell)