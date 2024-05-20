<!--fused:preview-->
<p align="center"><img src="https://luke2019-eng.github.io/Fused-LA_parking_occupancy/LA_Fused.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `transportation`

<!--fused:readme-->
Exported from Fused UDF Workbench

This User-Defined Function (UDF) integrates two datasets, the PMO (Parking Meter Occupancy) and PIP (Parking Inventory Program), to provide insights into parking occupancy and adjacent buildings. It starts by fetching data from the provided URLs, converting them into Pandas DataFrames, and merging them based on a shared identifier, 'spaceid'. The UDF then converts latitude and longitude coordinates from the PIP dataset into Point geometries and filters out rows with missing geometry values. It further converts the resulting DataFrame into a GeoDataFrame, merging it with the PMO DataFrame. Subsequently, it filters out rows with missing occupancy states and geometry values, printing the number of occupied and vacant parking spots. It defines a function to fetch adjacent buildings for each parking spot, buffers the parking spot geometry, retrieves buildings using a custom function, and concatenates the results into a GeoDataFrame. If adjacent buildings are found, they are printed; otherwise, a message indicating no adjacent buildings are found is printed. Finally, the UDF returns the GeoDataFrame with adjacent buildings, or None if no buildings are found. This UDF facilitates comprehensive parking analysis, including occupancy status and nearby infrastructure, supporting urban planning and mobility initiatives.
