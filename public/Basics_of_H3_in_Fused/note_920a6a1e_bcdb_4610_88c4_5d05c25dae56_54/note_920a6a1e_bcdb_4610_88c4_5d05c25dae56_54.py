### DuckDB WASM — query UDF output in the browser

1. **Load UDF as** `.parquet`: the UDF above exposes its output as a parquet file via [its HTTP endpoint](https://docs.fused.io/guide/working-with-udfs/run-udfs-as-api)
2. **Query in the frontend**: DuckDB WASM runs entirely in the browser, so you can query that parquet directly on the client without backend calls!
3. **Iterate instantly**: try changing the temperature threshold in the SQL query below and re-run. Because everything runs client-side, each tweak is near-instant