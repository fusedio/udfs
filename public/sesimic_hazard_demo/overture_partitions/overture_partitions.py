{
  "type": "fused-map",
  "props": {
    "basemap": "mapbox://styles/mapbox/dark-v11",
    "centerLng": 12.5,
    "centerLat": 42,
    "zoom": 5,
    "showControls": true,
    "showScale": true,
    "showBasemapSwitcher": true,
    "showLegend": false,
    "showLayerPanel": true,
    "autoSend": false,
    "autoSendDebounceMs": 600,
    "layers": [
      {
        "id": "partitions-layer",
        "name": "Overture Partitions",
        "type": "deck-geojson",
        "visible": true,
        "sql": "SELECT file_id, ST_AsGeoJSON(geometry) as geometry FROM {{Overture_partitions}}",
        "geometryColumn": "geometry",
        "tooltip": [
          "file_id"
        ],
        "style": {
          "opacity": 0.8,
          "lineColor": [
            250,
            250,
            250
          ],
          "lineWidth": 1,
          "fillColor": [
            0,
            0,
            0,
            0
          ]
        }
      }
    ]
  }
}