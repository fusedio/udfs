{
  "type": "fused-map",
  "props": {
    "basemap": "mapbox://styles/mapbox/dark-v11",
    "centerLng": 12.5,
    "centerLat": 41.9,
    "zoom": 5,
    "showControls": true,
    "showScale": true,
    "showBasemapSwitcher": true,
    "showLegend": true,
    "showLayerPanel": true,
    "autoSend": false,
    "autoSendDebounceMs": 600,
    "layers": [
      {
        "id": "italy-hex",
        "name": "Italy Hex",
        "type": "h3",
        "visible": true,
        "sql": "SELECT printf('%016x', hex) AS hex, building_count FROM {{Overture_to_hex_single_file}}",
        "h3Column": "hex",
        "tooltip": true,
        "style": {
          "fillColor": {
            "type": "continuous",
            "attr": "building_count",
            "domain": [10, 5000],
            "colors": "YlOrRd"
          },
          "lineColor": [0, 0, 0, 0],
          "opacity": 0.8,
          "coverage": 0.85
        }
      }
    ]
  }
}