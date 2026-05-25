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
        "sql": "SELECT printf('%016x', hex) AS hex, sesimic_hazard FROM {{hexagonified_seismic_hazard}}",
        "h3Column": "hex",
        "tooltip": true,
        "style": {
          "fillColor": {
            "type": "continuous",
            "attr": "sesimic_hazard",
            "domain": [0, 100],
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