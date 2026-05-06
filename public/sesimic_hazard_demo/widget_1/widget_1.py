{
  "type": "div",
  "props": {
    "style": "display: flex; flex-direction: column; gap: 10px; padding: 14px; background: #1a1c1f; height: 100%; font-family: sans-serif; box-sizing: border-box;"
  },
  "children": [
    {
      "type": "div",
      "props": {
        "style": "display: flex; flex-direction: row; align-items: baseline; gap: 12px; flex: initial;"
      },
      "children": [
        {
          "type": "text",
          "props": {
            "content": "Seismic Hazard & Building Risk Dashboard",
            "variant": "h3",
            "style": "color: #f1f5f9; margin: 0; flex: initial;"
          }
        },
        {
          "type": "text",
          "props": {
            "content": "Hazard score vs. building count per H3 hex (res 6)",
            "variant": "muted",
            "style": "color: #94a3b8; font-size: 12px; flex: initial;"
          }
        }
      ]
    },
    {
      "type": "div",
      "props": {
        "style": "display: flex; flex-direction: row; gap: 10px; flex: initial;"
      },
      "children": [
        {
          "type": "big-number",
          "props": {
            "label": "High-Risk Zones",
            "sql": "SELECT COUNT(*) FROM {{join_over_buildings}} WHERE sesimic_hazard > 70 AND building_count > 1000",
            "style": "flex: 1; background: #212326; border-radius: 8px; padding: 8px 14px; border: 1px solid #E8FF59; color: #E8FF59;"
          }
        },
        {
          "type": "big-number",
          "props": {
            "label": "Avg Hazard Score",
            "sql": "SELECT ROUND(AVG(sesimic_hazard), 1) FROM {{join_over_buildings}} WHERE sesimic_hazard > 70 AND building_count > 1000",
            "style": "flex: 1; background: #212326; border-radius: 8px; padding: 8px 14px; border: 1px solid #E8FF59; color: #E8FF59;"
          }
        },
        {
          "type": "big-number",
          "props": {
            "label": "Buildings at Risk",
            "sql": "SELECT SUM(building_count) FROM {{join_over_buildings}} WHERE sesimic_hazard > 70 AND building_count > 1000",
            "style": "flex: 1; background: #212326; border-radius: 8px; padding: 8px 14px; border: 1px solid #E8FF59; color: #E8FF59;"
          }
        }
      ]
    },
    {
      "type": "div",
      "props": {
        "style": "display: flex; flex-direction: row; gap: 10px; flex: 1; min-height: 0;"
      },
      "children": [
        {
          "type": "fused-map",
          "props": {
            "basemap": "mapbox://styles/mapbox/dark-v11",
            "centerLng": 12.5,
            "centerLat": 42,
            "zoom": 5,
            "showControls": true,
            "showScale": false,
            "showBasemapSwitcher": true,
            "showLegend": true,
            "showLayerPanel": false,
            "autoSend": false,
            "autoSendDebounceMs": 600,
            "style": "flex: 1; border-radius: 10px; overflow: hidden; min-height: 0;",
            "layers": [
              {
                "id": "high-risk-h3",
                "name": "High-Risk Zones",
                "type": "h3",
                "visible": true,
                "sql": "SELECT hex, sesimic_hazard, building_count FROM {{join_over_buildings}} WHERE sesimic_hazard > 70 AND building_count > 1000",
                "h3Column": "hex",
                "tooltip": ["hex", "sesimic_hazard", "building_count"],
                "legend": { "title": "Seismic Hazard Score" },
                "style": {
                  "fillColor": {
                    "type": "continuous",
                    "attr": "sesimic_hazard",
                    "domain": [70, 100],
                    "colors": ["#fde68a", "#f97316", "#dc2626", "#7f1d1d"]
                  },
                  "opacity": 0.85,
                  "coverage": 0.9,
                  "extruded": false
                }
              }
            ]
          }
        },
        {
          "type": "scatter-chart",
          "props": {
            "title": "Seismic Hazard vs. Number of Buildings",
            "sql": "SELECT sesimic_hazard AS x, building_count AS y, CASE WHEN sesimic_hazard > 70 AND building_count > 1000 THEN 'High Risk' ELSE 'Lower Risk' END AS series FROM {{join_over_buildings}}",
            "pointColor": "#E8FF59",
            "pointOpacity": 0.75,
            "defaultPointSize": 5,
            "pointStrokeWidth": 0.35,
            "pointStrokeColor": "#1a1c1f",
            "minBubbleSize": 10,
            "maxBubbleSize": 140,
            "showGrid": true,
            "showLegend": true,
            "xAxisFontSize": 11,
            "yAxisFontSize": 11,
            "animationMs": 300,
            "style": "flex: 1; min-width: 260px; background: #212326; border-radius: 10px; padding: 10px; min-height: 0; overflow: hidden;"
          }
        }
      ]
    }
  ]
}