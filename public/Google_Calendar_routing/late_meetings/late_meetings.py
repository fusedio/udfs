{
  "type": "div",
  "props": {
    "style": "display: flex; flex-direction: column; gap: 16px; padding: 16px;"
  },
  "children": [
    {
      "type": "sql-table",
      "props": {
        "title": "🚨 Meetings I'll Be Late To",
        "sql": "SELECT to_meeting AS \"Meeting\", to_time AS \"Time\", to_location AS \"Location\", from_meeting AS \"Coming From\", travel_minutes AS \"Travel (min)\", gap_minutes AS \"Gap (min)\", buffer_minutes AS \"Buffer (min)\", status AS \"Status\" FROM {{nyc_meeting_routes}} WHERE status = 'Will Be Late' ORDER BY to_time",
        "sortable": true,
        "filterable": false
      }
    },
    {
      "type": "fused-map",
      "props": {
        "basemap": "mapbox://styles/mapbox/dark-v11",
        "centerLng": -73.98,
        "centerLat": 40.75,
        "zoom": 11,
        "showControls": true,
        "showScale": false,
        "showBasemapSwitcher": true,
        "showLegend": false,
        "showLayerPanel": false,
        "style": "height: 300px; border-radius: 8px; overflow: hidden;",
        "layers": [
          {
            "id": "late-meetings",
            "name": "🔴 Will Be Late",
            "type": "scatterplot",
            "visible": true,
            "sql": "SELECT g.title, g.location, g.latitude AS lat, g.longitude AS lng FROM {{gcal_events_geocode}} g INNER JOIN {{nyc_meeting_routes}} r ON g.title = r.to_meeting WHERE r.status = 'Will Be Late' AND g.latitude IS NOT NULL AND g.longitude IS NOT NULL",
            "latColumn": "lat",
            "lngColumn": "lng",
            "tooltip": ["title", "location"],
            "style": {
              "fillColor": [220, 53, 53],
              "lineColor": [255, 255, 255],
              "pointRadius": 10,
              "opacity": 0.95
            }
          },
          {
            "id": "ontime-meetings",
            "name": "🟢 On Time",
            "type": "scatterplot",
            "visible": true,
            "sql": "SELECT g.title, g.location, g.latitude AS lat, g.longitude AS lng FROM {{gcal_events_geocode}} g INNER JOIN {{nyc_meeting_routes}} r ON g.title = r.to_meeting WHERE r.status = 'On Time' AND g.latitude IS NOT NULL AND g.longitude IS NOT NULL",
            "latColumn": "lat",
            "lngColumn": "lng",
            "tooltip": ["title", "location"],
            "style": {
              "fillColor": [34, 197, 94],
              "lineColor": [255, 255, 255],
              "pointRadius": 10,
              "opacity": 0.95
            }
          }
        ]
      }
    }
  ]
}