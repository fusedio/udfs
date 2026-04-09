{
  "type": "render",
  "props": {
    "defaultValue": {
  "type": "bar-chart",
  "props": {
    "sql":"SELECT neighbourhood_cleansed as label, ROUND(AVG(price_in_dollar), 2) as value FROM {{airbnb_data}} GROUP BY neighbourhood_cleansed ORDER BY value DESC LIMIT 15",
    "title": "Top 15 Neighbourhoods"
  }
},
    "showEditor": true,
    "aiBuilderMode": "enabled",
    "aiPanel": "right"
  }
}