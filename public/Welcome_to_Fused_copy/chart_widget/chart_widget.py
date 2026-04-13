{
  "type": "scatter-chart",
  "props": {
    "sql": "SELECT number_of_reviews AS x, price_in_dollar AS y, room_type AS series, review_scores_rating AS size, name AS label FROM {{sf_airbnb_listings}} WHERE review_scores_rating IS NOT NULL AND price_in_dollar <= 2000 AND number_of_reviews <= 1000",
    "title": "Price vs Reviews (filtered)",
    "showLegend": true,
    "pointOpacity": 0.6,
    "defaultPointSize": 50,
    "xMin": 0,
    "yMin": 0,
    "xMax": 300,
    "yMax": 500
  }
}