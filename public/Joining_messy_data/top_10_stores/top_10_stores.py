{
  "type": "bar-chart",
  "props": {
    "sql": "SELECT S_NAME AS label, avg_rating AS value FROM {{join_store_infos}} ORDER BY avg_rating DESC LIMIT 10",
    "title": "Top 10 Best Performing Stores",
    "barColor": "blue",
    "barRadius": 6
  }
}