{
  "type": "div",
  "props": {
    "style": "padding: 20px; background: #0f0f0f; min-height: 100%; font-family: sans-serif;"
  },
  "children": [
    {
      "type": "text",
      "props": {
        "content": "# Sales Dashboard",
        "variant": "h2",
        "style": "color: #E8FF59; margin-bottom: 4px;"
      }
    },
    {
      "type": "text",
      "props": {
        "content": "Category breakdown of total sales revenue",
        "variant": "muted",
        "style": "margin-bottom: 24px;"
      }
    },
    {
      "type": "div",
      "props": {
        "style": "display: flex; gap: 16px; margin-bottom: 24px;"
      },
      "children": [
        {
          "type": "big-number",
          "props": {
            "sql": "SELECT SUM(sales) FROM {{sales_data}}",
            "label": "Total Revenue",
            "prefix": "$",
            "format": "compact",
            "decimals": 1,
            "size": 36,
            "color": "#E8FF59",
            "style": "background: #1a1a1a; border-radius: 12px; padding: 16px; flex: 1;"
          }
        },
        {
          "type": "big-number",
          "props": {
            "sql": "SELECT MAX(sales) FROM {{sales_data}}",
            "label": "Top Category Sales",
            "prefix": "$",
            "format": "comma",
            "decimals": 0,
            "size": 36,
            "color": "#7df0a0",
            "style": "background: #1a1a1a; border-radius: 12px; padding: 16px; flex: 1;"
          }
        },
        {
          "type": "big-number",
          "props": {
            "sql": "SELECT COUNT(*) FROM {{sales_data}}",
            "label": "Categories",
            "format": "none",
            "decimals": 0,
            "size": 36,
            "color": "#7eb8f7",
            "style": "background: #1a1a1a; border-radius: 12px; padding: 16px; flex: 1;"
          }
        }
      ]
    },
    {
      "type": "bar-chart",
      "props": {
        "sql": "SELECT category AS label, sales AS value FROM {{sales_data}} ORDER BY sales DESC",
        "title": "Sales by Category",
        "barColor": "#E8FF59",
        "hoverColor": "#b8cc30",
        "barOpacity": 1,
        "barRadius": 6,
        "showGrid": true,
        "rotateLabels": false,
        "horizontal": false,
        "showValues": true,
        "sort": "none",
        "xAxisFontSize": 12,
        "yAxisFontSize": 11,
        "beginAtZero": true,
        "animationMs": 400,
        "style": "background: #1a1a1a; border-radius: 12px; padding: 16px; height: 300px;"
      }
    }
  ]
}