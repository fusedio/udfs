@fused.udf(cache_max_age="0s")
def udf():
    import pandas as pd
    import json
    import requests

    # Load channel config
    config = json.loads(fused.api.get("fd://fused-system/integrations/slack.json"))
    print(config)
    channel_map = {r["channel_id"]: r["canvas_id"] for r in config}

    # Look up channel names via Slack API
    token = fused.secrets["SLACK_BOT_TOKEN"]
    rows = []
    for channel_id, canvas_id in channel_map.items():
        resp = requests.get(
            "https://slack.com/api/conversations.info",
            headers={"Authorization": f"Bearer {token}"},
            params={"channel": channel_id},
        )
        data = resp.json()
        if data.get("ok"):
            name = data["channel"]["name"]
        else:
            name = f"[error: {data.get('error', 'unknown')}]"
        rows.append({"channel_id": channel_id, "channel_name": name, "canvas_id": canvas_id})

    df = pd.DataFrame(rows)
    print(df)
    return df
