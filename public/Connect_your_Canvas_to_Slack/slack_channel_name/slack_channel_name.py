@fused.udf(cache_max_age="0s")
def udf(channel_id:str="C0ATL1RJP8Q"):
    import requests

    if channel_id is "":
        return "No channel returned"

    # Look up channel names via Slack API
    token = fused.secrets["SLACK_BOT_TOKEN"]

    resp = requests.get(
        "https://slack.com/api/conversations.info",
        headers={"Authorization": f"Bearer {token}"},
        params={"channel": channel_id},
    )
    data = resp.json()
    if data.get("ok"):
        return data["channel"]["name"]
    else:
        raise ValueError(f"Slack API error: {data.get('error', 'Unknown error')}")