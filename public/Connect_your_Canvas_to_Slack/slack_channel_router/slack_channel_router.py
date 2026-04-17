@fused.udf(cache_max_age=0) # Setting cache to zero to always fetch latest tools
def udf(
    message_text: str = "what are the GERS IDs within 30m of the train station of Florence, Italy?",
    user_id: str = "",
    channel_id: str = "C0ATJ4Y0L3B", # Don't pass anything
    team_id: str = "",
    thread_ts: str = "",
    event_type: str = "app_mention",
    raw_event: dict = {},
    debug: bool = False,
):
    import pandas as pd
    import json
    import requests

    ai_udf = fused.load("canvas_bot")
    bot_token = fused.secrets["SLACK_BOT_TOKEN"]

    # =================== CHANNEL → CANVAS ROUTING ===================
    # Map each Slack channel ID to a Fused canvas share token.
    # To add a new channel: paste its channel ID (right-click channel
    # in Slack → "View channel details" → copy the ID at the bottom)
    # and the canvas share token (fc_...) you want it to use.
    # CHANNEL_CONFIG = {
    #     "C0AHV2AGBJ9": "fc_2Sy3BGtJj8U5l14ThFj2yS", # fused-bot-test         -> Fused usage for now
    #     "C0ATKRML34Y": "fc_70N994S9ZGgYAuDs6eHzXy", # fused-bot-google-cal   -> Google Calendar 
    #     "C0ATL1RJP8Q": "fc_UPF2QNZmfRPnJ9fWoG7rC"   # fused-docs-questions   -> Docs ragging
    # }

    channel_routing = json.loads(fused.api.get(
        # "s3://fused-users/fused/max/slack_canvas_routing/slack_canvas_channel_config.json"
        "fd://fused-system/integrations/slack.json"
    ))
    CHANNEL_CONFIG = {item["channel_id"]: item["canvas_id"] for item in channel_routing}

    print(CHANNEL_CONFIG)
    
    # ================================================================
    canvas_token = CHANNEL_CONFIG.get(channel_id)

    # =================== MENTION-ONLY FILTER ===================
    # Only respond to direct @mentions (app_mention) or replies
    # in a thread where the bot was already mentioned.
    # Silently ignore all other messages.
    is_mention = event_type == "app_mention"

    event = raw_event if isinstance(raw_event, dict) else {}
    bot_user_id = requests.get(
        "https://slack.com/api/auth.test",
        headers={"Authorization": f"Bearer {bot_token}"},
    ).json().get("user_id", "")

    # For thread replies: respond if the bot is directly mentioned (@bot),
    # OR if the bot has already posted in this thread (follow-up without mention).
    # Note: event_type may be empty string for some thread replies, so don't gate on it.
    is_thread_mention = (
        bool(thread_ts)
        and f"<@{bot_user_id}>" in message_text
    )
    print(f"{is_thread_mention=}")

    # Check if the bot has already participated in this thread.
    # Run this for any event that has a thread_ts (don't gate on event_type=="message"
    # because Slack sometimes sends thread follow-ups with an empty event_type).
    is_bot_in_thread = False
    if bool(thread_ts) and not is_mention and not is_thread_mention:
        thread_check = requests.get(
            "https://slack.com/api/conversations.replies",
            headers={"Authorization": f"Bearer {bot_token}"},
            params={"channel": channel_id, "ts": thread_ts, "limit": 50},
        ).json()
        thread_msgs = thread_check.get("messages", [])
        is_bot_in_thread = any(
            msg.get("bot_id") or msg.get("user") == bot_user_id
            for msg in thread_msgs
        )
        if is_bot_in_thread:
            print(f"[Filter] Bot has already replied in thread {thread_ts} — allowing follow-up without @mention")
    print(f"{is_bot_in_thread=}")

    if not is_mention and not is_thread_mention and not is_bot_in_thread:
        print(f"[Filter] Ignoring event_type='{event_type}' — not a mention (bot_user_id={bot_user_id})")
        return "Bot not mentioned with @ so skipping"
    # ===========================================================

    if not canvas_token:
        print(f"[Router] No canvas configured for channel {channel_id}")
        # Post a message back to the user and exit early
        requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {bot_token}"},
            json={
                "channel": channel_id,
                "thread_ts": thread_ts,
                "text": "This channel isn't configured for the bot. Ask an admin to add this channel ID to the CHANNEL_CONFIG dict in the `slack_channel_router` UDF.",
            },
        )
        return "unconfigured channel"

    print(f"[Router] Channel {channel_id} → canvas_token {canvas_token[:12]}...")

    event = raw_event if isinstance(raw_event, dict) else json.loads(raw_event)
    message_ts = event.get("ts", thread_ts)

    # Dedup: use eyes reaction as a lock — if it's already there, another instance is handling it
    react_resp = requests.post(
        "https://slack.com/api/reactions.add",
        headers={"Authorization": f"Bearer {bot_token}"},
        json={
            "channel": channel_id,
            "timestamp": message_ts,
            "name": "eyes",
        },
    ).json()

    if not react_resp.get("ok") and react_resp.get("error") == "already_reacted":
        print(f"[Dedup] Eyes already on {message_ts}, skipping retry")
        return "already handling"

    # Brief pause so Slack indexes recent messages before we fetch history.

    # Fetch thread history, excluding the current message.
    # We pass message_text explicitly as the current request below.
    prior_messages = []
    if thread_ts:
        import time
        time.sleep(1) # VERY HACKY WORKROUND TO GET PROPER THREAD RESPONSE
        
        resp = requests.get(
            "https://slack.com/api/conversations.replies",
            headers={"Authorization": f"Bearer {bot_token}"},
            params={"channel": channel_id, "ts": thread_ts, "limit": 50},
        ).json()
        all_messages = resp.get("messages", [])
        # Exclude the current message by matching its text, not position.
        # This is robust even if Slack hasn't finished indexing yet.
        clean = message_text.strip()
        # prior_messages = [
        #     m for m in all_messages
        #     if (m.get("text") or "").strip() != clean
        # ]
        prior_messages = [
            m for m in all_messages
            if m.get("ts") != message_ts
        ]

    history_lines = []
    for msg in prior_messages:
        text = (msg.get("text") or "").strip()
        if not text:
            continue
        is_bot = bool(msg.get("bot_id"))
        if is_bot:
            summary = text[:120] + ("…" if len(text) > 120 else "")
            history_lines.append(f"assistant: {summary}")
        else:
            history_lines.append(f"user: {text}")

    thread_context = "\n".join(history_lines) if history_lines else ""
    print(f"{thread_context=}")
    ai_response = ai_udf(prompt=message_text, thread_context=thread_context, canvas_token=canvas_token)

    if not debug:
        # Only send message when not debugging
        requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {bot_token}"},
            json={
                "channel": channel_id,
                "thread_ts": thread_ts,
                "text": f"{ai_response}",
            },
        )

    # Simply printing output for now to be able to read logs in mount/fused-system
    print(f"{ai_response=}")
    return ai_response
