@fused.udf(cache_max_age='10s')
def udf():
    """
    Walk /mount/fused-system/logs for the last 1 hour, return all runs
    matching the given email filter.

    Directory structure: logs/{date}/{HHMM}/{email}/{run_uuid}/
    Files read per run: udf.json (code extracted from .code field), metadata.json, stdout.log, stderr.log
    """

    # Configuration
    lookback_minutes= 15              # time in minutes
    udf_to_filter_for = "canvas_bot"  # only keep logs for this UDF 

    # Filtering only for calls you, owner of this canvas, have made (all calls in Slack will go through this Canvas)
    email_filter = fused.api.whoami()['name']
    
    import os
    import pandas as pd
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    lookback_start = now - timedelta(minutes=lookback_minutes)

    print(f"now (UTC):          {now.strftime('%Y-%m-%d %H:%M')}")
    print(f"lookback_start (UTC): {lookback_start.strftime('%Y-%m-%d %H:%M')} ({lookback_minutes}min ago)")

    # HHMM folders are per-minute, so walk every minute in the lookback window
    buckets = []
    t = lookback_start.replace(second=0, microsecond=0)
    while t <= now:
        buckets.append((t.strftime("%Y-%m-%d"), t.strftime("%H%M")))
        t += timedelta(minutes=1)

    skip = {"token", "shared"}

    base_log = "/mount/fused-system/logs"

    if not os.path.isdir(base_log):
        print(f"No log directory found at {base_log}")
        return pd.DataFrame()

    # Walk only the (date, HHMM) buckets that fall within the last hour
    run_entries = []
    for datestr, hhmm in buckets:
        bucket_path = os.path.join(base_log, datestr, hhmm)
        if not os.path.isdir(bucket_path):
            continue
        for email_dir in os.scandir(bucket_path):
            if email_dir.name in skip or not email_dir.is_dir():
                continue
            # Apply email filter
            if email_filter and email_dir.name != email_filter:
                continue
            for run in os.scandir(email_dir.path):
                if run.is_dir():
                    run_entries.append((datestr, hhmm, email_dir.name, run.name, run.path))

    print(f"Found {len(run_entries)} runs in the last {lookback_minutes}min for email={email_filter!r}")

    if not run_entries:
        print("Nothing to ingest.")
        return pd.DataFrame()

    # Use udf.map(engine='local') — Fused-native parallel execution pattern
    # Each entry is passed as a tuple arg to read_run_udf
    rows = read_run_udf.map(run_entries, engine='local', max_workers=64).df()

    df = pd.DataFrame(rows)
    df = df.sort_values("timestamp", ascending=False).reset_index(drop=True)
    df = df[df["udf_name"] == udf_to_filter_for].reset_index(drop=True)

    print("Done ✓")
    print(df.dtypes)
    # print(df['stderr'].iloc[3])
    return df


@fused.udf
def read_run_udf(entry: tuple = None):
    """Read a single run directory and return its metadata as a dict."""
    import json
    import os

    def read_file(path: str):
        try:
            with open(path, "r", errors="replace") as f:
                return f.read()
        except Exception:
            return None

    datestr, hhmm, email, run_uuid, run_path = entry

    # Parse udf.json → extract parameters
    udf_json_raw = read_file(os.path.join(run_path, "udf.json"))
    parameters = None
    if udf_json_raw:
        try:
            parameters = json.loads(udf_json_raw).get("parameters")
        except Exception:
            pass

    # Parse metadata.json → extract key execution fields
    meta_raw = read_file(os.path.join(run_path, "metadata.json"))
    udf_name = None
    timestamp = None
    execution_time = None
    has_exception = None
    errormsg = None
    if meta_raw:
        try:
            m = json.loads(meta_raw)
            udf_name       = m.get("udf_name")
            timestamp      = m.get("timestamp")
            execution_time = m.get("execution_time_seconds")
            has_exception  = m.get("has_exception")
            errormsg       = m.get("errormsg")
        except Exception:
            pass

    import pandas as pd
    return pd.DataFrame([{
        "timestamp":        timestamp,
        "udf_name":         udf_name,
        "email":            email,
        "parameters":       json.dumps(parameters) if parameters else None,
        "execution_time_s": execution_time,
        "has_exception":    has_exception,
        "errormsg":         errormsg,
        "stdout":           read_file(os.path.join(run_path, "stdout.log")),
        "stderr":           read_file(os.path.join(run_path, "stderr.log")),
        "run_uuid":         run_uuid,
    }])
