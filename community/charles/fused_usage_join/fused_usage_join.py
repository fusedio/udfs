@fused.udf
def udf(date: str = "2025-08-19"):
    import pandas as pd

    exec_envs: str = f"s3://fused-users/fused/usage_stats/raw/fact_exec_envs/{date}.parquet"
    users: str = f"s3://fused-users/fused/usage_stats/raw/fact_users/{date}.parquet"
    event: str = f"s3://fused-users/fused/usage_stats/raw/event/{date}.parquet"

    exec_envs_df = pd.read_parquet(exec_envs)
    users_df = pd.read_parquet(users)
    event_df = pd.read_parquet(event)

    exec_envs_records = exec_envs_df.to_dict(orient="records")
    users_records = users_df.to_dict(orient="records")

    # Map 1: exec_env.id -> name
    env_to_name = {
        env["id"]: env["name"]
        for env in exec_envs_records
        if "id" in env and "name" in env
    }

    # Map 2: user.id from exec_env.users -> env["name"]
    user_id_to_env_name = {}
    for env in exec_envs_records:
        for user in env.get("users", []):
            if "id" in user and "name" in env:
                user_id_to_env_name[user["id"]] = env["name"]

    # Map 3: fallback — user.id -> user["name"] (or email/handle) from user list
    user_id_to_name = {
        user["id"]: user.get("name") or user.get("email") or user.get("handle")
        for user in users_records
        if "id" in user
    }

    def resolve_name(row):
        user_id = row.get("user_id")
        env_id = row.get("execution_environment_id")

        if user_id in user_id_to_env_name:
            return user_id_to_env_name[user_id]
        if env_id in env_to_name:
            return env_to_name[env_id]
        if user_id in user_id_to_name:
            return user_id_to_name[user_id]
        return None

    event_df["final_name"] = event_df.apply(resolve_name, axis=1)
    return event_df