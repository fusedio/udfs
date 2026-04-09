@fused.udf(cache_max_age=0)
def udf(
    calendar_url = "https://calendar.google.com/calendar/u/3?cid=bWF4LmZ1c2VkLnRlc3RpbmdAZ21haWwuY29t",
    date: str = "2026-04-08"  # e.g. "2026-04-08", defaults to today
):
    import requests
    import pandas as pd
    from datetime import datetime, timedelta
    from urllib.parse import urlparse, parse_qs
    import base64 

    try:
        # CHANGE YOUR API KEY
        google_api_key = fused.secrets["google_maps_api"]
    except Exception as e:
        print(f"Error getting API key: {e}")
        return pd.DataFrame({"error": [f"Google API key not found in fused.secrets: {str(e)}"]})
    
    
    # Extract and decode calendar_id from the cid parameter
    parsed_url = urlparse(calendar_url)
    cid_param = parse_qs(parsed_url.query).get('cid', [None])[0]
    
    if not cid_param:
        return pd.DataFrame({"error": ["No calendar ID found in URL"]})
    
    # Base64 decode the cid parameter
    calendar_id = base64.b64decode(cid_param).decode('utf-8')
    
    # Use provided date or fall back to today
    if date:
        today = datetime.strptime(date, "%Y-%m-%d").date()
    else:
        today = datetime.utcnow().date()
    time_min = datetime.combine(today, datetime.min.time()).isoformat() + "Z"
    time_max = datetime.combine(today, datetime.max.time()).isoformat() + "Z"
    
    url = f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events"
    print(f"URL: {url}")
    print(f"Calendar ID: {calendar_id}")
    print(f"API Key: {google_api_key[:10]}***")
    
    params = {
        "key": google_api_key,
        "timeMin": time_min,
        "timeMax": time_max,
        "singleEvents": True,
        "orderBy": "startTime"
    }
    
    try:
        response = requests.get(url, params=params)
        print(f"Status Code: {response.status_code}")
        print(f"Response Text: {response.text}")
        
        data = response.json()
        print(f"Full JSON Response: {data}")
        
        # Check if there's an error in the response
        if 'error' in data:
            error_msg = data['error']
            print(f"API Error Found: {error_msg}")
            
            # Check for API_KEY_SERVICE_BLOCKED
            if isinstance(error_msg, dict) and 'details' in error_msg:
                for detail in error_msg.get('details', []):
                    if isinstance(detail, dict) and detail.get('reason') == 'API_KEY_SERVICE_BLOCKED':
                        project_id = detail.get('metadata', {}).get('consumer', 'unknown')
                        return pd.DataFrame({
                            "status": ["API_KEY_SERVICE_BLOCKED"],
                            "instruction": [
                                f"Your API key from project {project_id} is blocked from calling Calendar API.\n\n"
                                f"FIX: Go to Google Cloud Console > Project {project_id} > APIs & Services > Credentials\n"
                                f"1. Click on your API key\n"
                                f"2. Under 'API restrictions', add 'Google Calendar API' to the allowed list\n"
                                f"3. Or set API restrictions to 'None' to allow all APIs\n"
                                f"4. Save and retry"
                            ]
                        })
            
            return pd.DataFrame({
                "error": [str(error_msg)],
                "status_code": [response.status_code],
                "details": [str(data)]
            })
        
        # Check if response raised an HTTP error
        if response.status_code >= 400:
            print(f"HTTP Error {response.status_code}")
            return pd.DataFrame({
                "error": [f"HTTP {response.status_code}"],
                "response": [response.text]
            })
        
        events = data.get('items', [])
        print(f"Number of events found: {len(events)}")
        
        if not events:
            return pd.DataFrame({"message": [f"No events scheduled for today ({today})"], "calendar_id": [calendar_id]})
        
        event_list = []
        for event in events:
            event_list.append({
                'title': event.get('summary', 'No title'),
                'start': event.get('start', {}).get('dateTime', event.get('start', {}).get('date')),
                'end': event.get('end', {}).get('dateTime', event.get('end', {}).get('date')),
                'description': event.get('description', ''),
                'location': event.get('location', '')
            })
        
        return pd.DataFrame(event_list)
        
    except Exception as e:
        print(f"Exception occurred: {type(e).__name__}: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return pd.DataFrame({
            "error": [f"{type(e).__name__}: {str(e)}"],
            "calendar_id": [calendar_id],
            "url": [url]
        })