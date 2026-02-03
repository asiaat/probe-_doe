#!/usr/bin/env python3
"""
Sports Tracker Geo Intelligence Tool

Searches Sports Tracker for workouts and routes by geographic location.
Uses the undocumented Sports Tracker API.

API Base: https://api.sports-tracker.com/apiserver/v1/
Auth: STTAuthorization header with user token

Note: Sports Tracker doesn't have an official public API.
This tool uses reverse-engineered endpoints.
"""

import os
import sys
import json
import logging
import requests
import argparse
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from urllib.parse import urlencode

# Setup logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
date_str = datetime.now().strftime("%Y%m%d")
log_file = f"{log_dir}/exec_{date_str}.log"

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

API_BASE = "https://api.sports-tracker.com/apiserver/v1"
WEB_BASE = "https://www.sports-tracker.com"

def log_execution(tool_name, params, status, error=None):
    logging.info(f"Tool: {tool_name} | Params: {params} | Status: {status} | Error: {error}")

def load_env():
    """Load environment variables from .env file."""
    env_vars = {}
    if os.path.exists(".env"):
        with open(".env", "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    if "=" in line:
                        key, value = line.split("=", 1)
                        env_vars[key] = value
    return env_vars

def get_auth_header(token: str) -> dict:
    """Return authorization header for Sports Tracker API."""
    return {
        "STTAuthorization": token,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Origin": WEB_BASE,
        "Referer": f"{WEB_BASE}/"
    }

def authenticate(username: str, password: str) -> Optional[str]:
    """
    Authenticate with Sports Tracker and get session token.

    Returns:
        Session token string or None if authentication fails
    """
    url = f"{API_BASE}/login"

    payload = {
        "l": username,
        "p": password
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": WEB_BASE
    }

    try:
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status()
        data = response.json()

        if data.get("sessionkey"):
            logging.info("Sports Tracker authentication successful")
            return data["sessionkey"]
        else:
            logging.error(f"Auth failed: {data}")
            return None
    except Exception as e:
        logging.error(f"Authentication error: {e}")
        return None

def get_workouts(token: str, limit: int = 100, offset: int = 0) -> Optional[dict]:
    """
    Get user's workouts.

    Args:
        token: STTAuthorization token
        limit: Number of workouts to retrieve
        offset: Pagination offset
    """
    url = f"{API_BASE}/workouts"
    params = {
        "limit": limit,
        "offset": offset
    }

    try:
        response = requests.get(url, headers=get_auth_header(token), params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Error fetching workouts: {e}")
        return None

def get_workout_details(token: str, workout_key: str) -> Optional[dict]:
    """
    Get detailed workout data including GPS track.

    Args:
        token: STTAuthorization token
        workout_key: Workout identifier key
    """
    url = f"{API_BASE}/workouts/{workout_key}"

    try:
        response = requests.get(url, headers=get_auth_header(token))
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Error fetching workout {workout_key}: {e}")
        return None

def get_workout_gpx(token: str, workout_key: str) -> Optional[str]:
    """
    Get workout as GPX data.

    Args:
        token: STTAuthorization token
        workout_key: Workout identifier key
    """
    url = f"{API_BASE}/workouts/{workout_key}/exportGpx"

    try:
        response = requests.get(url, headers=get_auth_header(token))
        response.raise_for_status()
        return response.text
    except Exception as e:
        logging.error(f"Error exporting GPX for {workout_key}: {e}")
        return None

def explore_routes(token: str, bounds: List[float], activity_type: int = 0) -> Optional[dict]:
    """
    Explore public routes/heatmap data in geographic bounds.

    Args:
        token: STTAuthorization token
        bounds: [south_lat, west_lng, north_lat, east_lng]
        activity_type: 0=all, 1=running, 2=cycling, etc.

    Note: This endpoint may not exist or may require premium access.
    """
    # Try several potential endpoints
    endpoints = [
        f"{API_BASE}/routes/explore",
        f"{API_BASE}/routes/search",
        f"{API_BASE}/heatmap",
        f"{API_BASE}/explore/routes"
    ]

    params = {
        "swLat": bounds[0],
        "swLng": bounds[1],
        "neLat": bounds[2],
        "neLng": bounds[3],
        "activityType": activity_type
    }

    for endpoint in endpoints:
        try:
            response = requests.get(endpoint, headers=get_auth_header(token), params=params)
            if response.status_code == 200:
                logging.info(f"Found working endpoint: {endpoint}")
                return {"endpoint": endpoint, "data": response.json()}
            else:
                logging.debug(f"Endpoint {endpoint} returned {response.status_code}")
        except Exception as e:
            logging.debug(f"Endpoint {endpoint} failed: {e}")

    return None

def get_public_workout(workout_key: str) -> Optional[dict]:
    """
    Attempt to fetch a public workout without authentication.

    Args:
        workout_key: Public workout identifier
    """
    # Public workouts may be accessible via web scraping
    url = f"{WEB_BASE}/workout/{workout_key}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/json"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # Try to extract JSON data from the page
        content = response.text

        # Look for embedded workout data
        if "workoutData" in content or "payload" in content:
            # Extract JSON from script tags
            import re
            json_match = re.search(r'window\.__INITIAL_STATE__\s*=\s*({.*?});', content, re.DOTALL)
            if json_match:
                return json.loads(json_match.group(1))

        return {"status": "page_loaded", "url": url, "length": len(content)}
    except Exception as e:
        logging.error(f"Error fetching public workout {workout_key}: {e}")
        return None

def search_public_profiles(query: str, token: str = None) -> Optional[dict]:
    """
    Search for public user profiles.

    Args:
        query: Search query (username, location, etc.)
        token: Optional auth token
    """
    url = f"{API_BASE}/users/search"
    params = {"q": query}

    headers = get_auth_header(token) if token else {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            logging.info(f"User search returned {response.status_code}")
            return {"status": response.status_code}
    except Exception as e:
        logging.error(f"Error searching users: {e}")
        return None

def get_user_profile(user_key: str, token: str = None) -> Optional[dict]:
    """
    Get a user's public profile and workouts.

    Args:
        user_key: User identifier
        token: Optional auth token
    """
    url = f"{API_BASE}/users/{user_key}"

    headers = get_auth_header(token) if token else {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json"
    }

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            return {"status": response.status_code}
    except Exception as e:
        logging.error(f"Error fetching user profile {user_key}: {e}")
        return None

def filter_workouts_by_location(workouts: List[dict], bounds: List[float]) -> List[dict]:
    """
    Filter workouts by geographic bounds.

    Args:
        workouts: List of workout objects
        bounds: [south_lat, west_lng, north_lat, east_lng]
    """
    filtered = []
    south, west, north, east = bounds

    for workout in workouts:
        # Check if workout has location data
        start_lat = workout.get("startLatitude") or workout.get("latitude")
        start_lng = workout.get("startLongitude") or workout.get("longitude")

        if start_lat and start_lng:
            if south <= start_lat <= north and west <= start_lng <= east:
                filtered.append(workout)

    return filtered

def filter_workouts_by_date(workouts: List[dict], start_date: str, end_date: str) -> List[dict]:
    """
    Filter workouts by date range.

    Args:
        workouts: List of workout objects
        start_date: ISO date string (YYYY-MM-DD)
        end_date: ISO date string (YYYY-MM-DD)
    """
    filtered = []
    start_ts = datetime.fromisoformat(start_date).timestamp() * 1000  # milliseconds
    end_ts = datetime.fromisoformat(end_date).timestamp() * 1000

    for workout in workouts:
        workout_ts = workout.get("startTime") or workout.get("created")
        if workout_ts and start_ts <= workout_ts <= end_ts:
            filtered.append(workout)

    return filtered

def main():
    parser = argparse.ArgumentParser(description="Sports Tracker Geo Intelligence Tool")
    parser.add_argument("--action", required=True,
                        choices=["auth", "workouts", "workout", "explore", "gpx", "user-search", "user-profile"],
                        help="Action to perform")
    parser.add_argument("--username", help="Sports Tracker username/email")
    parser.add_argument("--password", help="Sports Tracker password")
    parser.add_argument("--token", help="STTAuthorization token (alternative to username/password)")
    parser.add_argument("--bounds", type=str,
                        help="Geographic bounds: south_lat,west_lng,north_lat,east_lng")
    parser.add_argument("--workout-key", help="Workout key for detail/export")
    parser.add_argument("--user-key", help="User key for profile lookup")
    parser.add_argument("--query", help="Search query")
    parser.add_argument("--start-date", help="Start date filter (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date filter (YYYY-MM-DD)")
    parser.add_argument("--limit", type=int, default=100, help="Number of results")
    parser.add_argument("--output-dir", default=".tmp", help="Output directory")

    args = parser.parse_args()

    # Load credentials from env if not provided
    env = load_env()
    token = args.token or env.get("SPORTS_TRACKER_TOKEN")
    username = args.username or env.get("SPORTS_TRACKER_USER")
    password = args.password or env.get("SPORTS_TRACKER_PASS")

    result = None
    params_log = {"action": args.action}

    if args.action == "auth":
        if not username or not password:
            print("Error: --username and --password required for auth")
            sys.exit(1)

        token = authenticate(username, password)
        if token:
            print(f"Authentication successful!")
            print(f"Token: {token}")
            print("\nAdd to .env file:")
            print(f"SPORTS_TRACKER_TOKEN={token}")
            result = {"token": token, "status": "success"}
            log_execution("sports_tracker_geo.py", params_log, "SUCCESS")
        else:
            print("Authentication failed")
            log_execution("sports_tracker_geo.py", params_log, "FAILURE", "Auth failed")
            sys.exit(1)

    elif args.action == "workouts":
        if not token:
            print("Error: --token or SPORTS_TRACKER_TOKEN required")
            sys.exit(1)

        result = get_workouts(token, limit=args.limit)

        if result and "payload" in result:
            workouts = result["payload"]

            # Apply filters if specified
            if args.bounds:
                bounds = [float(x) for x in args.bounds.split(",")]
                workouts = filter_workouts_by_location(workouts, bounds)
                params_log["bounds"] = args.bounds

            if args.start_date and args.end_date:
                workouts = filter_workouts_by_date(workouts, args.start_date, args.end_date)
                params_log["date_range"] = f"{args.start_date} to {args.end_date}"

            result["payload"] = workouts
            print(f"Found {len(workouts)} workouts")

            for w in workouts[:10]:
                name = w.get("workoutName", w.get("workoutKey", "Unknown"))
                dist = w.get("totalDistance", 0) / 1000
                print(f"  - {name} ({dist:.1f} km)")

    elif args.action == "workout":
        if not token:
            print("Error: --token required")
            sys.exit(1)
        if not args.workout_key:
            print("Error: --workout-key required")
            sys.exit(1)

        params_log["workout_key"] = args.workout_key
        result = get_workout_details(token, args.workout_key)

    elif args.action == "gpx":
        if not token:
            print("Error: --token required")
            sys.exit(1)
        if not args.workout_key:
            print("Error: --workout-key required")
            sys.exit(1)

        params_log["workout_key"] = args.workout_key
        gpx_data = get_workout_gpx(token, args.workout_key)

        if gpx_data:
            os.makedirs(args.output_dir, exist_ok=True)
            output_file = f"{args.output_dir}/workout_{args.workout_key}.gpx"
            with open(output_file, "w") as f:
                f.write(gpx_data)
            print(f"GPX saved to {output_file}")
            result = {"gpx_file": output_file, "status": "success"}
            log_execution("sports_tracker_geo.py", params_log, "SUCCESS")
        else:
            print("Failed to export GPX")
            log_execution("sports_tracker_geo.py", params_log, "FAILURE")
        sys.exit(0)

    elif args.action == "explore":
        if not args.bounds:
            print("Error: --bounds required for explore action")
            sys.exit(1)

        bounds = [float(x) for x in args.bounds.split(",")]
        params_log["bounds"] = args.bounds

        result = explore_routes(token, bounds)

        if result:
            print(f"Explore result from: {result.get('endpoint', 'unknown')}")
        else:
            print("No public explore endpoints found. Sports Tracker may not expose this data.")
            print("Alternative: Use authenticated workouts with location filter.")
            result = {"status": "no_public_endpoint", "note": "Try using workouts action with --bounds filter"}

    elif args.action == "user-search":
        if not args.query:
            print("Error: --query required for user-search")
            sys.exit(1)

        params_log["query"] = args.query
        result = search_public_profiles(args.query, token)

        if result:
            print(f"Search result: {json.dumps(result, indent=2)[:500]}")

    elif args.action == "user-profile":
        if not args.user_key:
            print("Error: --user-key required")
            sys.exit(1)

        params_log["user_key"] = args.user_key
        result = get_user_profile(args.user_key, token)

    # Save results
    if result:
        os.makedirs(args.output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{args.output_dir}/sports_tracker_{args.action}_{timestamp}.json"

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        print(f"Results saved to {output_file}")
        log_execution("sports_tracker_geo.py", params_log, "SUCCESS")
    else:
        log_execution("sports_tracker_geo.py", params_log, "FAILURE", "No results")

if __name__ == "__main__":
    main()
