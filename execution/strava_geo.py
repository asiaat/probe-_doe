#!/usr/bin/env python3
"""
Strava Geo Intelligence Tool

Searches Strava segments and leaderboards by geographic location.
Useful for OSINT investigations to find athletes active in a specific area.

API Reference: https://developers.strava.com/docs/reference/
"""

import os
import sys
import json
import logging
import requests
import argparse
from datetime import datetime
from typing import Optional

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

STRAVA_API_BASE = "https://www.strava.com/api/v3"

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

def refresh_access_token(client_id: str, client_secret: str, refresh_token: str) -> Optional[str]:
    """Refresh the Strava access token using the refresh token."""
    url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token"
    }

    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
        data = response.json()
        new_access_token = data.get("access_token")
        new_refresh_token = data.get("refresh_token")

        # Update .env file with new tokens
        if new_access_token and new_refresh_token:
            update_env_tokens(new_access_token, new_refresh_token)
            logging.info("Refreshed Strava access token successfully")
            return new_access_token
    except Exception as e:
        logging.error(f"Failed to refresh token: {e}")
        return None

def update_env_tokens(access_token: str, refresh_token: str):
    """Update .env file with new tokens."""
    if not os.path.exists(".env"):
        return

    with open(".env", "r") as f:
        lines = f.readlines()

    new_lines = []
    for line in lines:
        if line.startswith("STRAVA_ACCESS_TOKEN="):
            new_lines.append(f"STRAVA_ACCESS_TOKEN={access_token}\n")
        elif line.startswith("STRAVA_REFRESH_TOKEN="):
            new_lines.append(f"STRAVA_REFRESH_TOKEN={refresh_token}\n")
        else:
            new_lines.append(line)

    with open(".env", "w") as f:
        f.writelines(new_lines)

def get_auth_header(access_token: str) -> dict:
    """Return authorization header for Strava API."""
    return {"Authorization": f"Bearer {access_token}"}

def explore_segments(access_token: str, bounds: list, activity_type: str = "riding") -> Optional[dict]:
    """
    Explore segments within geographic bounds.

    Args:
        access_token: Strava OAuth access token
        bounds: [south_lat, west_lng, north_lat, east_lng]
        activity_type: 'riding' or 'running'

    Returns:
        Dict with segments found in the area
    """
    url = f"{STRAVA_API_BASE}/segments/explore"
    params = {
        "bounds": ",".join(map(str, bounds)),
        "activity_type": activity_type
    }

    try:
        response = requests.get(url, headers=get_auth_header(access_token), params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 401:
            logging.warning("Access token expired")
            return {"error": "token_expired", "status": 401}
        logging.error(f"Strava API error: {e}")
        return None
    except Exception as e:
        logging.error(f"Error exploring segments: {e}")
        return None

def get_segment_details(access_token: str, segment_id: int) -> Optional[dict]:
    """Get detailed information about a specific segment."""
    url = f"{STRAVA_API_BASE}/segments/{segment_id}"

    try:
        response = requests.get(url, headers=get_auth_header(access_token))
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Error getting segment {segment_id}: {e}")
        return None

def get_segment_leaderboard(access_token: str, segment_id: int,
                            date_range: str = None,
                            per_page: int = 200) -> Optional[dict]:
    """
    Get leaderboard for a segment (athletes who completed it).

    Args:
        access_token: Strava OAuth access token
        segment_id: The segment ID
        date_range: Filter by date range ('this_year', 'this_month', 'this_week', 'today')
        per_page: Number of results (max 200)

    Returns:
        Leaderboard data with athlete efforts
    """
    url = f"{STRAVA_API_BASE}/segments/{segment_id}/leaderboard"
    params = {"per_page": per_page}

    if date_range:
        params["date_range"] = date_range

    try:
        response = requests.get(url, headers=get_auth_header(access_token), params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Error getting leaderboard for segment {segment_id}: {e}")
        return None

def get_segment_efforts(access_token: str, segment_id: int,
                        start_date: str = None, end_date: str = None,
                        per_page: int = 200) -> Optional[dict]:
    """
    Get all efforts on a segment within a date range.

    Args:
        access_token: Strava OAuth access token
        segment_id: The segment ID
        start_date: ISO 8601 date string (e.g., '2026-01-01T00:00:00Z')
        end_date: ISO 8601 date string
        per_page: Number of results per page
    """
    url = f"{STRAVA_API_BASE}/segment_efforts"
    params = {
        "segment_id": segment_id,
        "per_page": per_page
    }

    if start_date:
        params["start_date_local"] = start_date
    if end_date:
        params["end_date_local"] = end_date

    try:
        response = requests.get(url, headers=get_auth_header(access_token), params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Error getting efforts for segment {segment_id}: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Strava Geo Intelligence Tool")
    parser.add_argument("--action", required=True,
                        choices=["explore", "segment", "leaderboard", "efforts"],
                        help="Action to perform")
    parser.add_argument("--bounds", type=str,
                        help="Geographic bounds: south_lat,west_lng,north_lat,east_lng")
    parser.add_argument("--segment-id", type=int, help="Segment ID for detail queries")
    parser.add_argument("--activity-type", default="riding",
                        choices=["riding", "running"], help="Activity type for segment search")
    parser.add_argument("--date-range", choices=["this_year", "this_month", "this_week", "today"],
                        help="Date range filter for leaderboard")
    parser.add_argument("--start-date", help="Start date (ISO 8601) for efforts query")
    parser.add_argument("--end-date", help="End date (ISO 8601) for efforts query")
    parser.add_argument("--output-dir", default=".tmp", help="Output directory")

    args = parser.parse_args()

    # Load credentials
    env = load_env()
    access_token = env.get("STRAVA_ACCESS_TOKEN")
    client_id = env.get("STRAVA_ID")
    client_secret = env.get("STRAVA_SECRET")
    refresh_token = env.get("STRAVA_REFRESH_TOKEN")

    if not access_token:
        print("Error: STRAVA_ACCESS_TOKEN not found in .env")
        log_execution("strava_geo.py", {"action": args.action}, "FAILURE", "Missing access token")
        sys.exit(1)

    result = None
    params_log = {"action": args.action}

    if args.action == "explore":
        if not args.bounds:
            print("Error: --bounds required for explore action")
            sys.exit(1)
        bounds = [float(x) for x in args.bounds.split(",")]
        params_log["bounds"] = args.bounds
        params_log["activity_type"] = args.activity_type

        result = explore_segments(access_token, bounds, args.activity_type)

        # Handle token expiration
        if result and result.get("error") == "token_expired":
            print("Access token expired, attempting refresh...")
            new_token = refresh_access_token(client_id, client_secret, refresh_token)
            if new_token:
                result = explore_segments(new_token, bounds, args.activity_type)
            else:
                print("Failed to refresh token. Please re-authenticate.")
                sys.exit(1)

    elif args.action == "segment":
        if not args.segment_id:
            print("Error: --segment-id required for segment action")
            sys.exit(1)
        params_log["segment_id"] = args.segment_id
        result = get_segment_details(access_token, args.segment_id)

    elif args.action == "leaderboard":
        if not args.segment_id:
            print("Error: --segment-id required for leaderboard action")
            sys.exit(1)
        params_log["segment_id"] = args.segment_id
        params_log["date_range"] = args.date_range
        result = get_segment_leaderboard(access_token, args.segment_id, args.date_range)

    elif args.action == "efforts":
        if not args.segment_id:
            print("Error: --segment-id required for efforts action")
            sys.exit(1)
        params_log["segment_id"] = args.segment_id
        params_log["start_date"] = args.start_date
        params_log["end_date"] = args.end_date
        result = get_segment_efforts(access_token, args.segment_id, args.start_date, args.end_date)

    if result:
        # Save output
        os.makedirs(args.output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{args.output_dir}/strava_{args.action}_{timestamp}.json"

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        print(f"Results saved to {output_file}")

        # Print summary
        if args.action == "explore" and "segments" in result:
            segments = result["segments"]
            print(f"\nFound {len(segments)} segments in area:")
            for seg in segments[:10]:
                print(f"  - {seg.get('name')} (ID: {seg.get('id')}, {seg.get('distance', 0):.0f}m)")
        elif args.action == "leaderboard" and "entries" in result:
            entries = result["entries"]
            print(f"\nLeaderboard has {len(entries)} entries")

        log_execution("strava_geo.py", params_log, "SUCCESS")
    else:
        print("No results or error occurred")
        log_execution("strava_geo.py", params_log, "FAILURE", "No results")
        sys.exit(1)

if __name__ == "__main__":
    main()
