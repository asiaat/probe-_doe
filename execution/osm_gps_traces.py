#!/usr/bin/env python3
"""
OpenStreetMap GPS Traces Tool

Fetches public GPS traces from OpenStreetMap for a geographic area.
Useful for OSINT to identify activity patterns in specific locations.

API: https://api.openstreetmap.org/api/0.6/trackpoints
"""

import os
import sys
import json
import logging
import requests
import argparse
from datetime import datetime
from typing import Optional, List
import xml.etree.ElementTree as ET

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

OSM_API_BASE = "https://api.openstreetmap.org/api/0.6"

def log_execution(tool_name, params, status, error=None):
    logging.info(f"Tool: {tool_name} | Params: {params} | Status: {status} | Error: {error}")

def get_trackpoints(bounds: List[float], page: int = 0) -> Optional[dict]:
    """
    Fetch GPS trackpoints from OSM within a bounding box.

    Args:
        bounds: [west, south, east, north] (lon, lat, lon, lat)
        page: Page number for pagination

    Returns:
        Dict with trackpoints data
    """
    # OSM API expects: left,bottom,right,top (west,south,east,north)
    bbox = f"{bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]}"
    url = f"{OSM_API_BASE}/trackpoints"

    params = {
        "bbox": bbox,
        "page": page
    }

    headers = {
        "User-Agent": "OSINT-Probe/1.0 (https://github.com/probe-doe)"
    }

    try:
        logging.info(f"Fetching OSM trackpoints for bbox: {bbox}, page: {page}")
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()

        # Parse GPX XML
        return parse_gpx_response(response.text, bbox)
    except Exception as e:
        logging.error(f"Error fetching trackpoints: {e}")
        return None

def parse_gpx_response(gpx_xml: str, bbox: str) -> dict:
    """
    Parse GPX XML response from OSM.

    Args:
        gpx_xml: Raw GPX XML string
        bbox: Bounding box string for reference

    Returns:
        Dict with parsed trackpoint data
    """
    result = {
        "bbox": bbox,
        "tracks": [],
        "total_points": 0,
        "raw_gpx": gpx_xml[:5000] if len(gpx_xml) > 5000 else gpx_xml  # Truncate for storage
    }

    try:
        # Parse XML
        root = ET.fromstring(gpx_xml)

        # Handle GPX namespace
        ns = {"gpx": "http://www.topografix.com/GPX/1/0"}

        # Find all track segments
        for trk in root.findall(".//gpx:trk", ns) or root.findall(".//trk"):
            track = {
                "name": None,
                "points": []
            }

            name_elem = trk.find("gpx:name", ns) or trk.find("name")
            if name_elem is not None:
                track["name"] = name_elem.text

            for trkseg in trk.findall(".//gpx:trkseg", ns) or trk.findall(".//trkseg"):
                for trkpt in trkseg.findall("gpx:trkpt", ns) or trkseg.findall("trkpt"):
                    point = {
                        "lat": float(trkpt.get("lat")),
                        "lon": float(trkpt.get("lon"))
                    }

                    # Get timestamp if available
                    time_elem = trkpt.find("gpx:time", ns) or trkpt.find("time")
                    if time_elem is not None:
                        point["time"] = time_elem.text

                    track["points"].append(point)
                    result["total_points"] += 1

            if track["points"]:
                result["tracks"].append(track)

        logging.info(f"Parsed {len(result['tracks'])} tracks with {result['total_points']} points")

    except ET.ParseError as e:
        logging.error(f"XML parse error: {e}")
        result["parse_error"] = str(e)

    return result

def get_public_traces_list(bounds: List[float] = None) -> Optional[dict]:
    """
    Get list of public GPS traces (metadata only).
    Note: This scrapes the public traces page as there's no direct API.

    Args:
        bounds: Optional [west, south, east, north] bounds
    """
    url = "https://www.openstreetmap.org/traces"

    params = {}
    if bounds:
        params["bbox"] = f"{bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]}"

    headers = {
        "User-Agent": "OSINT-Probe/1.0",
        "Accept": "text/html"
    }

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()

        # Parse HTML to extract trace info
        traces = []
        content = response.text

        # Simple extraction of trace IDs from HTML
        import re
        trace_matches = re.findall(r'/user/([^/]+)/traces/(\d+)', content)

        for username, trace_id in trace_matches:
            traces.append({
                "username": username,
                "trace_id": trace_id,
                "url": f"https://www.openstreetmap.org/user/{username}/traces/{trace_id}"
            })

        return {
            "traces": traces,
            "count": len(traces)
        }
    except Exception as e:
        logging.error(f"Error fetching traces list: {e}")
        return None

def get_trace_gpx(username: str, trace_id: str) -> Optional[str]:
    """
    Download a specific GPX trace.

    Args:
        username: OSM username
        trace_id: Trace ID number

    Returns:
        GPX XML string
    """
    url = f"https://www.openstreetmap.org/trace/{trace_id}/data"

    headers = {
        "User-Agent": "OSINT-Probe/1.0"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.text
    except Exception as e:
        logging.error(f"Error downloading trace {trace_id}: {e}")
        return None

def analyze_trackpoints(data: dict) -> dict:
    """
    Analyze trackpoints for patterns.

    Args:
        data: Parsed trackpoint data

    Returns:
        Analysis results
    """
    analysis = {
        "total_tracks": len(data.get("tracks", [])),
        "total_points": data.get("total_points", 0),
        "time_range": None,
        "unique_dates": set(),
        "points_by_date": {}
    }

    for track in data.get("tracks", []):
        for point in track.get("points", []):
            if "time" in point:
                try:
                    dt = datetime.fromisoformat(point["time"].replace("Z", "+00:00"))
                    date_str = dt.strftime("%Y-%m-%d")
                    analysis["unique_dates"].add(date_str)

                    if date_str not in analysis["points_by_date"]:
                        analysis["points_by_date"][date_str] = 0
                    analysis["points_by_date"][date_str] += 1
                except:
                    pass

    analysis["unique_dates"] = sorted(list(analysis["unique_dates"]))

    if analysis["unique_dates"]:
        analysis["time_range"] = {
            "earliest": analysis["unique_dates"][0],
            "latest": analysis["unique_dates"][-1]
        }

    return analysis

def main():
    parser = argparse.ArgumentParser(description="OpenStreetMap GPS Traces Tool")
    parser.add_argument("--action", required=True,
                        choices=["trackpoints", "traces-list", "download-trace", "analyze"],
                        help="Action to perform")
    parser.add_argument("--bounds", type=str,
                        help="Geographic bounds: west,south,east,north (lon,lat,lon,lat)")
    parser.add_argument("--page", type=int, default=0, help="Page number for pagination")
    parser.add_argument("--username", help="OSM username for trace download")
    parser.add_argument("--trace-id", help="Trace ID for download")
    parser.add_argument("--input-file", help="Input JSON file for analysis")
    parser.add_argument("--output-dir", default=".tmp", help="Output directory")

    args = parser.parse_args()

    result = None
    params_log = {"action": args.action}

    if args.action == "trackpoints":
        if not args.bounds:
            print("Error: --bounds required (format: west,south,east,north)")
            print("Example: --bounds 13.27,52.41,13.35,52.46")
            sys.exit(1)

        bounds = [float(x) for x in args.bounds.split(",")]
        params_log["bounds"] = args.bounds
        params_log["page"] = args.page

        result = get_trackpoints(bounds, args.page)

        if result:
            print(f"Fetched {result['total_points']} trackpoints in {len(result['tracks'])} tracks")

            # Show sample points
            for track in result["tracks"][:5]:
                name = track.get("name", "Unnamed")
                pts = len(track["points"])
                print(f"  - Track: {name} ({pts} points)")

    elif args.action == "traces-list":
        bounds = None
        if args.bounds:
            bounds = [float(x) for x in args.bounds.split(",")]
            params_log["bounds"] = args.bounds

        result = get_public_traces_list(bounds)

        if result:
            print(f"Found {result['count']} public traces")
            for trace in result["traces"][:10]:
                print(f"  - {trace['username']}: {trace['url']}")

    elif args.action == "download-trace":
        if not args.username or not args.trace_id:
            print("Error: --username and --trace-id required")
            sys.exit(1)

        params_log["username"] = args.username
        params_log["trace_id"] = args.trace_id

        gpx_data = get_trace_gpx(args.username, args.trace_id)

        if gpx_data:
            os.makedirs(args.output_dir, exist_ok=True)
            output_file = f"{args.output_dir}/osm_trace_{args.trace_id}.gpx"
            with open(output_file, "w") as f:
                f.write(gpx_data)
            print(f"GPX saved to {output_file}")
            result = {"gpx_file": output_file, "status": "success"}
        else:
            print("Failed to download trace")
            sys.exit(1)

    elif args.action == "analyze":
        if not args.input_file:
            print("Error: --input-file required for analysis")
            sys.exit(1)

        with open(args.input_file, "r") as f:
            data = json.load(f)

        result = analyze_trackpoints(data)

        print(f"Analysis:")
        print(f"  Total tracks: {result['total_tracks']}")
        print(f"  Total points: {result['total_points']}")
        if result['time_range']:
            print(f"  Time range: {result['time_range']['earliest']} to {result['time_range']['latest']}")
        print(f"  Unique dates: {len(result['unique_dates'])}")

    # Save results
    if result:
        os.makedirs(args.output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{args.output_dir}/osm_{args.action}_{timestamp}.json"

        # Convert sets to lists for JSON serialization
        if isinstance(result.get("unique_dates"), set):
            result["unique_dates"] = list(result["unique_dates"])

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False, default=str)

        print(f"Results saved to {output_file}")
        log_execution("osm_gps_traces.py", params_log, "SUCCESS")
    else:
        log_execution("osm_gps_traces.py", params_log, "FAILURE", "No results")

if __name__ == "__main__":
    main()
