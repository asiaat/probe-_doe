#!/usr/bin/env python3
"""
OpenStreetMap POI and Infrastructure Analysis Tool

Queries OSM Overpass API for points of interest, infrastructure,
and geographic features in a specified area.

Useful for OSINT terrain analysis and understanding access/egress routes.
"""

import os
import sys
import json
import logging
import requests
import argparse
from datetime import datetime
from typing import Optional, List, Dict

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

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

def log_execution(tool_name, params, status, error=None):
    logging.info(f"Tool: {tool_name} | Params: {params} | Status: {status} | Error: {error}")

def query_overpass(query: str) -> Optional[dict]:
    """
    Execute an Overpass QL query.

    Args:
        query: Overpass QL query string

    Returns:
        JSON response from Overpass API
    """
    headers = {
        "User-Agent": "OSINT-Probe/1.0",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    try:
        logging.info(f"Executing Overpass query: {query[:100]}...")
        response = requests.post(OVERPASS_URL, data={"data": query}, headers=headers, timeout=60)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Overpass query error: {e}")
        return None

def get_power_infrastructure(bounds: List[float]) -> Optional[dict]:
    """
    Query power infrastructure (lines, substations, transformers).

    Args:
        bounds: [south, west, north, east]
    """
    south, west, north, east = bounds
    query = f"""
    [out:json][timeout:60];
    (
      // Power lines
      way["power"="line"]({south},{west},{north},{east});
      way["power"="cable"]({south},{west},{north},{east});
      way["power"="minor_line"]({south},{west},{north},{east});

      // Substations and transformers
      node["power"="substation"]({south},{west},{north},{east});
      way["power"="substation"]({south},{west},{north},{east});
      node["power"="transformer"]({south},{west},{north},{east});
      node["power"="pole"]({south},{west},{north},{east});
      node["power"="tower"]({south},{west},{north},{east});

      // Power plants
      way["power"="plant"]({south},{west},{north},{east});
      way["power"="generator"]({south},{west},{north},{east});
    );
    out body;
    >;
    out skel qt;
    """
    return query_overpass(query)

def get_water_features(bounds: List[float]) -> Optional[dict]:
    """
    Query water features (canals, rivers, bridges).

    Args:
        bounds: [south, west, north, east]
    """
    south, west, north, east = bounds
    query = f"""
    [out:json][timeout:60];
    (
      // Waterways
      way["waterway"="canal"]({south},{west},{north},{east});
      way["waterway"="river"]({south},{west},{north},{east});
      way["waterway"="stream"]({south},{west},{north},{east});

      // Bridges
      way["bridge"="yes"]({south},{west},{north},{east});
      node["bridge"="yes"]({south},{west},{north},{east});
      way["man_made"="bridge"]({south},{west},{north},{east});

      // Locks and dams
      node["waterway"="lock_gate"]({south},{west},{north},{east});
      way["waterway"="dam"]({south},{west},{north},{east});
    );
    out body;
    >;
    out skel qt;
    """
    return query_overpass(query)

def get_access_routes(bounds: List[float]) -> Optional[dict]:
    """
    Query access routes (paths, roads, railways).

    Args:
        bounds: [south, west, north, east]
    """
    south, west, north, east = bounds
    query = f"""
    [out:json][timeout:60];
    (
      // Paths and footways
      way["highway"="footway"]({south},{west},{north},{east});
      way["highway"="path"]({south},{west},{north},{east});
      way["highway"="cycleway"]({south},{west},{north},{east});
      way["highway"="track"]({south},{west},{north},{east});

      // Roads
      way["highway"="residential"]({south},{west},{north},{east});
      way["highway"="unclassified"]({south},{west},{north},{east});
      way["highway"="service"]({south},{west},{north},{east});

      // Railways
      way["railway"="rail"]({south},{west},{north},{east});
      way["railway"="light_rail"]({south},{west},{north},{east});
      node["railway"="station"]({south},{west},{north},{east});
      node["railway"="halt"]({south},{west},{north},{east});

      // Barriers and access points
      node["barrier"="gate"]({south},{west},{north},{east});
      node["barrier"="fence"]({south},{west},{north},{east});
      node["entrance"="yes"]({south},{west},{north},{east});
    );
    out body;
    >;
    out skel qt;
    """
    return query_overpass(query)

def get_buildings_and_cover(bounds: List[float]) -> Optional[dict]:
    """
    Query buildings and potential cover/concealment.

    Args:
        bounds: [south, west, north, east]
    """
    south, west, north, east = bounds
    query = f"""
    [out:json][timeout:60];
    (
      // Industrial buildings
      way["building"="industrial"]({south},{west},{north},{east});
      way["building"="warehouse"]({south},{west},{north},{east});
      way["landuse"="industrial"]({south},{west},{north},{east});

      // Vegetation and natural cover
      way["natural"="wood"]({south},{west},{north},{east});
      way["natural"="scrub"]({south},{west},{north},{east});
      way["landuse"="forest"]({south},{west},{north},{east});

      // Fences and walls
      way["barrier"="fence"]({south},{west},{north},{east});
      way["barrier"="wall"]({south},{west},{north},{east});

      // Parking (staging areas)
      way["amenity"="parking"]({south},{west},{north},{east});
      node["amenity"="parking"]({south},{west},{north},{east});
    );
    out body;
    >;
    out skel qt;
    """
    return query_overpass(query)

def get_surveillance_and_security(bounds: List[float]) -> Optional[dict]:
    """
    Query surveillance cameras and security features.

    Args:
        bounds: [south, west, north, east]
    """
    south, west, north, east = bounds
    query = f"""
    [out:json][timeout:60];
    (
      // Surveillance cameras
      node["man_made"="surveillance"]({south},{west},{north},{east});
      node["surveillance"="camera"]({south},{west},{north},{east});

      // Lighting
      node["highway"="street_lamp"]({south},{west},{north},{east});

      // Police/emergency
      node["amenity"="police"]({south},{west},{north},{east});
      node["amenity"="fire_station"]({south},{west},{north},{east});

      // Security fencing
      way["fence_type"="security"]({south},{west},{north},{east});
    );
    out body;
    >;
    out skel qt;
    """
    return query_overpass(query)

def get_all_infrastructure(bounds: List[float]) -> Optional[dict]:
    """
    Comprehensive query for all relevant infrastructure.

    Args:
        bounds: [south, west, north, east]
    """
    south, west, north, east = bounds
    query = f"""
    [out:json][timeout:120];
    (
      // Power infrastructure
      way["power"]({south},{west},{north},{east});
      node["power"]({south},{west},{north},{east});

      // Waterways and bridges
      way["waterway"]({south},{west},{north},{east});
      way["bridge"="yes"]({south},{west},{north},{east});
      way["man_made"="bridge"]({south},{west},{north},{east});

      // Man-made structures
      way["man_made"="pipeline"]({south},{west},{north},{east});
      node["man_made"="tower"]({south},{west},{north},{east});
      node["man_made"="mast"]({south},{west},{north},{east});

      // Access routes
      way["highway"]({south},{west},{north},{east});

      // Buildings in industrial/utility areas
      way["building"]["building"!="residential"]["building"!="apartments"]["building"!="house"]({south},{west},{north},{east});

      // Surveillance
      node["man_made"="surveillance"]({south},{west},{north},{east});
    );
    out body;
    >;
    out skel qt;
    """
    return query_overpass(query)

def get_named_features(bounds: List[float]) -> Optional[dict]:
    """
    Query all named features in the area.

    Args:
        bounds: [south, west, north, east]
    """
    south, west, north, east = bounds
    query = f"""
    [out:json][timeout:60];
    (
      node["name"]({south},{west},{north},{east});
      way["name"]({south},{west},{north},{east});
    );
    out body;
    >;
    out skel qt;
    """
    return query_overpass(query)

def analyze_results(data: dict) -> dict:
    """
    Analyze Overpass API results and categorize features.

    Args:
        data: Raw Overpass API response

    Returns:
        Categorized analysis
    """
    analysis = {
        "total_elements": len(data.get("elements", [])),
        "nodes": 0,
        "ways": 0,
        "relations": 0,
        "categories": {},
        "named_features": [],
        "key_locations": []
    }

    elements = data.get("elements", [])

    for elem in elements:
        elem_type = elem.get("type")
        tags = elem.get("tags", {})

        if elem_type == "node":
            analysis["nodes"] += 1
        elif elem_type == "way":
            analysis["ways"] += 1
        elif elem_type == "relation":
            analysis["relations"] += 1

        # Categorize by primary tag
        for key in ["power", "waterway", "highway", "building", "man_made", "amenity", "barrier"]:
            if key in tags:
                cat = f"{key}:{tags[key]}"
                if cat not in analysis["categories"]:
                    analysis["categories"][cat] = 0
                analysis["categories"][cat] += 1

        # Extract named features
        if "name" in tags:
            feature = {
                "name": tags["name"],
                "type": elem_type,
                "tags": {k: v for k, v in tags.items() if k != "name"}
            }
            if elem_type == "node":
                feature["lat"] = elem.get("lat")
                feature["lon"] = elem.get("lon")
            analysis["named_features"].append(feature)

        # Identify key locations (power-related)
        if tags.get("power") in ["substation", "plant", "transformer"]:
            loc = {
                "type": tags.get("power"),
                "name": tags.get("name", "Unnamed"),
                "operator": tags.get("operator"),
                "voltage": tags.get("voltage")
            }
            if elem_type == "node":
                loc["lat"] = elem.get("lat")
                loc["lon"] = elem.get("lon")
            analysis["key_locations"].append(loc)

    # Sort categories by count
    analysis["categories"] = dict(sorted(
        analysis["categories"].items(),
        key=lambda x: x[1],
        reverse=True
    ))

    return analysis

def main():
    parser = argparse.ArgumentParser(description="OSM POI and Infrastructure Analysis")
    parser.add_argument("--action", required=True,
                        choices=["power", "water", "access", "buildings", "surveillance", "all", "named", "custom"],
                        help="Type of infrastructure to query")
    parser.add_argument("--bounds", type=str, required=True,
                        help="Geographic bounds: south,west,north,east")
    parser.add_argument("--query", type=str, help="Custom Overpass QL query (for --action custom)")
    parser.add_argument("--output-dir", default=".tmp", help="Output directory")

    args = parser.parse_args()

    bounds = [float(x) for x in args.bounds.split(",")]
    params_log = {"action": args.action, "bounds": args.bounds}

    result = None

    if args.action == "power":
        result = get_power_infrastructure(bounds)
    elif args.action == "water":
        result = get_water_features(bounds)
    elif args.action == "access":
        result = get_access_routes(bounds)
    elif args.action == "buildings":
        result = get_buildings_and_cover(bounds)
    elif args.action == "surveillance":
        result = get_surveillance_and_security(bounds)
    elif args.action == "all":
        result = get_all_infrastructure(bounds)
    elif args.action == "named":
        result = get_named_features(bounds)
    elif args.action == "custom":
        if not args.query:
            print("Error: --query required for custom action")
            sys.exit(1)
        result = query_overpass(args.query)

    if result:
        # Analyze results
        analysis = analyze_results(result)

        print(f"\n=== OSM Infrastructure Analysis ===")
        print(f"Total elements: {analysis['total_elements']}")
        print(f"  Nodes: {analysis['nodes']}, Ways: {analysis['ways']}, Relations: {analysis['relations']}")

        print(f"\nCategories:")
        for cat, count in list(analysis["categories"].items())[:15]:
            print(f"  {cat}: {count}")

        if analysis["key_locations"]:
            print(f"\nKey Power Locations:")
            for loc in analysis["key_locations"][:10]:
                print(f"  - {loc['type']}: {loc['name']} (Voltage: {loc.get('voltage', 'N/A')})")

        if analysis["named_features"]:
            print(f"\nNamed Features ({len(analysis['named_features'])} total):")
            for feat in analysis["named_features"][:10]:
                coords = f"({feat.get('lat', 'N/A')}, {feat.get('lon', 'N/A')})" if feat.get("lat") else ""
                print(f"  - {feat['name']} {coords}")

        # Save results
        os.makedirs(args.output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save raw data
        raw_file = f"{args.output_dir}/osm_{args.action}_raw_{timestamp}.json"
        with open(raw_file, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        # Save analysis
        analysis_file = f"{args.output_dir}/osm_{args.action}_analysis_{timestamp}.json"
        with open(analysis_file, "w", encoding="utf-8") as f:
            json.dump(analysis, f, indent=2, ensure_ascii=False)

        print(f"\nRaw data: {raw_file}")
        print(f"Analysis: {analysis_file}")

        log_execution("osm_poi_analysis.py", params_log, "SUCCESS")
    else:
        print("Query failed - check logs")
        log_execution("osm_poi_analysis.py", params_log, "FAILURE", "Query failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
