#!/usr/bin/env python3
"""
Deep OSINT: Construction Companies & Social Media Intelligence
Investigates construction work around Lichterfelde power plant (Nov 2025 - Jan 2026)
"""

import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# API Keys
SERP_API_KEY = os.getenv('SERP_API_KEY')
BRAVE_API_KEY = os.getenv('BRAVE_API_KEY')
FIRECRAWL_API_KEY = os.getenv('FIRECRAWL_API_KEY')

# Output directory
OUTPUT_DIR = "reports/20260213_142123_berlin_grid_attack_eg_volt/osint_construction"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def serp_search(query, location="Berlin, Germany", language="de"):
    """Search using SERP API"""
    url = "https://serpapi.com/search"
    params = {
        "q": query,
        "location": location,
        "hl": language,
        "gl": "de",
        "api_key": SERP_API_KEY,
        "num": 20
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"SERP API error: {e}")
        return None

def brave_search(query, count=20):
    """Search using Brave Search API"""
    url = "https://api.search.brave.com/res/v1/web/search"
    headers = {
        "Accept": "application/json",
        "X-Subscription-Token": BRAVE_API_KEY
    }
    params = {
        "q": query,
        "count": count,
        "country": "DE",
        "search_lang": "de"
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Brave API error: {e}")
        return None

def save_results(filename, data):
    """Save results to JSON file"""
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✓ Saved: {filepath}")

def main():
    print("=" * 80)
    print("DEEP OSINT: Construction Companies & Social Media Intelligence")
    print("Target: Lichterfelde Power Plant Area (Nov 2025 - Jan 2026)")
    print("=" * 80)
    
    # 1. Construction Companies - Stromnetz Berlin
    print("\n[1/8] Searching: Stromnetz Berlin contractors...")
    results_stromnetz = serp_search(
        "Stromnetz Berlin Bauarbeiten Ostpreußendamm Lichterfelde 2025 Auftragnehmer"
    )
    if results_stromnetz:
        save_results("01_stromnetz_contractors.json", results_stromnetz)
    
    # 2. Construction Complaints - Social Media
    print("[2/8] Searching: Construction complaints on social media...")
    results_complaints = brave_search(
        "Lichterfelde Bauarbeiten Lärm Beschwerden November Dezember 2025 Twitter Facebook"
    )
    if results_complaints:
        save_results("02_construction_complaints.json", results_complaints)
    
    # 3. Vehicle Sightings - Construction Vehicles
    print("[3/8] Searching: Construction vehicle sightings...")
    results_vehicles = serp_search(
        "Lichterfelde Teltowkanal Baufahrzeuge LKW Transporter November Dezember 2025"
    )
    if results_vehicles:
        save_results("03_construction_vehicles.json", results_vehicles)
    
    # 4. Goerzallee Roadwork Companies
    print("[4/8] Searching: Goerzallee roadwork contractors...")
    results_goerzallee = brave_search(
        "Goerzallee Berlin Straßenbauarbeiten Bauunternehmen 2025 2026"
    )
    if results_goerzallee:
        save_results("04_goerzallee_contractors.json", results_goerzallee)
    
    # 5. Hindenburgdamm Heating Work Companies
    print("[5/8] Searching: Hindenburgdamm district heating contractors...")
    results_heating = serp_search(
        "Hindenburgdamm Fernwärme Bauarbeiten Vattenfall BEW 2025 Auftragnehmer"
    )
    if results_heating:
        save_results("05_heating_contractors.json", results_heating)
    
    # 6. Recruitment Platforms - Construction Workers
    print("[6/8] Searching: Construction worker recruitment (Nov-Dec 2025)...")
    results_jobs = brave_search(
        "Bauarbeiter Elektriker Lichterfelde Berlin Stellenangebote November Dezember 2025 Indeed StepStone"
    )
    if results_jobs:
        save_results("06_recruitment_platforms.json", results_jobs)
    
    # 7. Bäkebrücke Reconstruction Companies
    print("[7/8] Searching: Bäkebrücke reconstruction contractors...")
    results_bridge = serp_search(
        "Bäkebrücke Teltowkanal Sanierung Bauunternehmen 2025 Berlin"
    )
    if results_bridge:
        save_results("07_bridge_contractors.json", results_bridge)
    
    # 8. Social Media - Company Accounts
    print("[8/8] Searching: Construction company social media accounts...")
    results_social = brave_search(
        "Stromnetz Berlin Bauunternehmen Lichterfelde Twitter Instagram Facebook LinkedIn 2025"
    )
    if results_social:
        save_results("08_company_social_media.json", results_social)
    
    print("\n" + "=" * 80)
    print("✓ OSINT Collection Complete")
    print(f"✓ Results saved to: {OUTPUT_DIR}/")
    print("=" * 80)
    
    # Generate summary
    summary = {
        "timestamp": datetime.now().isoformat(),
        "investigation": "Construction Companies & Social Media Intelligence",
        "target_area": "Lichterfelde Power Plant, Berlin",
        "time_period": "November 2025 - January 2026",
        "searches_completed": 8,
        "output_directory": OUTPUT_DIR,
        "api_sources": ["SERP API", "Brave Search API"],
        "next_steps": [
            "Analyze contractor company names and addresses",
            "Cross-reference social media complaints with attack timeline",
            "Identify recruitment platforms used by construction companies",
            "Map company social media handlers for monitoring",
            "Extract vehicle descriptions and license plates (if mentioned)",
            "Correlate construction worker presence with reconnaissance window"
        ]
    }
    save_results("00_osint_summary.json", summary)

if __name__ == "__main__":
    main()
