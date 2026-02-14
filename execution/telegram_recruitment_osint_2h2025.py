#!/usr/bin/env python3
"""
Telegram OSINT: Construction Job Recruitment (2H 2025)
Investigation: Berlin Grid Attack (EG Volt)
Focus: Ukrainian/Eastern European construction worker recruitment
Period: July-December 2025
"""

import asyncio
import json
import os
from datetime import datetime, timedelta
from telethon import TelegramClient
from telethon.tl.functions.messages import SearchRequest
from telethon.tl.types import InputMessagesFilterEmpty
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Telegram API credentials
API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('TELEGRAM_PHONE')

# Investigation parameters
SEARCH_START_DATE = datetime(2025, 7, 1)  # July 1, 2025
SEARCH_END_DATE = datetime(2025, 12, 31)  # December 31, 2025

# Search queries (German and Russian)
SEARCH_QUERIES = [
    # German queries - Construction job recruitment
    "Bauarbeiter gesucht Berlin",
    "Stromnetz Berlin Mitarbeiter",
    "Elektriker gesucht Berlin",
    "Kabelverlegung Jobs Berlin",
    "Baustellenhelfer Berlin",
    "Zeitarbeit Bau Berlin",
    "Subunternehmer Stromnetz",
    "Monteur gesucht Berlin",
    
    # German queries - Ukrainian/Eastern European workers
    "Ukrainische Bauarbeiter Berlin",
    "Osteuropäische Arbeiter Bau",
    "Polnische Bauarbeiter Berlin",
    "Rumänische Arbeiter Berlin",
    
    # Russian queries - Construction jobs
    "работа строитель Берлин",  # work construction worker Berlin
    "электрик вакансия Берлин",  # electrician vacancy Berlin
    "монтажник кабель Берлин",  # cable installer Berlin
    "подсобник стройка Берлин",  # construction helper Berlin
    "работа для украинцев Берлин",  # work for Ukrainians Berlin
    "легальная работа Германия",  # legal work Germany
    "вакансии без немецкого",  # vacancies without German
    "Zeitarbeit для украинцев",  # temp work for Ukrainians
    
    # Specific to Stromnetz Berlin / power sector
    "Stromnetz Berlin вакансии",
    "электросеть работа Берлин",  # power grid work Berlin
    "кабельные работы Берлин",  # cable work Berlin
]

# Telegram channels to search (German job boards, Ukrainian communities)
CHANNELS_TO_SEARCH = [
    # German job boards
    "@berlinjobs",
    "@berlinarbeit",
    "@jobsberlin",
    "@baustellenberlin",
    
    # Ukrainian/Russian communities in Berlin
    "@berlinukraine",
    "@ukraineberlin",
    "@berlinrussian",
    "@russianberlin",
    "@ukrainiansberlin",
    "@berlinukrainians",
    
    # Construction/trade groups
    "@bauberlin",
    "@handwerkerberlin",
    "@elektrikerberlin",
    
    # Zeitarbeit/staffing agencies
    "@zeitarbeitberlin",
    "@personalberlin",
    "@staffingberlin",
]

async def search_telegram_recruitment():
    """
    Search Telegram for construction job recruitment in 2H 2025.
    """
    print(f"[*] Starting Telegram OSINT: Construction Job Recruitment (2H 2025)")
    print(f"[*] Search period: {SEARCH_START_DATE.date()} to {SEARCH_END_DATE.date()}")
    print(f"[*] Queries: {len(SEARCH_QUERIES)}")
    print(f"[*] Channels: {len(CHANNELS_TO_SEARCH)}")
    
    # Initialize Telegram client
    client = TelegramClient('telegram_recruitment_session', API_ID, API_HASH)
    
    await client.start(phone=PHONE)
    print("[+] Telegram client authenticated")
    
    all_results = {
        "investigation": "Berlin Grid Attack (EG Volt)",
        "search_focus": "Construction job recruitment (2H 2025)",
        "search_period": {
            "start": SEARCH_START_DATE.isoformat(),
            "end": SEARCH_END_DATE.isoformat()
        },
        "search_queries": SEARCH_QUERIES,
        "channels_searched": CHANNELS_TO_SEARCH,
        "timestamp": datetime.now().isoformat(),
        "messages": [],
        "statistics": {
            "total_messages": 0,
            "by_channel": {},
            "by_query": {},
            "by_language": {"german": 0, "russian": 0, "ukrainian": 0},
            "recruitment_types": {
                "direct_hiring": 0,
                "staffing_agency": 0,
                "subcontractor": 0,
                "ukrainian_specific": 0
            }
        }
    }
    
    # Search each channel
    for channel_username in CHANNELS_TO_SEARCH:
        print(f"\n[*] Searching channel: {channel_username}")
        
        try:
            # Get channel entity
            channel = await client.get_entity(channel_username)
            channel_messages = []
            
            # Search each query in this channel
            for query in SEARCH_QUERIES:
                print(f"  [*] Query: '{query}'")
                
                try:
                    # Search messages
                    result = await client(SearchRequest(
                        peer=channel,
                        q=query,
                        filter=InputMessagesFilterEmpty(),
                        min_date=SEARCH_START_DATE,
                        max_date=SEARCH_END_DATE,
                        offset_id=0,
                        add_offset=0,
                        limit=100,
                        max_id=0,
                        min_id=0,
                        hash=0
                    ))
                    
                    # Process results
                    for message in result.messages:
                        if message.date >= SEARCH_START_DATE and message.date <= SEARCH_END_DATE:
                            message_data = {
                                "channel": channel_username,
                                "message_id": message.id,
                                "date": message.date.isoformat(),
                                "text": message.message,
                                "query": query,
                                "views": message.views if hasattr(message, 'views') else 0,
                                "forwards": message.forwards if hasattr(message, 'forwards') else 0,
                            }
                            
                            # Classify message
                            text_lower = message.message.lower() if message.message else ""
                            
                            # Language detection
                            if any(word in text_lower for word in ["работа", "вакансия", "требуется"]):
                                message_data["language"] = "russian"
                                all_results["statistics"]["by_language"]["russian"] += 1
                            elif any(word in text_lower for word in ["робота", "вакансія"]):
                                message_data["language"] = "ukrainian"
                                all_results["statistics"]["by_language"]["ukrainian"] += 1
                            else:
                                message_data["language"] = "german"
                                all_results["statistics"]["by_language"]["german"] += 1
                            
                            # Recruitment type classification
                            if any(word in text_lower for word in ["zeitarbeit", "personalvermittlung", "staffing"]):
                                message_data["recruitment_type"] = "staffing_agency"
                                all_results["statistics"]["recruitment_types"]["staffing_agency"] += 1
                            elif any(word in text_lower for word in ["subunternehmer", "nachunternehmer", "подрядчик"]):
                                message_data["recruitment_type"] = "subcontractor"
                                all_results["statistics"]["recruitment_types"]["subcontractor"] += 1
                            elif any(word in text_lower for word in ["ukrainer", "украинец", "українець"]):
                                message_data["recruitment_type"] = "ukrainian_specific"
                                all_results["statistics"]["recruitment_types"]["ukrainian_specific"] += 1
                            else:
                                message_data["recruitment_type"] = "direct_hiring"
                                all_results["statistics"]["recruitment_types"]["direct_hiring"] += 1
                            
                            # Stromnetz Berlin specific
                            if "stromnetz" in text_lower or "электросеть" in text_lower:
                                message_data["stromnetz_related"] = True
                            
                            channel_messages.append(message_data)
                            all_results["messages"].append(message_data)
                    
                    # Update statistics
                    query_count = len([m for m in result.messages if m.date >= SEARCH_START_DATE and m.date <= SEARCH_END_DATE])
                    all_results["statistics"]["by_query"][query] = all_results["statistics"]["by_query"].get(query, 0) + query_count
                    
                    print(f"    [+] Found {query_count} messages")
                    
                except Exception as e:
                    print(f"    [-] Error searching query '{query}': {e}")
                    continue
            
            # Update channel statistics
            all_results["statistics"]["by_channel"][channel_username] = len(channel_messages)
            print(f"  [+] Total messages from {channel_username}: {len(channel_messages)}")
            
        except Exception as e:
            print(f"  [-] Error accessing channel {channel_username}: {e}")
            all_results["statistics"]["by_channel"][channel_username] = 0
            continue
    
    # Update total count
    all_results["statistics"]["total_messages"] = len(all_results["messages"])
    
    # Save results
    output_dir = "reports/20260213_142123_berlin_grid_attack_eg_volt/osint_construction"
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(output_dir, "10_telegram_recruitment_2h2025.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    
    print(f"\n[+] Results saved to: {output_file}")
    print(f"[+] Total messages found: {all_results['statistics']['total_messages']}")
    print(f"[+] By language: German={all_results['statistics']['by_language']['german']}, Russian={all_results['statistics']['by_language']['russian']}, Ukrainian={all_results['statistics']['by_language']['ukrainian']}")
    print(f"[+] Recruitment types: Direct={all_results['statistics']['recruitment_types']['direct_hiring']}, Staffing={all_results['statistics']['recruitment_types']['staffing_agency']}, Subcontractor={all_results['statistics']['recruitment_types']['subcontractor']}, Ukrainian-specific={all_results['statistics']['recruitment_types']['ukrainian_specific']}")
    
    await client.disconnect()
    
    return all_results

if __name__ == "__main__":
    asyncio.run(search_telegram_recruitment())
