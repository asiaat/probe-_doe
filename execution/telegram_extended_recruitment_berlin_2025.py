#!/usr/bin/env python3
"""
Telegram OSINT: Extended Russian Recruitment Search (Berlin 2025)
Additional queries and broader time window for Russian recruitment data
"""

import asyncio
import json
import os
from datetime import datetime
from telethon import TelegramClient
from telethon.tl.functions.messages import SearchRequest, GetHistoryRequest
from telethon.tl.types import InputMessagesFilterEmpty
from dotenv import load_dotenv

load_dotenv()

API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('TELEGRAM_PHONE')

# Extended time window - full year 2025
SEARCH_START_DATE = datetime(2025, 1, 1)  # January 1, 2025
SEARCH_END_DATE = datetime(2025, 12, 31)  # December 31, 2025

# Extended search queries - additional Russian recruitment terms
ADDITIONAL_QUERIES = [
    # Additional Russian queries for Berlin recruitment
    "вербовка Берлин",  # recruitment Berlin
    "работа в Германии 2025",  # work in Germany 2025
    "трудоустройство Берлин",  # employment Berlin
    "строительные вакансии Берлин",  # construction vacancies Berlin
    "электрики в Германию",  # electricians to Germany
    "работа без визы Германия",  # work without visa Germany
    "легализация в Германии",  # legalization in Germany
    "помощь с документами Германия",  # document help Germany
    "строительная бригада Берлин",  # construction crew Berlin
    "высокооплачиваемая работа Берлин",  # high-paying work Berlin
    
    # Russian military/nationalist channels
    "Русские в Берлине работа",  # Russians in Berlin work
    "соотечественники Германия работа",  # compatriots Germany work
    "русскоязычные вакансии Берлин",  # Russian-speaking vacancies Berlin
    
    # Additional infrastructure terms
    "электроподстанция",  # substation
    "кабельные сети",  # cable networks
    "энергетика Германия",  # energy sector Germany
    "ремонт сетей Берлин",  # network repair Berlin
    
    # Agency and recruiter terms
    "кадровое агентство Берлин",  # recruitment agency Berlin
    "подбор персонала Берлин",  # personnel selection Berlin
    "аренда рабочих Германия",  # worker rental Germany
    
    # Terms related to Russian operations
    "ватник Берлин работа",  # vatnik (slang) Berlin work
    "Z-патриоты Германия",  # Z-patriots Germany
    "русский мир Берлин",  # Russian World Berlin
]

# Additional channels to search
ADDITIONAL_CHANNELS = [
    # Russian expat communities
    "@russiangermany",
    "@germany_ru",
    "@berlin_ru",
    "@moskva_berlin",
    "@russians_berlin",
    "@deutschland_ru",
    
    # Russian job boards
    "@rabota_eu",
    "@rabota_germany",
    "@rabota_berlin_ru",
    "@vakansii_germany",
    "@job_germany",
    "@arbeit_ru",
    
    # Construction/trade specific
    "@stroyka_eu",
    "@elektrik_eu",
    "@montazhniki_eu",
    "@santehnik_eu",
    "@str builders_germany",
    
    # Ukrainian communities (may have cross-over)
    "@ukrainians_germany",
    "@ukrainegermany",
    "@ua_de_work",
    
    # Dubious/potential operative channels
    "@germania_job",
    "@eu_rabota",
    "@work_europe_ru",
    "@europe_jobs_ru",
    "@germania_vakansii",
]

async def extended_telegram_search():
    """
    Extended search for Russian recruitment activity in Berlin 2025.
    """
    print(f"[*] Starting Extended Telegram OSINT: Russian Recruitment (2025)")
    print(f"[*] Search period: {SEARCH_START_DATE.date()} to {SEARCH_END_DATE.date()}")
    print(f"[*] Additional queries: {len(ADDITIONAL_QUERIES)}")
    print(f"[*] Additional channels: {len(ADDITIONAL_CHANNELS)}")
    
    client = TelegramClient('niemci_de4ru_session', API_ID, API_HASH)
    await client.start(phone=PHONE)
    print("[+] Telegram client authenticated")
    
    results = {
        "investigation": "Berlin Russian Recruitment Extended Search",
        "search_period": {
            "start": SEARCH_START_DATE.isoformat(),
            "end": SEARCH_END_DATE.isoformat()
        },
        "search_queries": ADDITIONAL_QUERIES,
        "channels_searched": ADDITIONAL_CHANNELS,
        "timestamp": datetime.now().isoformat(),
        "accessible_channels": [],
        "inaccessible_channels": [],
        "messages": [],
        "statistics": {
            "total_messages": 0,
            "by_channel": {},
            "by_query": {},
            "by_language": {"russian": 0, "ukrainian": 0, "german": 0, "other": 0},
            "by_month": {}
        }
    }
    
    # Search each channel
    for channel_username in ADDITIONAL_CHANNELS:
        print(f"\n[*] Searching channel: {channel_username}")
        
        try:
            channel = await client.get_entity(channel_username)
            channel_messages = []
            
            # Try history scan first (get recent messages)
            try:
                history = await client(GetHistoryRequest(
                    peer=channel,
                    limit=200,
                    offset_date=SEARCH_END_DATE,
                    offset_id=0,
                    max_id=0,
                    min_id=0,
                    add_offset=0,
                    hash=0
                ))
                
                for message in history.messages:
                    if message.date >= SEARCH_START_DATE and message.date <= SEARCH_END_DATE:
                        message_data = {
                            "channel": channel_username,
                            "message_id": message.id,
                            "date": message.date.isoformat(),
                            "text": message.message if hasattr(message, 'message') else str(message),
                            "query": "_history_scan",
                            "views": getattr(message, 'views', 0),
                            "forwards": getattr(message, 'forwards', 0),
                        }
                        
                        # Language detection
                        text = message_data["text"].lower() if message_data["text"] else ""
                        if any(word in text for word in ["работа", "вакансия", "требуется", "зарплата", "трудоустройство"]):
                            message_data["language"] = "russian"
                            results["statistics"]["by_language"]["russian"] += 1
                        elif any(word in text for word in ["робота", "вакансія", "зарплата"]):
                            message_data["language"] = "ukrainian"
                            results["statistics"]["by_language"]["ukrainian"] += 1
                        elif any(word in text for word in ["arbeit", "stelle", "gesucht"]):
                            message_data["language"] = "german"
                            results["statistics"]["by_language"]["german"] += 1
                        else:
                            message_data["language"] = "other"
                            results["statistics"]["by_language"]["other"] += 1
                        
                        # Track by month
                        month_key = message.date.strftime("%Y-%m")
                        results["statistics"]["by_month"][month_key] = results["statistics"]["by_month"].get(month_key, 0) + 1
                        
                        channel_messages.append(message_data)
                        results["messages"].append(message_data)
                        
            except Exception as e:
                print(f"    [-] History scan error: {e}")
            
            # Search specific queries
            for query in ADDITIONAL_QUERIES:
                print(f"  [*] Query: '{query}'")
                
                try:
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
                    
                    for message in result.messages:
                        if message.date >= SEARCH_START_DATE and message.date <= SEARCH_END_DATE:
                            # Check if not already from history
                            existing_ids = [(m["channel"], m["message_id"]) for m in channel_messages]
                            if (channel_username, message.id) not in existing_ids:
                                message_data = {
                                    "channel": channel_username,
                                    "message_id": message.id,
                                    "date": message.date.isoformat(),
                                    "text": message.message,
                                    "query": query,
                                    "views": getattr(message, 'views', 0),
                                    "forwards": getattr(message, 'forwards', 0),
                                }
                                
                                # Language detection
                                text = message.message.lower() if message.message else ""
                                if any(word in text for word in ["работа", "вакансия", "требуется"]):
                                    message_data["language"] = "russian"
                                    results["statistics"]["by_language"]["russian"] += 1
                                elif any(word in text for word in ["робота", "вакансія"]):
                                    message_data["language"] = "ukrainian"
                                    results["statistics"]["by_language"]["ukrainian"] += 1
                                else:
                                    message_data["language"] = "other"
                                    results["statistics"]["by_language"]["other"] += 1
                                
                                # Track by month
                                month_key = message.date.strftime("%Y-%m")
                                results["statistics"]["by_month"][month_key] = results["statistics"]["by_month"].get(month_key, 0) + 1
                                
                                channel_messages.append(message_data)
                                results["messages"].append(message_data)
                    
                    query_count = len([m for m in result.messages if m.date >= SEARCH_START_DATE and m.date <= SEARCH_END_DATE])
                    results["statistics"]["by_query"][query] = results["statistics"]["by_query"].get(query, 0) + query_count
                    print(f"    [+] Found {query_count} messages")
                    
                except Exception as e:
                    print(f"    [-] Error: {e}")
                    continue
            
            # Update channel statistics
            results["statistics"]["by_channel"][channel_username] = len(channel_messages)
            results["accessible_channels"].append({
                "username": channel_username,
                "title": getattr(channel, 'title', channel_username),
                "message_count": len(channel_messages)
            })
            print(f"  [+] Total from {channel_username}: {len(channel_messages)} messages")
            
        except Exception as e:
            print(f"  [-] Channel inaccessible: {e}")
            results["inaccessible_channels"].append({
                "username": channel_username,
                "error": str(e)
            })
            results["statistics"]["by_channel"][channel_username] = 0
            continue
    
    # Update totals
    results["statistics"]["total_messages"] = len(results["messages"])
    
    # Save results
    report_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"reports/{report_id}_telegram_extended_recruitment_berlin_2025"
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(f"{output_dir}/raw_data", exist_ok=True)
    
    output_file = f"{output_dir}/telegram_extended_search_{report_id}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    # Also save to raw_data for persistence
    raw_file = f"{output_dir}/raw_data/telegram_recruitment_extended_{report_id}.json"
    with open(raw_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'='*60}")
    print(f"[+] Search complete!")
    print(f"[+] Results saved to: {output_file}")
    print(f"[+] Total messages found: {results['statistics']['total_messages']}")
    print(f"[+] Accessible channels: {len(results['accessible_channels'])}")
    print(f"[+] Inaccessible channels: {len(results['inaccessible_channels'])}")
    print(f"[+] By language: Russian={results['statistics']['by_language']['russian']}, Ukrainian={results['statistics']['by_language']['ukrainian']}, German={results['statistics']['by_language']['german']}, Other={results['statistics']['by_language']['other']}")
    print(f"[+] Messages by month: {results['statistics']['by_month']}")
    print(f"{'='*60}")
    
    await client.disconnect()
    return results

if __name__ == "__main__":
    asyncio.run(extended_telegram_search())
