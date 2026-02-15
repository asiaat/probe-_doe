#!/usr/bin/env python3
"""
Telegram OSINT: @Niemci Channel Deep Search with Extended Queries
Focus on @Niemci (Layboard.com) channel with additional Russian recruitment terms
"""

import asyncio
import json
import os
from datetime import datetime, timezone
from telethon import TelegramClient
from telethon.tl.functions.messages import SearchRequest, GetHistoryRequest
from telethon.tl.types import InputMessagesFilterEmpty
from dotenv import load_dotenv

load_dotenv()

API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('TELEGRAM_PHONE')

# Full year 2025 - timezone aware
SEARCH_START_DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)
SEARCH_END_DATE = datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

# Extended search queries
EXTENDED_QUERIES = [
    # Original queries from previous search
    "вербовка Берлин",
    "работа в Германии 2025",
    "трудоустройство Берлин",
    "строительные вакансии Берлин",
    "электрики в Германию",
    "работа без визы Германия",
    "легализация в Германии",
    "помощь с документами Германия",
    "строительная бригада Берлин",
    "высокооплачиваемая работа Берлин",
    "Русские в Берлине работа",
    "соотечественники Германия работа",
    "русскоязычные вакансии Берлин",
    "электроподстанция",
    "кабельные сети",
    "энергетика Германия",
    "ремонт сетей Берлин",
    "кадровое агентство Берлин",
    "подбор персонала Берлин",
    "аренда рабочих Германия",
    
    # Additional specific terms
    "Berlin Arbeitgeber",
    "Stromnetz Berlin Jobs",
    "Kabelarbeiten Berlin",
    "Elektroinstallation Berlin",
    "Tiefbau Berlin Jobs",
    "Infrastruktur Berlin",
    "Energieversorgung Berlin",
    "Bau Berlin 2025",
    "Baustelle Berlin Süd",
    "Steglitz Arbeit",
    "Zehlendorf Jobs",
    "Lichterfelde Stellen",
    "Tempelhof Bau",
    "Mariendorf Arbeit",
    
    # Russian translations of Berlin-specific terms
    "работа Штеглиц",
    "работа Целлендорф",
    "электрик Берлин",
    "строитель Берлин зарплата",
    "немецкая фирма работа",
    "официальное трудоустройство Берлин",
    
    # Potential recruitment code words
    "срочно Берлин",
    "зарплата евро Берлин",
    "жилье работа Берлин",
    "без опыта Берлин",
]

TARGET_CHANNEL = "@Niemci"

async def search_niemci_extended():
    """
    Deep search of @Niemci channel with extended queries.
    """
    print(f"[*] Starting Deep Search: @Niemci (Layboard.com)")
    print(f"[*] Search period: {SEARCH_START_DATE.date()} to {SEARCH_END_DATE.date()}")
    print(f"[*] Extended queries: {len(EXTENDED_QUERIES)}")
    
    client = TelegramClient('niemci_de4ru_session', API_ID, API_HASH)
    await client.start(phone=PHONE)
    print("[+] Telegram client authenticated")
    
    results = {
        "investigation": "Berlin Russian Recruitment - @Niemci Extended Search",
        "channel": TARGET_CHANNEL,
        "search_period": {
            "start": SEARCH_START_DATE.isoformat(),
            "end": SEARCH_END_DATE.isoformat()
        },
        "search_queries": EXTENDED_QUERIES,
        "timestamp": datetime.now().isoformat(),
        "messages": [],
        "statistics": {
            "total_messages": 0,
            "by_query": {},
            "by_month": {},
            "by_language": {"russian": 0, "ukrainian": 0, "german": 0, "mixed": 0},
            "by_recruitment_type": {
                "construction": 0,
                "electrical": 0,
                "general_labor": 0,
                "skilled_trade": 0,
                "other": 0
            },
            "housing_mentioned": 0,
            "no_german_required": 0,
            "berlin_specific": 0
        }
    }
    
    try:
        channel = await client.get_entity(TARGET_CHANNEL)
        print(f"[+] Connected to: {getattr(channel, 'title', TARGET_CHANNEL)}")
        
        # First, get full history for 2025
        print("\n[*] Retrieving full channel history for 2025...")
        all_messages = []
        offset_id = 0
        
        while True:
            try:
                history = await client(GetHistoryRequest(
                    peer=channel,
                    limit=100,
                    offset_date=SEARCH_END_DATE,
                    offset_id=offset_id,
                    max_id=0,
                    min_id=0,
                    add_offset=0,
                    hash=0
                ))
                
                if not history.messages:
                    break
                
                for msg in history.messages:
                    if msg.date >= SEARCH_START_DATE and msg.date <= SEARCH_END_DATE:
                        all_messages.append(msg)
                        
                if len(history.messages) < 100:
                    break
                    
                offset_id = history.messages[-1].id
                print(f"  [*] Retrieved {len(all_messages)} messages so far...")
                
            except Exception as e:
                print(f"  [-] History error: {e}")
                break
        
        print(f"[+] Total messages in 2025: {len(all_messages)}")
        
        # Process all messages
        print("\n[*] Analyzing messages for recruitment content...")
        for msg in all_messages:
            if not hasattr(msg, 'message') or not msg.message:
                continue
                
            text = msg.message
            text_lower = text.lower()
            
            # Check if any query matches
            matched_queries = []
            for query in EXTENDED_QUERIES:
                query_lower = query.lower()
                if query_lower in text_lower:
                    matched_queries.append(query)
            
            # Also include all messages to analyze patterns
            message_data = {
                "message_id": msg.id,
                "date": msg.date.isoformat(),
                "text": text,
                "views": getattr(msg, 'views', 0),
                "forwards": getattr(msg, 'forwards', 0),
                "matched_queries": matched_queries,
                "has_media": bool(getattr(msg, 'media', None))
            }
            
            # Language detection
            if any(word in text_lower for word in ["работа", "вакансия", "требуется", "зарплата", "трудоустройство", "электрик", "строитель"]):
                message_data["language"] = "russian"
                results["statistics"]["by_language"]["russian"] += 1
            elif any(word in text_lower for word in ["робота", "вакансія"]):
                message_data["language"] = "ukrainian"
                results["statistics"]["by_language"]["ukrainian"] += 1
            elif any(word in text_lower for word in ["arbeit", "stelle", "gesucht", "berlin", "germany"]):
                message_data["language"] = "german"
                results["statistics"]["by_language"]["german"] += 1
            else:
                message_data["language"] = "mixed"
                results["statistics"]["by_language"]["mixed"] += 1
            
            # Recruitment type classification
            if any(word in text_lower for word in ["электрик", "электромонтаж", "elektriker", "kabel", "strom"]):
                message_data["recruitment_type"] = "electrical"
                results["statistics"]["by_recruitment_type"]["electrical"] += 1
            elif any(word in text_lower for word in ["строитель", "бетонщик", "bauarbeiter", "bau", "construction"]):
                message_data["recruitment_type"] = "construction"
                results["statistics"]["by_recruitment_type"]["construction"] += 1
            elif any(word in text_lower for word in ["разнорабочий", "подсобник", "helfer", "lager"]):
                message_data["recruitment_type"] = "general_labor"
                results["statistics"]["by_recruitment_type"]["general_labor"] += 1
            elif any(word in text_lower for word in ["сантехник", "маляр", "плиточник", "installateur", "fliesen"]):
                message_data["recruitment_type"] = "skilled_trade"
                results["statistics"]["by_recruitment_type"]["skilled_trade"] += 1
            else:
                message_data["recruitment_type"] = "other"
                results["statistics"]["by_recruitment_type"]["other"] += 1
            
            # Red flags
            if any(word in text_lower for word in ["жилье", "wohnung", "unterkunft", "housing"]):
                message_data["mentions_housing"] = True
                results["statistics"]["housing_mentioned"] += 1
            else:
                message_data["mentions_housing"] = False
                
            if any(word in text_lower for word in ["без немецкого", "без языка", "kein deutsch", "ohne sprache"]):
                message_data["no_german_required"] = True
                results["statistics"]["no_german_required"] += 1
            else:
                message_data["no_german_required"] = False
            
            # Berlin specific
            if any(word in text_lower for word in ["берлин", "berlin", "штеглиц", "zehlendorf", "lichterfelde"]):
                message_data["berlin_specific"] = True
                results["statistics"]["berlin_specific"] += 1
            else:
                message_data["berlin_specific"] = False
            
            # Track by month
            month_key = msg.date.strftime("%Y-%m")
            results["statistics"]["by_month"][month_key] = results["statistics"]["by_month"].get(month_key, 0) + 1
            
            # Update query stats
            for query in matched_queries:
                results["statistics"]["by_query"][query] = results["statistics"]["by_query"].get(query, 0) + 1
            
            results["messages"].append(message_data)
        
        results["statistics"]["total_messages"] = len(results["messages"])
        
        # Save results
        report_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = f"reports/{report_id}_niemci_extended_search_berlin_2025"
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(f"{output_dir}/raw_data", exist_ok=True)
        
        output_file = f"{output_dir}/niemci_extended_analysis_{report_id}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        raw_file = f"{output_dir}/raw_data/telegram_niemci_extended_{report_id}.json"
        with open(raw_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"\n{'='*60}")
        print(f"[+] Deep search complete!")
        print(f"[+] Results saved to: {output_file}")
        print(f"[+] Total messages analyzed: {results['statistics']['total_messages']}")
        print(f"[+] Messages by language: {results['statistics']['by_language']}")
        print(f"[+] Recruitment types: {results['statistics']['by_recruitment_type']}")
        print(f"[+] Housing mentioned: {results['statistics']['housing_mentioned']}")
        print(f"[+] No German required: {results['statistics']['no_german_required']}")
        print(f"[+] Berlin-specific: {results['statistics']['berlin_specific']}")
        print(f"[+] Matched queries: {len([q for q in results['statistics']['by_query'].values() if q > 0])}")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"[-] Error: {e}")
        import traceback
        traceback.print_exc()
    
    await client.disconnect()
    return results

if __name__ == "__main__":
    asyncio.run(search_niemci_extended())
