#!/usr/bin/env python3
"""
Telegram OSINT: Construction Complaints & Social Media
Searches German Telegram channels for construction-related discussions
"""

import os
import json
import asyncio
from datetime import datetime, timedelta
from telethon import TelegramClient
from telethon.tl.functions.messages import SearchGlobalRequest
from telethon.tl.types import InputMessagesFilterEmpty
from dotenv import load_dotenv

load_dotenv()

# Telegram API credentials
API_ID = int(os.getenv('TELEGRAM_API_ID'))
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('TELEGRAM_PHONE')

# Output directory
OUTPUT_DIR = "reports/20260213_142123_berlin_grid_attack_eg_volt/osint_construction"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Search queries (German)
SEARCH_QUERIES = [
    # Construction complaints
    "Lichterfelde Bauarbeiten Lärm",
    "Teltowkanal Baustelle",
    "Ostpreußendamm Bauarbeiten",
    "Goerzallee Sperrung",
    "Stromnetz Berlin Kabelarbeiten",
    "Hindenburgdamm Fernwärme",
    
    # Vehicle sightings
    "Lichterfelde Baufahrzeuge",
    "Teltowkanal LKW",
    "Baustellenverkehr Berlin",
    
    # Company mentions
    "Stromnetz Berlin Baustelle",
    "Vattenfall Bauarbeiten",
    "BEW Lichterfelde",
    
    # Time-specific
    "Lichterfelde November 2025 Baustelle",
    "Lichterfelde Dezember 2025 Bauarbeiten",
]

# Target channels (German local/construction channels)
TARGET_CHANNELS = [
    "@berlinverkehr",
    "@berlin_live",
    "@steglitzzehlendorf",
    "@lichterfelde_berlin",
]

async def search_telegram(client, query, start_date, end_date):
    """Search Telegram for specific query"""
    results = []
    
    try:
        # Global search
        search_result = await client(SearchGlobalRequest(
            q=query,
            filter=InputMessagesFilterEmpty(),
            min_date=start_date,
            max_date=end_date,
            offset_rate=0,
            offset_peer=None,
            offset_id=0,
            limit=100
        ))
        
        for message in search_result.messages:
            if hasattr(message, 'message') and message.message:
                results.append({
                    "date": message.date.isoformat(),
                    "message": message.message,
                    "chat_id": message.peer_id.channel_id if hasattr(message.peer_id, 'channel_id') else None,
                    "message_id": message.id
                })
    
    except Exception as e:
        print(f"  ⚠️  Error searching '{query}': {e}")
    
    return results

async def search_channel(client, channel_username, keywords, start_date, end_date):
    """Search specific channel for keywords"""
    results = []
    
    try:
        entity = await client.get_entity(channel_username)
        
        async for message in client.iter_messages(entity, offset_date=end_date, reverse=True):
            if message.date < start_date:
                break
            
            if message.message:
                # Check if any keyword is in message
                message_lower = message.message.lower()
                if any(keyword.lower() in message_lower for keyword in keywords):
                    results.append({
                        "channel": channel_username,
                        "date": message.date.isoformat(),
                        "message": message.message,
                        "message_id": message.id,
                        "views": message.views if hasattr(message, 'views') else None
                    })
    
    except Exception as e:
        print(f"  ⚠️  Error searching channel {channel_username}: {e}")
    
    return results

async def main():
    print("=" * 80)
    print("TELEGRAM OSINT: Construction Complaints & Social Media")
    print("Target Period: November 1, 2025 - January 3, 2026")
    print("=" * 80)
    
    # Date range
    start_date = datetime(2025, 11, 1)
    end_date = datetime(2026, 1, 3, 23, 59, 59)
    
    # Initialize Telegram client
    client = TelegramClient('osint_session', API_ID, API_HASH)
    
    try:
        await client.start(phone=PHONE)
        print("✓ Connected to Telegram\n")
        
        all_results = {
            "timestamp": datetime.now().isoformat(),
            "search_period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "global_searches": {},
            "channel_searches": {}
        }
        
        # 1. Global searches
        print("[1/2] Performing global searches...")
        for i, query in enumerate(SEARCH_QUERIES, 1):
            print(f"  [{i}/{len(SEARCH_QUERIES)}] Searching: {query}")
            results = await search_telegram(client, query, start_date, end_date)
            all_results["global_searches"][query] = {
                "count": len(results),
                "messages": results
            }
            print(f"      Found: {len(results)} messages")
            await asyncio.sleep(2)  # Rate limiting
        
        # 2. Channel-specific searches
        print("\n[2/2] Searching specific channels...")
        keywords = ["Lichterfelde", "Teltowkanal", "Ostpreußendamm", "Goerzallee", 
                   "Stromnetz", "Bauarbeiten", "Baustelle", "Kabelarbeiten"]
        
        for i, channel in enumerate(TARGET_CHANNELS, 1):
            print(f"  [{i}/{len(TARGET_CHANNELS)}] Searching channel: {channel}")
            results = await search_channel(client, channel, keywords, start_date, end_date)
            all_results["channel_searches"][channel] = {
                "count": len(results),
                "messages": results
            }
            print(f"      Found: {len(results)} messages")
            await asyncio.sleep(2)  # Rate limiting
        
        # Save results
        output_file = os.path.join(OUTPUT_DIR, "09_telegram_osint.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, ensure_ascii=False, indent=2)
        
        print("\n" + "=" * 80)
        print(f"✓ Telegram OSINT Complete")
        print(f"✓ Results saved to: {output_file}")
        print(f"✓ Total messages found: {sum(s['count'] for s in all_results['global_searches'].values()) + sum(s['count'] for s in all_results['channel_searches'].values())}")
        print("=" * 80)
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
    
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
