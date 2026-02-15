#!/usr/bin/env python3
"""
Telegram OSINT: Search for crypto payment mentions in job channels
Focus: Russian companies in Berlin paying in crypto (USDT, etc.)
Period: 2025-2026
"""

import asyncio
import json
import os
import re
from datetime import datetime, timezone
from telethon import TelegramClient
from telethon.tl.functions.messages import SearchRequest
from telethon.tl.types import InputMessagesFilterEmpty
from dotenv import load_dotenv

load_dotenv()

API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('TELEGRAM_PHONE')

# Crypto payment keywords
CRYPTO_QUERIES = [
    # Russian
    "крипто",
    "криптовалюта",
    "USDT",
    "оплата крипто",
    "оплата в криптовалюте",
    "оплата в USDT",
    "зарплата крипто",
    "зарплата в крипте",
    "платим в крипте",
    "платим USDT",
    "Bitcoin",
    "BTC оплата",
    # English
    "crypto payment",
    "pay in crypto",
    "pay in USDT",
    "salary crypto",
]

# Known entities from prior investigations to cross-reference
ENTITY_QUERIES = [
    "Anatoly",
    "Paliy",
    "JPG Recruitment",
    "ITAAR",
    "Belova",
]

ALL_QUERIES = CRYPTO_QUERIES + ENTITY_QUERIES

CHANNELS = [
    "@Niemci",
    "@rabota_v_germanii",
    "@rabotavgermanii",
    "@germany_ua",
    "@ukraine_germany",
    "@ukr_de",
    "@rabota_berlin",
    "@job_germany_ru",
    "@rabota_za_granicey",
    "@job_berlin",
    "@layboard",
]

# Crypto patterns for detection
CRYPTO_PATTERNS = [
    r'USDT',
    r'BTC',
    r'биткоин',
    r'крипт',
    r'\+?[\d]{10,}.*USDT',
    r'TRC20|ERC20',
]


async def search_channel(client, channel_name, queries):
    results = []
    try:
        entity = await client.get_entity(channel_name)
        print(f"  [+] {channel_name}")
    except Exception as e:
        print(f"  [-] {channel_name}: {e}")
        return results

    for query in queries:
        try:
            search_result = await client(SearchRequest(
                peer=entity,
                q=query,
                filter=InputMessagesFilterEmpty(),
                min_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
                max_date=datetime(2026, 2, 15, tzinfo=timezone.utc),
                limit=50,
                max_id=0,
                min_id=0,
                hash=0,
                add_offset=0,
                offset_id=0,
            ))

            for msg in search_result.messages:
                if not msg.message:
                    continue
                
                # Check for crypto patterns in message
                has_crypto = any(re.search(p, msg.message, re.IGNORECASE) for p in CRYPTO_PATTERNS)
                
                results.append({
                    "channel": channel_name,
                    "query": query,
                    "message_id": msg.id,
                    "date": msg.date.isoformat(),
                    "text": msg.message[:500],
                    "has_crypto_mention": has_crypto,
                })

            await asyncio.sleep(0.3)
        except Exception as e:
            continue

    return results


async def main():
    print("=" * 60)
    print("  Telegram OSINT: Crypto Payment Search")
    print("  Period: 2025-2026")
    print(f"  Channels: {len(CHANNELS)}")
    print(f"  Queries: {len(ALL_QUERIES)}")
    print("=" * 60)

    client = TelegramClient('osint_session', int(API_ID), API_HASH)
    await client.start(phone=PHONE)
    print("[+] Connected\n")

    all_results = []
    for channel in CHANNELS:
        results = await search_channel(client, channel, ALL_QUERIES)
        all_results.extend(results)
        print(f"    Found: {len(results)} messages")

    await client.disconnect()

    # Filter to crypto-relevant
    crypto_results = [r for r in all_results if r.get('has_crypto_mention')]

    print(f"\n[=] Total: {len(all_results)} messages")
    print(f"[=] Crypto-relevant: {len(crypto_results)} messages")

    # Save
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs("reports", exist_ok=True)
    out_file = f"reports/telegram_crypto_search_{ts}.json"
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump({
            "period": "2025-2026",
            "total_messages": len(all_results),
            "crypto_relevant": len(crypto_results),
            "results": all_results,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n[+] Saved: {out_file}")


if __name__ == "__main__":
    asyncio.run(main())
