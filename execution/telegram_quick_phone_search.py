#!/usr/bin/env python3
"""
Quick Telegram OSINT: Search for specific phone numbers
Target: +44 7881 228865 and +44 7777 213139
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

TARGET_PHONES = [
    "+44 7881 228865",
    "+44 7777 213139",
    "7881228865",
    "7777213139",
]

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
    "@work_europe_ru",
    "@rabota_europe",
    "@job_berlin",
    "@layboard",
]

async def search_channel(client, channel_name, phone_numbers):
    results = []
    try:
        entity = await client.get_entity(channel_name)
        print(f"  [+] {channel_name}")
    except Exception as e:
        print(f"  [-] {channel_name}: {e}")
        return results

    for phone in phone_numbers:
        try:
            search_result = await client(SearchRequest(
                peer=entity,
                q=phone,
                filter=InputMessagesFilterEmpty(),
                min_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
                max_date=datetime(2026, 2, 15, tzinfo=timezone.utc),
                offset_id=0,
                add_offset=0,
                limit=50,
                max_id=0,
                min_id=0,
                hash=0
            ))

            for msg in search_result.messages:
                if not msg.message:
                    continue
                results.append({
                    "channel": channel_name,
                    "phone_query": phone,
                    "message_id": msg.id,
                    "date": msg.date.isoformat(),
                    "text": msg.message[:500],
                })

            await asyncio.sleep(0.3)
        except Exception as e:
            print(f"    [!] {phone}: {e}")
            continue

    return results


async def main():
    print("=" * 60)
    print("  Quick Telegram Phone Search")
    print(f"  Targets: {TARGET_PHONES}")
    print(f"  Channels: {len(CHANNELS)}")
    print("=" * 60)

    client = TelegramClient('osint_session', int(API_ID), API_HASH)
    await client.start(phone=PHONE)
    print("[+] Connected\n")

    all_results = []
    for channel in CHANNELS:
        results = await search_channel(client, channel, TARGET_PHONES)
        all_results.extend(results)
        print(f"    Found: {len(results)} messages")

    await client.disconnect()

    print(f"\n[=] Total: {len(all_results)} messages")

    if all_results:
        print("\n[+] Results:")
        for r in all_results[:20]:
            print(f"  [{r['date'][:10]}] {r['channel']}: {r['text'][:100]}...")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs("reports/telegram_search", exist_ok=True)
    out_file = f"reports/telegram_search/phone_search_{ts}.json"
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump({
            "phones": TARGET_PHONES,
            "results": all_results,
            "timestamp": datetime.now().isoformat(),
        }, f, ensure_ascii=False, indent=2)
    print(f"\n[+] Saved: {out_file}")


if __name__ == "__main__":
    asyncio.run(main())
