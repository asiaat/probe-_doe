#!/usr/bin/env python3
"""
Enhanced Telegram OSINT: Phone Number & Contact Infrastructure Search
Investigation: Berlin Grid Attack (Operation EG VOLT)
Focus: JPG Recruitment / ITAAR / Pink Salon phone numbers across ALL accessible channels
Compliance: OSINT SOP v2.3 / Persistence SOP v1.0
"""

import asyncio
import json
import os
import hashlib
import re
from datetime import datetime, timezone
from telethon import TelegramClient
from telethon.tl.functions.messages import SearchRequest
from telethon.tl.types import InputMessagesFilterEmpty
from dotenv import load_dotenv

# ─── Configuration ───
load_dotenv()

API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('TELEGRAM_PHONE')

# Extended window: May 2025 – Feb 2026 (wider for phone number hits)
SEARCH_START = datetime(2025, 5, 1, tzinfo=timezone.utc)
SEARCH_END = datetime(2026, 2, 14, tzinfo=timezone.utc)

# ─── Target Channels (ONLY accessible ones from first run) ───
TARGET_CHANNELS = [
    "@Niemci",                # Работа в Германии - Layboard.com (ID: 1263846030)
    "@rabota_v_germanii",     # MRB Recruitment (ID: 1594274015)
    "@rabotavgermanii",       # вакансии (ID: 1496862209)
    "@germany_ua",            # English teachers (ID: 1754173350)
    "@ukraine_germany",       # Наши в Германии 🇺🇦🇩🇪 (ID: 1594077041)
    "@ukr_de",                # INFO Беженцы 🇺🇦/🇩🇪 (ID: 1408570944)
    "@rabota_berlin",         # РАБОТА БЕРЛИН (ID: 1511147723)
    "@bau_arbeit",            # @bau_arbeit (ID: 5467271142)
    "@job_germany_ru",        # @jobs_germany_ru (ID: 3771056615)
    # Additional channels for phone search
    "@rabota_za_granicey",    # Work abroad
    "@work_europe_ru",        # Work in Europe (Russian)
    "@rabota_europe",         # Work in Europe
    "@job_berlin",            # Job Berlin
    "@layboard",              # Layboard official
]

# ─── Contact Infrastructure Queries ───
# PUBLIC phone numbers from JPG Recruitment / ITAAR job postings
# These are publicly advertised contact points from job ads

PHONE_QUERIES = [
    # JPG Recruitment / Beauty Salon contact
    "+49 30 88475588",        # JK Beauty Institut (official)
    "88475588",               # Short form
    "jkbeautyinstitut",       # Instagram/Facebook handle
    "jk_beautyinstitut",      # Instagram handle
    "contact@jkbeautyinstitut.de",  # Email
    "Pfalzburger",            # Pfalzburger Str. (address)

    # UK WhatsApp numbers (from job ad postings)
    "+44 7881 228865",        # WhatsApp
    "7881228865",             # Compact
    "+44 7777 213139",        # Telegram
    "7777213139",             # Compact
    "+44 7387 793670",        # Telegram
    "7387793670",             # Compact
    "+44 7459 878923",        # Telegram
    "7459878923",             # Compact
    "+44 7908 973686",        # Telegram
    "7908973686",             # Compact
    "+44 7494 401874",        # WhatsApp
    "7494401874",             # Compact

    # Swedish number (from job ads)
    "+46 73 788 40 15",       # WhatsApp Business
    "737884015",              # Compact

    # German mobile (from job ads)
    "+49 1577 7103445",       # WhatsApp
    "15777103445",            # Compact

    # Agency names
    "JPG Recruitment",
    "JPG Recruit",
    "ITAAR",
    "ITAAR Recruitment",
    "Sol-Tech",
]

# Entity / Person queries from investigation
ENTITY_QUERIES = [
    # Recruitment-side persons
    "Anastasia",              # Known recruiter first name
    "Anatoliy",               # Known recruiter first name
    "Elmira",                 # Known recruiter first name

    # Primaholding / voxenergie network
    "Bagratuni",              # Dimitri Bagratuni (MD primaholding)
    "Dimitri Bagratuni",
    "primaholding",
    "voxenergie",
    "nowenergy",
    "primastrom",
    "primacall",
    "primasales",
    "Vukusic",                # Marijan/Marijana Vukusic
    "Marijana Fenster",
    "Mario Kovac",

    # Subcontractors
    "STRABAG",
    "Stratieka",
    "Boran GmbH",
    "Planmann",

    # Specific addresses  
    "Großbeerenstraße",       # Primaholding HQ
    "Großbeerenstr",          # Short form
]

# Berlin construction specifics (narrower, targeted)
BERLIN_CONSTRUCTION_QUERIES = [
    "Lichterfelde",
    "Steglitz Berlin",
    "Stromnetz Berlin",
    "Kabelverlegung Berlin",
    "Электрик Берлин",
    "электромонтаж Берлин",
    "кабельщик Берлин",
    "монтажник Берлин",
]

ALL_QUERIES = PHONE_QUERIES + ENTITY_QUERIES + BERLIN_CONSTRUCTION_QUERIES

# ─── Phone number patterns for detection in message text ───
PHONE_PATTERNS = [
    r'\+44\s*7\d{3}\s*\d{3}\s*\d{3}',    # UK mobile
    r'\+44\s*7\d{9}',                      # UK mobile compact
    r'\+46\s*7\d[\s\d]{7,10}',             # Swedish mobile
    r'\+49\s*1[5-7]\d[\s\d]{7,10}',        # German mobile
    r'\+49\s*30\s*\d{6,9}',                # Berlin landline
    r'7881\s*228\s*865',                    # Known JPG number
    r'7777\s*213\s*139',                    # Known JPG number
    r'7387\s*793\s*670',                    # Known JPG number
    r'7459\s*878\s*923',                    # Known JPG number
    r'7908\s*973\s*686',                    # Known JPG number
    r'7494\s*401\s*874',                    # Known JPG number
    r'1577\s*710\s*3445',                   # Known German number
]


def compute_doc_id(channel: str, message_id: int) -> str:
    return hashlib.sha256(f"{channel}:{message_id}".encode()).hexdigest()


def extract_phones(text: str) -> list:
    """Extract all phone numbers from message text."""
    phones = []
    for pattern in PHONE_PATTERNS:
        matches = re.findall(pattern, text)
        phones.extend(matches)
    return list(set(phones))


def score_message(text: str) -> dict:
    """Score a message for investigation relevance."""
    text_lower = text.lower() if text else ""
    score = 0
    indicators = []

    # CRITICAL: Known phone numbers (score +5 each)
    known_phones = [
        "7881228865", "7777213139", "7387793670",
        "7459878923", "7908973686", "7494401874",
        "15777103445", "737884015", "88475588"
    ]
    text_digits = re.sub(r'\D', '', text)
    for phone in known_phones:
        if phone in text_digits:
            score += 5
            indicators.append(f"KNOWN_PHONE:{phone}")

    # HIGH: Entity names (score +3 each)
    high_entities = [
        "jpg recruitment", "itaar", "pfalzburger",
        "jkbeautyinstitut", "jk_beauty", "bagratuni",
        "primaholding", "voxenergie", "sol-tech"
    ]
    for entity in high_entities:
        if entity in text_lower:
            score += 3
            indicators.append(f"ENTITY:{entity}")

    # MEDIUM: Person names (score +2)
    person_names = [
        "anastasia", "anatoliy", "elmira",
        "vukusic", "marijana fenster", "mario kovac"
    ]
    for name in person_names:
        if name in text_lower:
            score += 2
            indicators.append(f"PERSON:{name}")

    # MEDIUM: Location matches (score +2)
    locations = [
        "lichterfelde", "stromnetz", "steglitz",
        "110kv", "kabelbrücke", "ostpreußendamm"
    ]
    for loc in locations:
        if loc in text_lower:
            score += 2
            indicators.append(f"LOCATION:{loc}")

    # LOW: Construction Berlin (score +1)
    construction = [
        "bauhelfer berlin", "электрик берлин", "монтажник берлин",
        "строитель берлин", "кабельщик", "scaffolder berlin",
        "strabag", "stratieka", "boran", "planmann"
    ]
    for kw in construction:
        if kw in text_lower:
            score += 1
            indicators.append(f"CONSTRUCTION:{kw}")

    # Extract phones found in text
    phones_found = extract_phones(text)

    return {
        "relevance_score": min(score, 20),
        "indicators": indicators,
        "phones_extracted": phones_found,
    }


async def search_channel_phones(client, channel_name: str, results: dict):
    """Search a channel with all phone/entity queries."""
    print(f"\n{'='*60}")
    print(f"[*] CHANNEL: {channel_name}")
    print(f"{'='*60}")

    channel_data = {
        "channel": channel_name,
        "status": "unknown",
        "total_messages": 0,
        "messages": [],
    }

    try:
        entity = await client.get_entity(channel_name)
        channel_data["status"] = "accessible"
        channel_data["channel_title"] = getattr(entity, 'title', channel_name)
        channel_data["channel_id"] = entity.id
        print(f"  [+] Connected: {channel_data['channel_title']} (ID: {entity.id})")
    except Exception as e:
        channel_data["status"] = "inaccessible"
        print(f"  [-] FAILED: {e}")
        results["channels"][channel_name] = channel_data
        return

    seen_ids = set()

    for query in ALL_QUERIES:
        try:
            search_result = await client(SearchRequest(
                peer=entity,
                q=query,
                filter=InputMessagesFilterEmpty(),
                min_date=SEARCH_START,
                max_date=SEARCH_END,
                offset_id=0,
                add_offset=0,
                limit=100,
                max_id=0,
                min_id=0,
                hash=0
            ))

            new_count = 0
            for msg in search_result.messages:
                if msg.id in seen_ids:
                    continue
                if not msg.message:
                    continue

                seen_ids.add(msg.id)
                new_count += 1

                scoring = score_message(msg.message)

                message_data = {
                    "doc_id": compute_doc_id(channel_name, msg.id),
                    "message_id": msg.id,
                    "date": msg.date.isoformat(),
                    "text": msg.message,
                    "matched_query": query,
                    "views": getattr(msg, 'views', 0),
                    "forwards": getattr(msg, 'forwards', 0),
                    **scoring,
                }
                channel_data["messages"].append(message_data)

                if scoring["relevance_score"] >= 3:
                    print(f"  🔴 HIGH RELEVANCE (score={scoring['relevance_score']}): "
                          f"msg_id={msg.id}, date={msg.date.date()}")
                    print(f"     Indicators: {scoring['indicators']}")
                    if scoring['phones_extracted']:
                        print(f"     📞 Phones: {scoring['phones_extracted']}")
                    print(f"     Text: {msg.message[:200]}...")

            if new_count > 0:
                print(f"  [+] Query '{query}': {new_count} new messages")

            await asyncio.sleep(0.5)

        except Exception as e:
            if "FLOOD" in str(e).upper():
                print(f"  [⚠] FLOOD WAIT — sleeping 30s")
                await asyncio.sleep(30)
            continue

    channel_data["total_messages"] = len(channel_data["messages"])
    results["channels"][channel_name] = channel_data
    print(f"\n  [=] TOTAL for {channel_name}: {channel_data['total_messages']} messages")


async def main():
    print("=" * 70)
    print("  ENHANCED TELEGRAM OSINT: Phone Number & Contact Infrastructure")
    print("  Operation EG VOLT — Berlin Grid Attack Investigation")
    print(f"  Window: {SEARCH_START.date()} → {SEARCH_END.date()}")
    print(f"  Channels: {len(TARGET_CHANNELS)}")
    print(f"  Queries: {len(ALL_QUERIES)}")
    print(f"    Phone/Contact: {len(PHONE_QUERIES)}")
    print(f"    Entity/Person: {len(ENTITY_QUERIES)}")
    print(f"    Berlin Construction: {len(BERLIN_CONSTRUCTION_QUERIES)}")
    print("=" * 70)

    client = TelegramClient('osint_session', int(API_ID), API_HASH)
    await client.start(phone=PHONE)
    print("[+] Telegram client authenticated\n")

    results = {
        "investigation": "Berlin Grid Attack — Operation EG VOLT",
        "collection_type": "Enhanced Phone Number & Contact Infrastructure Search",
        "search_timestamp": datetime.now().isoformat(),
        "search_period": {
            "start": SEARCH_START.isoformat(),
            "end": SEARCH_END.isoformat(),
        },
        "target_phones": PHONE_QUERIES[:18],  # Phone numbers only
        "target_entities": ENTITY_QUERIES,
        "channels": {},
    }

    for channel in TARGET_CHANNELS:
        await search_channel_phones(client, channel, results)
        await asyncio.sleep(1)

    # ─── Aggregate ───
    all_messages = []
    for ch_data in results["channels"].values():
        all_messages.extend(ch_data.get("messages", []))

    # Sort by relevance score
    all_messages.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)

    results["statistics"] = {
        "total_messages": len(all_messages),
        "high_relevance_count": sum(1 for m in all_messages if m.get("relevance_score", 0) >= 3),
        "known_phone_hits": sum(1 for m in all_messages if any("KNOWN_PHONE" in i for i in m.get("indicators", []))),
        "entity_hits": sum(1 for m in all_messages if any("ENTITY" in i for i in m.get("indicators", []))),
        "person_hits": sum(1 for m in all_messages if any("PERSON" in i for i in m.get("indicators", []))),
        "top_findings": all_messages[:20],
    }

    # ─── Save ───
    output_dir = "reports/20260213_142123_berlin_grid_attack_eg_volt/osint_construction"
    raw_dir = "reports/20260213_142123_berlin_grid_attack_eg_volt/raw_data"
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(raw_dir, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = os.path.join(output_dir, f"12_telegram_phone_entity_search_{ts}.json")
    raw_file = os.path.join(raw_dir, f"telegram_phone_entity_search_{ts}.json")

    for fp in [out_file, raw_file]:
        with open(fp, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

    # ─── Summary ───
    print("\n" + "=" * 70)
    print("  COLLECTION SUMMARY")
    print("=" * 70)
    stats = results["statistics"]
    print(f"  Total Messages:          {stats['total_messages']}")
    print(f"  High Relevance (≥3):     {stats['high_relevance_count']}")
    print(f"  Known Phone Hits:        {stats['known_phone_hits']}")
    print(f"  Entity Name Hits:        {stats['entity_hits']}")
    print(f"  Person Name Hits:        {stats['person_hits']}")

    print(f"\n  Channel Status:")
    for ch_name, ch_data in results["channels"].items():
        status = ch_data.get("status", "unknown")
        total = ch_data.get("total_messages", 0)
        emoji = "✅" if status == "accessible" else "❌"
        print(f"    {emoji} {ch_name}: {status} ({total} messages)")

    if stats["high_relevance_count"] > 0:
        print(f"\n  🔴 TOP FINDINGS:")
        for finding in stats["top_findings"][:10]:
            print(f"    Score {finding['relevance_score']}: [{finding['date'][:10]}] "
                  f"{finding['indicators']}")
            print(f"      {finding['text'][:150]}...")

    print(f"\n  [+] Results: {out_file}")
    print(f"  [+] Raw data: {raw_file}")

    await client.disconnect()
    print("\n[+] Done.")


if __name__ == "__main__":
    asyncio.run(main())
