#!/usr/bin/env python3
"""
Telegram OSINT: @Niemci & DE4RU Channel Monitoring
Investigation: Berlin Grid Attack (Operation EG VOLT)
Focus: Historical construction job posts (Oct 2025 – Jan 2026)
Channels: @Niemci (Layboard Germany), DE4RU (Работа в Германии)
Compliance: OSINT SOP v2.3 / Persistence SOP v1.0
"""

import asyncio
import json
import os
import hashlib
from datetime import datetime, timezone
from telethon import TelegramClient
from telethon.tl.functions.messages import SearchRequest, GetHistoryRequest
from telethon.tl.types import InputMessagesFilterEmpty
from dotenv import load_dotenv

# ─── Configuration ───
load_dotenv()

API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('TELEGRAM_PHONE')

# Investigation window: October 1, 2025 – January 10, 2026
SEARCH_START = datetime(2025, 10, 1, tzinfo=timezone.utc)
SEARCH_END = datetime(2026, 1, 10, tzinfo=timezone.utc)

# ─── Target Channels ───
# Primary investigation channels identified in OSINT reports
TARGET_CHANNELS = [
    # PRIMARY TARGETS (from Master Intelligence Report)
    "@Niemci",               # Layboard Germany official channel
    "@de4ru",                # DE4RU — Работа в Германии
    "@de4ru_jobs",           # DE4RU jobs variant
    "@de4ru_rabota",         # DE4RU work variant
    "@rabota_v_germanii",    # "Work in Germany" (Russian)
    "@rabotavgermanii",      # variant spelling

    # SECONDARY TARGETS (Ukrainian worker communities)
    "@germany_ua",           # Germany-Ukraine community
    "@ukraine_germany",      # Ukraine-Germany channel
    "@ukr_de",               # Ukrainian in Germany
    "@praca_niemcy",         # "Work Germany" (Polish)
    "@rabota_berlin",        # Work in Berlin (Russian)
    "@berlin_rabota",        # Berlin work variant

    # TERTIARY TARGETS (Construction-specific)
    "@bau_arbeit",           # Construction work
    "@montage_germany",      # Assembly/installation Germany
    "@stroyka_germaniya",    # Construction Germany (Russian)
    "@job_germany_ru",       # Russian-language job board Germany
    "@niemcy_praca",         # Polish work in Germany
]

# ─── Search Queries ───
# Organized by investigation priority

# Priority 1: Direct location matches (Lichterfelde, Steglitz, South Berlin)
LOCATION_QUERIES = [
    "Lichterfelde",
    "Steglitz",
    "Zehlendorf",
    "Süd Berlin",
    "Tempelhof",
    "Hindenburgdamm",
    "Ostpreußendamm",
    "Лихтерфельде",           # Lichterfelde in Russian
    "юг Берлина",             # South Berlin
    "Штеглиц",               # Steglitz in Russian
]

# Priority 2: Infrastructure / electrical work
INFRASTRUCTURE_QUERIES = [
    "Stromnetz",
    "Kabelverlegung",         # Cable laying
    "Elektriker Berlin",      # Electrician Berlin
    "Monteur Berlin",         # Installer Berlin
    "электрик Берлин",        # Electrician Berlin (Ru)
    "монтажник Берлин",       # Installer Berlin (Ru)
    "кабельщик",              # Cable worker (Ru)
    "кабельные работы",       # Cable work (Ru)
    "электромонтаж",          # Electrical installation (Ru)
    "Tiefbau Berlin",         # Underground construction
    "высоковольтный",         # High-voltage (Ru)
    "подстанция",             # Substation (Ru)
    "Fernwärme",              # District heating
]

# Priority 3: Construction helper / general
CONSTRUCTION_QUERIES = [
    "Bauhelfer Berlin",       # Construction helper Berlin
    "Bauarbeiter Berlin",     # Construction worker Berlin
    "Gerüstbauer Berlin",     # Scaffolder Berlin
    "подсобник Берлин",       # Helper Berlin (Ru)
    "строитель Берлин",       # Builder Berlin (Ru)
    "без немецкого Берлин",   # Without German Berlin (Ru)
    "проживание Берлин",      # Accommodation Berlin (Ru)
    "жилье предоставляется",  # Housing provided (Ru)
    "scaffolder Berlin",
    "installer Berlin",
    "Helfer Berlin",
]

# Priority 4: Agency and recruitment terms
AGENCY_QUERIES = [
    "JPG Recruitment",
    "ITAAR",
    "Sol-Tech",
    "Zeitarbeit Berlin",      # Temp work Berlin
    "staffing Berlin",
    "BauProfi",
    "MetallWeld",
    "Bonava Berlin",
    "Anastasia",              # Known recruiter name
    "Anatoliy",               # Known recruiter name
    "+44 7777",               # UK number pattern from Pink Salon
    "+44 7881",               # UK number pattern
    "+44 7944",               # ITAAR number
    "+44 7365",               # Known number
    "recruitment Berlin",
]

# Priority 5: Subcontractor names
SUBCONTRACTOR_QUERIES = [
    "STRABAG Berlin",
    "Stratieka",
    "Boran GmbH",
    "Planmann",
    "BEW Berlin",
    "Vattenfall Berlin",
    "Groth Gruppe",
]

ALL_QUERIES = (
    LOCATION_QUERIES +
    INFRASTRUCTURE_QUERIES +
    CONSTRUCTION_QUERIES +
    AGENCY_QUERIES +
    SUBCONTRACTOR_QUERIES
)

# ─── Red Flag Keywords ───
RED_FLAG_KEYWORDS = [
    # Location red flags
    "lichterfelde", "ostpreußendamm", "hindenburgdamm", "teltowkanal",
    "bäkestraße", "goerzallee", "heizkraftwerk", "kraftwerk",
    # Infrastructure red flags
    "stromnetz", "110kv", "hochspannung", "kabelbrücke", "cable bridge",
    "umspannwerk", "электросеть", "кабельный мост",
    # Recruitment red flags
    "pfalzburger", "jpg recruitment", "itaar", "+44 77", "+44 78", "+44 73",
    "anastasia", "anatoliy", "elmira",
    # Agency red flags
    "без документов", "без опыта", "жилье предоставляется",
    "housing provided", "no german required", "без немецкого",
    # Subcontractor red flags
    "strabag", "stratieka", "boran", "planmann",
]


def compute_doc_id(channel: str, message_id: int) -> str:
    """SHA256 hash for Elasticsearch dedup per Persistence SOP."""
    raw = f"{channel}:{message_id}"
    return hashlib.sha256(raw.encode()).hexdigest()


def classify_message(text: str) -> dict:
    """Classify a message by language, recruitment type, and red flags."""
    text_lower = text.lower() if text else ""

    # Language detection
    lang = "unknown"
    if any(w in text_lower for w in ["работа", "вакансия", "требуется", "зарплата", "опыт"]):
        lang = "russian"
    elif any(w in text_lower for w in ["робота", "вакансія", "зарплатня"]):
        lang = "ukrainian"
    elif any(w in text_lower for w in ["praca", "wynagrodzenie", "zatrudnienie"]):
        lang = "polish"
    elif any(w in text_lower for w in ["arbeit", "gesucht", "baustelle", "gehalt"]):
        lang = "german"
    else:
        lang = "other"

    # Recruitment type
    recruitment_type = "general"
    if any(w in text_lower for w in ["zeitarbeit", "personalvermittlung", "staffing", "recruitment", "agency"]):
        recruitment_type = "staffing_agency"
    elif any(w in text_lower for w in ["subunternehmer", "nachunternehmer", "подрядчик"]):
        recruitment_type = "subcontractor"
    elif any(w in text_lower for w in ["direkt", "einstellung", "прямой наём", "direct"]):
        recruitment_type = "direct_hire"
    elif any(w in text_lower for w in ["helfer", "подсобник", "helper", "pomoc"]):
        recruitment_type = "helper_role"

    # Red flag scoring
    red_flags_found = [kw for kw in RED_FLAG_KEYWORDS if kw in text_lower]
    red_flag_score = min(len(red_flags_found), 10)

    # Specific pattern detection
    contains_phone = any(c.isdigit() and text_lower.count(c) > 5 for c in "0123456789") or "+" in text_lower
    mentions_housing = any(w in text_lower for w in [
        "жилье", "проживание", "unterkunft", "housing", "wohnung", "zakwaterowanie"
    ])
    mentions_no_language = any(w in text_lower for w in [
        "без немецкого", "ohne deutsch", "no german", "без языка", "bez języka"
    ])

    return {
        "language": lang,
        "recruitment_type": recruitment_type,
        "red_flags": red_flags_found,
        "red_flag_score": red_flag_score,
        "contains_phone_number": contains_phone,
        "mentions_housing": mentions_housing,
        "mentions_no_language_required": mentions_no_language,
    }


async def search_channel(client, channel_name: str, queries: list, results: dict):
    """Search a single channel with all queries."""
    print(f"\n{'='*60}")
    print(f"[*] CHANNEL: {channel_name}")
    print(f"{'='*60}")

    channel_data = {
        "channel": channel_name,
        "status": "unknown",
        "total_messages": 0,
        "messages": [],
        "errors": [],
    }

    try:
        entity = await client.get_entity(channel_name)
        channel_data["status"] = "accessible"
        channel_data["channel_title"] = getattr(entity, 'title', channel_name)
        channel_data["channel_id"] = entity.id
        channel_data["participants_count"] = getattr(entity, 'participants_count', None)
        print(f"  [+] Connected: {channel_data['channel_title']} (ID: {entity.id})")
        if channel_data["participants_count"]:
            print(f"  [+] Members: {channel_data['participants_count']}")
    except Exception as e:
        channel_data["status"] = "inaccessible"
        channel_data["errors"].append(f"Cannot access channel: {str(e)}")
        print(f"  [-] FAILED: {e}")
        results["channels"][channel_name] = channel_data
        return

    seen_ids = set()

    for query in queries:
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
                if msg.date < SEARCH_START or msg.date > SEARCH_END:
                    continue

                seen_ids.add(msg.id)
                new_count += 1

                classification = classify_message(msg.message)

                message_data = {
                    "doc_id": compute_doc_id(channel_name, msg.id),
                    "message_id": msg.id,
                    "date": msg.date.isoformat(),
                    "text": msg.message,
                    "matched_query": query,
                    "views": getattr(msg, 'views', 0),
                    "forwards": getattr(msg, 'forwards', 0),
                    "reply_to": msg.reply_to.reply_to_msg_id if msg.reply_to else None,
                    **classification,
                }

                channel_data["messages"].append(message_data)

                # Print high-priority findings immediately
                if classification["red_flag_score"] >= 3:
                    print(f"  🔴 HIGH RED FLAG (score={classification['red_flag_score']}): "
                          f"msg_id={msg.id}, date={msg.date.date()}")
                    print(f"     Flags: {classification['red_flags']}")
                    print(f"     Text: {msg.message[:200]}...")

            if new_count > 0:
                print(f"  [+] Query '{query}': {new_count} new messages")

            # Rate limiting
            await asyncio.sleep(0.5)

        except Exception as e:
            err_msg = f"Query '{query}': {str(e)}"
            channel_data["errors"].append(err_msg)
            if "FLOOD" in str(e).upper():
                print(f"  [⚠] FLOOD WAIT — sleeping 30s")
                await asyncio.sleep(30)
            continue

    # Also pull recent channel history (last 500 messages in window)
    print(f"\n  [*] Pulling full channel history (within investigation window)...")
    try:
        history_offset_id = 0
        history_batch = 0
        while True:
            history = await client(GetHistoryRequest(
                peer=entity,
                offset_id=history_offset_id,
                offset_date=SEARCH_END,
                add_offset=0,
                limit=100,
                max_id=0,
                min_id=0,
                hash=0
            ))
            if not history.messages:
                break

            batch_new = 0
            for msg in history.messages:
                if msg.id in seen_ids:
                    continue
                if not msg.message:
                    continue
                if msg.date < SEARCH_START:
                    # We've gone past our window
                    history_offset_id = 0  # Signal to stop
                    break
                if msg.date > SEARCH_END:
                    continue

                seen_ids.add(msg.id)
                classification = classify_message(msg.message)

                # Only keep messages with construction/recruitment relevance
                text_lower = msg.message.lower()
                is_relevant = (
                    classification["red_flag_score"] > 0 or
                    any(kw in text_lower for kw in [
                        "bau", "elektr", "montag", "install", "gerüst", "scaffold",
                        "строй", "электр", "монтаж", "кабел", "подсоб",
                        "helfer", "arbeiter", "работ", "вакан",
                        "berlin", "берлин",
                    ])
                )

                if is_relevant:
                    batch_new += 1
                    message_data = {
                        "doc_id": compute_doc_id(channel_name, msg.id),
                        "message_id": msg.id,
                        "date": msg.date.isoformat(),
                        "text": msg.message,
                        "matched_query": "_history_scan",
                        "views": getattr(msg, 'views', 0),
                        "forwards": getattr(msg, 'forwards', 0),
                        "reply_to": msg.reply_to.reply_to_msg_id if msg.reply_to else None,
                        **classification,
                    }
                    channel_data["messages"].append(message_data)

                    if classification["red_flag_score"] >= 3:
                        print(f"  🔴 HISTORY HIT (score={classification['red_flag_score']}): "
                              f"msg_id={msg.id}, date={msg.date.date()}")
                        print(f"     Flags: {classification['red_flags']}")
                        print(f"     Text: {msg.message[:200]}...")

            history_batch += 1
            if batch_new > 0:
                print(f"  [+] History batch {history_batch}: {batch_new} relevant messages")

            if history_offset_id == 0:
                break  # Signal from inner loop
            history_offset_id = history.messages[-1].id

            # Cap at 2000 messages (20 batches)
            if history_batch >= 20:
                print(f"  [*] Hit history cap (2000 messages)")
                break

            await asyncio.sleep(0.3)

    except Exception as e:
        channel_data["errors"].append(f"History scan: {str(e)}")
        print(f"  [-] History scan error: {e}")

    channel_data["total_messages"] = len(channel_data["messages"])
    results["channels"][channel_name] = channel_data
    print(f"\n  [=] TOTAL for {channel_name}: {channel_data['total_messages']} messages collected")


async def main():
    print("=" * 70)
    print("  TELEGRAM OSINT: @Niemci & DE4RU Channel Monitoring")
    print("  Operation EG VOLT — Berlin Grid Attack Investigation")
    print(f"  Window: {SEARCH_START.date()} → {SEARCH_END.date()}")
    print(f"  Channels: {len(TARGET_CHANNELS)}")
    print(f"  Queries: {len(ALL_QUERIES)}")
    print("=" * 70)

    client = TelegramClient(
        'niemci_de4ru_session',
        int(API_ID),
        API_HASH
    )
    await client.start(phone=PHONE)
    print("[+] Telegram client authenticated\n")

    results = {
        "investigation": "Berlin Grid Attack — Operation EG VOLT",
        "collection_type": "Telegram Channel Monitoring (@Niemci, DE4RU)",
        "search_timestamp": datetime.now().isoformat(),
        "search_method": "Telethon API (SearchRequest + GetHistoryRequest)",
        "search_period": {
            "start": SEARCH_START.isoformat(),
            "end": SEARCH_END.isoformat(),
        },
        "target_channels": TARGET_CHANNELS,
        "query_categories": {
            "location": LOCATION_QUERIES,
            "infrastructure": INFRASTRUCTURE_QUERIES,
            "construction": CONSTRUCTION_QUERIES,
            "agency": AGENCY_QUERIES,
            "subcontractor": SUBCONTRACTOR_QUERIES,
        },
        "channels": {},
        "statistics": {
            "total_messages": 0,
            "total_red_flag_messages": 0,
            "by_language": {},
            "by_recruitment_type": {},
            "high_priority_findings": [],
        },
    }

    # Search all channels
    for channel in TARGET_CHANNELS:
        await search_channel(client, channel, ALL_QUERIES, results)
        await asyncio.sleep(1)  # Inter-channel rate limit

    # ─── Aggregate Statistics ───
    all_messages = []
    for ch_name, ch_data in results["channels"].items():
        all_messages.extend(ch_data.get("messages", []))

    results["statistics"]["total_messages"] = len(all_messages)

    # Language breakdown
    lang_counts = {}
    for m in all_messages:
        lang = m.get("language", "unknown")
        lang_counts[lang] = lang_counts.get(lang, 0) + 1
    results["statistics"]["by_language"] = lang_counts

    # Recruitment type breakdown
    type_counts = {}
    for m in all_messages:
        rt = m.get("recruitment_type", "unknown")
        type_counts[rt] = type_counts.get(rt, 0) + 1
    results["statistics"]["by_recruitment_type"] = type_counts

    # High-priority findings (red_flag_score >= 3)
    high_priority = [m for m in all_messages if m.get("red_flag_score", 0) >= 3]
    results["statistics"]["total_red_flag_messages"] = len(high_priority)
    results["statistics"]["high_priority_findings"] = sorted(
        high_priority, key=lambda x: x.get("red_flag_score", 0), reverse=True
    )[:50]  # Top 50

    # ─── Save Results ───
    output_dir = "reports/20260213_142123_berlin_grid_attack_eg_volt/osint_construction"
    os.makedirs(output_dir, exist_ok=True)

    # Raw data (Persistence SOP compliant)
    raw_dir = "reports/20260213_142123_berlin_grid_attack_eg_volt/raw_data"
    os.makedirs(raw_dir, exist_ok=True)

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"11_telegram_niemci_de4ru_{timestamp_str}.json")
    raw_file = os.path.join(raw_dir, f"telegram_niemci_de4ru_{timestamp_str}.json")

    for filepath in [output_file, raw_file]:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

    # ─── Print Summary ───
    print("\n" + "=" * 70)
    print("  COLLECTION SUMMARY")
    print("=" * 70)
    print(f"  Total Messages Collected:  {results['statistics']['total_messages']}")
    print(f"  Red Flag Messages (≥3):    {results['statistics']['total_red_flag_messages']}")
    print(f"\n  Language Breakdown:")
    for lang, count in sorted(lang_counts.items(), key=lambda x: -x[1]):
        print(f"    {lang}: {count}")
    print(f"\n  Recruitment Type Breakdown:")
    for rt, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"    {rt}: {count}")

    print(f"\n  Channel Status:")
    for ch_name, ch_data in results["channels"].items():
        status = ch_data.get("status", "unknown")
        total = ch_data.get("total_messages", 0)
        emoji = "✅" if status == "accessible" else "❌"
        print(f"    {emoji} {ch_name}: {status} ({total} messages)")

    if high_priority:
        print(f"\n  🔴 TOP RED FLAG FINDINGS:")
        for hp in results["statistics"]["high_priority_findings"][:10]:
            print(f"    Score {hp['red_flag_score']}: [{hp['date'][:10]}] "
                  f"{hp.get('channel', '?')}: {hp['text'][:120]}...")

    print(f"\n  [+] Results saved to: {output_file}")
    print(f"  [+] Raw data saved to: {raw_file}")

    await client.disconnect()
    print("\n[+] Done. Client disconnected.")


if __name__ == "__main__":
    asyncio.run(main())
