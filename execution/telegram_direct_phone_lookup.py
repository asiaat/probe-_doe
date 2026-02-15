#!/usr/bin/env python3
"""
Direct Telegram phone number lookup
"""

import asyncio
import os
from telethon import TelegramClient
from telethon.tl.functions.contacts import ResolvePhoneRequest
from dotenv import load_dotenv

load_dotenv()

API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('TELEGRAM_PHONE')

TARGETS = [
    "+447881228865",
    "+447777213139",
]

async def main():
    client = TelegramClient('osint_session', int(API_ID), API_HASH)
    await client.start(phone=PHONE)
    print("[+] Connected\n")

    for target in TARGETS:
        print(f"[*] Looking up: {target}")
        try:
            result = await client(ResolvePhoneRequest(phone=target))
            user = result.user
            print(f"  [+] FOUND: {user.first_name} {user.last_name or ''}")
            print(f"      Username: @{user.username or 'N/A'}")
            print(f"      Phone: {user.phone}")
            print(f"      ID: {user.id}")
            print(f"      Status: {user.status}")
        except Exception as e:
            print(f"  [-] Not found: {e}")
        print()

    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
