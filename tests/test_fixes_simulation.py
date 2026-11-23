"""
Comprehensive Verification & Simulation Test for Fixes:
1. Creator & Target Channels Immunity (Faten's issue)
2. Trading Signals / Deals Protection in Host Channels
3. Accurate Manual Ad & Paired Sticker Detection
4. Modular Permutation Engine Output & Diversity
"""

import sys
import os
from unittest.mock import MagicMock, AsyncMock
from cryptography.fernet import Fernet

os.environ['SESSION_ENCRYPTION_KEY'] = Fernet.generate_key().decode()

sys.path.append('d:/telegram_ad_exchange_saas app/telegram_ad_exchange_saas')

import asyncio
from permutation_ad_engine import permutation_engine
from worker import sweep_single_channel, format_user_template

class FakeMessage:
    def __init__(self, msg_id, text=None, caption=None, outgoing=True, sticker=None, forward_from_chat=None, entities=None, caption_entities=None):
        self.id = msg_id
        self.text = text
        self.caption = caption
        self.outgoing = outgoing
        self.from_user = MagicMock()
        self.from_user.is_self = outgoing
        self.from_user.id = 12345
        self.sticker = sticker
        self.forward_from_chat = forward_from_chat
        self.forward_from = None
        self.reply_markup = None
        self.poll = None
        self.author_signature = None
        self.entities = entities or []
        self.caption_entities = caption_entities or []

class FakeSticker:
    def __init__(self, file_unique_id):
        self.file_unique_id = file_unique_id

class FakeChat:
    def __init__(self, cid, username="my_channel", invite_link="https://t.me/+my_invite"):
        self.id = cid
        self.username = username
        self.invite_link = invite_link
        self.pinned_message = None

class FakeClient:
    def __init__(self, history):
        self.history = history
        self.me = MagicMock()
        self.me.id = 12345
        self.me.first_name = "Tamer"
        self.me.last_name = "Trader"
        self.me.username = "tamertrader"
        self.deleted_msgs = []

    async def get_chat_history(self, chat_id, limit=25):
        for msg in self.history:
            yield msg

    async def get_chat(self, chat_id):
        return FakeChat(chat_id)

    async def delete_messages(self, chat_id, message_ids):
        self.deleted_msgs.extend(message_ids)

async def test_faten_creator_channel_immunity():
    print("Testing Test 1: Faten Creator Channel Immunity...")
    # Channel owned by user (is_creator = True)
    ch = {"id": -1001111111111, "title": "Faten Gold Channel", "is_creator": True, "is_broadcast": True}
    
    # History contains:
    # 1. Her own trading post with promo words: "انضموا لجروب VIP صفقات الذهب خصم خاص" (msg 101)
    # 2. Her analysis with links: "شارت الذهب اليوم ومقاومة 2700 https://t.me/my_channel" (msg 102)
    # 3. An automated ad previously recorded in known_msg_ids (msg 103)
    msg_owner_deal = FakeMessage(101, text="انضموا لجروب VIP صفقات الذهب اليوم خصم خاص https://t.me/+random")
    msg_owner_analysis = FakeMessage(102, text="شارت الذهب اليوم ومقاومة 2700 https://t.me/my_channel")
    msg_automated_ad = FakeMessage(103, text="إعلان تبادلي مسجل بالسيرفر")
    
    history = [msg_owner_deal, msg_owner_analysis, msg_automated_ad]
    client = FakeClient(history)
    known_msg_ids = {(-1001111111111, 103)} # Only 103 is recorded in DB
    
    deleted = await sweep_single_channel(
        client=client,
        ch=ch,
        known_msg_ids=known_msg_ids,
        sticker_unique_id="ad_sticker_999",
        me=client.me,
        ad_keywords=["vip", "خصم", "ذهب"],
        target_ids=set()
    )
    
    assert deleted == 1, f"Expected 1 deleted message (only automated ad), got {deleted}"
    assert client.deleted_msgs == [103], f"Expected deleted [103], got {client.deleted_msgs}"
    print("  PASS: Owner channel content (101, 102) 100% PROTECTED! Only recorded ad (103) deleted.\n")

async def test_target_channel_immunity():
    print("Testing Test 2: Target Channel Immunity...")
    # Channel is being advertised (in target_ids)
    target_cid = -1002222222222
    ch = {"id": target_cid, "title": "Egyptian King", "is_creator": False, "is_broadcast": True}
    
    msg_owner_post = FakeMessage(201, text="توصيات مجانية للاعضاء اشترك الان في القناة")
    msg_ad = FakeMessage(202, text="اعلان مسجل")
    
    history = [msg_owner_post, msg_ad]
    client = FakeClient(history)
    known_msg_ids = {(target_cid, 202)}
    
    deleted = await sweep_single_channel(
        client=client,
        ch=ch,
        known_msg_ids=known_msg_ids,
        sticker_unique_id=None,
        me=client.me,
        ad_keywords=["اشترك"],
        target_ids={target_cid}
    )
    
    assert deleted == 1, f"Expected 1, got {deleted}"
    assert client.deleted_msgs == [202], f"Expected [202], got {client.deleted_msgs}"
    print("  PASS: Target channel content 100% PROTECTED!\n")

async def test_deals_protection_in_host_channels():
    print("Testing Test 3: Deals & Signals Protection in Host Channels...")
    host_cid = -1003333333333
    ch = {"id": host_cid, "title": "Host Exchange Channel", "is_creator": False, "is_broadcast": True}
    
    # 1. Trading signal with buy/sell/tp/sl: MUST NOT BE DELETED
    msg_deal = FakeMessage(301, text="XAUUSD BUY NOW 2650\nSL: 2645\nTP1: 2655\nTP2: 2665\nانضموا للفرصة")
    # 2. Manual exchange ad with external invite link and promo CTA: MUST BE DELETED
    msg_manual_ad = FakeMessage(302, text="فرصة ذهبية لا تعوض، انضموا لقناة النخبة عبر الرابط الخاص:\nhttps://t.me/+exclusive_link")
    
    history = [msg_deal, msg_manual_ad]
    client = FakeClient(history)
    known_msg_ids = set() # No DB record, checking heuristic logic
    
    deleted = await sweep_single_channel(
        client=client,
        ch=ch,
        known_msg_ids=known_msg_ids,
        sticker_unique_id=None,
        me=client.me,
        ad_keywords=["فرصة", "انضم"],
        target_ids=set()
    )
    
    assert deleted == 1, f"Expected 1 (only manual ad), got {deleted}"
    assert client.deleted_msgs == [302], f"Expected [302], got {client.deleted_msgs}"
    print("  PASS: Trading signal (301) 100% PROTECTED! Manual exchange ad (302) correctly deleted.\n")

async def test_paired_sticker_safety():
    print("Testing Test 4: Paired Sticker Safety...")
    host_cid = -1004444444444
    ch = {"id": host_cid, "title": "Host Exchange Channel", "is_creator": False, "is_broadcast": True}
    
    # Ad accompanied by verified ad sticker:
    verified_sticker = FakeSticker("ad_sticker_vip_123")
    random_sticker = FakeSticker("random_emoji_sticker_555")
    
    msg_ad_sticker = FakeMessage(401, sticker=verified_sticker)
    msg_ad_text = FakeMessage(402, text="انضموا الآن للقناة الخاصة 👇\nhttps://t.me/+join_now")
    msg_random_sticker = FakeMessage(403, sticker=random_sticker)
    msg_normal_chat = FakeMessage(404, text="شكراً جزيلاً لكم")
    
    history = [msg_ad_sticker, msg_ad_text, msg_random_sticker, msg_normal_chat]
    client = FakeClient(history)
    known_msg_ids = set()
    
    deleted = await sweep_single_channel(
        client=client,
        ch=ch,
        known_msg_ids=known_msg_ids,
        sticker_unique_id="ad_sticker_vip_123",
        me=client.me,
        ad_keywords=["انضم"],
        target_ids=set()
    )
    
    # Both 401 (verified sticker) and 402 (ad text) should be deleted. 403 (random sticker) and 404 must NOT be touched.
    assert 401 in client.deleted_msgs, "Verified sticker should be deleted"
    assert 402 in client.deleted_msgs, "Ad text should be deleted"
    assert 403 not in client.deleted_msgs, "Random sticker must NOT be deleted"
    assert 404 not in client.deleted_msgs, "Normal message must NOT be deleted"
    print("  PASS: Verified ad sticker package deleted, unrelated sticker/text 100% PROTECTED!\n")

def test_permutation_engine():
    print("Testing Test 5: Modular Permutation Engine Diversity & Formatting...")
    assert permutation_engine.total_permutations >= 200000, f"Expected >= 200,000, got {permutation_engine.total_permutations}"
    
    generated = set()
    for i in range(100):
        tmpl = permutation_engine.generate_raw_template(seed=i)
        ad = format_user_template(tmpl, "VIP GOLD TRADING", "https://t.me/+GOLD123", members_count=18500)
        assert "https://t.me/+GOLD123" in ad, "Link must be present"
        assert len(ad) > 50, "Ad length must be substantial"
        generated.add(ad)
    
    assert len(generated) == 100, f"Expected 100 unique ads from 100 seeds, got {len(generated)}"
    print(f"  PASS: Generated 100 unique, well-formatted ads without any duplicates! Total pool: {permutation_engine.total_permutations:,}\n")

async def main():
    print("=" * 70)
    print("STARTING COMPREHENSIVE VERIFICATION SUITE")
    print("=" * 70)
    await test_faten_creator_channel_immunity()
    await test_target_channel_immunity()
    await test_deals_protection_in_host_channels()
    await test_paired_sticker_safety()
    test_permutation_engine()
    print("=" * 70)
    print("ALL 5 TESTS PASSED WITH 100% SUCCESS! MATHEMATICALLY & LOGICALLY VERIFIED.")
    print("=" * 70)

if __name__ == "__main__":
    asyncio.run(main())
