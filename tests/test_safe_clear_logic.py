import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock

sys.path.append(".")
from worker import sweep_single_channel

async def test_safe_sweep():
    print("=== Running Safe Clear Sweep Logic Tests ===")

    client = AsyncMock()
    me = MagicMock()
    me.id = 999
    me.first_name = "MyBot"
    me.last_name = "Owner"
    me.username = "mybot_user"
    client.me = me

    known_msg_ids = {(100, 1), (200, 10)}
    sticker_unique_id = "our_unique_ad_sticker"
    ad_keywords = ["إعلان", "تبادل", "توصيات"]

    class MockMsg:
        def __init__(self, id, outgoing=False, text=None, caption=None, sticker=None, 
                     from_user=None, author_signature=None, poll=None, forward_from_chat=None,
                     forward_from=None, reply_markup=None):
            self.id = id
            self.outgoing = outgoing
            self.text = text
            self.caption = caption
            self.sticker = sticker
            self.from_user = from_user
            self.author_signature = author_signature
            self.poll = poll
            self.forward_from_chat = forward_from_chat
            self.forward_from = forward_from
            self.reply_markup = reply_markup

    # ----------------------------------------------------
    # TEST 1: Channel where client is NOT creator (قنوات الناس)
    # ----------------------------------------------------
    other_ch = {
        "id": 200,
        "is_creator": False,
        "is_group": False,
        "is_broadcast": True,
        "username": "other_channel",
        "invite_link": "https://t.me/other_channel"
    }

    messages_in_other_ch = [
        MockMsg(id=10, outgoing=False, text="Our ad from DB"),
        MockMsg(id=11, outgoing=False, text="Owner ad https://t.me/+xyz اشترك الآن"),
        MockMsg(id=12, outgoing=False, text="Admin post without links"),
        MockMsg(id=13, outgoing=False, sticker=MagicMock(file_unique_id=sticker_unique_id))
    ]

    deleted_in_other_ch = []
    async def mock_history_other(chat_id, limit=20):
        for m in messages_in_other_ch:
            yield m
    client.get_chat_history = mock_history_other
    client.get_chat = AsyncMock(return_value=MagicMock(pinned_message=None))

    async def mock_delete_other(chat_id, message_ids):
        deleted_in_other_ch.extend(message_ids)
        return True
    client.delete_messages = mock_delete_other

    await sweep_single_channel(client, other_ch, known_msg_ids, sticker_unique_id, me, ad_keywords)

    print(f"Test 1 (Partner Channel) deleted: {deleted_in_other_ch}")
    assert 10 in deleted_in_other_ch, "Known DB ad must be deleted"
    assert 13 in deleted_in_other_ch, "Our unique sticker ad must be deleted"
    assert 11 not in deleted_in_other_ch, "Partner channel owner/admin ad MUST NOT be deleted"
    assert 12 not in deleted_in_other_ch, "Partner channel content MUST NOT be deleted"
    print("✓ Test 1 Passed: Partner channels are 100% protected!")

    # ----------------------------------------------------
    # TEST 2: Group chat (مجموعة / جروب)
    # ----------------------------------------------------
    group_ch = {
        "id": 300,
        "is_creator": False,
        "is_group": True,
        "is_broadcast": False,
    }

    other_user = MagicMock(id=888, is_self=False)
    my_user = MagicMock(id=999, is_self=True)

    messages_in_group = [
        MockMsg(id=20, outgoing=False, from_user=other_user, text="Other user ad https://t.me/+123 اشترك"),
        MockMsg(id=21, outgoing=False, from_user=other_user, text="Other user normal chat"),
        MockMsg(id=22, outgoing=True, from_user=my_user, text="Our ad https://t.me/+joinchat_test اشترك"),
    ]

    deleted_in_group = []
    async def mock_history_group(chat_id, limit=20):
        for m in messages_in_group:
            yield m
    client.get_chat_history = mock_history_group
    async def mock_delete_group(chat_id, message_ids):
        deleted_in_group.extend(message_ids)
        return True
    client.delete_messages = mock_delete_group

    await sweep_single_channel(client, group_ch, known_msg_ids, sticker_unique_id, me, ad_keywords)

    print(f"Test 2 (Group) deleted: {deleted_in_group}")
    assert 20 not in deleted_in_group, "Other user's message in group MUST NOT be deleted"
    assert 21 not in deleted_in_group, "Other user's message in group MUST NOT be deleted"
    assert 22 in deleted_in_group, "Our outgoing ad in group should be deleted"
    print("✓ Test 2 Passed: Other users' messages in groups are 100% protected!")

    # ----------------------------------------------------
    # TEST 3: Client's Own Channel (قناة العميل الخاصة - is_creator: True)
    # ----------------------------------------------------
    my_ch = {
        "id": 100,
        "is_creator": True,
        "is_group": False,
        "is_broadcast": True,
        "username": "my_vip_channel",
        "invite_link": "https://t.me/+my_channel_invite"
    }

    class MockBtn:
        def __init__(self, url):
            self.url = url
    class MockMarkup:
        def __init__(self, url):
            self.inline_keyboard = [[MockBtn(url)]]

    messages_in_my_ch = [
        # 30: Pinned post -> MUST NOT BE DELETED
        MockMsg(id=30, text="قوانين القناة المثبتة والشروط"),
        # 31: Poll -> MUST NOT BE DELETED
        MockMsg(id=31, text="ما رأيكم في تحليل الذهب اليوم؟", poll=MagicMock()),
        # 32: Normal post: Market analysis with NO external links -> MUST NOT BE DELETED
        MockMsg(id=32, text="تحليل الذهب اليوم: نتوقع الصعود نحو مستويات 2650 مع ثبات الدعم 2630."),
        # 33: Normal post: self-referencing channel link -> MUST NOT BE DELETED
        MockMsg(id=33, text="تابعوا قناتنا الرسمية: https://t.me/my_vip_channel أو الرابط https://t.me/+my_channel_invite"),
        # 34: Manual ad: External invite link (t.me/+...) -> MUST BE DELETED
        MockMsg(id=34, text="قناة توصيات VIP ممتازة انضموا الآن: https://t.me/+external_channel_invite"),
        # 35: Manual ad: External link + promotional keywords/emojis -> MUST BE DELETED
        MockMsg(id=35, text="أقوى صفقات العملات الرقمية 🔥 تابعوا الرابط https://t.me/other_crypto_channel"),
        # 36: Manual ad: Forward from external channel -> MUST BE DELETED
        MockMsg(id=36, text="فرصة ذهبية للاستثمار", forward_from_chat=MagicMock(id=99999)),
        # 37: Manual ad: Inline button with external link -> MUST BE DELETED
        MockMsg(id=37, text="اضغط على الزر للاشتراك", reply_markup=MockMarkup("https://t.me/external_bot?start=123")),
    ]

    deleted_in_my_ch = []
    async def mock_history_my_ch(chat_id, limit=20):
        for m in messages_in_my_ch:
            yield m
    client.get_chat_history = mock_history_my_ch
    client.get_chat = AsyncMock(return_value=MagicMock(pinned_message=MagicMock(id=30)))
    async def mock_delete_my_ch(chat_id, message_ids):
        deleted_in_my_ch.extend(message_ids)
        return True
    client.delete_messages = mock_delete_my_ch

    await sweep_single_channel(client, my_ch, known_msg_ids, sticker_unique_id, me, ad_keywords)

    print(f"Test 3 (My Channel) deleted: {deleted_in_my_ch}")
    assert 30 not in deleted_in_my_ch, "Pinned message MUST be preserved"
    assert 31 not in deleted_in_my_ch, "Poll MUST be preserved"
    assert 32 not in deleted_in_my_ch, "Normal market analysis MUST be preserved"
    assert 33 not in deleted_in_my_ch, "Self channel links MUST be preserved"
    assert 34 in deleted_in_my_ch, "Manual external invite ad MUST be deleted"
    assert 35 in deleted_in_my_ch, "Manual external link + keywords ad MUST be deleted"
    assert 36 in deleted_in_my_ch, "External forward ad MUST be deleted"
    assert 37 in deleted_in_my_ch, "External inline button ad MUST be deleted"
    print("✓ Test 3 Passed: Manual ads deleted while normal posts, analysis, and channel links are 100% preserved!")

    print("\nALL CLEAR LOGIC SAFETY TESTS PASSED SUCCESSFULLY! 🎉")

if __name__ == "__main__":
    asyncio.run(test_safe_sweep())
