import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock

import worker

async def test_chunking():
    print("Testing reply_long_message chunking behavior in Docker...")
    mock_message = AsyncMock()
    dummy_lines = ["A" * 100 for _ in range(50)]
    await worker.reply_long_message(mock_message, dummy_lines)
    print("Number of replies sent:", mock_message.reply_text.call_count)
    assert mock_message.reply_text.call_count == 2
    for call in mock_message.reply_text.call_args_list:
        text = call[0][0]
        print(f"Chunk sent of length: {len(text)}")
        assert len(text) <= 4000
    print("✅ reply_long_message chunking tests passed inside Docker!")

if __name__ == "__main__":
    asyncio.run(test_chunking())
