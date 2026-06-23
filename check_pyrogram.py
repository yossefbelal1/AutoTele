import pyrogram
from pyrogram.types import Message
print("Attributes in Message:")
for attr in sorted(dir(Message)):
    if not attr.startswith("_"):
        print(f"  {attr}")


