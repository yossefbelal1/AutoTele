import sys, logging
logging.basicConfig(level=logging.DEBUG)

print("1. Importing main_api...")
sys.stdout.flush()

import main_api

print("2. main_api imported successfully!")
sys.stdout.flush()

import asyncio

async def test_startup():
    print("3. Calling on_startup()...")
    sys.stdout.flush()
    await main_api.on_startup()
    print("4. on_startup() completed successfully!")
    sys.stdout.flush()

asyncio.run(test_startup())
