import asyncio
from db_manager import async_engine
from sqlalchemy import inspect

async def main():
    async with async_engine.connect() as conn:
        def get_cols(connection):
            inst = inspect(connection)
            return [c['name'] for c in inst.get_columns("telegram_accounts")]
        columns = await conn.run_sync(get_cols)
        print("Columns in telegram_accounts table:")
        print(columns)
        assert "sticker_file_id" in columns, "sticker_file_id column missing!"
        print("Success: sticker_file_id is present in the database table!")

if __name__ == "__main__":
    asyncio.run(main())
