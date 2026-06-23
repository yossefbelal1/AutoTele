import asyncio
from db_manager import AsyncSessionLocal, CryptoPayment, select

async def main():
    async with AsyncSessionLocal() as session:
        res = await session.execute(select(CryptoPayment))
        payments = res.scalars().all()
        print(f"Total payments: {len(payments)}")
        for p in payments:
            print(f"ID: {p.id} | User ID: {p.user_id} | Plan: {p.plan_selected} | Status: {p.status} | TxID: {p.txid}")

if __name__ == "__main__":
    asyncio.run(main())
