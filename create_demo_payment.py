import asyncio
from db_manager import AsyncSessionLocal, CryptoPayment

async def main():
    async with AsyncSessionLocal() as session:
        payment = CryptoPayment(
            user_id=3,
            plan_selected="yearly",
            txid="tx_demo_pending_socks5",
            status="pending"
        )
        session.add(payment)
        await session.commit()
        print(f"Created demo pending payment ID: {payment.id} for User 3")

if __name__ == "__main__":
    asyncio.run(main())
