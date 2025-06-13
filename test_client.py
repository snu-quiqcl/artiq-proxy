import asyncio
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_connection():
    try:
        reader, writer = await asyncio.open_connection('127.0.0.1', 5000)
        logger.info("Connection established successfully.") 
        writer.close()
        await writer.wait_closed()
    except Exception as e:
        logger.error(f"Connection failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_connection())