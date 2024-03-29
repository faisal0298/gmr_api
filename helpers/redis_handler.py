import asyncio
import async_timeout
import aioredis
from helpers.logger import console_logger
import json
import os

host = os.environ.get("HOST", "192.168.1.99")

STOPWORD = "STOP"
redis = aioredis.from_url(f"redis://{host}",password="91f7e0815477ae4e3ab95be4c9a513a1e69001b55a75a5360461b7b7ed34debf",decode_responses=True)
pubsub = redis.pubsub()

async def reader(channel: aioredis.client.PubSub):
    while True:
        try:
            async with async_timeout.timeout(1):
                message = await channel.get_message(ignore_subscribe_messages=True)
                if message is not None:
                    console_logger.debug(f"(Reader) Message Received: {message}")
                    if message["data"] == STOPWORD:
                        console_logger.debug("(Reader) STOP")
                        break
                await asyncio.sleep(0.01)
        except asyncio.TimeoutError:
            pass


async def redis_test():
    console_logger.debug(" ------- Redis Running ------- ")
    await pubsub.subscribe("GMR_AI",json.dumps({"actions":"DB_update","source":"gmr","added_obj":"id"}))
    asyncio.create_task(reader(pubsub))
    

# redis_test()
# asyncio.run(redis_test())
