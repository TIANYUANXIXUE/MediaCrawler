# db_mongo.py
import asyncio
import json

import aiofiles
from motor.motor_asyncio import AsyncIOMotorClient

from async_db_mongo import AsyncMongoDB
from config.db_config import MONGO_DB_HOST, MONGO_DB_PORT, MONGO_DB_NAME, MONGO_USER_NAME, MONGO_PASSWORD
from tools import utils
from var import db_mongo_client_var, media_crawler_mongo_db_var


async def init_mongo():
    """
    初始化 Mongo 客户端并存入 var.py 的全局变量中。
    """
    uri = f"mongodb://{MONGO_USER_NAME}:{MONGO_PASSWORD}@{MONGO_DB_HOST}:{MONGO_DB_PORT}"
    # 创建客户端 —— 连接池在幕后自动管理
    mongo_client = AsyncIOMotorClient(uri)
    async_db_mongo_obj = AsyncMongoDB(mongo_client, MONGO_DB_NAME)
    db_mongo_client_var.set(mongo_client)
    media_crawler_mongo_db_var.set(async_db_mongo_obj)


async def init_db():
    """
    初始化db连接池
    Returns:

    """
    utils.logger.info("[init_db] start init mongo db connect object")
    await init_mongo()
    utils.logger.info("[init_db] end init mongo db connect object")


async def close_mongo():
    """
    关闭 Mongo 客户端连接。
    """
    mongo_client: AsyncIOMotorClient = db_mongo_client_var.get()
    if mongo_client:
        mongo_client.close()


async def init_collection():
    """
    根据 config/mongo_collections.json 中的配置，逐个创建 collection 与索引。
    """
    await init_mongo()
    mongo_client: AsyncIOMotorClient = db_mongo_client_var.get()
    db = mongo_client[MONGO_DB_NAME]

    async with aiofiles.open("schema/mongo_collection.json", mode="r", encoding="utf-8") as f:
        coll_text = await f.read()
        coll_configs = json.loads(coll_text)

    for coll in coll_configs.get('collections', []):
        name = coll['name']
        options = coll.get('options', {})
        try:
            await db.create_collection(name, **options)
            utils.logger.info(f"Created collection '{name}'")
        except Exception as e:
            if 'already exists' in str(e):
                utils.logger.debug(f"Collection '{name}' already exists")
            else:
                utils.logger.error(f"Error creating collection '{name}': {e}")
                raise

        for idx in coll.get('indexes', []):
            keys = idx['keys']
            idx_opts = idx.get('options', {})
            await db[name].create_index(keys, **idx_opts)
            utils.logger.info(f"Created index on '{name}': {keys}")

        await close_mongo()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(init_collection())
