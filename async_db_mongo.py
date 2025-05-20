from typing import Any, Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection


class AsyncMongoDB:
    """
    通用的 MongoDB 异步操作封装类。

    Attributes:
        mongo_client (AsyncIOMotorClient): Motor 异步客户端实例
        db_name (str): 默认操作的数据库名称
    """

    def __init__(self, mongo_client: AsyncIOMotorClient, db_name: str):
        """
        初始化 MongoDB 客户端并指定数据库。

        Args:
            mongo_client (AsyncIOMotorClient): Mongo 客户端
            db_name (str): 默认操作的数据库名称
        """
        self.mongo_client: AsyncIOMotorClient = mongo_client
        self.db_name: str = db_name

    def _get_collection(self, collection_name: str) -> AsyncIOMotorCollection:
        """
        获取指定集合的引用（无需 await）。

        Args:
            collection_name (str): 集合名称
        Returns:
            AsyncIOMotorCollection: 集合对象
        """
        return self.mongo_client[self.db_name][collection_name]

    # -------------------- 插入操作 --------------------

    async def insert_one(self, collection_name: str, document: Dict[str, Any]) -> Any:
        """
        插入单个文档。

        Returns:
            InsertOneResult.inserted_id
        """
        coll = self._get_collection(collection_name)
        result = await coll.insert_one(document)
        return result.inserted_id

    async def insert_many(self, collection_name: str, documents: List[Dict[str, Any]]) -> List[Any]:
        """
        批量插入文档。

        Returns:
            InsertManyResult.inserted_ids
        """
        coll = self._get_collection(collection_name)
        result = await coll.insert_many(documents)
        return result.inserted_ids

    # -------------------- 查询操作 --------------------

    async def find_one(
            self,
            collection_name: str,
            filter: Dict[str, Any],
            projection: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        查询单个文档。

        Args:
            filter: 查询条件
            projection: 投影字段，如 {"_id": 0, "name": 1}
        Returns:
            匹配的文档或 None
        """
        coll = self._get_collection(collection_name)
        return await coll.find_one(filter, projection)

    async def find_many(
            self,
            collection_name: str,
            filter: Dict[str, Any],
            projection: Optional[Dict[str, Any]] = None,
            sort: Optional[List[tuple]] = None,
            skip: int = 0,
            limit: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        查询多个文档。

        Args:
            sort: 排序列表，如 [("created_at", -1)]
            skip: 跳过文档数
            limit: 限制返回条数
        Returns:
            文档列表
        """
        coll = self._get_collection(collection_name)
        cursor = coll.find(filter, projection)
        if sort:
            cursor = cursor.sort(sort)
        if skip:
            cursor = cursor.skip(skip)
        if limit:
            cursor = cursor.limit(limit)
        return await cursor.to_list(length=limit or 100)

    # -------------------- 更新操作 --------------------

    async def update_one(
            self,
            collection_name: str,
            filter: Dict[str, Any],
            update: Dict[str, Any],
            upsert: bool = False
    ) -> Dict[str, int]:
        """
        更新单个文档。

        Returns:
            {"matched_count": int, "modified_count": int, "upserted_id": ObjectId|None}
        """
        coll = self._get_collection(collection_name)
        result = await coll.update_one(filter, update, upsert=upsert)
        return {
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
            "upserted_id": result.upserted_id,
        }

    async def update_many(
            self,
            collection_name: str,
            filter: Dict[str, Any],
            update: Dict[str, Any],
            upsert: bool = False
    ) -> Dict[str, int]:
        """
        批量更新文档。

        Returns:
            {"matched_count": int, "modified_count": int}
        """
        coll = self._get_collection(collection_name)
        result = await coll.update_many(filter, update, upsert=upsert)
        return {
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
        }

    # -------------------- 删除操作 --------------------

    async def delete_one(self, collection_name: str, filter: Dict[str, Any]) -> int:
        """
        删除单个文档。

        Returns:
            删除数量 (0 或 1)
        """
        coll = self._get_collection(collection_name)
        result = await coll.delete_one(filter)
        return result.deleted_count

    async def delete_many(self, collection_name: str, filter: Dict[str, Any]) -> int:
        """
        批量删除文档。

        Returns:
            删除数量
        """
        coll = self._get_collection(collection_name)
        result = await coll.delete_many(filter)
        return result.deleted_count

    # -------------------- 索引与聚合 --------------------

    async def create_index(
            self,
            collection_name: str,
            keys: List[tuple],
            **kwargs
    ) -> str:
        """
        为集合创建索引。

        Args:
            keys: 索引字段列表，如 [("post_id", 1)]
            kwargs: 其他 create_index 参数（unique, name 等）
        Returns:
            索引名称
        """
        coll = self._get_collection(collection_name)
        return await coll.create_index(keys, **kwargs)

    async def aggregate(
            self,
            collection_name: str,
            pipeline: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        执行聚合管道。

        Returns:
            聚合结果列表
        """
        coll = self._get_collection(collection_name)
        cursor = coll.aggregate(pipeline)
        return await cursor.to_list(length=None)

    # -------------------- 客户端管理 --------------------

    def close(self):
        """
        关闭底层连接池（同步方法）。
        """
        self.mongo_client.close()
