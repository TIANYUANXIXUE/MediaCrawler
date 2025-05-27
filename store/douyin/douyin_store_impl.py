# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：
# 1. 不得用于任何商业用途。
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。
# 3. 不得进行大规模爬取或对平台造成运营干扰。
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。
# 5. 不得用于任何非法或不当的用途。
#
# 详细许可条款请参阅项目根目录下的LICENSE文件。
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。


# -*- coding: utf-8 -*-
# @Author  : relakkes@gmail.com
# @Time    : 2024/1/14 18:46
# @Desc    : 抖音存储实现类
import asyncio
import csv
import json
import os
import pathlib

import aiofiles

import config
from base.base_crawler import AbstractStore
from tools import utils, words
from async_db_mongo import AsyncMongoDB
from var import crawler_type_var, media_crawler_mongo_db_var, note_id_list_all_var
from typing import Any, Dict
from uuid import uuid4
from datetime import datetime, timezone, timedelta

def calculate_number_of_files(file_store_path: str) -> int:
    """计算数据保存文件的前部分排序数字，支持每次运行代码不写到同一个文件中
    Args:
        file_store_path;
    Returns:
        file nums
    """
    if not os.path.exists(file_store_path):
        return 1
    try:
        return max([int(file_name.split("_")[0]) for file_name in os.listdir(file_store_path)]) + 1
    except ValueError:
        return 1


class DouyinCsvStoreImplement(AbstractStore):
    csv_store_path: str = "data/douyin"
    file_count: int = calculate_number_of_files(csv_store_path)

    def make_save_file_name(self, store_type: str) -> str:
        """
        make save file name by store type
        Args:
            store_type: contents or comments

        Returns: eg: data/douyin/search_comments_20240114.csv ...

        """
        return f"{self.csv_store_path}/{self.file_count}_{crawler_type_var.get()}_{store_type}_{utils.get_current_date()}.csv"

    async def save_data_to_csv(self, save_item: Dict, store_type: str):
        """
        Below is a simple way to save it in CSV format.
        Args:
            save_item:  save content dict info
            store_type: Save type contains content and comments（contents | comments）

        Returns: no returns

        """
        pathlib.Path(self.csv_store_path).mkdir(parents=True, exist_ok=True)
        save_file_name = self.make_save_file_name(store_type=store_type)
        async with aiofiles.open(save_file_name, mode='a+', encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            if await f.tell() == 0:
                await writer.writerow(save_item.keys())
            await writer.writerow(save_item.values())

    async def store_content(self, content_item: Dict):
        """
        Douyin content CSV storage implementation
        Args:
            content_item: note item dict

        Returns:

        """
        await self.save_data_to_csv(save_item=content_item, store_type="contents")

    async def store_comment(self, comment_item: Dict):
        """
        Douyin comment CSV storage implementation
        Args:
            comment_item: comment item dict

        Returns:

        """
        await self.save_data_to_csv(save_item=comment_item, store_type="comments")

    async def store_creator(self, creator: Dict):
        """
        Douyin creator CSV storage implementation
        Args:
            creator: creator item dict

        Returns:

        """
        await self.save_data_to_csv(save_item=creator, store_type="creator")


class DouyinDbStoreImplement(AbstractStore):
    async def store_content(self, content_item: Dict):
        """
        Douyin content DB storage implementation
        Args:
            content_item: content item dict

        Returns:

        """

        from .douyin_store_sql import (add_new_content,
                                       query_content_by_content_id,
                                       update_content_by_content_id)
        aweme_id = content_item.get("aweme_id")
        aweme_detail: Dict = await query_content_by_content_id(content_id=aweme_id)
        if not aweme_detail:
            content_item["add_ts"] = utils.get_current_timestamp()
            if content_item.get("title"):
                await add_new_content(content_item)
        else:
            await update_content_by_content_id(aweme_id, content_item=content_item)

    async def store_comment(self, comment_item: Dict):
        """
        Douyin content DB storage implementation
        Args:
            comment_item: comment item dict

        Returns:

        """
        from .douyin_store_sql import (add_new_comment,
                                       query_comment_by_comment_id,
                                       update_comment_by_comment_id)
        comment_id = comment_item.get("comment_id")
        comment_detail: Dict = await query_comment_by_comment_id(comment_id=comment_id)
        if not comment_detail:
            comment_item["add_ts"] = utils.get_current_timestamp()
            await add_new_comment(comment_item)
        else:
            await update_comment_by_comment_id(comment_id, comment_item=comment_item)

    async def store_creator(self, creator: Dict):
        """
        Douyin content DB storage implementation
        Args:
            creator: creator dict

        Returns:

        """
        from .douyin_store_sql import (add_new_creator,
                                       query_creator_by_user_id,
                                       update_creator_by_user_id)
        user_id = creator.get("user_id")
        user_detail: Dict = await query_creator_by_user_id(user_id)
        if not user_detail:
            creator["add_ts"] = utils.get_current_timestamp()
            await add_new_creator(creator)
        else:
            await update_creator_by_user_id(user_id, creator)

class DouyinJsonStoreImplement(AbstractStore):
    json_store_path: str = "data/douyin/json"
    words_store_path: str = "data/douyin/words"

    lock = asyncio.Lock()
    file_count: int = calculate_number_of_files(json_store_path)
    WordCloud = words.AsyncWordCloudGenerator()

    def make_save_file_name(self, store_type: str) -> (str,str):
        """
        make save file name by store type
        Args:
            store_type: Save type contains content and comments（contents | comments）

        Returns:

        """

        return (
            f"{self.json_store_path}/{crawler_type_var.get()}_{store_type}_{utils.get_current_date()}.json",
            f"{self.words_store_path}/{crawler_type_var.get()}_{store_type}_{utils.get_current_date()}"
        )
    async def save_data_to_json(self, save_item: Dict, store_type: str):
        """
        Below is a simple way to save it in json format.
        Args:
            save_item: save content dict info
            store_type: Save type contains content and comments（contents | comments）

        Returns:

        """
        pathlib.Path(self.json_store_path).mkdir(parents=True, exist_ok=True)
        pathlib.Path(self.words_store_path).mkdir(parents=True, exist_ok=True)
        save_file_name,words_file_name_prefix = self.make_save_file_name(store_type=store_type)
        save_data = []

        async with self.lock:
            if os.path.exists(save_file_name):
                async with aiofiles.open(save_file_name, 'r', encoding='utf-8') as file:
                    save_data = json.loads(await file.read())

            save_data.append(save_item)
            async with aiofiles.open(save_file_name, 'w', encoding='utf-8') as file:
                await file.write(json.dumps(save_data, ensure_ascii=False))

            if config.ENABLE_GET_COMMENTS and config.ENABLE_GET_WORDCLOUD:
                try:
                    await self.WordCloud.generate_word_frequency_and_cloud(save_data, words_file_name_prefix)
                except:
                    pass

    async def store_content(self, content_item: Dict):
        """
        content JSON storage implementation
        Args:
            content_item:

        Returns:

        """
        await self.save_data_to_json(content_item, "contents")

    async def store_comment(self, comment_item: Dict):
        """
        comment JSON storage implementatio
        Args:
            comment_item:

        Returns:

        """
        await self.save_data_to_json(comment_item, "comments")


    async def store_creator(self, creator: Dict):
        """
        Douyin creator CSV storage implementation
        Args:
            creator: creator item dict

        Returns:

        """
        await self.save_data_to_json(save_item=creator, store_type="creator")

class DouyinMongoStoreImplement(AbstractStore):
    async def store_content(self, content_item: Dict):
        mongo_db_conn: AsyncMongoDB = media_crawler_mongo_db_var.get()
        content = await mongo_db_conn.find_one("dy_contents", {"aweme_id": content_item.get("aweme_id")})
        if content:
            await mongo_db_conn.delete_one("dy_contents", {"_id": content.get("_id")})
        await mongo_db_conn.insert_one("dy_contents", content_item)

        new_item = transform_save_content_item(content_item)
        note_id_list_all_var.get().append(new_item['post_id'])
        post = await mongo_db_conn.find_one("post_dy", {"third_party_post_id": new_item.get("third_party_post_id")})
        if post:
            await mongo_db_conn.delete_one("post_dy", {"_id": post.get("_id")})
        await mongo_db_conn.insert_one("post_dy", new_item)

    async def store_comment(self, comment_item: Dict):
        mongo_db_conn: AsyncMongoDB = media_crawler_mongo_db_var.get()
        comment = await mongo_db_conn.find_one("dy_comments", {"comment_id": comment_item.get("comment_id")})
        if comment:
            await mongo_db_conn.delete_one("dy_comments", {"_id": comment.get("_id")})
        await mongo_db_conn.insert_one("dy_comments", comment_item)

        new_item = transform_save_comment_item(comment_item)
        post_comment = await mongo_db_conn.find_one("post_comment_dy", {"third_party_comment_id": new_item.get("third_party_comment_id")})
        if post_comment:
            await mongo_db_conn.delete_one("post_comment_dy", {"_id": post_comment.get("_id")})
        await mongo_db_conn.insert_one("post_comment_dy", new_item)

    # todo 后期添加
    async def store_creator(self, creator: Dict):
        pass



def transform_save_content_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    → 输出完全符合 validator 要求的文档。
    → publish_time / fetch_time / last_modified 都是 datetime。
    → status 一定是 active/deleted/updated，这里默认 active。
    → 只写有值的可选字段，避免 None 或错误类型触发校验。
    """
    doc: Dict[str, Any] = {}
    # —— 必填字段 ——
    doc['platform'] = "dy"
    doc['post_id'] = str(uuid4())
    doc['content'] = item.get('desc') or ''
    doc['url'] = item.get('aweme_url') or ''
    # —— 可选字段 ——
    if item.get("aweme_id"):
        doc["third_party_post_id"] = str(item.get("aweme_id"))
    # todo 需要任务管理程序传入
    # if isinstance(item.get("scheme_id"), int):
    #     doc["scheme_id"] = item["scheme_id"]
    if item.get('title'):
        doc["title"] = str(item["title"])
    if item.get('user_id') or item.get('nickname'):
        doc["author"] = {
            "user_id": str(item.get("user_id") or ""),
            "username": str(item.get("nickname") or "")
        }
    if item.get('create_time'):
        doc['publish_time'] = _ts_to_datetime(item.get("create_time"))
    if item.get("ip_location"):
        doc["ip_location"] = str(item["ip_location"])
    if item.get('last_modify_ts'):
        doc['last_modified'] = _ts_to_datetime(item.get("last_modify_ts"))
    doc["content_hash"] = str(hash(doc["content"]))
    doc['fetch_time'] = datetime.now(tz=timezone.utc)
    # todo 需要确认状态含义
    doc["status"] = "active"
    # todo 需要确认是什么关键词
    if item.get("source_keyword"):
        doc["tags"] = [item.get("source_keyword")]
    doc['engagement_metrics'] = {
        "likes": int(item.get("liked_count") or 0),
        "comments": int(item.get("comment_count") or 0),
        "shares": int(item.get("share_count") or 0),
    }
    # todo AI分析得分，难以统一标准，可以用正，中，负替代，热度得分和是否为热点文章，需要确认判断规则
    '''
    需要通过分析获得的数据
    1.情感分析得分
    2.当前热度得分
    3.当前是否为热点文章
    '''
    return doc


def transform_save_comment_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    → 输出完全符合 validator 要求的评论文档。
    → timestamp / fetch_time 都是 datetime。
    → sentiment_label 一定是 positive/negative/neutral，这里默认 neutral。
    → 只写有值的可选字段，避免 None 或错误类型触发校验。
    """
    doc: Dict[str, Any] = {}
    doc['post_id'] = str(item.get("aweme_id") or '')
    doc['content'] = str(item.get('content') or '')
    doc['comment_id'] = str(uuid4())
    doc['third_party_comment_id'] = str(item.get("comment_id") or '')
    if item.get("parent_comment_id"):
        doc["parent_comment_id"] = str(item["parent_comment_id"])
    if item.get('user_id') or item.get('nickname'):
        doc["author"] = {
            "user_id": str(item.get("user_id") or ""),
            "username": str(item.get("nickname") or "")
        }
    if item.get("ip_location"):
        doc["ip_location"] = item["ip_location"]
    if item.get("create_time"):
        doc["publish_time"] = _ts_to_datetime(item["create_time"])

    doc["fetch_time"] = _ts_to_datetime(item.get("last_modify_ts"))
    doc["reply_count"] =int(item.get("sub_comment_count") or 0)
    doc["likes"] = int(item.get("comment_like_count") or 0)
    # todo sentiment_score 和 sentiment_label 待分析后入库，其中分数是否要取消数字类型限制改为级别文本需要确认
    return doc

def _ts_to_datetime(ts: Any) -> datetime:
    """
    把秒级或毫秒级的时间戳转换为 datetime（本地时区）。
    如果 ts 为 None、非法或超范围，会抛出异常（让上层知道数据有问题）。
    """
    if ts is None:
        raise ValueError("timestamp is None")
    ts_int = int(ts)
    # 毫秒级的话（13 位以上），缩到秒级
    if ts_int > 10 ** 12:
        ts_int //= 1000
    bj_time= datetime.fromtimestamp(ts_int, timezone(timedelta(hours=8)))
    time1= bj_time.astimezone(timezone.utc)
    return time1;
