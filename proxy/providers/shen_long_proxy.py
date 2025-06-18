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
# @Time    : 2024/4/5 09:43
# @Desc    : 快代理HTTP实现，官方文档：https://www.kuaidaili.com/?ref=ldwkjqipvz6c
import os
import re
from typing import Dict, List
import time


import httpx
from pydantic import BaseModel, Field

from proxy import IpCache, IpInfoModel, ProxyProvider
from proxy.types import ProviderNameEnum
from tools import utils


class ShenLongProxyModel(BaseModel):
    ip: str = Field("ip")
    port: int = Field("端口")
    expire_ts: int = Field("过期时间")


def parse_kuaidaili_proxy(proxy_info: str) -> ShenLongProxyModel:
    """
    解析快代理的IP信息
    Args:
        proxy_info:

    Returns:

    """
    proxies: List[str] = proxy_info.split(":")
    if len(proxies) != 2:
        raise Exception("not invalid kuaidaili proxy info")

    pattern = r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{1,5}),(\d+)'
    match = re.search(pattern, proxy_info)
    if not match.groups():
        raise Exception("not match kuaidaili proxy info")

    return ShenLongProxyModel(
        ip=match.groups()[0],
        port=int(match.groups()[1]),
        expire_ts=int(match.groups()[2])
    )


class ShenLongProxy(ProxyProvider):
    def __init__(self, kdl_user_name: str, kdl_user_pwd: str, kdl_secret_id: str, kdl_signature: str):
        """

        Args:
            kdl_user_name:
            kdl_user_pwd:
        """
        self.kdl_user_name = kdl_user_name
        self.kdl_user_pwd = kdl_user_pwd
        self.api_base = "http://api.shenlongip.com"
        self.secret_id = kdl_secret_id
        self.signature = kdl_signature
        self.ip_cache = IpCache()
        self.proxy_brand_name = ProviderNameEnum.SHEN_LONG_PROVIDER.value
        self.params = {
            "key": self.secret_id,
            "protocol":2,
            "mr":1,
            "pattern":"json",
            "sign": self.signature,
            "need": 1111,
            "count": 1
        }

    @staticmethod
    def datetime_to_timestamp(datetime_str: str) -> int:
        """
        将日期时间字符串转换为时间戳
        Args:
            datetime_str: 日期时间字符串，格式为 'YYYY-MM-DD HH:MM:SS'

        Returns:
            时间戳
        """
        try:
            time_struct = time.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
            return int(time.mktime(time_struct))
        except ValueError as e:
            utils.logger.error(f"[ShenLongProxy.datetime_to_timestamp] Error converting datetime to timestamp: {e}")
            raise

    async def get_proxies(self, num: int) -> List[IpInfoModel]:
        """
        快代理实现
        Args:
            num:

        Returns:

        """
        uri = "/ip"

        # 优先从缓存中拿 IP
        ip_cache_list = self.ip_cache.load_all_ip(proxy_brand_name=self.proxy_brand_name)
        if len(ip_cache_list) >= num:
            return ip_cache_list[:num]

        # 如果缓存中的数量不够，从IP代理商获取补上，再存入缓存中
        need_get_count = num - len(ip_cache_list)
        self.params.update({"num": need_get_count})

        ip_infos: List[IpInfoModel] = []
        async with httpx.AsyncClient() as client:
            response = await client.get(self.api_base + uri, params=self.params)

            if response.status_code != 200:
                utils.logger.error(f"[ShenLongProxy.get_proxies] statuc code not 200 and response.txt:{response.text}")
                raise Exception("get ip error from proxy provider and status code not 200 ...")

            ip_response: Dict = response.json()
            if ip_response.get("code") != 200:
                utils.logger.error(f"[ShenLongProxy.get_proxies]  code not 0 and msg:{ip_response.get('msg')}")
                raise Exception("get ip error from proxy provider and  code not 0 ...")

            proxy_list: List[str] = ip_response.get("data",[])
            for proxy in proxy_list:
                ip_info_model = IpInfoModel(
                    ip=proxy['ip'],
                    port=proxy['port'],
                    user=self.kdl_user_name,
                    password=self.kdl_user_pwd,
                    expired_time_ts=self.datetime_to_timestamp(proxy['expire']),

                )
                ip_key = f"{self.proxy_brand_name}_{ip_info_model.ip}_{ip_info_model.port}"
                self.ip_cache.set_ip(ip_key, ip_info_model.model_dump_json(), ex=ip_info_model.expired_time_ts)
                ip_infos.append(ip_info_model)

        return ip_cache_list + ip_infos


def new_shen_long_proxy() -> ShenLongProxy:
    """
    构造快代理HTTP实例
    Returns:

    """
    return ShenLongProxy(
        kdl_secret_id=os.getenv("kdl_secret_id", "s36i1mlk"),
        kdl_signature=os.getenv("kdl_signature", "7f946dd629b4bd36b8d7f9f37d46fe13"),
        kdl_user_name=os.getenv("kdl_user_name", "fyhfbu"),
        kdl_user_pwd=os.getenv("kdl_user_pwd", "ypacp8d1"),
    )
