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
# @Time    : 2023/12/24 17:37
# @Desc    :

from typing import Dict, List


def filter_search_result_card(card_list: List[Dict]) -> List[Dict]:
    """
    过滤微博搜索的结果，只保留card_type为9类型的数据
    :param card_list:
    :return:
    """
    note_list: List[Dict] = []

    # # 添加假数据进行测试
    # card_list = [
    #     {"card_type": 9, "mlog": {"retweeted_status": "转发内容"}},
    #     {"card_type": 9, "mlog": {}},
    #     {"card_type": 10, "mlog": {"retweeted_status": "转发内容"}},
    #     {"card_type": 9, "card_group": [{"card_type": 9, "mlog": {"retweeted_status": "转发内容"}}]},
    #     {"card_type": 9, "card_group": [{"card_type": 9, "mlog": {}}]}
    # ]

    for card_item in card_list:
        if card_item.get("card_type") == 9:
            # 判断mlog是否存在且包含retweeted_status字段
            mlog = card_item.get("mlog", {})
            if "retweeted_status" in mlog:
                continue
            note_list.append(card_item)
        if len(card_item.get("card_group", [])) > 0:
            card_group = card_item.get("card_group")
            for card_group_item in card_group:
                # 临时过滤掉微博转发的帖子
                if card_group_item.get("card_type") == 9:
                    # 判断mlog是否存在且包含retweeted_status字段
                    mlog = card_group_item.get("mlog", {})
                    if "retweeted_status" in mlog:
                        continue
                    note_list.append(card_item)

    # 打印过滤后的结果
    print("Filtered note_list:", note_list)
    return note_list
