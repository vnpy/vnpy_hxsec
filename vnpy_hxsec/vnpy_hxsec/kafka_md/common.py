from collections import namedtuple
from typing import Tuple
from struct import calcsize


def get_struct_info(
    struct_field: dict,
    struct_name: str
) -> Tuple[namedtuple, int, str]:
    """获取C Struct结构体信息"""
    # 拼接字段格式
    struct_format: str = "!" + "".join(struct_field.values())

    # 计算内存占用
    struct_size: int = calcsize(struct_format)

    # 构造命名元组
    struct_tuple: namedtuple = namedtuple(struct_name, struct_field.keys())

    # 返回信息
    return struct_tuple, struct_size, struct_format
