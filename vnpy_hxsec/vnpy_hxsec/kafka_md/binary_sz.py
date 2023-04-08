from collections import namedtuple
from typing import Optional
from struct import unpack
from datetime import datetime

from kafka import KafkaConsumer

from vnpy.trader.object import TickData
from vnpy.trader.object import Exchange

from .common import get_struct_info


# 指数字段
INDEX_FIELD = {
    "MDEntryType": "2s",
    "MDEntryPx": "q"
}
INDEX_TUPLE, INDEX_SIZE, INDEX_FORMAT = get_struct_info(INDEX_FIELD, "SZ_INDEX")


# 股票字段
STOCK_FIELD = {
    "MDEntryType": "2s",        # char[2]
    "MDEntryPx": "q",           # Int64
    "MDEntrySize": "q",         # Int64
    "MDPriceLevel": "H",        # uInt16
    "NumberOfOrders": "q",      # Int64
    "NoOrders": "I",            # uInt32
}
STOCK_TUPLE, STOCK_SIZE, STOCK_FORMAT = get_struct_info(STOCK_FIELD, "SZ_STOCK")


# 头部字段
HEADER_FIELD = {
    "MsgType": "i",         # 消息类型
    "BodyLength": "I",      # 消息体长度  uint32
}
HEADER_TUPLE, HEADER_SIZE, HEADER_FORMAT = get_struct_info(HEADER_FIELD, "SZ_HEADER")


# 行情字段
DATA_FIELD = {
    "OrigTime": "q",            # int64
    "ChannelNo": "H",           # uint16
    "MDStreamID": "3s",         # char[3]
    "SecurityID": "8s",         # char[8]
    "SecurityIDSource": "4s",   # char[4]
    "TradingPhaseCode": "8s",   # char[8]
    "PrevClosePx": "q",         # Int64
    "NumTrades": "q",           # Int64
    "TotalVolumeTrade": "q",    # Int64
    "TotalValueTrade": "q",     # Int64
    "ExtendFields": "2s"
}
DATA_TUPLE, DATA_SIZE, DATA_FORMAT = get_struct_info(DATA_FIELD, "SZ_DATA")


def parse_msg_sz(buf: bytes) -> Optional[TickData]:
    """解析深交所行情"""
    # 跟踪索引
    ix: int = 0

    # 解析头部
    header_buf: bytes = buf[ix: ix + HEADER_SIZE]
    header: HEADER_TUPLE = HEADER_TUPLE(*(unpack(HEADER_FORMAT, header_buf)))
    ix += HEADER_SIZE

    # 检查类型
    if header.MsgType not in (300111, 309011):
        return None

    # 解析数据
    data_buf: bytes = buf[ix: ix + DATA_SIZE]
    data: DATA_TUPLE = DATA_TUPLE(*(unpack(DATA_FORMAT, data_buf)))
    ix += DATA_SIZE

    # 处理时间
    dt: datetime = datetime.strptime(str(data.OrigTime), "%Y%m%d%H%M%S%f")

    # 通用字段
    tick: TickData = TickData(
        gateway_name="KAFKA",
        symbol=data.SecurityID.decode().strip(),
        exchange=Exchange.SZSE,
        datetime=dt,
        pre_close=data.PrevClosePx / 1e4,
        volume=data.TotalVolumeTrade,
        turnover=data.TotalValueTrade
    )

    # 判断类型
    if data.MDStreamID == b"010":
        item_tuple: namedtuple = STOCK_TUPLE
        item_format: str = STOCK_FORMAT
        item_size: int = STOCK_SIZE
    elif data.MDStreamID == b"900":
        item_tuple: namedtuple = INDEX_TUPLE
        item_format: str = INDEX_FORMAT
        item_size: int = INDEX_SIZE
    else:
        return None

    # 解析条数
    count_buf: bytes = buf[ix: ix + 2]
    count: int = unpack("!h", count_buf)[0]
    ix += 2

    # 逐行处理
    for _ in range(count):
        # 解析本行
        item_buf: bytes = buf[ix: ix + item_size]
        item: namedtuple = item_tuple(*unpack(item_format, item_buf))

        # 读取量价字段
        p: float = item.MDEntryPx / 1e6
        v: int = getattr(item, "MDEntrySize", 0)                # 只有股票才有
        level: int = getattr(item, "MDPriceLevel", 1)           # 只有股票才有

        # 股票相关
        if item.MDEntryType == b"2 ":       # 最新价
            tick.last_price = p
            tick.volume = v
        elif item.MDEntryType == b"0 ":     # 买盘口
            setattr(tick, f"bid_price_{level}", p)
            setattr(tick, f"bid_volume_{level}", v)
        elif item.MDEntryType == b"1 ":     # 卖盘口
            setattr(tick, f"ask_price_{level}", p)
            setattr(tick, f"ask_volume_{level}", v)
        elif item.MDEntryType == b"4 ":     # 开盘价
            tick.open_price = p
        elif item.MDEntryType == b"7 ":     # 最高价
            tick.high_price = p
        elif item.MDEntryType == b"8 ":     # 最低价
            tick.low_price = p
        # 指数相关
        elif item.MDEntryType == b"3 ":     # 最新价
            tick.last_price = p
        elif item.MDEntryType == b"xb":     # 开盘价
            tick.open_price = p
        elif item.MDEntryType == b"xc":     # 最高价
            tick.high_price = p
        elif item.MDEntryType == b"xd":     # 最低价
            tick.low_price = p

        # 更新索引
        ix += item_size

    return tick


def main():
    bootstrap_servers = [
        "172.16.22.25:9092",
        "172.16.22.26:9092",
        "172.16.22.27:9092"
    ]

    consumer: KafkaConsumer = KafkaConsumer(
        "sz-lv1-binary",
        bootstrap_servers=bootstrap_servers
    )

    for message in consumer:
        quote = parse_msg_sz(message.value)
        print(quote)


if __name__ == "__main__":
    main()
