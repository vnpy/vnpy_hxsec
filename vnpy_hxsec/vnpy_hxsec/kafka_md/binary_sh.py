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
    "MDEntryPx": "Q"
}
INDEX_TUPLE, INDEX_SIZE, INDEX_FORMAT = get_struct_info(INDEX_FIELD, "SH_INDEX")


# 股票字段
STOCK_FIELD = {
    "MDEntryType": "2s",
    "MDEntryPx": "Q",
    "MDEntrySize": "Q",
    "MDEntryPositionNo": "B"
}
STOCK_TUPLE, STOCK_SIZE, STOCK_FORMAT = get_struct_info(STOCK_FIELD, "SH_STOCK")


# 头部字段
HEADER_FIELD = {
    "MsgType": "4s",        # 消息类型
    "SendingTime": "Q",     # 发送时间 格式：YYYYMMDD HH mm SS sss
    "MsgSeNum": "Q",        # 消息序号 uint64
    "BodyLength": "I",      # 消息体长度  uint32
}
HEADER_TUPLE, HEADER_SIZE, HEADER_FORMAT = get_struct_info(HEADER_FIELD, "SH_HEADER")


# 行情字段
DATA_FIELD = {
    "SecurityType": "B",        # unsigned int
    "TradeSesMode": "B",        # uint8   证券类型 1 股票（含指数）2 衍生品 3 =综合 业务
    "TradeDate": "I",           # 交易日期 YYYYMMDD
    "LastUpdateTime": "I",      # uint32, N9
    "MDStreamID": "5s",         # char[5]
    "SecurityID": "8s",         # 产品代码 char[8]
    "Symbol": "8s",             # 产品简称char [8]
    "PreClosPx": "Q",           # 昨收盘uint64,N13 ()
    "TotalVolumeTraded": "Q",   # 昨收盘uint64,N16 ()
    "NumTrades": "Q",           # 昨收盘uint64,N16 ()
    "TotalValueTraded": "Q",    # 昨收盘uint64,N16 ()
    "TradingPhaseCode": "8s"
}
DATA_TUPLE, DATA_SIZE, DATA_FORMAT = get_struct_info(DATA_FIELD, "SH_DATA")


def parse_msg_sh(buf: bytes) -> Optional[TickData]:
    """解析上交所行情"""
    # 跟踪索引
    ix: int = 0

    # 解析头部
    header_buf: bytes = buf[ix: ix + HEADER_SIZE]
    header: HEADER_TUPLE = HEADER_TUPLE(*(unpack(HEADER_FORMAT, header_buf)))
    ix += HEADER_SIZE

    # 检查类型
    if header.MsgType != b"M102":
        return None

    # 解析数据
    data_buf: bytes = buf[ix: ix + DATA_SIZE]
    data: DATA_TUPLE = DATA_TUPLE(*(unpack(DATA_FORMAT, data_buf)))
    ix += DATA_SIZE

    # 过滤时间为0
    if not data.LastUpdateTime:
        return None

    # 处理时间
    time_str: str = str(data.TradeDate) + str(data.LastUpdateTime)
    dt: datetime = datetime.strptime(time_str, "%Y%m%d%H%M%S%f")

    # 通用字段
    tick: TickData = TickData(
        gateway_name="KAFKA",
        symbol=data.SecurityID.decode().strip(),
        exchange=Exchange.SSE,
        datetime=dt,
        pre_close=data.PreClosPx / 1e5,
        volume=data.TotalVolumeTraded,
        turnover=data.TotalValueTraded
    )

    # 判断类型
    if data.MDStreamID == b"MD002":
        item_tuple: namedtuple = STOCK_TUPLE
        item_format: str = STOCK_FORMAT
        item_size: int = STOCK_SIZE
    elif data.MDStreamID == b"MD001":
        item_tuple: namedtuple = INDEX_TUPLE
        item_format: str = INDEX_FORMAT
        item_size: int = INDEX_SIZE
    else:
        # print("type pass", data.MDStreamID)
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
        p: float = item.MDEntryPx / 1e5
        v: int = getattr(item, "MDEntrySize", 0)                # 只有股票才有
        level: int = getattr(item, "MDEntryPositionNo", 0) + 1  # 只有股票才有

        # 执行更新
        if item.MDEntryType == b"2 ":       # 股票最新价
            tick.last_price = p
            tick.volume = v
        elif item.MDEntryType == b"3 ":     # 指数最新价
            tick.last_price = p
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

        # 更新索引
        ix += item_size

    # 返回结果
    return tick


def main():
    bootstrap_servers = [
        "172.16.22.25:9092",
        "172.16.22.26:9092",
        "172.16.22.27:9092"
    ]

    consumer: KafkaConsumer = KafkaConsumer(
        "sh-lv1-binary",
        bootstrap_servers=bootstrap_servers
    )

    for message in consumer:
        quote = parse_msg_sh(message.value)
        print(quote)


if __name__ == "__main__":
    main()
