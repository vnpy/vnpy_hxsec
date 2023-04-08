import json
from typing import Dict, Set
from datetime import datetime

from iFinDPy import (
    THS_iFinDLogin,
    THS_QuotesPushing
)

from vnpy.event import EventEngine
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import TickData, SubscribeRequest
from vnpy.trader.constant import Exchange

from vnpy_hxsec.event import EVENT_IFIND

# from vnpy.trader.utility import ZoneInfo
# CHINA_TZ = ZoneInfo("Asia/Shanghai")

from pytz import timezone
CHINA_TZ = timezone("Asia/Shanghai")


EXCHANGE_IFIND_VT: Dict[str, Exchange] = {
    "SH": Exchange.SSE,
    "SZ": Exchange.SZSE,
}
EXCHANGE_VT_IFIND = {v: k for k, v in EXCHANGE_IFIND_VT.items()}


IFIND_FIELDS = [
    "tradeDate",
    "tradeTime",
    "preClose",
    "open",
    "high",
    "low",
    "latest",
    "upperLimit",
    "downLimit",
    "amount",
    "volume",
    "lastest_price",
    "bid1",
    "bid2",
    "bid3",
    "bid4",
    "bid5",
    "ask1",
    "ask2",
    "ask3",
    "ask4",
    "ask5",
    "bidSize1",
    "bidSize2",
    "bidSize3",
    "bidSize4",
    "bidSize5",
    "askSize1",
    "askSize2",
    "askSize3",
    "askSize4",
    "askSize5",
]


class IfindMdApi:
    """iFinD行情API实例"""

    def __init__(self, gateway: BaseGateway):
        """"""
        self.gateway: BaseGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.event_engine: EventEngine = gateway.event_engine

        self.connected: bool = False
        self.subscribed: Set[str] = set()

    def connect(self, username: str, password: str) -> bool:
        """"""
        if self.connected:
            return True

        code: int = THS_iFinDLogin(username, password)
        if code:
            self.gateway.write_log(f"iFinD行情接口连接失败，原因{code}")
            self.gateway.on_event(EVENT_IFIND, False)
            return False
        else:
            self.connected = True

            self.gateway.write_log("iFinD行情接口连接成功")
            self.gateway.on_event(EVENT_IFIND, True)
            return True

    def subscribe(self, req: SubscribeRequest):
        """"""
        ifind_exchange: str = EXCHANGE_VT_IFIND.get(req.exchange, None)
        if not ifind_exchange:
            return

        ifind_symbol: str = f"{req.symbol}.{ifind_exchange}"
        ifind_fields = ";".join(IFIND_FIELDS)

        THS_QuotesPushing(ifind_symbol, ifind_fields, self.on_quote_pushing)
        self.subscribed.add(ifind_symbol)

    def on_quote_pushing(
        self,
        data: object,
        id: int,
        result: str,
        lens: int,
        error_code: int,
        reserved: int
    ) -> None:
        """iFinD推送回调"""
        tick_data: dict = json.loads(result)

        for d in tick_data["tables"]:
            # 过滤第一条推送缺失time的字段
            if "time" not in d:
                continue

            # 过滤缺失最新价的行情
            table: dict = d["table"]
            if "latest" not in table:
                continue

            ifind_symbol: str = d["thscode"]
            symbol, ifind_exchange = ifind_symbol.split(".")
            exchange = EXCHANGE_IFIND_VT[ifind_exchange]

            dt: datetime = datetime.fromtimestamp(d["time"][0])

            tick = TickData(
                symbol=symbol,
                exchange=exchange,
                datetime=dt.replace(tzinfo=CHINA_TZ),
                gateway_name=self.gateway_name,
                last_price=table["latest"][0],
                volume=table["volume"][0],
                turnover=table["amount"][0],
                open_price=table["open"][0],
                high_price=table["high"][0],
                low_price=table["low"][0],
                pre_close=table["preClose"][0],
                # limit_up=table["upperLimit"][0],
                # limit_down=table["downLimit"][0],

                bid_price_1=table["bid1"][0],
                bid_price_2=table["bid2"][0],
                bid_price_3=table["bid3"][0],
                bid_price_4=table["bid4"][0],
                bid_price_5=table["bid5"][0],
                ask_price_1=table["ask1"][0],
                ask_price_2=table["ask2"][0],
                ask_price_3=table["ask3"][0],
                ask_price_4=table["ask4"][0],
                ask_price_5=table["ask5"][0],

                bid_volume_1=table["bidSize1"][0],
                bid_volume_2=table["bidSize2"][0],
                bid_volume_3=table["bidSize3"][0],
                bid_volume_4=table["bidSize4"][0],
                bid_volume_5=table["bidSize5"][0],
                ask_volume_1=table["askSize1"][0],
                ask_volume_2=table["askSize2"][0],
                ask_volume_3=table["askSize3"][0],
                ask_volume_4=table["askSize4"][0],
                ask_volume_5=table["askSize5"][0],
            )

            self.gateway.on_tick(tick)

        return 0

    def close(self) -> None:
        """"""
        pass


if __name__ == "__main__":
    event_engine = EventEngine()

    gateway = BaseGateway(event_engine, "test")

    api = IfindMdApi(gateway)

    api.connect("hxzq2579", "493716")

    for req in [
        SubscribeRequest("600036", Exchange.SSE),
        SubscribeRequest("000001", Exchange.SZSE)
    ]:
        api.subscribe(req)

    input()
