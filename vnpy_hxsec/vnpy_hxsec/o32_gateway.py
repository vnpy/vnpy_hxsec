from typing import Dict

from vnpy.event import EventEngine
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.constant import Exchange
from vnpy.trader.object import (
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    TickData,
    ContractData
)

from vnpy_o32 import O32TdApi

from .kafka_md import KafkaMdApi
from .wind_md import WindMdApi
from .ifind_md import IfindMdApi


symbol_name_map: Dict[str, str] = {}


class O32Gateway(BaseGateway):
    """
    VN Trader Gateway for Hundsun O32.
    """

    default_name: str = "O32"

    default_setting = {
        "操作员": "",
        "密码": "",
        "组合编号": "",
        "授权码": "",
        "交易服务器": "",
        "推送服务器": "",
        "行情服务器1": "",
        "行情服务器2": "",
        "iFinD账号": "",
        "iFinD密码": ""
    }

    exchanges = [Exchange.SSE, Exchange.SZSE]

    def __init__(self, event_engine: EventEngine, gateway_name: str):
        """Constructor"""
        super().__init__(event_engine, gateway_name)

        self.td_api = O32TdApi(self)
        self.md_api = None

        self.subscribed: Dict[str, SubscribeRequest] = {}

    def connect(self, setting: dict) -> None:
        """"""
        # 连接行情
        md_server1 = setting["行情服务器1"]
        md_server2 = setting["行情服务器2"]

        ifind_username = setting["iFinD账号"]
        ifind_password = setting["iFinD密码"]

        if md_server1 or md_server2:
            servers = [md_server1, md_server2]
            self.md_api = KafkaMdApi(self)
            self.md_api.connect(servers)
        elif ifind_username and ifind_password:
            self.md_api = IfindMdApi(self)
            self.md_api.connect(ifind_username, ifind_password)
        else:
            self.md_api = WindMdApi(self)
            self.md_api.connect()

        # 订阅行情
        for req in list(self.subscribed.values()):
            self.subscribe(req)

        # 连接交易
        o32_operator = setting["操作员"]
        o32_password = setting["密码"]
        o32_combi_no = setting["组合编号"]
        o32_authorization_id = setting["授权码"]
        o32_td_server = setting["交易服务器"]
        o32_sub_server = setting["推送服务器"]
        stock_types = ["01", "0F"]              # 包括股票和基金

        self.td_api.connect(
            o32_operator,
            o32_password,
            o32_combi_no,
            o32_td_server,
            o32_sub_server,
            stock_types,
            equity_trading=True,
            authorization_id=o32_authorization_id
        )

        self.init_query()

    def subscribe(self, req: SubscribeRequest) -> None:
        """"""
        self.subscribed[req.vt_symbol] = req
        self.md_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """"""
        return self.td_api.equity_send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """"""
        return self.td_api.equity_cancel_order(req)

    def query_account(self) -> None:
        """"""
        self.td_api.query_asset()

    def query_position(self) -> None:
        """"""
        self.td_api.equity_query_position()

    def close(self) -> None:
        """"""
        if self.md_api:
            self.md_api.close()

    def process_timer_event(self, event) -> None:
        """"""
        self.count += 1
        if self.count < 2:
            return
        self.count = 0

        self.query_position()
        self.query_account()

    def init_query(self) -> None:
        """"""
        self.count = 0
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def on_contract(self, contract: ContractData) -> None:
        """"""
        symbol_name_map[contract.vt_symbol] = contract.name
        super().on_contract(contract)

    def on_tick(self, tick: TickData) -> None:
        """"""
        tick.name = symbol_name_map.get(tick.vt_symbol, "")
        super().on_tick(tick)
