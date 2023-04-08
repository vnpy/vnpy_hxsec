from vnpy.event import EventEngine
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.constant import Exchange
from vnpy.trader.object import (
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
)

from vnpy_o32 import O32TdApi


class O32Gateway(BaseGateway):
    """
    VN Trader Gateway for Hundsun O32.
    """

    default_name: str = "O32"

    default_setting = {
        "操作员": "",
        "密码": "",
        "组合编号": "",
        "交易服务器": "",
        "推送服务器": "",
        "授权码": "00B3416A4275BFE67F6AF84C3B06E8AF",
    }

    exchanges = [Exchange.SSE, Exchange.SZSE]

    def __init__(self, event_engine: EventEngine, gateway_name: str):
        """Constructor"""
        super().__init__(event_engine, gateway_name)

        self.td_api = O32TdApi(self)

    def connect(self, setting: dict) -> None:
        """"""
        o32_operator: str = setting["操作员"]
        o32_password: str = setting["密码"]
        o32_combi_no: str = setting["组合编号"]
        o32_td_server: str = setting["交易服务器"]
        o32_sub_server: str = setting["推送服务器"]
        o32_authorization_id: str = setting["授权码"]
        stock_types = ["01", "0F"]              # 包括股票和基金

        self.td_api.connect(
            o32_operator,
            o32_password,
            o32_combi_no,
            o32_td_server,
            o32_sub_server,
            stock_types,
            futures_trading=True,
            authorization_id=o32_authorization_id,
        )

        self.init_query()

    def subscribe(self, req: SubscribeRequest) -> None:
        """"""
        pass

    def send_order(self, req: OrderRequest) -> str:
        """"""
        return self.td_api.equity_send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """"""
        return self.td_api.equity_cancel_order(req)

    def query_account(self) -> None:
        """"""
        self.td_api.query_account()

    def query_position(self) -> None:
        """"""
        self.td_api.equity_query_position()

    def close(self) -> None:
        """"""
        pass

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
