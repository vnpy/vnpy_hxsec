from typing import List, Set
from threading import Thread
from kafka import KafkaConsumer

from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import TickData, SubscribeRequest

from .binary_sh import parse_msg_sh
from .binary_sz import parse_msg_sz

from vnpy_hxsec.event import EVENT_KAFKA


class KafkaMdApi:
    """华兴Kafka行情接口"""

    def __init__(self, gateway: BaseGateway) -> None:
        """"""
        self.gateway: BaseGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.active: bool = False
        self.sz_thread: Thread = None
        self.sh_thread: Thread = None

        self.servers: List[str] = []
        self.subscribed: Set[str] = set()

    def connect(self, servers: List[str]) -> None:
        """连接服务器"""
        if self.active:
            return
        self.active = True

        self.servers = servers

        self.sh_thread = Thread(target=self.run_sh, daemon=True)
        self.sh_thread.start()

        self.sz_thread = Thread(target=self.run_sz, daemon=True)
        self.sz_thread.start()

        self.gateway.on_event(EVENT_KAFKA, True)

    def run_sh(self) -> None:
        """获取上海行情"""
        consumer: KafkaConsumer = KafkaConsumer(
            "sh-lv1-binary",
            bootstrap_servers=self.servers
        )

        for message in consumer:
            tick: TickData = parse_msg_sh(message.value)

            if tick and tick.vt_symbol in self.subscribed:
                tick.gateway_name = self.gateway_name
                self.gateway.on_tick(tick)

            if not self.active:
                break

    def run_sz(self) -> None:
        """获取深圳行情"""
        consumer: KafkaConsumer = KafkaConsumer(
            "sz-lv1-binary",
            bootstrap_servers=self.servers
        )

        for message in consumer:
            tick: TickData = parse_msg_sz(message.value)

            if tick and tick.vt_symbol in self.subscribed:
                tick.gateway_name = self.gateway_name
                self.gateway.on_tick(tick)

            if not self.active:
                break

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.subscribed.add(req.vt_symbol)

    def close(self) -> None:
        """关闭接口"""
        self.active = False

        if self.sh_thread:
            self.sh_thread.join()

        if self.sz_thread:
            self.sz_thread.join()

        self.gateway.on_event(EVENT_KAFKA, False)
