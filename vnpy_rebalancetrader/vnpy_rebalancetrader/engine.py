from dataclasses import dataclass
from typing import Optional
from copy import copy

from vnpy.event import EventEngine, Event
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.event import (
    EVENT_TIMER,
    EVENT_ORDER,
    EVENT_TRADE,
    EVENT_POSITION,
    EVENT_CONTRACT
)
from vnpy.trader.object import (
    ContractData,
    TickData,
    OrderData,
    TradeData,
    PositionData,
    LogData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest
)
from vnpy.trader.constant import Direction, OrderType, Exchange
from vnpy.trader.converter import OffsetConverter
from vnpy.trader.utility import load_json, save_json
from vnpy_hxsec.event import EVENT_O32, EVENT_KAFKA, EVENT_WIND, EVENT_IFIND

from .algo import AlgoStatus, PriceType, TwapAlgo


APP_NAME: str = "RebalanceTrader"

EVENT_REBALANCE_LOG = "eRebalanceLog"
EVENT_REBALANCE_ALGO = "eRebalanceAlgo"
EVENT_REBALANCE_EXPOSURE = "eRebalanceExposure"
EVENT_REBALANCE_HOLDING = "eRebalanceHolding"


@dataclass
class HoldingData:
    """组合持仓数据"""
    vt_positionid: str
    symbol: str
    exchange: Exchange
    name: str
    direction: Direction
    volume: int = 0
    frozen: int = 0
    price: float = 0
    pnl: float = 0
    value: float = 0

    def __post_init__(self) -> None:
        """"""
        self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"


class RebalanceEngine(BaseEngine):
    """篮子执行引擎"""

    data_filename = "rebalance_trader_data.json"

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """构造函数"""
        super().__init__(main_engine, event_engine, APP_NAME)

        # 对象字典
        self.algos: dict[str, TwapAlgo] = {}    # vt_symbol: TwapAlgo
        self.orders: dict[str, OrderData] = {}
        self.trades: dict[str, TradeData] = {}
        self.holdings: dict[str, HoldingData] = {}

        self.algo_orderids: set[str] = set()
        self.order_batch_map: dict[str, int] = {}

        # 订阅代码
        self.subscribed: set[str] = set()

        # 开平转换
        self.offset_converter: OffsetConverter = OffsetConverter(self.main_engine)

        # 统计数据
        self.long_value: float = 0      # 买入敞口
        self.short_value: float = 0     # 卖出敞口
        self.net_value: float = 0       # 净长空
        self.long_left: float = 0       # 买入剩余
        self.short_left: float = 0      # 卖出剩余

        self.long_plan: float = 0       # 买入计划
        self.short_plan: float = 0      # 卖出计划

        # 敞口限制
        self.exposure_maximum: int = 200_000
        self.exposure_minimum: int = -200_000
        self.long_pause: bool = False
        self.short_pause: bool = False

        # 查询函数
        self.get_contract = main_engine.get_contract
        self.get_tick = main_engine.get_tick

        # 算法状态
        self.algo_started = False

        # 连接状态
        self.connection_status: dict = {
            "kafka": False,
            "wind": False,
            "o32": False,
            "ifind": False
        }

        # 注册连接事件监听
        self.event_engine.register(EVENT_KAFKA, self.process_kafka_event)
        self.event_engine.register(EVENT_WIND, self.process_wind_event)
        self.event_engine.register(EVENT_IFIND, self.process_ifind_event)
        self.event_engine.register(EVENT_O32, self.process_o32_event)
        self.event_engine.register(EVENT_CONTRACT, self.process_contract_event)

    def init(self) -> bool:
        """初始化引擎"""
        self.register_event()

        n: bool = self.load_data()

        self.write_log("引擎初始化完成")

        return n

    def close(self) -> None:
        """关闭引擎"""
        self.save_data()

    def register_event(self) -> None:
        """注册事件监听"""
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)
        self.event_engine.register(EVENT_POSITION, self.process_position_event)

    def process_timer_event(self, event: Event) -> None:
        """处理定时事件"""
        # 更新计划交易市值
        if self.algos:
            if not self.long_plan and not self.short_plan:
                self.update_plan()

        # 检查敞口
        self.check_exposure()

        # 生成列表避免字典改变
        algos: list = list(self.algos.values())

        for algo in algos:
            if algo.status == AlgoStatus.RUNNING:
                algo.on_timer()

                self.put_algo_event(algo)

    def process_position_event(self, event: Event):
        """处理持仓事件"""
        position: PositionData = event.data

        self.offset_converter.update_position(position)

        # 推送组合持仓更新
        tick: TickData = self.get_tick(position.vt_symbol)
        if not tick:
            self.subscribe(position.vt_symbol)

        contract: ContractData = self.get_contract(position.vt_symbol)

        if tick and contract:
            value: float = tick.last_price * position.volume * contract.size
            name: str = contract.name
        else:
            value: float = 0
            name: str = ""

        # 更新到持仓数据
        holding: HoldingData = self.holdings.get(position.vt_symbol, None)
        if not holding:
            holding = HoldingData(
                vt_positionid=position.vt_positionid,
                symbol=position.symbol,
                exchange=position.exchange,
                name=name,
                direction=position.direction,
            )
            self.holdings[position.vt_symbol] = holding

        holding.volume = position.volume
        holding.frozen = position.frozen
        holding.price = position.price
        holding.pnl = round(position.pnl, 0)
        holding.value = round(value, 0)

        # 推送事件
        event: Event = Event(EVENT_REBALANCE_HOLDING, holding)
        self.event_engine.put(event)

    def process_trade_event(self, event: Event) -> None:
        """处理成交事件"""
        trade: TradeData = event.data

        # 添加批次信息
        trade.batch = self.order_batch_map.get(trade.vt_orderid, "")

        # 过滤非本轮成交
        if trade.vt_orderid not in self.algo_orderids:
            return

        # 过滤重复推送
        if trade.vt_tradeid in self.trades:
            return
        self.trades[trade.vt_tradeid] = trade

        # 更新到开平转换器
        self.offset_converter.update_trade(trade)

        # 更新到持仓数据
        holding: HoldingData = self.holdings.get(trade.vt_symbol, None)
        if holding:
            if trade.direction == Direction.LONG:
                holding.volume += trade.volume
            else:
                holding.volume -= trade.volume
            holding.volume = max(holding.volume, 0)
            holding.frozen = min(holding.volume, holding.frozen)

        # 计算成交市值
        self.update_value(trade)

        # 推送给算法
        algo: TwapAlgo = self.algos.get(trade.vt_symbol, None)
        if algo:
            algo.on_trade(trade)

            # 检查是否结束
            if algo.traded_volume >= algo.total_volume:
                algo.status = AlgoStatus.FINISHED
                self.write_log(f"算法执行结束{trade.vt_symbol}")

            self.put_algo_event(algo)

            # 成交后立即写入缓存文件
            self.save_data()

        # 计算剩余市值
        self.update_left()

    def process_order_event(self, event: Event) -> None:
        """处理委托事件"""
        order: OrderData = event.data

        # 添加批次信息
        order.batch = self.order_batch_map.get(order.vt_orderid, "")

        # 过滤已经结束的委托推送
        existing_order = self.orders.get(order.vt_orderid, None)
        if existing_order and not existing_order.is_active():
            return
        self.orders[order.vt_orderid] = copy(order)

        self.offset_converter.update_order(order)

        algo: TwapAlgo = self.algos.get(order.vt_symbol, None)
        if algo:
            algo.on_order(order)

    def process_kafka_event(self, event: Event) -> None:
        """"""
        self.connection_status["kafka"] = event.data

    def process_wind_event(self, event: Event) -> None:
        """"""
        self.connection_status["wind"] = event.data

    def process_ifind_event(self, event: Event) -> None:
        """"""
        self.connection_status["ifind"] = event.data

    def process_o32_event(self, event: Event) -> None:
        """"""
        self.connection_status["o32"] = event.data

    def process_contract_event(self, event: Event) -> None:
        """处理合约事件"""
        # 各订阅1个沪深交易所代码行情，方便检查行情状态
        contract: ContractData = event.data
        if contract.vt_symbol not in {"000001.SZSE", "600000.SSE"}:
            return

        req = SubscribeRequest(contract.symbol, contract.exchange)
        self.main_engine.subscribe(req, contract.gateway_name)

    def subscribe(self, vt_symbol: str) -> None:
        """订阅行情"""
        if vt_symbol in self.subscribed:
            return
        self.subscribed.add(vt_symbol)

        contract: ContractData = self.get_contract(vt_symbol)
        if not contract:
            self.write_log(f"行情订阅失败{vt_symbol}，请检查交易接口登录初始化是否正常！")
            return

        req = SubscribeRequest(
            symbol=contract.symbol,
            exchange=contract.exchange
        )
        self.main_engine.subscribe(req, contract.gateway_name)

    def add_algo(
        self,
        vt_symbol: str,
        direction: Direction,
        limit_price: float,
        total_volume: int,
        time_interval: int,
        batch_count: int,
        price_range: float,
        price_type: PriceType,
        pay_up: int,
        check_position: bool = True
    ) -> bool:
        """添加算法"""
        # 检查合约信息
        contract: ContractData = self.get_contract(vt_symbol)
        if not contract:
            self.write_log(f"添加算法失败，找不到合约：{vt_symbol}")
            return False

        # 卖出需要检查持仓数量
        if direction == Direction.SHORT:
            holding: HoldingData = self.get_holding(vt_symbol)

            if check_position:
                if not holding:
                    self.write_log(
                        f"{vt_symbol}算法卖出数量{total_volume}，"
                        f"超过当前持仓0，进行调整"
                    )
                    total_volume = 0
                else:
                    available = holding.volume - holding.frozen
                    if total_volume > available:
                        self.write_log(
                            f"{vt_symbol}算法卖出数量{total_volume}，"
                            f"超过当前可用{available}，进行调整"
                        )
                        total_volume = min(total_volume, available)

        # 订阅行情推送
        self.subscribe(vt_symbol)

        # 创建算法实例
        algo: TwapAlgo = TwapAlgo(
            self,
            vt_symbol,
            direction,
            limit_price,
            total_volume,
            time_interval,
            batch_count,
            price_range,
            price_type,
            pay_up
        )
        self.algos[vt_symbol] = algo
        self.put_algo_event(algo)

        self.write_log(f"添加算法成功{vt_symbol}")

        return True

    def start_algo(self, vt_symbol: str) -> bool:
        """启动算法"""
        algo: TwapAlgo = self.algos[vt_symbol]

        # 只允许启动【等待】状态的算法
        if algo.status not in {AlgoStatus.WAITING, AlgoStatus.STOPPED}:
            return False

        algo.status = AlgoStatus.RUNNING
        self.put_algo_event(algo)

        self.write_log(f"启动算法执行{vt_symbol}")
        return True

    def pause_algo(self, vt_symbol: str) -> bool:
        """暂停算法"""
        algo: TwapAlgo = self.algos[vt_symbol]

        # 只允许暂停【运行】状态的算法
        if algo.status != AlgoStatus.RUNNING:
            return False

        algo.status = AlgoStatus.PAUSED
        self.put_algo_event(algo)

        self.write_log(f"暂停算法执行{vt_symbol}")
        return True

    def resume_algo(self, vt_symbol: str) -> bool:
        """恢复算法"""
        algo: TwapAlgo = self.algos[vt_symbol]

        # 只允许恢复【暂停】状态的算法
        if algo.status != AlgoStatus.PAUSED:
            return False

        algo.status = AlgoStatus.RUNNING
        self.put_algo_event(algo)

        self.write_log(f"恢复算法执行{vt_symbol}")
        return True

    def stop_algo(self, vt_symbol: str) -> bool:
        """暂停算法"""
        algo: TwapAlgo = self.algos[vt_symbol]

        # 只允许暂停【运行】、【暂停】状态的算法
        if algo.status not in {AlgoStatus.RUNNING, AlgoStatus.PAUSED}:
            return False

        algo.status = AlgoStatus.STOPPED
        self.put_algo_event(algo)

        self.write_log(f"停止算法执行{vt_symbol}")
        return True

    def start_algos(self) -> None:
        """批量启动算法"""
        for vt_symbol in self.algos.keys():
            self.start_algo(vt_symbol)

        self.algo_started = True

    def pause_algos(self, direction: Direction) -> None:
        """批量暂停算法"""
        for vt_symbol, algo in self.algos.items():
            if algo.direction == direction:
                self.pause_algo(vt_symbol)

        if direction == Direction.LONG:
            self.long_pause = True
        else:
            self.short_pause = True

    def resume_algos(self) -> None:
        """批量恢复算法"""
        for vt_symbol in self.algos.keys():
            self.resume_algo(vt_symbol)

        self.long_pause = False
        self.short_pause = False

    def stop_algos(self) -> None:
        """批量启动算法"""
        for vt_symbol in self.algos.keys():
            self.stop_algo(vt_symbol)

    def clear_algos(self) -> bool:
        """清空所有算法"""
        # 检查没有算法在运行
        for algo in self.algos.values():
            if algo.status in {AlgoStatus.RUNNING, AlgoStatus.PAUSED}:
                return False

        # 清空算法对象
        self.algos.clear()

        # 清空订阅记录
        self.subscribed.clear()

        # 清空算法委托号
        self.algo_orderids.clear()

        # 清空暂停状态
        self.long_plan = 0
        self.short_plan = 0

        # 清空统计数据
        self.long_value = 0
        self.short_value = 0
        self.net_value = 0
        self.long_left = 0
        self.short_left = 0
        self.long_pause = False
        self.short_pause = False

        self.put_exposure_event()

        # 清空启动状态
        self.algo_started = False

        self.write_log("清空所有算法")

        return True

    def send_order(
        self,
        algo: TwapAlgo,
        vt_symbol: str,
        direction: Direction,
        price: float,
        volume: float,
        batch: int
    ) -> str:
        """委托下单"""
        # 创建原始委托
        contract: ContractData = self.main_engine.get_contract(vt_symbol)

        original_req: OrderRequest = OrderRequest(
            symbol=contract.symbol,
            exchange=contract.exchange,
            direction=direction,
            type=OrderType.LIMIT,
            volume=volume,
            price=price,
            reference=f"{APP_NAME}_{vt_symbol}"
        )

        # 进行净仓位转换
        reqs: list[OrderRequest] = self.offset_converter.convert_order_request(
            original_req,
            lock=False,
            net=True
        )

        vt_orderids: list[str] = []
        for req in reqs:
            vt_orderid: str = self.main_engine.send_order(req, contract.gateway_name)

            if not vt_orderid:
                continue

            vt_orderids.append(vt_orderid)
            self.offset_converter.update_order_request(req, vt_orderid)

        # 绑定映射关系
        self.algo_orderids.update(vt_orderids)
        for vt_orderid in vt_orderids:
            self.order_batch_map[vt_orderid] = batch

        return vt_orderids

    def cancel_order(self, algo: TwapAlgo, vt_orderid: str) -> None:
        """委托撤单"""
        order: OrderData = self.main_engine.get_order(vt_orderid)

        if not order:
            self.write_log(f"委托撤单失败，找不到委托：{vt_orderid}")
            return

        req: CancelRequest = order.create_cancel_request()
        self.main_engine.cancel_order(req, order.gateway_name)

    def get_holding(self, vt_symbol: str) -> Optional[HoldingData]:
        """查询持仓数据"""
        return self.holdings.get(vt_symbol, None)

    def write_log(self, msg: str) -> None:
        """输出日志"""
        log: LogData = LogData(msg=msg, gateway_name=APP_NAME)
        event: Event = Event(EVENT_REBALANCE_LOG, data=log)
        self.event_engine.put(event)

    def put_algo_event(self, algo: TwapAlgo) -> None:
        """推送事件"""
        event: Event = Event(EVENT_REBALANCE_ALGO, data=algo)
        self.event_engine.put(event)

    def update_value(self, trade: TradeData) -> None:
        """更新成交市值"""
        contract: ContractData = self.get_contract(trade.vt_symbol)
        trade_value: float = trade.price * trade.volume * contract.size

        if trade.direction == Direction.LONG:
            self.long_value += trade_value
        else:
            self.short_value += trade_value
        self.net_value = self.long_value - self.short_value

    def update_left(self) -> None:
        """更新剩余市值"""
        self.long_left = 0
        self.short_left = 0

        for algo in self.algos.values():
            contract: ContractData = self.get_contract(algo.vt_symbol)
            tick: TickData = self.get_tick(algo.vt_symbol)

            if not contract or not tick:
                self.write_log(f"{algo.vt_symbol}合约或者行情数据缺失，无法计算剩余市值")
                continue

            left_volume = algo.total_volume - algo.traded_volume
            left_value = left_volume * tick.last_price * contract.size

            if algo.direction == Direction.LONG:
                self.long_left += left_value
            else:
                self.short_left += left_value

    def update_plan(self) -> None:
        """更新计划交易市值"""
        long_plan = 0
        short_plan = 0

        for algo in self.algos.values():
            tick: TickData = self.get_tick(algo.vt_symbol)
            if not tick or not tick.last_price:
                return

            value = algo.total_volume * tick.last_price
            if algo.direction == Direction.LONG:
                long_plan += value
            else:
                short_plan += value

        self.long_plan = long_plan
        self.short_plan = short_plan

    def check_exposure(self) -> None:
        """检查敞口"""
        if self.algo_started:
            # 空头已经执行完毕
            if self.check_short_finished():
                self.resume_algos()
            # 多头执行太快
            elif self.net_value > self.exposure_maximum:
                if not self.long_pause:
                    self.pause_algos(Direction.LONG)
                    self.long_pause = True
            # 空头执行太块
            elif self.net_value < self.exposure_minimum:
                if not self.short_pause:
                    self.pause_algos(Direction.SHORT)
                    self.short_pause = True
            # 其他正常情况
            else:
                self.resume_algos()

        self.put_exposure_event()

    def put_exposure_event(self) -> None:
        """推送敞口事件"""
        event: Event = Event(
            type=EVENT_REBALANCE_EXPOSURE,
            data={
                "long_value": self.long_value,
                "long_left": self.long_left,
                "long_plan": self.long_plan,
                "short_plan": self.short_plan,
                "short_value": self.short_value,
                "short_left": self.short_left,
                "net_value": self.net_value,
                "long_pause": self.long_pause,
                "short_pause": self.short_pause,
            }
        )
        self.event_engine.put(event)

    def save_data(self) -> None:
        """保存数据"""
        algo_data: list[dict] = []
        for algo in self.algos.values():
            d: dict = {
                # 参数
                "vt_symbol": algo.vt_symbol,
                "direction": algo.direction.value,
                "limit_price": algo.limit_price,
                "total_volume": algo.total_volume,
                "time_interval": algo.time_interval,
                "batch_count": algo.batch_count,
                "price_range": algo.price_range,
                "price_type": algo.price_type.value,
                "pay_up": algo.pay_up,
                # 变量
                "total_count": algo.total_count,
                "traded_volume": algo.traded_volume
            }
            algo_data.append(d)

        data: dict = {
            "algo": algo_data,
            "long_value": self.long_value,
            "short_value": self.short_value,
            "net_value": self.net_value,
            "long_left": self.long_left,
            "short_left": self.short_left,
            "long_plan": self.long_plan,
            "short_plan": self.short_plan,
        }
        save_json(self.data_filename, data)

    def load_data(self) -> bool:
        """载入数据"""
        data: dict = load_json(self.data_filename)

        algo_data: list[dict] = data.get("algo", [])
        for d in algo_data:
            # 添加算法实例
            result = self.add_algo(
                d["vt_symbol"],
                Direction(d["direction"]),
                d["limit_price"],
                d["total_volume"],
                d["time_interval"],
                d["batch_count"],
                d["price_range"],
                PriceType(d["price_type"]),
                d["pay_up"],
                check_position=False
            )

            # 恢复算法变量
            if result:
                algo: TwapAlgo = self.algos[d["vt_symbol"]]
                algo.total_count = d["total_count"]
                algo.traded_volume = d["traded_volume"]

                # 判断算法状态
                if algo.traded_volume >= algo.total_volume:
                    algo.status = AlgoStatus.FINISHED
                else:
                    algo.status = AlgoStatus.WAITING

                self.put_algo_event(algo)

        # 恢复统计数据
        self.long_value = data.get("long_value", 0)
        self.short_value = data.get("short_value", 0)
        self.net_value = data.get("net_value", 0)
        self.long_left = data.get("long_left", 0)
        self.short_left = data.get("short_left", 0)
        self.long_plan = data.get("long_plan", 0)
        self.short_plan = data.get("short_plan", 0)

        return bool(data)

    def check_short_finished(self) -> bool:
        """检查卖出执行结束"""
        # 遍历所有算法
        for algo in self.algos.values():
            # 过滤买入算法
            if algo.direction == Direction.LONG:
                continue

            # 判断运行中状态
            if algo.status != AlgoStatus.FINISHED:
                return False

        return True
