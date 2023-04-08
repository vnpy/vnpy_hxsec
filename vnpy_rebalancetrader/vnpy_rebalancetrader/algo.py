from enum import Enum
from math import ceil
from typing import TYPE_CHECKING

from vnpy.trader.constant import Direction
from vnpy.trader.object import ContractData, OrderData, TickData, TradeData
from vnpy.trader.utility import floor_to, round_to

if TYPE_CHECKING:
    from .engine import RebalanceEngine


class AlgoStatus(Enum):
    """算法状态"""
    WAITING = "等待"
    RUNNING = "运行"
    PAUSED = "暂停"
    FINISHED = "结束"
    STOPPED = "停止"


class PriceType(Enum):
    """算法价格类型"""
    MAKER_1 = "本方1档"
    MAKER_2 = "本方2档"
    MAKER_3 = "本方3档"
    MAKER_4 = "本方4档"
    MAKER_5 = "本方5档"
    TAKER_1 = "对方1档"
    TAKER_2 = "对方2档"
    TAKER_3 = "对方3档"
    TAKER_4 = "对方4档"
    TAKER_5 = "对方5档"


LONG_PRICE_MAP = {
    PriceType.TAKER_5: "ask_price_5",
    PriceType.TAKER_4: "ask_price_4",
    PriceType.TAKER_3: "ask_price_3",
    PriceType.TAKER_2: "ask_price_2",
    PriceType.TAKER_1: "ask_price_1",
    PriceType.MAKER_1: "bid_price_1",
    PriceType.MAKER_2: "bid_price_2",
    PriceType.MAKER_3: "bid_price_3",
    PriceType.MAKER_4: "bid_price_4",
    PriceType.MAKER_5: "bid_price_5",
}

SHORT_PRICE_MAP = {
    PriceType.TAKER_5: "bid_price_5",
    PriceType.TAKER_4: "bid_price_4",
    PriceType.TAKER_3: "bid_price_3",
    PriceType.TAKER_2: "bid_price_2",
    PriceType.TAKER_1: "bid_price_1",
    PriceType.MAKER_1: "ask_price_1",
    PriceType.MAKER_2: "ask_price_2",
    PriceType.MAKER_3: "ask_price_3",
    PriceType.MAKER_4: "ask_price_4",
    PriceType.MAKER_5: "ask_price_5",
}


class TwapAlgo:
    """TWAP算法"""

    def __init__(
        self,
        engine: "RebalanceEngine",
        vt_symbol: str,
        direction: Direction,
        limit_price: float,
        total_volume: int,
        time_interval: int,
        batch_count: int,
        price_range: float,
        price_type: PriceType,
        pay_up: int
    ) -> None:
        """构造函数"""
        self.engine: RebalanceEngine = engine

        # 参数
        self.vt_symbol: str = vt_symbol             # 代码
        self.direction: Direction = direction       # 买卖
        self.limit_price: float = limit_price       # 限价
        self.total_volume: int = total_volume       # 总量
        self.time_interval: int = time_interval     # 间隔
        self.batch_total: int = batch_count         # 总轮数
        self.batch_count: int = batch_count         # 轮数
        self.price_range: float = price_range       # 价格范围
        self.price_type: PriceType = price_type     # 委托参考
        self.pay_up: int = pay_up                   # 委托超价

        # 变量
        self.status: AlgoStatus = AlgoStatus.WAITING
        self.timer_count: int = 0
        self.total_count: int = 0
        self.traded_volume: int = 0
        self.left_volume: int = total_volume
        self.active_orderids: set[str] = set()
        self.to_run: bool = False

        # 名称
        contract = self.engine.get_contract(vt_symbol)
        self.name: str = contract.name

    def on_trade(self, trade: TradeData):
        """成交推送"""
        # 累加成交量
        self.traded_volume += trade.volume

        self.left_volume = self.total_volume - self.traded_volume

    def on_order(self, order: OrderData) -> None:
        """委托推送"""
        # 移除活动委托号
        if not order.is_active() and order.vt_orderid in self.active_orderids:
            self.active_orderids.remove(order.vt_orderid)

        # 检查是否要执行
        if self.to_run and not self.active_orderids:
            self.to_run = False
            self.run()

    def on_timer(self) -> None:
        """定时推送"""
        self.total_count += 1

        # 间隔检查
        self.timer_count += 1
        if self.timer_count < self.time_interval:
            return
        self.timer_count = 0

        # 委托检查
        if self.active_orderids:
            for vt_orderid in self.active_orderids:
                self.engine.cancel_order(self, vt_orderid)

            self.to_run = True
            return

        # 执行下单
        self.run()

    def run(self) -> None:
        """执行下单"""
        # 剩余批次数减1
        self.batch_count -= 1
        self.batch_count = max(self.batch_count, 0)

        # 过滤无行情的情况
        tick: TickData = self.engine.get_tick(self.vt_symbol)
        if not tick:
            self.engine.write_log(f"{self.vt_symbol}当前行情缺失，无法执行委托")
            return

        contract: ContractData = self.engine.get_contract(self.vt_symbol)

        # 检查当前价格是否在限制范围内
        if self.price_range and tick.pre_close:
            price_change = (tick.last_price - tick.pre_close) / tick.pre_close
            if abs(price_change) >= self.price_range:
                self.engine.write_log(f"{self.vt_symbol}当前涨跌{price_change:.2%}，超过算法限制的{self.price_range:.2%}，无法执行委托")
                return

        # 检查盘口买卖均有挂单
        if not tick.ask_volume_1 or not tick.ask_price_1:
            self.engine.write_log(f"{self.vt_symbol}当前行情盘口卖单缺失，无法执行交易")
            return
        elif not tick.bid_volume_1 or not tick.bid_price_1:
            self.engine.write_log(f"{self.vt_symbol}当前行情盘口买单缺失，无法执行交易")
            return

        # 检查价格满足限价条件
        if self.limit_price:
            if self.direction == Direction.LONG and tick.ask_price_1 > self.limit_price:
                self.engine.write_log(f"{self.vt_symbol}卖1{tick.ask_price_1}大于{self.limit_price}，无法执行委托")
                return
            elif self.direction == Direction.SHORT and tick.bid_price_1 < self.limit_price:
                self.engine.write_log(f"{self.vt_symbol}买1{tick.bid_price_1}大于{self.limit_price}，无法执行委托")
                return

        # 计算剩余委托量
        volume_left: int = self.total_volume - self.traded_volume

        # 计算本轮委托数量
        order_volume: int = ceil(volume_left / (self.batch_count + 1))      # 首轮开始时先减了1
        order_volume: int = min(order_volume, volume_left)

        # 检查卖出不超过持仓量
        if self.direction == Direction.SHORT:
            holding = self.engine.get_holding(self.vt_symbol)
            if not holding:
                self.engine.write_log(f"{self.vt_symbol}获取持仓数量失败，无法执行卖出操作")
                return

            if not holding.volume:
                self.engine.write_log(f"{self.vt_symbol}持仓数量为0，无法执行卖出操作，停止算法")
                self.status = AlgoStatus.FINISHED
                return

            order_volume = min(order_volume, holding.volume)

        # 判断最小交易数量
        if self.vt_symbol.startswith("688"):
            min_volume: int = 200
        else:
            min_volume: int = 100

        # 买入对委托数量取整
        if self.direction == Direction.LONG:
            order_volume = round_to(order_volume, min_volume)
        # 卖出时允许零股
        else:
            holding = self.engine.get_holding(self.vt_symbol)

            if holding.volume >= min_volume:
                order_volume = floor_to(order_volume, min_volume)
            else:
                order_volume = floor_to(order_volume, 1)

        # 如果取整后数量为0，则跳过本轮执行
        if not order_volume:
            self.engine.write_log(f"{self.vt_symbol}委托数量为0，跳过本轮执行")
            return

        # 计算委托价格
        if self.direction == Direction.LONG:
            field: str = LONG_PRICE_MAP[self.price_type]
            order_price: float = getattr(tick, field, None)

            # 能够正常获取到价格
            if order_price:
                order_price += self.pay_up * contract.pricetick
            # 如果跌停则用ask1买
            elif check_limit_down(tick):
                order_price = tick.ask_price_1
            # 否则无法执行交易
            else:
                self.engine.write_log(f"{self.vt_symbol}当前行情盘口{field}缺失，无法执行交易")
                return
        else:
            field: str = SHORT_PRICE_MAP[self.price_type]
            order_price: float = getattr(tick, field, None)

            # 能够正常获取到价格
            if order_price:
                order_price -= self.pay_up * contract.pricetick
            # 如果涨停则用bid1卖
            elif check_limit_up(tick):
                order_price = tick.bid_price_1
            # 否则无法执行交易
            else:
                self.engine.write_log(f"{self.vt_symbol}当前行情盘口{field}缺失，无法执行交易")
                return

        # 对委托价格取整
        order_price = round_to(order_price, contract.pricetick)

        # 发出委托
        vt_orderids = self.engine.send_order(
            self,
            self.vt_symbol,
            self.direction,
            order_price,
            order_volume,
            self.batch_count + 1
        )

        self.active_orderids.update(vt_orderids)


def check_limit_up(tick: TickData) -> bool:
    """检查涨停"""
    # 基于昨收计算涨跌停
    limit_up: float = tick.pre_close * 1.1

    # 计算最新价偏差值
    if abs(limit_up - tick.last_price) > 0.02:
        return False

    # 检查盘口挂单情况
    ask_status: bool = all([
        tick.ask_volume_1,
        tick.ask_volume_2,
        tick.ask_volume_3,
        tick.ask_volume_4,
        tick.ask_volume_5
    ])
    bid_status: bool = all([
        tick.bid_volume_1,
        tick.bid_volume_2,
        tick.bid_volume_3,
        tick.bid_volume_4,
        tick.bid_volume_5
    ])

    # 还有卖单，说明没涨停
    if ask_status:
        return False
    # 没有卖单只有买单，说明涨停
    elif bid_status:
        return True


def check_limit_down(tick: TickData) -> bool:
    """检查跌停"""
    # 基于昨收计算涨跌停
    limit_down: float = tick.pre_close * 0.9

    # 计算最新价偏差值
    if abs(limit_down - tick.last_price) > 0.02:
        return False

    # 检查盘口挂单情况
    ask_status: bool = all([
        tick.ask_volume_1,
        tick.ask_volume_2,
        tick.ask_volume_3,
        tick.ask_volume_4,
        tick.ask_volume_5
    ])
    bid_status: bool = all([
        tick.bid_volume_1,
        tick.bid_volume_2,
        tick.bid_volume_3,
        tick.bid_volume_4,
        tick.bid_volume_5
    ])

    # 还有买单，说明没跌停
    if bid_status:
        return False
    # 没有买单只有卖单，说明跌停
    elif ask_status:
        return True
