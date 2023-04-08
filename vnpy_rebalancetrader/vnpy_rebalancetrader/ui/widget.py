import traceback
from csv import DictReader

import chardet

from vnpy.event import EventEngine, Event
from vnpy.trader.object import LogData
from vnpy.trader.engine import MainEngine
from vnpy.trader.constant import Direction
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.ui import QtWidgets, QtCore
from vnpy.trader.ui.widget import (
    BaseMonitor,
    BaseCell,
    EnumCell,
    DirectionCell,
    PnlCell,
    TimeCell,
    EVENT_TRADE,
    EVENT_ORDER
)

from ..engine import APP_NAME, EVENT_REBALANCE_ALGO, EVENT_REBALANCE_EXPOSURE, EVENT_REBALANCE_HOLDING, EVENT_REBALANCE_LOG, RebalanceEngine
from ..algo import TwapAlgo, PriceType


class RebalanceWidget(QtWidgets.QWidget):
    """篮子交易监控组件"""

    signal_log = QtCore.pyqtSignal(Event)
    signal_timer = QtCore.pyqtSignal(Event)
    signal_algo = QtCore.pyqtSignal(Event)
    signal_exposure = QtCore.pyqtSignal(Event)

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__()

        self.main_engine: MainEngine = main_engine
        self.event_engine: EventEngine = event_engine

        self.engine: RebalanceEngine = main_engine.get_engine(APP_NAME)

        self.show = self.showMaximized

        self.init_ui()
        self.register_event()

    def init_ui(self) -> None:
        """初始化界面"""
        self.setWindowTitle("组合调仓")

        self.long_monitor: AlgoMonitor = AlgoMonitor(self.main_engine, self.event_engine)
        self.short_monitor: AlgoMonitor = AlgoMonitor(self.main_engine, self.event_engine)

        self.order_monitor: RebalanceOrderMonitor = RebalanceOrderMonitor(self.main_engine, self.event_engine)
        self.trade_monitor: RebalanceTradeMonitor = RebalanceTradeMonitor(self.main_engine, self.event_engine)
        self.holding_monitor: RebalanceHoldingMonitor = RebalanceHoldingMonitor(self.main_engine, self.event_engine)

        self.log_monitor: QtWidgets.QTextEdit = QtWidgets.QTextEdit()
        self.log_monitor.setReadOnly(True)
        self.log_monitor.setMaximumWidth(500)

        for monitor in [
            self.long_monitor,
            self.short_monitor,
            self.order_monitor,
            self.trade_monitor,
            self.holding_monitor
        ]:
            monitor.verticalHeader().setVisible(True)

        self.init_button = QtWidgets.QPushButton("初始化")
        self.init_button.clicked.connect(self.init_engine)

        self.csv_button = QtWidgets.QPushButton("载入CSV")
        self.csv_button.clicked.connect(self.load_csv)
        self.csv_button.setEnabled(False)

        self.start_button = QtWidgets.QPushButton("启动算法")
        self.start_button.clicked.connect(self.start_algos)
        self.start_button.setEnabled(False)

        self.stop_button = QtWidgets.QPushButton("停止算法")
        self.stop_button.clicked.connect(self.stop_algos)
        self.stop_button.setEnabled(False)

        self.clear_button = QtWidgets.QPushButton("清空算法")
        self.clear_button.clicked.connect(self.clear_algos)
        self.clear_button.setEnabled(False)

        self.long_value_label = QtWidgets.QLabel()
        self.short_value_label = QtWidgets.QLabel()
        self.net_value_label = QtWidgets.QLabel()
        self.long_plan_label = QtWidgets.QLabel()
        self.short_plan_label = QtWidgets.QLabel()
        self.long_pause_label = QtWidgets.QLabel()
        self.short_pause_label = QtWidgets.QLabel()

        self.maximum_spin = QtWidgets.QSpinBox()
        self.maximum_spin.setRange(-1_000_000_000, 1_000_000_000)
        self.maximum_spin.setSingleStep(10_000)
        self.maximum_spin.setValue(self.engine.exposure_maximum)
        self.maximum_spin.valueChanged.connect(self.update_exposure_maximum)

        self.minimum_spin = QtWidgets.QSpinBox()
        self.minimum_spin.setRange(-1_000_000_000, 1_000_000_000)
        self.minimum_spin.setSingleStep(10_000)
        self.minimum_spin.setValue(self.engine.exposure_minimum)
        self.minimum_spin.valueChanged.connect(self.update_exposure_minimum)

        self.kafka_label = QtWidgets.QLabel("")
        self.wind_label = QtWidgets.QLabel("")
        self.ifind_label = QtWidgets.QLabel("")
        self.o32_label = QtWidgets.QLabel("")

        form1 = QtWidgets.QFormLayout()
        form1.addRow("敞口上限", self.maximum_spin)
        form1.addRow("敞口下限", self.minimum_spin)

        hbox1 = QtWidgets.QHBoxLayout()
        hbox1.addWidget(self.init_button)
        hbox1.addWidget(self.csv_button)
        hbox1.addWidget(self.start_button)
        hbox1.addWidget(self.stop_button)
        hbox1.addStretch()
        hbox1.addWidget(self.kafka_label)
        hbox1.addWidget(self.ifind_label)
        hbox1.addWidget(self.wind_label)
        hbox1.addWidget(self.o32_label)
        hbox1.addStretch()
        hbox1.addLayout(form1)
        hbox1.addStretch()
        hbox1.addWidget(self.clear_button)

        vbox1 = QtWidgets.QVBoxLayout()
        vbox1.addWidget(self.long_monitor)
        vbox1.addWidget(self.short_monitor)

        vbox2 = QtWidgets.QVBoxLayout()
        vbox2.addWidget(self.order_monitor)
        vbox2.addWidget(self.trade_monitor)
        vbox2.addWidget(self.holding_monitor)

        hbox2 = QtWidgets.QHBoxLayout()
        hbox2.addLayout(vbox1)
        hbox2.addLayout(vbox2)
        hbox2.addWidget(self.log_monitor)

        vbox3 = QtWidgets.QVBoxLayout()
        vbox3.addWidget(self.long_value_label)
        vbox3.addWidget(self.short_value_label)

        vbox4 = QtWidgets.QVBoxLayout()
        vbox4.addWidget(self.long_plan_label)
        vbox4.addWidget(self.short_plan_label)

        vbox5 = QtWidgets.QVBoxLayout()
        vbox5.addWidget(self.long_pause_label)
        vbox5.addWidget(self.short_pause_label)

        hbox3 = QtWidgets.QHBoxLayout()
        hbox3.addLayout(vbox3)
        hbox3.addStretch()
        hbox3.addWidget(self.net_value_label)
        hbox3.addStretch()
        hbox3.addLayout(vbox4)
        hbox3.addStretch()
        hbox3.addLayout(vbox5)

        vbox = QtWidgets.QVBoxLayout()
        vbox.addLayout(hbox1)
        vbox.addLayout(hbox2)
        vbox.addLayout(hbox3)

        self.setLayout(vbox)

    def register_event(self) -> None:
        """注册事件监听"""
        self.signal_log.connect(self.process_log_event)
        self.signal_timer.connect(self.process_timer_event)
        self.signal_algo.connect(self.process_algo_event)
        self.signal_exposure.connect(self.process_exposure_event)

        self.event_engine.register(EVENT_REBALANCE_LOG, self.signal_log.emit)
        self.event_engine.register(EVENT_TIMER, self.signal_timer.emit)
        self.event_engine.register(EVENT_REBALANCE_ALGO, self.signal_algo.emit)
        self.event_engine.register(EVENT_REBALANCE_EXPOSURE, self.signal_exposure.emit)

    def process_algo_event(self, event: Event) -> None:
        """处理算法事件"""
        algo: TwapAlgo = event.data

        if algo.direction == Direction.LONG:
            self.long_monitor.process_event(event)
        else:
            self.short_monitor.process_event(event)

    def process_log_event(self, event: Event) -> None:
        """处理日志事件"""
        log: LogData = event.data
        self.log_monitor.append(f"{log.time}: {log.msg}")

    def process_timer_event(self, event: Event) -> None:
        """处理定时事件"""
        self.refresh_connection_status()

    def process_exposure_event(self, event: Event) -> None:
        """处理敞口事件"""
        data: dict = event.data

        long_value: float = data["long_value"]
        long_left: float = data["long_left"]
        long_plan: float = data["long_plan"]

        long_total: float = long_value + long_left
        if long_total:
            long_percent: float = long_value / long_total
        else:
            long_percent: float = 0

        short_value: float = data["short_value"]
        short_left: float = data["short_left"]
        short_plan: float = data["short_plan"]

        short_total: float = short_value + short_left
        if short_total:
            short_percent: float = short_value / short_total
        else:
            short_percent: float = 0

        net_value: float = data["net_value"]
        long_pause: bool = data["long_pause"]
        short_pause: bool = data["short_pause"]

        self.long_value_label.setText(f"已买入 {long_value:.2f} [{long_percent:.2%}]")
        self.short_value_label.setText(f"已卖出 {short_value:.2f} [{short_percent:.2%}]")
        self.net_value_label.setText(f"净敞口 {net_value:.2f}")

        self.long_plan_label.setText(f"总分配买入 {long_plan:.2f}")
        self.short_plan_label.setText(f"总分配卖出 {short_plan:.2f}")

        if long_pause:
            self.long_pause_label.setText("买入暂停")
        else:
            self.long_pause_label.setText("买入正常")

        if short_pause:
            self.short_pause_label.setText("卖出暂停")
        else:
            self.short_pause_label.setText("卖出正常")

    def init_engine(self) -> None:
        """初始化引擎"""
        n: bool = self.engine.init()

        if not n:
            self.csv_button.setEnabled(True)
        else:
            self.csv_button.setEnabled(False)
            self.start_button.setEnabled(True)
            self.stop_button.setEnabled(True)
            self.clear_button.setEnabled(True)

    def load_csv(self) -> None:
        """加载CSV文件"""
        path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self,
            u"导入委托篮子",
            "",
            "CSV(*.csv)"
        )

        if not path:
            return

        # 判断文件编码
        encoding: str = get_encoding(path)

        # 遍历CSV文件加载
        try:
            with open(path, "r", encoding=encoding) as f:
                # 创建读取器
                reader = DictReader(f)

                # 逐行遍历
                for row in reader:
                    self.engine.add_algo(
                        str(row["vt_symbol"]),
                        Direction(row["direction"]),
                        float(row["limit_price"]),
                        int(row["total_volume"]),
                        int(row["time_interval"]),
                        int(row["batch_count"]),
                        float(row["price_range"]),
                        PriceType(row["price_type"]),
                        int(row["pay_up"]),
                    )
        except Exception:
            self.engine.write_log(f"委托篮子数据导入失败：{path}")
            self.engine.write_log(f"报错信息：{traceback.format_exc()}")
            return

        self.engine.write_log(f"委托篮子数据导入成功：{path}")

        self.csv_button.setEnabled(False)
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(True)
        self.clear_button.setEnabled(True)

    def clear_algos(self) -> None:
        """清空所有算法"""
        n = self.engine.clear_algos()
        if not n:
            return

        for monitor in [self.long_monitor, self.short_monitor]:
            monitor.setRowCount(0)
            monitor.cells.clear()

        self.csv_button.setEnabled(True)
        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(False)
        self.clear_button.setEnabled(False)

    def start_algos(self) -> None:
        """启动所有算法"""
        self.engine.start_algos()

    def stop_algos(self) -> None:
        """停止所有算法"""
        self.engine.stop_algos()

    def update_exposure_maximum(self, value: int) -> None:
        """更新敞口上限"""
        self.engine.exposure_maximum = value

    def update_exposure_minimum(self, value: int) -> None:
        """更新敞口下限"""
        self.engine.exposure_minimum = value

    def refresh_connection_status(self) -> None:
        """刷新连接状态"""
        status = self.engine.connection_status

        if status["kafka"]:
            self.kafka_label.setText("Kafka行情已连接")
            self.kafka_label.setStyleSheet("color:green;")
        else:
            self.kafka_label.setText("Kafka行情未连接")
            self.kafka_label.setStyleSheet("color:red;")

        if status["wind"]:
            self.wind_label.setText("Wind行情已连接")
            self.wind_label.setStyleSheet("color:green;")
        else:
            self.wind_label.setText("Wind行情未连接")
            self.wind_label.setStyleSheet("color:red;")

        if status["ifind"]:
            self.ifind_label.setText("iFinD行情已连接")
            self.ifind_label.setStyleSheet("color:green;")
        else:
            self.ifind_label.setText("iFinD行情未连接")
            self.ifind_label.setStyleSheet("color:red;")

        if status["o32"]:
            self.o32_label.setText("O32交易已连接")
            self.o32_label.setStyleSheet("color:green;")
        else:
            self.o32_label.setText("O32交易未连接")
            self.o32_label.setStyleSheet("color:red;")


class AlgoMonitor(BaseMonitor):
    """算法监控组件"""

    event_type = EVENT_REBALANCE_ALGO
    data_key = "vt_symbol"

    headers = {
        "vt_symbol": {"display": "算法标的", "cell": BaseCell, "update": False},
        "name": {"display": "标的名称", "cell": BaseCell, "update": False},
        "direction": {"display": "方向", "cell": DirectionCell, "update": False},
        "limit_price": {"display": "价格", "cell": BaseCell, "update": False},
        "total_volume": {"display": "总数量", "cell": BaseCell, "update": False},
        "time_interval": {"display": "每轮间隔", "cell": BaseCell, "update": False},
        "batch_total": {"display": "总批次", "cell": BaseCell, "update": False},
        "batch_count": {"display": "剩余批次", "cell": BaseCell, "update": True},
        "price_range": {"display": "价格范围", "cell": BaseCell, "update": False},
        "price_type": {"display": "委托价格", "cell": EnumCell, "update": False},
        "pay_up": {"display": "委托超价", "cell": BaseCell, "update": False},
        "status": {"display": "状态", "cell": EnumCell, "update": True},
        "timer_count": {"display": "本轮读秒", "cell": BaseCell, "update": True},
        "total_count": {"display": "总读秒", "cell": BaseCell, "update": True},
        "traded_volume": {"display": "已成交", "cell": BaseCell, "update": True},
        "left_volume": {"display": "剩余", "cell": BaseCell, "update": True},
    }

    def register_event(self) -> None:
        """重载移除"""
        pass

    def save_setting(self) -> None:
        """"""
        pass

    def load_stting(self) -> None:
        """"""
        pass


class RebalanceTradeMonitor(BaseMonitor):

    event_type: str = EVENT_TRADE
    data_key: str = ""
    sorting: bool = True

    headers: dict = {
        "tradeid": {"display": "成交号 ", "cell": BaseCell, "update": False},
        "orderid": {"display": "委托号", "cell": BaseCell, "update": False},
        "batch": {"display": "批次", "cell": BaseCell, "update": False},
        "symbol": {"display": "代码", "cell": BaseCell, "update": False},
        "name": {"display": "名称", "cell": BaseCell, "update": True},
        "exchange": {"display": "交易所", "cell": EnumCell, "update": False},
        "direction": {"display": "方向", "cell": DirectionCell, "update": False},
        "offset": {"display": "开平", "cell": EnumCell, "update": False},
        "price": {"display": "价格", "cell": BaseCell, "update": False},
        "volume": {"display": "数量", "cell": BaseCell, "update": False},
        "datetime": {"display": "时间", "cell": TimeCell, "update": False},
        "gateway_name": {"display": "接口", "cell": BaseCell, "update": False},
    }

    def process_event(self, event: Event) -> None:
        """"""
        data = event.data
        contract = self.main_engine.get_contract(data.vt_symbol)
        if contract:
            data.name = contract.name
        else:
            data.name = ""

        if not hasattr(data, "batch"):
            data.batch = ""

        super().process_event(event)

    def save_setting(self) -> None:
        pass


class RebalanceOrderMonitor(BaseMonitor):

    event_type: str = EVENT_ORDER
    data_key: str = "vt_orderid"
    sorting: bool = True

    headers: dict = {
        "orderid": {"display": "委托号", "cell": BaseCell, "update": False},
        "reference": {"display": "来源", "cell": BaseCell, "update": False},
        "batch": {"display": "批次", "cell": BaseCell, "update": False},
        "symbol": {"display": "代码", "cell": BaseCell, "update": False},
        "name": {"display": "名称", "cell": BaseCell, "update": True},
        "exchange": {"display": "交易所", "cell": EnumCell, "update": False},
        "type": {"display": "类型", "cell": EnumCell, "update": False},
        "direction": {"display": "方向", "cell": DirectionCell, "update": False},
        "offset": {"display": "开平", "cell": EnumCell, "update": False},
        "price": {"display": "价格", "cell": BaseCell, "update": False},
        "volume": {"display": "总数量", "cell": BaseCell, "update": True},
        "traded": {"display": "已成交", "cell": BaseCell, "update": True},
        "status": {"display": "状态", "cell": EnumCell, "update": True},
        "datetime": {"display": "时间", "cell": TimeCell, "update": True},
        "gateway_name": {"display": "接口", "cell": BaseCell, "update": False},
    }

    def process_event(self, event: Event) -> None:
        """"""
        data = event.data
        contract = self.main_engine.get_contract(data.vt_symbol)
        if contract:
            data.name = contract.name
        else:
            data.name = ""

        if not hasattr(data, "batch"):
            data.batch = ""

        super().process_event(event)

    def save_setting(self) -> None:
        pass


class RebalanceHoldingMonitor(BaseMonitor):

    event_type = EVENT_REBALANCE_HOLDING
    data_key = "vt_positionid"
    sorting = True

    headers = {
        "symbol": {"display": "持仓代码", "cell": BaseCell, "update": False},
        "exchange": {"display": "交易所", "cell": EnumCell, "update": False},
        "name": {"display": "名称", "cell": BaseCell, "update": True},
        "direction": {"display": "方向", "cell": DirectionCell, "update": False},
        "volume": {"display": "数量", "cell": BaseCell, "update": True},
        "price": {"display": "均价", "cell": BaseCell, "update": True},
        "pnl": {"display": "盈亏", "cell": PnlCell, "update": True},
        "value": {"display": "市值", "cell": BaseCell, "update": True},
    }

    def process_event(self, event: Event) -> None:
        """"""
        data = event.data
        contract = self.main_engine.get_contract(data.vt_symbol)
        if contract:
            data.name = contract.name
        else:
            data.name = ""

        super().process_event(event)

    def save_setting(self) -> None:
        pass


def get_encoding(path: str) -> str:
    """获取文件编码格式"""
    with open(path, mode="rb") as f:
        result: dict = chardet.detect(f.read())
        return result["encoding"]
