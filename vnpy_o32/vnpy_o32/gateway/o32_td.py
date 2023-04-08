from typing import Dict, List
from datetime import datetime
from pytz import timezone
from copy import copy
import traceback

import socket

from vnpy.trader.gateway import BaseGateway
from vnpy.trader.constant import (
    Direction,
    Offset,
    Exchange,
    OrderType,
    Product,
    Status
)
from vnpy.trader.object import (
    OrderData,
    TradeData,
    PositionData,
    AccountData,
    ContractData,
    OrderRequest,
    CancelRequest,
)

from .py_t2sdk import (
    pyConnectionInterface,
    pyCConfigInterface,
    pyCallbackInterface,
    pySubCallBack,
    pySubscribeParamInterface,
    pyIF2Packer,
    pyIF2UnPacker,
    pyIBizMessage,
)

from vnpy_hxsec.event import EVENT_O32


CHINA_TZ = timezone("Asia/Shanghai")

PRODUCT_O32_VT: Dict[str, Product] = {
    "01": Product.EQUITY,       # 股票
    "03": Product.BOND,         # 国债
    "04": Product.BOND,         # 企债
    "05": Product.BOND,         # 可转债
    "06": Product.BOND,         # 政策性金融债
    "0A": Product.BOND,         # 企债标准券
    "0S": Product.BOND,         # 公司债
    "0T": Product.BOND,         # 地方债
    "02": Product.FUND,         # 封闭式基金
    "0F": Product.FUND          # 开放式基金
}

EXCHANGE_O32_VT: Dict[str, Exchange] = {
    "1": Exchange.SSE,
    "2": Exchange.SZSE,
    "7": Exchange.CFFEX,
    "3": Exchange.SHFE,
    "4": Exchange.CZCE,
    "9": Exchange.DCE,
    "k": Exchange.INE
}
EXCHANGE_VT_O32 = {v: k for k, v in EXCHANGE_O32_VT.items()}

DIRECTION_VT_O32: Dict[Direction, str] = {
    Direction.LONG: "1",
    Direction.SHORT: "2",
}
DIRECTION_O32_VT = {v: k for k, v in DIRECTION_VT_O32.items()}
DIRECTION_O32_VT["3"] = Direction.LONG
DIRECTION_O32_VT["4"] = Direction.SHORT

POS_DIRECTION_O32_VT: Dict[str, Direction] = {
    "1": Direction.LONG,
    "2": Direction.SHORT
}

OFFSET_O32_VT: Dict[str, Offset] = {
    "1": Offset.OPEN,
    "2": Offset.CLOSE
}
OFFSET_VT_O32 = {v: k for k, v in OFFSET_O32_VT.items()}
OFFSET_VT_O32[Offset.CLOSETODAY] = "2"
OFFSET_VT_O32[Offset.CLOSEYESTERDAY] = "2"

STATUS_O32_VT: Dict[str, Status] = {
    "1": Status.SUBMITTING,
    "2": Status.SUBMITTING,
    "3": Status.SUBMITTING,
    "4": Status.NOTTRADED,
    "5": Status.REJECTED,
    "6": Status.PARTTRADED,
    "7": Status.ALLTRADED,
    "8": Status.CANCELLED,
    "9": Status.CANCELLED,
    "a": Status.SUBMITTING
}
STATUS_VT_O32 = {v: k for k, v in STATUS_O32_VT.items()}

ORDERTYPE_VT_O32 = {
    OrderType.LIMIT: "0",
    OrderType.MARKET: "1"
}
ORDERTYPE_O32_VT = {v: k for k, v in ORDERTYPE_VT_O32.items()}

# 请求功能常量
FUNCTION_USER_LOGIN = 10001
FUNCTION_QUERY_COMBI = 30003
FUNCTION_QUERY_ASSET = 35011
FUNCTION_QUERY_ACCOUNT = 35010

FUNCTION_EQUITY_QUERY_CONTRACT = 30011
FUNCTION_EQUITY_QUERY_ORDER = 32001
FUNCTION_EQUITY_QUERY_TRADE = 33001
FUNCTION_EQUITY_QUERY_POSITION = 31001
FUNCTION_EQUITY_SEND_ORDER = 91001
FUNCTION_EQUITY_CANCEL_ORDER = 91114

# 推送消息常量
MSGTYPE_ORDER_SUBMIT = "a"
MSGTYPE_ORDER_CONFIRM = "b"
MSGTYPE_ORDER_REJECT = "c"
MSGTYPE_ORDER_TRADE = "g"
MSGTYPE_CANCEL_SUBMIT = "d"
MSGTYPE_CANCEL_CONFIRM = "e"
MSGTYPE_CANCEL_REJECT = "f"

# 全局的O32交易API对象
td_api: "O32TdApi" = None


class O32TdApi:
    """O32交易API"""

    def __init__(self, gateway: BaseGateway) -> None:
        """构造函数"""
        # 绑定全局实例
        global td_api
        if not td_api:
            td_api = self

        # 连接参数
        self.operator: str = ""                     # 操作员
        self.password: str = ""                     # 密码
        self.asset_no: str = ""                     # 资产单元
        self.account_code: str = ""                 # 账户号
        self.combi_no: str = ""                     # 组合编号
        self.td_server: str = ""                    # 交易服务器
        self.sub_server: str = ""                   # 订阅服务器（消息中心）
        self.stock_types: List[str] = ["01"]        # 查询合约类型（默认股票）
        self.authorization_id: str = ""             # O32开发者授权编号
        self.app_id: str = ""                       # 期货AppID
        self.authorize_code: str = ""               # 期货授权码

        self.equity_trading: bool = False           # 是否交易股票
        self.futures_trading: bool = False          # 是否交易期货
        self.option_trading: bool = False           # 是否交易期权

        # self.terminal_info: str = get_terminal_info()
        self.terminal_info = "BFEBFBFF000406X4;192.168.xxx.xx;;;MYCOMPUTER;C,NFTS,99,32F03533;O32-ZGV201901.02.000;MICROSOFT WINDOWS 10 专业版;总公司;192.168.xxx.xx;3V8XFCX;DELL INC.;LATITUDE E7470;;;[cyy直连]192.168.xx.xx 15827"

        # 状态变量
        self.connect_status: bool = False
        self.login_status: bool = False
        self.user_token: bool = False               # 用户密钥

        # 上层Gateway
        self.gateway: BaseGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        # 接口实例
        self.td_connection: pyConnectionInterface = None
        self.sub_connection: pyConnectionInterface = None

        # 委托相关缓存字典
        self.localid_sysid_map: Dict[str, str] = {}     # 本地委托号：系统委托号
        self.sysid_localid_map: Dict[str, str] = {}     # 系统委托号：本地委托号
        self.reqid_localid_map: Dict[int, str] = {}     # 请求编号：本地委托号

        self.orders: Dict[str, OrderData] = {}
        self.order_count = 100_000                      # 委托号从10万开始

        self.init_callbacks()

    def init_callbacks(self) -> None:
        """初始化回调函数"""
        self.callbacks = {
            FUNCTION_USER_LOGIN: self.on_login,
            FUNCTION_QUERY_COMBI: self.on_query_combi,
            FUNCTION_QUERY_ASSET: self.on_query_asset,
            FUNCTION_QUERY_ACCOUNT: self.on_query_account,

            FUNCTION_EQUITY_QUERY_CONTRACT: self.on_equity_query_contract,
            FUNCTION_EQUITY_QUERY_POSITION: self.on_equity_query_position,
            FUNCTION_EQUITY_QUERY_ORDER: self.on_equity_query_order,
            FUNCTION_EQUITY_QUERY_TRADE: self.on_equity_query_trade,
            FUNCTION_EQUITY_SEND_ORDER: self.on_equity_send_order,
            FUNCTION_EQUITY_CANCEL_ORDER: self.on_equity_cancel_order,

            MSGTYPE_ORDER_SUBMIT: self.on_order_submit,
            MSGTYPE_ORDER_CONFIRM: self.on_order_confirm,
            MSGTYPE_ORDER_REJECT: self.on_order_reject,
            MSGTYPE_ORDER_TRADE: self.on_order_trade,
            MSGTYPE_CANCEL_SUBMIT: self.on_cancel_submit,
            MSGTYPE_CANCEL_CONFIRM: self.on_cancel_confirm,
            MSGTYPE_CANCEL_REJECT: self.on_cancel_reject,
        }

    def connect(
        self,
        operator: str,
        password: str,
        combi_no: str,
        td_server: str,
        sub_server: str,
        stock_types: List[str],
        equity_trading: bool = False,
        futures_trading: bool = False,
        option_trading: bool = False,
        authorization_id: str = "",
        app_id: str = "",
        authorize_code: str = ""
    ) -> None:
        """连接服务器"""
        self.operator = operator
        self.password = password
        self.combi_no = combi_no
        self.td_server = td_server
        self.sub_server = sub_server
        self.stock_types = stock_types
        self.equity_trading = equity_trading
        self.futures_trading = futures_trading
        self.option_trading = option_trading
        self.authorization_id = authorization_id
        self.app_id = app_id
        self.authorize_code = authorize_code

        # 如果尚未连接，则立即创建实例
        if not self.connect_status:
            self.td_connection = self.init_connection("交易", self.td_server)
            self.sub_connection = self.init_connection("推送", self.sub_server)

            if self.td_connection:
                self.connect_status = True

        # 如果已经连接，则立即发起登录
        if self.connect_status and not self.login_status:
            self.login()

    def init_connection(self, name: str, server: str) -> pyConnectionInterface:
        """创建连接实例"""
        config = pyCConfigInterface()
        config.SetString("t2sdk", "servers", server)
        config.SetString("t2sdk", "license_file", "license.dat")
        config.SetInt("t2sdk", "send_queue_size", 100)
        config.SetInt("t2sdk", "auto_reconnect", 1)
        config.SetInt("t2sdk", "if_sendRecv_log", 1)

        async_callback = pyCallbackInterface(__name__, "TdAsyncCallback")
        async_callback.InitInstance()

        connection = pyConnectionInterface(config)
        if not connection:
            self.gateway.write_log(f"{name}连接对象创建失败")
            return None

        ret = connection.Create2BizMsg(async_callback)
        if ret:
            msg = str(connection.GetErrorMsg(ret))
            self.gateway.write_log(f"{name}连接初始化失败，错误码：{ret}，信息：{msg}")
            return None

        ret = connection.Connect(1000)
        if ret:
            msg = str(connection.GetErrorMsg(ret))
            self.gateway.write_log(f"{name}服务器连接失败，错误码：{ret}，信息：{msg}")
            return None

        self.gateway.write_log(f"{name}服务器连接成功")
        return connection

    def init_subscriber(self) -> None:
        """创建订阅者实例"""
        # 创建订阅回调接口实例
        sub_callback = pySubCallBack(__name__, "TdSubCallback")
        sub_callback.initInstance()

        # 创建订阅者实例
        ret, self.subscriber = self.sub_connection.NewSubscriber(
            sub_callback,
            self.operator,
            5000
        )

        # 检查是否订阅成功
        if ret != 0:
            error_msg = str(self.sub_connection.GetMCLastError(), encoding="gbk")
            msg = f"订阅初始化失败：{error_msg}"
            self.gateway.write_log(msg)

    def subscribe_data(self) -> None:
        """订阅推送主题"""
        # 设置订阅参数
        sub_param = pySubscribeParamInterface()
        sub_param.SetTopicName("ufx_topic")
        sub_param.SetFromNow(1)
        sub_param.SetFilter("operator_no", self.operator)
        sub_param.SetReplace(1)

        # 打包数据
        packer = pyIF2Packer()
        packer.BeginPack()
        packer.AddField("login_operator_no")
        packer.AddField("password")
        packer.AddStr(self.operator)
        packer.AddStr(self.password)
        packer.EndPack()

        # 发起主题订阅
        unpacker = pyIF2UnPacker()
        result = self.subscriber.SubscribeTopic(sub_param, 5000, unpacker, packer)

        if result < 0:
            data = unpack_data(unpacker)

            if data:
                error_msg = data[0]["MsgDetail"]
                self.gateway.write_log(f"订阅主题失败：{error_msg}")
            else:
                self.gateway.write_log("订阅主题失败：没有返回错误信息，请检查是否配置了正确的订阅服务器")

        packer.FreeMem()
        packer.Release()
        unpacker.Release()
        sub_param.Release()

    def check_error(self, data: List[Dict[str, str]]) -> bool:
        """检查主动请求回调数据，是否为错误信息"""
        error: dict = data.pop(0)
        error_code: str = error["ErrorCode"]

        if error_code != "0":
            error_msg = error["ErrorMsg"]
            self.gateway.write_log(f"请求失败，错误代码：{error_code}，错误信息：{error_msg}")
            return True
        else:
            return False

    def on_login(self, data: List[Dict[str, str]], reqid: int) -> None:
        """登录回调"""
        if self.check_error(data):
            self.gateway.write_log("O32系统登录失败")
            self.gateway.on_event(EVENT_O32, False)
            return

        self.gateway.write_log("O32系统登录成功")
        self.gateway.on_event(EVENT_O32, True)
        self.login_status = True

        for d in data:
            self.user_token = d["user_token"]

        self.init_subscriber()
        self.subscribe_data()

        self.query_combi()

        if self.equity_trading:
            self.equity_query_contract()
            self.equity_query_order()
            self.equity_query_trade()

        # req = {"user_token": self.user_token}
        # self.send_req(30001, req)
        # self.send_req(30002, req)
        # self.send_req(10002, req)

    def on_query_combi(self, data: List[Dict[str, str]], reqid: int) -> None:
        """组合查询回调"""
        if self.check_error(data):
            self.gateway.write_log("查询交易组合失败")
            return

        for d in data:
            self.account_code = d["account_code"]
            self.asset_no = d["asset_no"]

        if not self.account_code:
            self.gateway.write_log("当前操作员没有配置资金账号权限，请检查！")
        else:
            self.gateway.write_log(f"当前使用资金账号：{self.account_code}")

        if not self.asset_no:
            self.gateway.write_log("当前操作员没有配置资产单元权限，请检查！")
        else:
            self.gateway.write_log(f"当前使用资产单元：{self.asset_no}")

    def on_query_asset(self, data: List[Dict[str, str]], reqid: int) -> None:
        """资产单元查询回调"""
        if self.check_error(data):
            self.gateway.write_log("查询资产单元失败")
            return

        for d in data:
            account = AccountData(
                accountid=d["asset_no"],
                balance=float(d["current_balance"]),
                gateway_name=self.gateway_name
            )

        self.gateway.on_account(account)

    def on_query_account(self, data: List[Dict[str, str]], reqid: int) -> None:
        """账户资金查询回调"""
        if self.check_error(data):
            self.gateway.write_log("查询账户资金失败")
            return

        for d in data:
            account = AccountData(
                accountid=d["account_code"] + "_期货",
                balance=float(d["futu_deposit_balance"]),
                frozen=float(d["occupy_deposit_balance"]),
                gateway_name=self.gateway_name
            )

        self.gateway.on_account(account)

    def on_equity_query_order(self, data: List[Dict[str, str]], reqid: int) -> None:
        """股票委托查询回调"""
        if self.check_error(data):
            self.gateway.write_log("证券委托信息查询失败")
            return

        for d in data:
            timestamp = d["entrust_date"] + " " + d["entrust_time"]
            dt = datetime.strptime(timestamp, "%Y%m%d %H%M%S")
            dt = dt.replace(tzinfo=CHINA_TZ)

            localid = d["extsystem_id"]
            sysid = d["entrust_no"]

            self.order_count = max(self.order_count, int(localid))

            self.localid_sysid_map[localid] = sysid
            self.sysid_localid_map[sysid] = localid

            order = OrderData(
                symbol=d["stock_code"],
                exchange=EXCHANGE_O32_VT[d["market_no"]],
                direction=DIRECTION_O32_VT[d["entrust_direction"]],
                status=STATUS_O32_VT.get(d["entrust_state"], Status.SUBMITTING),
                orderid=d["extsystem_id"],
                offset=Offset.NONE,
                volume=int(d["entrust_amount"]),
                traded=int(d["deal_amount"]),
                price=float(d["entrust_price"]),
                datetime=dt,
                gateway_name=self.gateway_name
            )

            self.orders[localid] = order
            self.gateway.on_order(order)

        self.gateway.write_log("证券委托信息查询成功")

    def on_equity_query_trade(self, data: List[Dict[str, str]], reqid: int) -> None:
        """股票成交查询回调"""
        if self.check_error(data):
            self.gateway.write_log("证券成交信息查询失败")
            return

        for d in data:
            timestamp = d["deal_date"] + " " + d["deal_time"]
            dt = datetime.strptime(timestamp, "%Y%m%d %H%M%S")
            dt = dt.replace(tzinfo=CHINA_TZ)

            trade = TradeData(
                orderid=d["extsystem_id"],
                tradeid=d["deal_no"],
                symbol=d["stock_code"],
                exchange=EXCHANGE_O32_VT[d["market_no"]],
                direction=DIRECTION_O32_VT[d["entrust_direction"]],
                offset=Offset.NONE,
                price=float(d["deal_price"]),
                volume=int(d["deal_amount"]),
                datetime=dt,
                gateway_name=self.gateway_name
            )
            self.gateway.on_trade(trade)

        self.gateway.write_log("证券成交信息查询成功")

    def on_equity_query_contract(self, data: List[Dict[str, str]], reqid: int) -> None:
        """股票合约查询回调"""
        if self.check_error(data):
            self.gateway.write_log("证券合约信息查询失败")
            return

        for d in data:
            exchange = EXCHANGE_O32_VT.get(d["market_no"], None)
            if not exchange:
                continue

            contract = ContractData(
                symbol=d["stock_code"],
                exchange=EXCHANGE_O32_VT[d["market_no"]],
                name=d["stock_name"],
                size=1,
                pricetick=float(d["price_interval"]),
                product=PRODUCT_O32_VT.get(d["stock_type"], Product.EQUITY),
                min_volume=int(float(d["buy_unit"])),
                gateway_name=self.gateway_name
            )
            self.gateway.on_contract(contract)

        if len(data) == 10000:
            position_str = d["position_str"]
            stock_type = d["stock_type"]
            self.equity_query_contract(position_str, stock_type)
        else:
            self.gateway.write_log("证券合约信息查询成功")

    def on_equity_query_position(self, data: List[Dict[str, str]], reqid: int) -> None:
        """股票持仓查询回调"""
        if self.check_error(data):
            self.gateway.write_log("证券持仓信息查询失败")
            return

        for d in data:
            position = PositionData(
                symbol=d["stock_code"],
                exchange=EXCHANGE_O32_VT[d["market_no"]],
                direction=Direction.LONG,
                volume=int((d["current_amount"])),
                price=float(d["begin_cost"]),
                frozen=(int(d["current_amount"]) - int(d["enable_amount"])),
                gateway_name=self.gateway_name
            )
            self.gateway.on_position(position)

    def on_equity_send_order(self, data: List[Dict[str, str]], reqid: int) -> None:
        """股票委托回调"""
        # 检查头部错误信息
        if self.check_error(data):
            localid = self.reqid_localid_map[reqid]

            order = self.orders[localid]
            order.status = Status.REJECTED

            self.gateway.on_order(copy(order))

        # 检查主体委托信息
        for d in data:
            # 只处理O32提供的回报信息（另一条来自联合风控）
            if "fail_cause" in d:
                # 委托失败
                if d["fail_cause"]:
                    self.gateway.write_log("委托失败：" + d["fail_cause"])
                # 委托成功
                else:
                    localid = d["extsystem_id"]
                    sysid = d["entrust_no"]

                    self.localid_sysid_map[localid] = sysid
                    self.sysid_localid_map[sysid] = localid

    def on_equity_cancel_order(self, data: List[Dict[str, str]], reqid: int) -> None:
        """股票撤单回调"""
        if self.check_error(data):
            self.gateway.write_log("撤单失败")
            return

    def on_order_submit(self, data: List[Dict[str, str]]) -> None:
        """委托提交推送"""
        for d in data:
            timestamp = d["business_date"] + " " + d["business_time"]
            dt = datetime.strptime(timestamp, "%Y%m%d %H%M%S")
            dt = dt.replace(tzinfo=CHINA_TZ)

            localid = d["extsystem_id"]
            sysid = d["entrust_no"]

            self.localid_sysid_map[localid] = sysid
            self.sysid_localid_map[sysid] = localid

            order = OrderData(
                symbol=d["stock_code"],
                exchange=EXCHANGE_O32_VT[d["market_no"]],
                orderid=d["extsystem_id"],
                direction=DIRECTION_O32_VT[d["entrust_direction"]],
                offset=OFFSET_O32_VT.get(d["futures_direction"], Offset.NONE),
                status=STATUS_O32_VT.get(d["entrust_state"], Status.SUBMITTING),
                volume=int(d["entrust_amount"]),
                price=float(d["entrust_price"]),
                datetime=dt,
                gateway_name=self.gateway_name
            )

            self.orders[localid] = order
            self.gateway.on_order(order)

    def on_order_confirm(self, data: List[Dict[str, str]]) -> None:
        """委托确认推送"""
        for d in data:
            localid = d["extsystem_id"]
            order = self.orders.get(localid, None)
            if not order:
                return

            # 过滤乱序推送导致的问题
            if not order.is_active():
                return

            order.status = STATUS_O32_VT.get(d["entrust_state"], Status.SUBMITTING)
            self.gateway.on_order(order)

    def on_order_reject(self, data: List[Dict[str, str]]) -> None:
        """委托拒单推送"""
        for d in data:
            localid = d["extsystem_id"]
            order = self.orders.get(localid, None)
            if not order:
                return

            order.status = STATUS_O32_VT.get(d["entrust_state"], Status.SUBMITTING)
            self.gateway.on_order(order)

            msg: str = d["revoke_cause"]
            self.gateway.write_log(msg)

    def on_order_trade(self, data: List[Dict[str, str]]) -> None:
        """委托成交推送"""
        for d in data:
            localid = d["extsystem_id"]
            order = self.orders.get(localid, None)
            if not order:
                return

            order.status = STATUS_O32_VT.get(d["entrust_state"], Status.SUBMITTING)
            order.traded = int(d["total_deal_amount"])
            self.gateway.on_order(order)

            timestamp = d["deal_date"] + " " + d["deal_time"]
            dt = datetime.strptime(timestamp, "%Y%m%d %H%M%S")
            dt = dt.replace(tzinfo=CHINA_TZ)

            trade = TradeData(
                symbol=order.symbol,
                exchange=order.exchange,
                orderid=order.orderid,
                tradeid=d["deal_no"],
                direction=order.direction,
                offset=order.offset,
                price=float(d["deal_price"]),
                volume=int(float(d["deal_amount"])),
                datetime=dt,
                gateway_name=self.gateway_name
            )
            self.gateway.on_trade(trade)

    def on_cancel_submit(self, data: List[Dict[str, str]]) -> None:
        """撤单提交推送"""
        for d in data:
            localid = d["extsystem_id"]
            self.gateway.write_log(f"发出撤单请求：{localid}")

    def on_cancel_confirm(self, data: List[Dict[str, str]]) -> None:
        """撤单确认推送"""
        for d in data:
            localid = d["extsystem_id"]
            order = self.orders.get(localid, None)
            if not order:
                return

            order.status = STATUS_O32_VT.get(d["entrust_state"], Status.SUBMITTING)
            self.gateway.on_order(order)

    def on_cancel_reject(self, data: List[Dict[str, str]]) -> None:
        """撤单拒绝推送"""
        for d in data:
            localid = d["extsystem_id"]
            self.gateway.write_log(f"撤单请求失败：{localid}")

    def on_callback(self, function: int, data: dict, reqid: int = 0) -> None:
        """回调数据推送"""
        try:
            func = self.callbacks.get(function, None)
            if func:
                if reqid:
                    func(data, reqid)
                else:
                    func(data)
            else:
                print("找不到对应回调函数", function, data)
        except Exception:
            traceback.print_exc()

    def send_req(self, function: int, req: dict) -> int:
        """发送请求"""
        packer = pyIF2Packer()
        packer.BeginPack()

        for Field in req.keys():
            packer.AddField(Field)

        for value in req.values():
            packer.AddStr(str(value))

        packer.EndPack()

        msg = pyIBizMessage()
        msg.SetFunction(function)
        msg.SetPacketType(0)
        msg.SetContent(packer.GetPackBuf(), packer.GetPackLen())

        reqid: int = self.td_connection.SendBizMsg(msg, 1)

        packer.FreeMem()
        packer.Release()
        msg.Release()

        return reqid

    def generate_req(self) -> Dict[str, str]:
        """生成基础请求字典"""
        req: dict = {
            "user_token": self.user_token,
            "combi_no": self.combi_no
        }

        return req

    def login(self) -> None:
        """操作员登录"""
        packer = pyIF2Packer()
        packer.BeginPack()

        # 第一部分
        req = {
            "operator_no": self.operator,
            "password": self.password,
            "mac_address": "adfasdf",
            "op_station": "1",
            "ip_address": "192.168.1.1",
            "authorization_id": self.authorization_id,
            "app_id": self.app_id,
            "authorize_code": self.authorize_code,
            "port_id": 80,
            "terminal_info": self.terminal_info
        }

        for Field in req.keys():
            packer.AddField(Field)

        for value in req.values():
            packer.AddStr(str(value))

        packer.EndPack()

        msg = pyIBizMessage()
        msg.SetFunction(FUNCTION_USER_LOGIN)
        msg.SetPacketType(0)
        msg.SetContent(packer.GetPackBuf(), packer.GetPackLen())

        self.td_connection.SendBizMsg(msg, 1)

        packer.FreeMem()
        packer.Release()
        msg.Release()

    def query_combi(self) -> int:
        """查询组合信息"""
        if not self.login_status:
            return

        req = self.generate_req()
        self.send_req(FUNCTION_QUERY_COMBI, req)

    def query_asset(self) -> int:
        """查询资产单元"""
        if not self.login_status:
            return

        if not self.asset_no:
            return

        req = self.generate_req()
        req["account_code"] = self.account_code
        req["asset_no"] = self.asset_no
        self.send_req(FUNCTION_QUERY_ASSET, req)

    def query_account(self) -> int:
        """查询账户资金"""
        if not self.login_status:
            return

        if not self.account_code:
            return

        req = self.generate_req()
        req["account_code"] = self.account_code
        self.send_req(FUNCTION_QUERY_ACCOUNT, req)

    def equity_send_order(self, req: OrderRequest) -> str:
        """股票委托"""
        self.order_count += 1
        orderid = str(self.order_count)

        hs_req = self.generate_req()

        hs_req["market_no"] = EXCHANGE_VT_O32[req.exchange]
        hs_req["stock_code"] = req.symbol
        hs_req["entrust_direction"] = DIRECTION_VT_O32[req.direction]
        hs_req["price_type"] = ORDERTYPE_VT_O32[req.type]
        hs_req["entrust_amount"] = str(req.volume)
        hs_req["entrust_price"] = str(req.price)
        hs_req["extsystem_id"] = orderid
        hs_req["third_ref"] = req.reference
        hs_req["terminal_info"] = self.terminal_info

        reqid: int = self.send_req(FUNCTION_EQUITY_SEND_ORDER, hs_req)

        # 如果为正数说明发送成功
        if reqid > 0:
            # 记录请求号对应的本地委托号
            self.reqid_localid_map[reqid] = orderid

            # 生成提交中的委托数据并推送
            order = req.create_order_data(orderid, self.gateway_name)
            self.orders[orderid] = order

            self.gateway.on_order(order)

            self.gateway.write_log(f"委托请求发送成功，委托号{orderid}")

            return order.vt_orderid
        # 否则说明请求发送失败
        else:
            self.gateway.write_log(f"委托请求发送失败，错误号{reqid}")
            return ""

    def equity_cancel_order(self, req: CancelRequest) -> None:
        """股票撤单"""
        sysid = self.localid_sysid_map.get(req.orderid, "")
        if not sysid:
            self.gateway.write_log(f"无法撤单，找不到本地委托号{req.orderid}对应的系统委托号")
            return

        hs_req = self.generate_req()
        hs_req["entrust_no"] = sysid
        hs_req["terminal_info"] = self.terminal_info
        hs_req["account_code"] = self.account_code

        self.send_req(FUNCTION_EQUITY_CANCEL_ORDER, hs_req)

    def equity_query_position(self) -> None:
        """股票查询持仓"""
        if not self.user_token:
            return

        req = self.generate_req()
        req["request_num"] = "10000"

        self.send_req(FUNCTION_EQUITY_QUERY_POSITION, req)

    def equity_query_trade(self) -> None:
        """股票查询成交"""
        req = self.generate_req()
        req["request_num"] = "10000"
        self.send_req(FUNCTION_EQUITY_QUERY_TRADE, req)

    def equity_query_order(self) -> None:
        """股票查询委托"""
        req = self.generate_req()
        req["request_num"] = "10000"
        self.send_req(FUNCTION_EQUITY_QUERY_ORDER, req)

    def equity_query_contract(
        self,
        position_str: str = None,
        stock_type: str = None
    ) -> None:
        """股票查询合约"""
        if stock_type:
            stock_types = [stock_type]
        else:
            stock_types = self.stock_types

        for st in stock_types:
            req = self.generate_req()
            req["request_num"] = "10000"
            req["stock_type"] = st

            if position_str:
                req["position_str"] = position_str

            self.send_req(FUNCTION_EQUITY_QUERY_CONTRACT, req)


class TdAsyncCallback:
    """异步请求回调接口"""

    def __init__(self) -> None:
        """构造函数"""
        global td_api
        self.td_api: O32TdApi = td_api

    def OnRegister(self) -> None:
        """注册回调"""
        pass

    def OnClose(self) -> None:
        """关闭回调"""
        pass

    def OnReceivedBizMsg(self, hSend, sBuff, iLen) -> None:
        """数据推送回调"""
        biz_msg = pyIBizMessage()
        biz_msg.SetBuff(sBuff, iLen)

        function = biz_msg.GetFunction()
        buf, len = biz_msg.GetContent()

        unpacker = pyIF2UnPacker()
        unpacker.Open(buf, len)
        data = unpack_data(unpacker)
        self.td_api.on_callback(function, data, hSend)

        unpacker.Release()
        biz_msg.Release()


class TdSubCallback:
    """订阅推送回调接口"""

    def __init__(self) -> None:
        """构造函数"""
        global td_api
        self.td_api: O32TdApi = td_api

    def OnReceived(self, topic, sBuff, iLen) -> None:
        """数据推送回调"""
        biz_msg = pyIBizMessage()
        biz_msg.SetBuff(sBuff, iLen)
        buf, len = biz_msg.GetContent()

        unpacker = pyIF2UnPacker()
        unpacker.Open(buf, len)
        data = unpack_data(unpacker)

        msg_type = data[0]["msgtype"]
        self.td_api.on_callback(msg_type, data)

        unpacker.Release()
        biz_msg.Release()


def unpack_data(unpacker: pyIF2UnPacker) -> List[Dict[str, str]]:
    """对T2SDK数据进行解包"""
    data = []

    dataset_count = unpacker.GetDatasetCount()

    for dataset_index in range(dataset_count):
        unpacker.SetCurrentDatasetByIndex(dataset_index)

        row_count = unpacker.GetRowCount()
        col_count = unpacker.GetColCount()

        for row_index in range(row_count):
            d = {}
            for col_index in range(col_count):
                name = unpacker.GetColName(col_index)

                # 联合风控拒单返回的数据，可能有字符串无法解析（改用int获取）
                try:
                    value = unpacker.GetStrByIndex(col_index)
                except UnicodeDecodeError:
                    value = unpacker.GetIntByIndex(col_index)

                d[name] = value

            unpacker.Next()
            data.append(d)

    return data


def get_terminal_info() -> str:
    """获取终端信息"""
    import wmi
    c = wmi.WMI()

    cpuid = ""
    internal_ip = ""
    mobile_no = ""
    mobile_device = ""
    computer_name = ""
    disk_info = ""
    client_info = "Veighna Trader 机构版 3.0"
    os_info = ""
    company_name = ""
    external_ip = ""
    pc_serial = ""
    pc_manufacturer = ""
    pc_model = ""
    ip2 = ""
    mac2 = ""
    node_name = ""
    ip = ""
    port = ""

    for system in c.Win32_ComputerSystem():
        computer_name = system.Name

        pc_manufacturer = system.Manufacturer
        pc_model = system.Model

    for os in c.Win32_OperatingSystem():
        os_info = os.Caption

    for product in c.Win32_ComputerSystemProduct():
        pc_serial = product.IdentifyingNumber

    disk = c.Win32_LogicalDisk()[0]
    disk_info = ",".join([disk.Caption, disk.FileSystem, disk.Size, disk.VolumeSerialNumber])

    cpu = c.Win32_Processor()[0]
    cpuid = cpu.ProcessorId

    internal_ip = socket.gethostbyname(socket.gethostname())

    info = ";".join([
        cpuid,
        internal_ip,
        mobile_no,
        mobile_device,
        computer_name,
        disk_info,
        client_info,
        os_info,
        company_name,
        external_ip,
        pc_serial,
        pc_manufacturer,
        pc_model,
        ip2,
        mac2,
        node_name,
        ip,
        port,
    ])
    return info
