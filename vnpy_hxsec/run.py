from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import create_qapp

from vnpy_hxsec.o32_gateway import O32Gateway
from vnpy_rebalancetrader import RebalanceTraderApp
# from vnpy_tora import ToraStockGateway
from vnpy_hxsec.main_window import HxMainWindow


def main():
    """主入口函数"""
    qapp = create_qapp()

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(O32Gateway)
    main_engine.add_app(RebalanceTraderApp)
    # main_engine.add_gateway(ToraStockGateway)

    main_window = HxMainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()


if __name__ == "__main__":
    main()
