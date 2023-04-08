from pathlib import Path

from vnpy.trader.app import BaseApp

from .engine import RebalanceEngine, APP_NAME


class RebalanceTraderApp(BaseApp):
    """"""
    
    app_name: str = APP_NAME
    app_module: str = __module__
    app_path: Path = Path(__file__).parent
    display_name: str = "组合调仓"
    engine_class: RebalanceEngine = RebalanceEngine
    widget_name: str = "RebalanceWidget"
    icon_name: str = str(app_path.joinpath("ui", "bt.ico"))
