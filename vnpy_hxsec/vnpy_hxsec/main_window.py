from vnpy.trader.ui.mainwindow import (
    MainWindow,
    QtWidgets,
    BaseMonitor,
    Tuple,
    MainEngine,
    EventEngine,
    TRADER_DIR,
    vnpy,
    ConnectDialog
)


class HxMainWindow(MainWindow):

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__(main_engine, event_engine)

        self.setWindowTitle(f"VeighNa Trader 华兴资管版 - {vnpy.__version__}   [{TRADER_DIR}]")

    def create_dock(
        self,
        widget_class: QtWidgets.QWidget,
        name: str,
        area: int
    ) -> Tuple[QtWidgets.QWidget, QtWidgets.QDockWidget]:
        """
        Initialize a dock widget.
        """
        widget: QtWidgets.QWidget = widget_class(self.main_engine, self.event_engine)
        if isinstance(widget, BaseMonitor):
            self.monitors[name] = widget
            widget.verticalHeader().setVisible(True)

        dock: QtWidgets.QDockWidget = QtWidgets.QDockWidget(name)
        dock.setWidget(widget)
        dock.setObjectName(name)
        dock.setFeatures(dock.DockWidgetFloatable | dock.DockWidgetMovable)
        self.addDockWidget(area, dock)
        return widget, dock

    def connect(self, gateway_name: str) -> None:
        """
        Open connect dialog for gateway connection.
        """
        dialog: ConnectDialog = ConnectDialog(self.main_engine, gateway_name)
        dialog.exec_()
