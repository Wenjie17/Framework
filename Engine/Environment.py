
class TradingEnvironment(object):
    def __init__(self,
                 benchmark_symbol='000300.SH',
                 treasury_curves=None,
                 trading_calendar=None,
                 database=None,
                 realtimeView=None):
        self._benchmark_symbol = benchmark_symbol
        self._treasury_curves = treasury_curves
        self.trading_calender = trading_calendar
        self.database = database
        self.realtimeView = realtimeView

    @property
    def BenchmarkSymbol(self):
        return self._benchmark_symbol

    @property
    def TradingCalendar(self):
        return self.trading_calender

    @property
    def Database(self):
        return self.database

    @property
    def BatchView(self):
        return self.database

    @property
    def RealTimeView(self):
        return self.realtimeView