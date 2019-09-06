from Engine.Defaults import *

class SimulationParameters(object):
    def __init__(self, datetime1, datetime2,
                 trading_calendar,
                 capital_base=DEFAULT_CAPITAL_BASE,
                 emission_rate='daily',
                 data_frequency='daily',
                 settlement_frequency = 'monthly',
                 arena='backtest'):

        self._start_session = normalize_date(datetime1)
        self._end_session = normalize_date(datetime2)
        self._capital_base = capital_base

        self._emission_rate = emission_rate
        self._data_frequency = data_frequency
        self._settlement_frequency = settlement_frequency

        # copied to algorithm's environment for runtime access
        self._arena = arena

        self.trading_calendar = trading_calendar

    @property
    def CapitalBase(self):
        return self._capital_base

    @property
    def EmissionRate(self):
        return self._emission_rate

    @property
    def DataFrequency(self):
        return self._data_frequency

    @property
    def SettlementFrequency(self):
        return self._settlement_frequency

    @property
    def Arena(self):
        return self._arena

    @property
    def DateTime1(self):
        return self._start_session

    @property
    def DateTime2(self):
        return self._end_session