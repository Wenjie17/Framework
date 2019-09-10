import datetime
import Core.Gadget as Gadget
import pytz
import datetime

default_calendar_aliases = {
    'SH': 'SH',
    'SZ': 'SH',
    'NASDAQ': 'NYSE',
    'BATS': 'NYSE',
    'CBOT': 'CME',
    'COMEX': 'CME',
    'NYMEX': 'CME',
    'ICEUS': 'ICE',
    'NYFE': 'ICE',
}

calendars = {}


def resolve_alias(name):
    """
    Resolve a calendar alias for retrieval.

    Parameters
    ----------
    name : str
        The name of the requested calendar.

    Returns
    -------
    canonical_name : str
        The real name of the calendar to create/return.
    """
    if name in default_calendar_aliases:
        return default_calendar_aliases[name]

    name = "SH"
    return name


def GetCalender(name):
    #
    canonical_name = resolve_alias(name)
    #
    if canonical_name == "SH":
        return ShangHaiExchangeCalendar()
    return None


class TradingCalendar(object):
    def __init__(self):
        # Midnight in UTC for each trading day.
        self._currentDateTime = None
    def GetNextDay(self):
        pass


class ShangHaiExchangeCalendar(TradingCalendar):
    @property
    def Name(self):
        return "SH"

    @property
    def TimeZone(self):
        return pytz.timezone("Asia/Shanghai")

    @property
    def OpenTime(self):
        return datetime.time(9, 30)

    @property
    def CloseTime(self):
        return datetime.time(15)