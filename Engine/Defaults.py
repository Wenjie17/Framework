import Core.Gadget as Gadget
import datetime

DEFAULT_CAPITAL_BASE = 1000000

DAILY = 'daily'
WEEKLY = 'weekly'
MONTHLY = 'monthly'
YEARLY = 'yearly'

APPROX_BDAYS_PER_MONTH = 21
APPROX_BDAYS_PER_YEAR = 252

MONTHS_PER_YEAR = 12
WEEKS_PER_YEAR = 52


ANNUALIZATION_FACTORS = {
    DAILY: APPROX_BDAYS_PER_YEAR,
    WEEKLY: WEEKS_PER_YEAR,
    MONTHLY: MONTHS_PER_YEAR,
    YEARLY: 1
}

def normalize_date(dt):
    """
    Returns datetime.date as a datetime.datetime at midnight
    normalized : datetime.datetime or Timestamp
    """
    #return dt.normalize()

    dt_midnight = datetime.datetime(dt.year,dt.month,dt.day)
    dt_midnight += datetime.timedelta(hours=24)
    dt_utc = Gadget.ToUTCDateTime(dt_midnight)
    return dt_utc

