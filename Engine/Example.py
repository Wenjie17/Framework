from Engine.Algorithm import TradingAlgorithm
from Engine.Parameters import SimulationParameters
from Engine.Calender import GetCalender
from Engine.Environment import TradingEnvironment
import Engine

import Core.Gadget as Gadget
import datetime
import random

from Core.Config import *
config = Config()
database = config.DataBase()
realtime = config.RealTime(db=0)

# Define algorithm
def Initialize(api, context):
    print("initialize")
    pass

def HandledData(api, context, data, dt):
    pass

def OnMonthly(api, context, dt):
    print("  --On Monthly-- " + str(dt))
    portfolio = api.Portfolio()
    # print(portfolio.Value)
    position = api.Position("000001.SZ")
    symbols = []
    symbols.append({"Symbol": "000001.SZ"})
    api.Rebalance(symbols)
    pass

tradingCalender = GetCalender("SH")

datetime1 = datetime.datetime(2015, 1, 1)
datetime2 = datetime.datetime(2016, 1, 3)

# Set Parameters
simulatorParameters = SimulationParameters(datetime1=datetime1,
                                           datetime2=datetime2,
                                           trading_calendar=tradingCalender,
                                           data_frequency="monthly")
# Set Environment: e.g. Benchmark
tradingEnvironment = TradingEnvironment(benchmark_symbol="000300.SH",
                                        database=database,
                                        realtimeView=realtime,
                                        trading_calendar=tradingCalender)

# Algorithm
strategy = TradingAlgorithm(name="TestStrategy",
                            initialize=Initialize,
                            handle_data=HandledData,
                            on_daily=OnDaily,
                            on_weekly=OnWeekly,
                            on_monthly=OnMonthly,
                            on_monthly_begin=OnMonthlyBegin,
                            analyze=StrategyEngine.Analyze,
                            simulator_parameters=simulatorParameters,
                            trading_environment=tradingEnvironment)

context = {}
instruments = Gadget.FindListedInstrument(database, datetime1, datetime2)
context["Instruments"] = instruments

# GO
statistics = strategy.Run(context=context)




