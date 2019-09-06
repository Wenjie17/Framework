import Core.Portfolio as Portfolio
import Core.Gadget as Gadget
import Core.Quote as Quote
import math
import numpy as np
import datetime
from Engine.PerformanceTracker import PerformanceTracker


class Strategy(object):

    def __init__(self, name, database, realtimeView=None):
        print("Create Strategy " + name)
        self.name = name
        self.database = database
        self.realTimeView=realtimeView
        self.portfolio = Portfolio.Portfolio(name)
        self.positions = []
        self.context = {}

    def Display(self):
        print("Base Function")

    def BackTest(self):
        pass

    def BackTestDaily(self):
        print("Backtesting Daily" + self.name)

    def BackTestMonthly(self, datetime1, datetime2):
        print("Backtesting Monthly" + self.name)

    def QuickBackTest(self, datetime1, datetime2):

        returnBySymbol = {}
        performances = []
        #
        self.OnStrategyStart()
        #
        datetimes = Gadget.GenerateEndDayofMonth(datetime1, datetime2)
        #
        for dt in datetimes:
            self.context["DateTime"] = dt
            rets = []
            # ---Loop Positions---
            for position in self.positions:
                symbol = position["Symbol"]
                if symbol not in returnBySymbol:
                    returnSeries = self.database.getDataSeries(symbol + "_MonthlyReturn_Factor", stdDatetime1=datetime1, stdDatetime2=datetime2)
                    returnBySymbol[symbol] = returnSeries
                returnSeries = returnBySymbol[symbol]
                ret = returnSeries.Get(dt)
                #
                value = 0
                if ret != None:
                    value = ret["Value"]
                rets.append(value)

            # ---
            meanRets = np.mean(rets)
            if math.isnan(meanRets):
                meanRets = 0

            #
            lastDoc = None
            if len(performances) > 0:
                lastDoc = performances[len(performances)-1]

            #
            performanceDoc = {}
            performanceDoc["DateTime"] = dt
            performanceDoc["MonthlyReturn"] = meanRets
            #
            if lastDoc != None:
                performanceDoc["Return"] = (lastDoc["Return"] + 1) * (performanceDoc["MonthlyReturn"] + 1) - 1
                performanceDoc["UnitValue"] = lastDoc["UnitValue"] * (performanceDoc["MonthlyReturn"] + 1)
            else:
                performanceDoc["Return"] = 0
                performanceDoc["UnitValue"] = 1

            performances.append(performanceDoc)
            self.positions = []
            self.OnMonthEnd(self.context)

        # ---Print Performances---
        for performance in performances:
            print(str(performance["DateTime"]) + " Monthly: " + str(performance["MonthlyReturn"]) + " Return: "
                  + str(performance["Return"]) + " UnitValue: " + str(performance["UnitValue"]))

    def OnStrategyStart(self):
        print("OnStrategyStart")

    def OnMonthBegin(self, onDateTime):
        print("OnMonth")

    def OnMonthEnd(self, onDateTime):
        print("OnMonth")

    def QuickBuy(self, symbol):
        doc = {}
        doc["Symbol"] = symbol
        self.positions.append(doc)


class TradingAlgorithm(object):
    def __init__(self, *args, **kwargs):

        def noop(*args, **kwargs):
            pass

        self._initialize = kwargs.pop('initialize', noop)
        self._handle_data = kwargs.pop('handle_data', noop)
        self._name = kwargs.pop('name', noop)
        self._on_daily = kwargs.pop('on_daily', None)
        self._on_weekly = kwargs.pop('on_weekly', None)
        self._on_monthly = kwargs.pop('on_monthly', None)
        self._on_monthly_begin = kwargs.pop('on_monthly_begin', None)
        self._analyze = kwargs.pop('analyze', None)
        #
        self.simulator_parameters = kwargs.pop('simulator_parameters', None)
        self.performance_tracker = None
        self.trading_calendar = self.simulator_parameters.trading_calendar
        self.trading_environment = kwargs.pop('trading_environment', None)

        # ---Create Portfolio---
        self.portfolio = Portfolio.Portfolio(self._name)
        self.portfolio.Deposit(self.simulator_parameters.CapitalBase,
                               self.simulator_parameters.DateTime1)
        #
        self.database = self.trading_environment.Database
        if self.database == None:
            print("No database specified")

        self.realtimeView = self.trading_environment.RealTimeView

        #
        self._instruments = {}
        # No more UTCDateTime
        # self._currentUTCDatetime = Gadget.ToUTCDateTime(self.simulator_parameters.DateTime1)
        # self._currentLocalDatetime = Gadget.ToLocalDateTime(self.simulator_parameters.DateTime1)
        self._currentDatetime = self.simulator_parameters.DateTime1

    #def Initialize(self, *args, **kwargs):
    #    self._initialize(self, *args, **kwargs)

    def Initialize(self, context):

        # ---Init Instrument Manager---
        instruments = self.database.Find("Instruments","Stock")
        for instrument in instruments:
            self._instruments[instrument["Symbol"]] = instrument

        #
        if self._initialize:
            self._initialize(self, context)


    def HandleData(self, data, dt=None, context=None):
        if self._handle_data:
            self._handle_data(self, context, data, dt)

    def OnMonthlyBegin(self, dt, context=None):
        if self._on_monthly_begin:
            self._on_monthly_begin(self, context, dt)

    def OnMonthly(self, dt, context=None):
        if self._on_monthly:
            self._on_monthly(self, context, dt)

    def OnDaily(self, dt, context=None):
        if self._on_daily:
            self._on_daily(self, context, dt)

    def OnWeekly(self, dt, context=None):
        if self._on_weekly:
            self._on_weekly(self, context, dt)

    def Analyze(self, context, performance):
        if self._analyze is None:
            return
        self._analyze(self, context, performance)


    def Run(self, context=None):
        #
        self.context = context

        #
        # self.Initialize(*self.initialize_args, **self.initialize_kwargs)
        self.Initialize(context)

        # ---Performance Tracker---
        if self.performance_tracker is None:
            self.performance_tracker = PerformanceTracker(
                sim_params=self.simulator_parameters,
                trading_calendar=self.trading_calendar,
                trading_enviroment=self.trading_environment,
                portfolio=self.portfolio)
        # ---First Update---
        # self.performance_tracker.UpdatePerformance()

        self.portfolio.Summary()

        # ---Create loop through simulated_trading---
        # ---Each iteration returns a perf dictionary---
        try:
            performances = []
            #for perf in self.get_generator():
            #    performances.append(perf)

            # ---Use Benchmark to figure out out Trading Date---
            datetime1 = self.simulator_parameters.DateTime1
            datetime2 = self.simulator_parameters.DateTime2
            bmBarSeries = self.database.GetDataSeries(symbol="000300.SH", dataType="DailyBar", instrumentType="Index",
                                                      datetime1=datetime1, datetime2=datetime2)

            #
            theFirstTradingDay = None
            theLastTradingDay = None
            lastLocalDateTime = None
            lastDateTime = None
            for i in range(bmBarSeries.Count()):
                bar = bmBarSeries[i]
                curDateTime = bar["DateTime"]
                # curLocalDateTime = Gadget.ToLocalDateTime(currentDateTime)
                # curTradeDate = Gadget.StdDateTimeToTradeDate(bar["StdDateTime"])
                # endDateTime = curTradeDate + datetime.timedelta(days=1)
                # stdEndDateTime = Gadget.ToUTCDateTime(endDateTime)

                #
                # self._currentUTCDatetime = currentDateTime
                # self._currentLocalDatetime = curLocalDateTime
                self._currentDatetime = curDateTime

                # ---Check Key DateTime Point---
                isEndofMonth = False
                if i < bmBarSeries.Count()-1:
                    nextDateTime = bmBarSeries[i+1]["DateTime"]

                    # nextLocalDateTime = Gadget.ToLocalDateTime(nextDateTime)
                    # if nextLocalDateTime.month != curLocalDateTime.month:
                    #     isEndofMonth = True

                    # No more UTC Time
                    if nextDateTime.month != curDateTime.month:
                        isEndofMonth = True

                isBeginofMonth = False
                # if lastLocalDateTime != None and lastLocalDateTime.month != curLocalDateTime.month:
                if lastDateTime != None and lastDateTime.month != curDateTime.month:
                    isBeginofMonth = True

                isEndofWeek = False
                # if curLocalDateTime.weekday() == 4:
                if curDateTime.weekday() == 4:
                    isEndofWeek = True


                # ---在交易前校正仓位---
                if self.simulator_parameters.DataFrequency == "daily":
                    self.portfolio.Valuate(self.database, curDateTime, self.realtimeView)

                elif self.simulator_parameters.DataFrequency == "monthly" and isEndofMonth:
                    self.portfolio.Valuate(self.database, curDateTime, self.realtimeView)

                # --- Handle Data---
                if self.simulator_parameters.DataFrequency == "daily":
                    self.HandleData(data=None, dt=curDateTime, context=context)

                elif self.simulator_parameters.DataFrequency == "monthly" and isEndofMonth:
                    self.HandleData(data=None, dt=curDateTime, context=context)

                # ---Process Event---
                # ---Run Everyday---
                self.OnDaily(dt=curDateTime, context=context)
                # ---Weekly Event---
                if isEndofWeek:
                    self.OnWeekly(dt=curDateTime, context=context)
                # ---Monthly Event---
                if isBeginofMonth:
                    self.OnMonthlyBegin(dt=curDateTime, context=context)
                # ---Monthly Event---
                if isEndofMonth:
                    self.OnMonthly(dt=curDateTime, context=context)

                # ---Time to To Valuate / Settlement---
                # ---Settlement at the Beginning and the Ending---
                settlement = False
                if i == 0 or i == bmBarSeries.Count()-1:
                    settlement = True

                if self.simulator_parameters.DataFrequency == "daily":
                    settlement = True

                elif self.simulator_parameters.DataFrequency == "monthly" and isEndofMonth:
                    settlement = True

                if settlement:
                    print("Settlement @ " + str(curDateTime) + " ProcessTime " + str(datetime.datetime.now()))
                    self.portfolio.Valuate(self.database, curDateTime, self.realtimeView)
                    self.performance_tracker.UpdatePerformance()

                # ---Next Day---
                # currentDateTime = self.tradingCalender.GetNextDay(currentDateTime)
                # lastLocalDateTime = curLocalDateTime
                lastDateTime = curDateTime

            # ---Print Statistics---
            returnStats = self.performance_tracker.ReturnStatistics()
            print("")
            print("Strategy Statistics")
            for key, value in returnStats.items():
                print(key, value)

            # ---convert perf dict to pandas dataframe---
            performances = self.performance_tracker.Perfomence
            # daily_stats = self._create_daily_stats(performances)
            self.Analyze(context, performances)
        finally:
            pass

        return performances

    #
    def Instrument(self, symbol):
        if symbol in self._instruments:
            return self._instruments[symbol]
        return None

    def Position(self, symbol):
        if symbol not in self.portfolio.positionsBySymbol:
            return None
        return self.portfolio.positionsBySymbol[symbol]

    def Positions(self):
        return self.portfolio.positionsBySymbol

    def Portfolio(self):
        return self.portfolio

    #
    def PlaceOrder(self,
                   symbol,
                   qty=None,
                   side="Buy",
                   limit_price=None,
                   stop_price=None,
                   style=None):
        print("Place Order")

        # ---Direction---
        if qty != None and qty < 0:
            side = "Sell"
        #
        quote = Quote.GetQuote(self.database, symbol, self._currentDatetime, realtimeView=self.realtimeView)
        #
        price = None
        if quote == None:
            # Enter Positions
            if side == "Buy" or side == "Short":
                print("No Quote PlaceOrder " + symbol + " @ " + str(self._currentDatetime))
                return
            # Exit Positions
            elif side == "Sell" or side == "Cover":
                position = self.portfolio.Position(symbol)
                if position != None:
                    price = position["Price"]
                    adjFactor = position["AdjFactor"]
                    pass
                else: # No Positions, Wrong Order
                    print("PlaceOrder Try To Exit No-Existed Position")
        #
        else:
            price = quote["Close"]
            adjFactor = quote["AdjFactor"]
        #
        if price == None:
            print("PlaceOrder No Quote")
            return
        #
        if qty == None:
            # Exit Positions
            if side == "Sell" or side == "Cover":
                position = self.portfolio.Position(symbol)
                if position != None:
                    qty = position["Qty"]
                else:
                    print("PlaceOrder Try To Exit No-Existed Position")
        #
        if qty == None:
            print("PlaceOrder No Qty")
            return
        #
        self.portfolio.AddTrade(symbol, price=price, qty=math.fabs(qty), side=side,
                                tradeDateTime=self._currentDatetime,
                                adjFactor=adjFactor)


    def Rebalance(self, targetPositions=[]):
        self.portfolio.Rebalance(self.database, targetPositions, self._currentDatetime, self.realtimeView)