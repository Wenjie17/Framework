import Core.Gadget as Gadget
import Core.MongoDB
import copy
import math
import datetime
import pymongo
import Core.Quote as Quote


def GetDataSeries(database,portfolioName,datetime1=None,datetime2=None):
    query = {}
    query["Portfolio"] = portfolioName
    if(datetime1 != None and datetime2 != None):
        query["StdDateTime"] = {"$gte": datetime1, "$lte": datetime2}

    #portfolioSeries = Core.DataSeries.DataSeries(portfolioName + "_Portfolio")
    #multiSeries = database.find("Portfolio","PortfolioHistData",query=query,asc = True)
    #for doc in multiSeries:
    #    portfolioSeries.add(doc)

    portfolio = database.findWithFilter("Instruments","Portfolio", {"Portfolio" : portfolioName} )
    portfolio = portfolio[0]

    portfolioHistData = database.getDataSeries(portfolio["Type"] + "_" + portfolioName + "_Portfolio", "Portfolio", datetime1, datetime2)
    return portfolioHistData

    #portfolioHistData = database.findWithFilter("Portfolio" , portfolio["Type"] + "_" + portfolioName + "_Portfolio", query, sort = [("StdDateTime",pymongo.ASCENDING)])
    #return portfolioHistData


def GetPortfolio(database, portfolioName, datetime, lastPortfolio = None):
    Load(database,portfolioName.datetiem)


#---模拟每日结算，评估净值---
#---Recalculate Daily---
def EquityWithBars(database, name, datetime1, datetime2, portfolio=None):
    #
    stdDatetime1 = Core.Gadget.ToUTCDateTime(datetime1)
    stdDatetime2 = Core.Gadget.ToUTCDateTime(datetime2)

    # --- Check length of Portfolio in Database ---
    accounts = database.find("Portfolio", "Account", query={"Portfolio": name})

    #
    countAccounts = len(accounts)
    if countAccounts > 0: #---组合存在---
        # --- 最近一期的Portdolio状态，但不能是结算日当天---
        portfolio = Load(database, name, stdDatetime1 + datetime.timedelta(days=-1))

        # --- Wrong ---
        if stdDatetime2 < accounts[countAccounts-1]["StdDateTime"]:
            stdDatetime2 = accounts[countAccounts-1]["StdDateTime"]

    else: #---组合根本不存在---
        portfolio = Portfolio(name, database)


    # --- 向前突破边界 ---
    if portfolio._datetime1 == None:
        portfolio._datetime1 = stdDatetime1
    # --- 向前突破边界 ---
    if stdDatetime1 < portfolio._datetime1:
        portfolio._datetime1 = stdDatetime1

    #
    print("Calc Portfolio From " + str(stdDatetime1) + " To " + str(stdDatetime2))

    #
    trades = database.find("Portfolio", "Trade", stdDatetime1, stdDatetime2, query={"Portfolio":name}) #
    iTrade = 0
    countTrade = len(trades)
    trade = None
    if countTrade > 0:
        trade = trades[0]

    #
    bmBarSeries = database.getDataSeries("000300.SH_Time_86400_Bar",  stdDatetime1, stdDatetime2, "Index")


    # ---不去处理 datetime1 之前的trade---
    #if len(self.trades) == 0:
    #    trade = None
    #else:
    #    trade = self.trades[0]
    #    tradeDate = Gadget.StdDateTimeToTradeDate(trade["StdDateTime"])
    #    stdDate1 = Gadget.StdDateTimeToTradeDate(datetime1)
    #    while trade != None and tradeDate < stdDate1:
    #        #
    #        iTrade += 1
    #        if iTrade >= countTrade:
    #            trade = None
    #            break
    #        trade = self.trades[iTrade]
    #        tradeDate = Gadget.StdDateTimeToTradeDate(trade["StdDateTime"])


    # ---Loop (Trading) Days----
    accounts = []
    for day in bmBarSeries.items:
        #
        curDateTime = day["StdDateTime"]
        curLocalDateTime = Gadget.ToLocalDateTime(curDateTime)
        curTradeDate = Gadget.StdDateTimeToTradeDate(day["StdDateTime"])
        endDateTime = curTradeDate + datetime.timedelta(days=1)
        stdEndDateTime = Gadget.ToUTCDateTime(endDateTime)

        #print(day["DateTime"] + " UTC " + str(curDateTime) + " " + str(curTradeDate))
        # continue

        # ---A new Day---
        # Adjust Position
        for position in portfolio.positionsBySymbol.values():
            symbol = position["Symbol"]

            # ---Get Quote---
            quote = Quote.GetQuote(database, symbol, curDateTime)
            if quote == None:
                print("Portfolio::Update Can't Value Position " + symbol + " No Quote")
                continue

            # update adjusted factor && position qty
            CorrectPositionWithAdjFactor(quote["AdjFactor"], position)


        # while trade != None and tradeDate <= curTradeDate:
        while trade != None and trade["StdDateTime"] <= stdEndDateTime:
            # trade
            print(trade)
            #
            adjFactor = 1
            if "AdjFactor" in trade:
                adjFactor = trade["AdjFactor"]
            #
            portfolio.AddTrade(symbol=trade["Symbol"],
                               side=trade["Side"],
                               price=trade["Price"],
                               qty=trade["Qty"],
                               tradeDateTime=trade["StdDateTime"],
                               adjFactor=adjFactor)
            #
            iTrade += 1
            if iTrade >= countTrade:
                trade = None
                break
            trade = trades[iTrade]
            tradeDate = Gadget.StdDateTimeToTradeDate(trade["StdDateTime"])

        # --- Valuate Avery Day---
        portfolio.Valuate(database, curDateTime)

        # --- print---
        portfolio.Summary()
        #PrintPositions()

        # ---
        account = portfolio.GenerateAccountDocument()
        accounts.append(account)

    # ---Calculate Return---
    retValue = Performance2(accounts, name)
    portfolio._compoundAnnualReturn = retValue["CompoundAnnualReturn"]
    portfolio._latestWeekly = retValue["LatestWeekly"]
    portfolio._latestMonthly = retValue["LatestMonthly"]

    #
    if save:
        SaveAccount(database, accounts)
        SavePortfolio(database, portfolio)


def Performance(database, name):
    #
    accounts = database.find("Portfolio", "Account", query={"Portfolio": name})
    return Performance2(accounts, name)


def Performance2(accounts, name):
    # ---Annualized Return---
    beginAcount = accounts[0]
    endingAccount = accounts[len(accounts)-1]
    term = (endingAccount["StdDateTime"] - beginAcount["StdDateTime"]).days
    if term > 0:
        compoundAnnualReturn = (endingAccount["UnitNetValue"] - 1) / term * 365
    else:
        compoundAnnualReturn = endingAccount["UnitNetValue"] - 1

    # ---Last Week---
    lastWeekDateTime = endingAccount["StdDateTime"] + datetime.timedelta(days=-8)
    lastWeekAccount = Gadget.Find(accounts, lastWeekDateTime)
    if lastWeekAccount != None:
        latestWeekly = endingAccount["UnitNetValue"] / lastWeekAccount["UnitNetValue"] - 1
    else:
        latestWeekly = endingAccount["UnitNetValue"] - 1

    # ---Last Month---
    lastMonthDateTime = endingAccount["StdDateTime"] + datetime.timedelta(days=-31)
    lastMonthAccount = Gadget.Find(accounts, lastMonthDateTime)
    if lastMonthAccount != None:
        latestMonthly = endingAccount["UnitNetValue"] / lastMonthAccount["UnitNetValue"] - 1
    else:
        latestMonthly = endingAccount["UnitNetValue"] - 1


    return {"CompoundAnnualReturn": compoundAnnualReturn,
            "LatestWeekly": latestWeekly,
            "LatestMonthly": latestMonthly,
            }


def ValuatePositions(database, positions, updateDateTime, realtimeView=None):

    # ---Valuate each Position---
    for position in positions:
        symbol = position["Symbol"]

        # ---Get Quote---
        quote = Quote.GetQuote(database, symbol, updateDateTime, realtimeView)

        #
        if quote == None:
            print("Portfolio::Update Can't Value Position " + symbol + " No Quote")
            continue

        # update price
        price = quote["Close"]
        position["Price"] = price

        # update adjusted factor && position qty
        if "AdjFactor" in quote:
            adjFactor = quote["AdjFactor"]
            CorrectPositionWithAdjFactor(adjFactor, position)

        # ---Equity and Notional---
        position["Equity"] = position["Amount"] * price  # Position Value
        position["Notional"] = position["Equity"]

        # ---Profitloss---
        position["ProfitLoss"] = (price - position["Cost"]) * position["Amount"]

        # ---position value---
        # position["Value"] = price * position["Qty"]
        # backup: position value + cum Cashflow
        # position["PL"] = position["Value"] + position["CashFlow"]
        # profitloss2 += position["PL"]

        # ---datetime---
        position["StdDateTime"] = updateDateTime
        # print(symbol + "," + str(endDateTime) + "," + str(position["Qty"]) + "," +  str(position["Price"]) + "," + str(position["Equity"]))




def RebalancePosition(database, portfolio, targetPositions, tradeDateTime, realtimeView=None):
    #
    value = portfolio._value
    fills = []

    # ---Get Current Portfolio---
    targetPositionsBySymbol = {}
    # currentPositionsBySymbol = {}
    for position in targetPositions:
        symbol = position["Symbol"]
        targetPositionsBySymbol[symbol] = position

    # ---Clear Position not in Target---
    # ---Sell Position---
    for symbol, current in portfolio.positionsBySymbol.items():
        if current["Symbol"] not in targetPositionsBySymbol:
            fill = {}
            fill["Symbol"] = current["Symbol"]
            fill["Side"] = "Sell"
            quote = Quote.GetQuote(database, fill["Symbol"], tradeDateTime, realtimeView)
            if quote == None:
                print("Sell at NoQuote", symbol, tradeDateTime)
                continue
                # pass
            fill["Price"] = quote["Close"]
            fill["AdjFactor"] = quote["AdjFactor"]
            fill["Qty"] = current["Qty"]
            fills.append(fill)

    # ---Check if Quote Existed---
    quoteBySymbol = {}
    for target in targetPositions:
        symbol = target["Symbol"]
        quote = Quote.GetQuote(database, symbol, tradeDateTime, realtimeView)
        if quote == None:
            print("Buy at NoQuote", symbol, tradeDateTime)
        else:
            quoteBySymbol[symbol] = quote

    #
    for target in targetPositions:
        symbol = target["Symbol"]

        quote = quoteBySymbol.get(symbol)
        if quote == None:
            continue

        fill = {}
        fill["Symbol"] = symbol
        fill["Price"] = quote["Close"]
        fill["AdjFactor"] = quote["AdjFactor"]

        # ---Target Money or Target Weight to Reblance---
        if "Weight" in target:
            target["Money"] = target["Weight"] * value
        else:
            target["Money"] = value / len(quoteBySymbol) # only count valid symbol

        targetAmount = target["Money"] / fill["Price"]  # Positive or Negtive
        #
        fill["Qty"] = 0
        # ---if not Exist in Current Positions -->To Buy
        if target["Symbol"] not in portfolio.positionsBySymbol:
            if target["Money"] > 0:
                fill["Side"] = "Buy"
            elif target["Money"] < 0:
                fill["Side"] = "Short"
            fill["Qty"] = int(target["Money"] / fill["Price"])

        else:  # adjust more or less
            current = portfolio.positionsBySymbol[target["Symbol"]]
            positionValue = current["Amount"] * fill["Price"]
            if current["Amount"] < targetAmount:  # ---if less than target --> To Buy
                fill["Side"] = "Buy"
                fill["Qty"] = targetAmount - current["Amount"]
            elif current["Amount"] > targetAmount:  # ---if greater than target ---> To Sell
                fill["Side"] = "Sell"
                fill["Qty"] = current["Amount"] - targetAmount

            # print(fill["Symbol"] + " Value " + str(positionValue) + " Target " + str(target["Money"]))

        if fill["Qty"] != 0:
            fills.append(fill)

    #self.AddTrades(fills, tradeDateTime)
    return fills


def Create(database, name, tradeDateTime, initCapital):
    # portfolio = Portfolio
    depositDate = tradeDateTime + datetime.timedelta(hours=-24)
    bmBarSeries = database.find("Index","000300.SH_Time_86400_Bar", None, depositDate)
    depositDate = bmBarSeries[len(bmBarSeries)-1]["StdDateTime"]
    Deposit(database, name, initCapital, depositDate)

    # ?
    EquityWithBars(database, name, depositDate, tradeDateTime)


def Delete(database, name):
    print("Delete Portfolio: " + name)
    database.delete("Portfolio", "Account", {"Portfolio": name})
    database.delete("Portfolio", "Trade", {"Portfolio": name})
    database.delete("Portfolio", "Portfolio", {"Portfolio": name})


#---Get Portfolio at Specific Datetime---
def Load(database, name, tradeDatetime = None):

    print("Load Portfolio " + name + " @ " + str(tradeDatetime))

    # ---Initialize a portfolio---
    portfolio = Portfolio(name, database)

    #
    beginAccount = None
    endAccount = None
    # ---未指定日期---
    if tradeDatetime == None:
        accounts = database.find("Portfolio", "Account", query={"Portfolio": name})
    # ---指定日期---
    else:
        accounts = database.find("Portfolio", "Account",
                                 endDateTime=tradeDatetime,
                                 query={"Portfolio": name})
    #
    if len(accounts) != 0:
        endAccount = accounts[len(accounts)-1]
        beginAccount = accounts[0]

    # ---Load from a Account---
    if endAccount != None: # 从最后一期复原
        FromAccount(portfolio, endAccount)

    # ---DateTime1?---
    if beginAccount != None:
        portfolio._datetime1 = beginAccount["StdDateTime"]
    #else:
    #    portfolio._datetime1 = tradeDatetime

    #
    return portfolio


# Load Portfolio from Account
def FromAccount(portfolio, account):
    #
    portfolio._equity = account["Equity"]
    portfolio._notional = account["Notional"]
    portfolio._cash = account["Cash"]
    portfolio._value = account["Value"]
    portfolio._positionPL = account["PositionPL"]
    portfolio._positionPL = account["ClosedPL"]
    portfolio._unitNetValue = account["UnitNetValue"]
    portfolio._deposit = account["Deposit"]
    portfolio._withdrawal = account["Withdrawal"]
    #
    for position in account["Positions"]:
        portfolio.positionsBySymbol[position["Symbol"]] = position
    #
    portfolio._datetime2 = account["StdDateTime"]


# Transform Portfolio to Account Document
def ToAccount(portfolio):
    accountDoc = {}
    accountDoc["Portfolio"] = portfolio.name
    accountDoc["Equity"] = portfolio._equity
    accountDoc["Notional"] = portfolio._notional
    accountDoc["Cash"] = portfolio._cash
    accountDoc["Value"] = portfolio._value
    accountDoc["PositionPL"] = portfolio._positionPL
    accountDoc["ClosedPL"] = portfolio._closedPL
    accountDoc["ProfitLoss"] = portfolio._positionPL + portfolio._closedPL
    accountDoc["UnitNetValue"] = portfolio._unitNetValue
    accountDoc["StdDateTime"] = portfolio._datetime2
    accountDoc["Deposit"] = portfolio._deposit
    accountDoc["Withdrawal"] = portfolio._withdrawal
    #
    positions = []
    for symbol, position in portfolio.positionsBySymbol.items():
        posDoc = copy.deepcopy(position)
        positions.append(posDoc)
    accountDoc["Positions"] = positions
    #
    if portfolio._datetime2 != None:
        localDateTime = Gadget.ToLocalDateTime(portfolio._datetime2)
        accountDoc["Key"] = portfolio.name + "_" + Gadget.ToDateTimeString(localDateTime)
    else:
        accountDoc["Key"] = None
    #
    return accountDoc


def Deposit(database, name, money, tradeDateTime):
    AddTrade(database, name, "Cash", 1, money, "Deposit", tradeDateTime)


def AddTrade(database, name, symbol, price, qty, side, tradeDateTime, adjFactor=1):
    #
    # print(symbol + " " + side + " Price:" + str(price) + " Qty:" + str(qty) + " @" + str(tradeDateTime))
    #
    tradeDoc = {}
    tradeDoc["Symbol"] = symbol
    tradeDoc["Portfolio"] = name
    tradeDoc["Price"] = price
    tradeDoc["Qty"] = qty
    tradeDoc["Side"] = side
    tradeDoc["AdjFactor"] = adjFactor
    tradeDoc["StdDateTime"] = tradeDateTime
    #
    localDateTime = Gadget.ToLocalDateTime(tradeDateTime)
    tradeDoc["Key"] = name + "_" + symbol + "_" + side + "_" + Gadget.ToDateTimeString(localDateTime)
    #
    tradingCost = 0
    cashflow = 0

    if side == "Deposit":
        cashflow = qty
    elif side == "Buy":
        cashflow = -1 * price * (1 + tradingCost) * qty
    elif side == "Sell":
        cashflow = price * (1 - tradingCost) * qty
    elif side == "Short":
        cashflow = price * (1 - tradingCost) * qty
    elif side == "Cover":
        cashflow = -1 * price * (1 + tradingCost) * qty
    #
    tradeDoc["CashFlow"] = cashflow
    #
    database.upsert("Portfolio", "Trade", {"Key": tradeDoc["Key"]}, tradeDoc)


def Rebalance(database, name, symbols, tradeDateTime):

    # ---Get Portfolio from Database---
    portfolio = Load(database, name, tradeDateTime)

    # ---Generate Target Position---
    targets = []
    for symbol in symbols:
        targets.append({"Symbol":symbol})

    # ---
    trades = RebalancePosition(database, portfolio, targets, tradeDateTime)

    # --- Add To Database ---
    for trade in trades:
        AddTrade(database,
                 name,
                 trade["Symbol"],
                 trade["Price"],
                 trade["Qty"],
                 trade["Side"],
                 tradeDateTime,
                 trade["AdjFactor"])


def SavePortfolio(database, portfolio):
    portfolioDoc = ToAccount(portfolio)
    portfolioDoc["Key"] = portfolio.name
    portfolioDoc["Name"] = portfolio.name
    portfolioDoc["DateTime1"] = portfolio._datetime1
    portfolioDoc["DateTime2"] = portfolio._datetime2
    portfolioDoc["CompoundAnnualReturn"] = portfolio._compoundAnnualReturn
    portfolioDoc["LatestWeekly"] = portfolio._latestWeekly
    portfolioDoc["LatestMonthly"] = portfolio._latestMonthly
    database.upsert("Portfolio", "Portfolio", {"Key": portfolio.name}, portfolioDoc)


def SaveAccount(database, accounts):
    database.saveCollection(accounts, "Portfolio", "Account")


# ---发生配股分红事件,User AdjFactor to Modify Amount---
def CorrectPositionWithAdjFactor(adjFactor, position):
    if "AdjFactor" not in position:
        position["AdjFactor"] = adjFactor
    if adjFactor != position["AdjFactor"]:  # 发生配股分红事件
        position["Qty"] = position["Qty"] * (adjFactor / position["AdjFactor"])
        position["Amount"] = position["Amount"] * (adjFactor / position["AdjFactor"])
        position["Cost"] = position["Cost"] * (position["AdjFactor"] / adjFactor)
        position["AdjFactor"] = adjFactor


def Statistics(database):
    kkwood = 0
    portfolios = database.find("Portfolio","Portfolios")
    for port in portfolios:
        portfolioName = port["Portfolio"]
        print("Statistics:" + portfolioName)

        if portfolioName != "含A股的B股":
            continue

        portfolioHistData = GetDataSeries(database,portfolioName)
        if portfolioHistData.count() > 0:
            firstPf = portfolioHistData[0]
            lastPf = portfolioHistData[portfolioHistData.count()-1]
            portfolio = lastPf
            del portfolio["_id"]
            del portfolio["Key"]
            #oldKey = portfolio["Key"]
            #portfolio["Key"] = portfolioName
            portfolio["DateTime1"] = firstPf["StdDateTime"]
            portfolio["DateTime2"] = lastPf["StdDateTime"]
            days = (lastPf["StdDateTime"] - firstPf["StdDateTime"]).days
            if days > 0:
                portfolio["CompoundAnnualReturn"] = (lastPf["UnitNetValue"] / firstPf["UnitNetValue"] - 1) / (days/365)
            else:
                portfolio["CompoundAnnualReturn"] = 0

            oneYearBack = lastPf["StdDateTime"] - datetime.timedelta(days = 365)
            oneYearBackPf =  portfolioHistData.get(oneYearBack,"Previous")
            ThreeMonthBack = lastPf["StdDateTime"] - datetime.timedelta(days = 90)
            ThreeMonthBackPf =  portfolioHistData.get(ThreeMonthBack,"Previous")

            oneMonthBack = lastPf["StdDateTime"] - datetime.timedelta(days = 30)
            oneMonthBackPf =  portfolioHistData.get(oneMonthBack,"Previous")
            oneWeekBack = lastPf["StdDateTime"] - datetime.timedelta(days = 7)
            oneWeekBackPf =  portfolioHistData.get(oneWeekBack,"Previous")

            if oneYearBackPf != None:
                portfolio["LatestYearly"] = lastPf["UnitNetValue"] / oneYearBackPf["UnitNetValue"] - 1
            if ThreeMonthBackPf != None:
                portfolio["Latest3Monthly"] = lastPf["UnitNetValue"] / ThreeMonthBackPf["UnitNetValue"] - 1
            if oneMonthBackPf != None:
                portfolio["LatestMonthly"] = lastPf["UnitNetValue"] / oneMonthBackPf["UnitNetValue"] - 1
            if oneWeekBackPf != None:
                portfolio["LatestWeekly"] = lastPf["UnitNetValue"] / oneWeekBackPf["UnitNetValue"] - 1

            portfolio["StdDateTime"] = datetime.datetime.now()
            database.update("Portfolio","Portfolios",{"Key":portfolioName},portfolio)
            #database.update("Portfolio","Portfolios",{"Key":oldKey},portfolio)


def MovingMixDatabaseToSeperate(database):
    #---Portfolio Tables---
    portfolios = database.findWithFilter("Portfolio", "Portfolios")
    #
    for portfolio in portfolios:
        portfolioName = portfolio["Portfolio"]
        query = {"Portfolio": portfolioName}
        sort = [("StdDateTime", pymongo.ASCENDING)]
        portfolioHistData = database.findWithFilter("Portfolio", "PortfolioHistData", query, sort)

        if len(portfolioHistData) == 0:
            continue

        #---Individual Database---
        collectionName = "Motif_" + portfolioName + "_Portfolio"
        database.insert_many("Portfolio", collectionName, portfolioHistData)
        #database.upsert("Portfolio", portfolioName + "_Portfolio", {"Key": portfolio["Key"]}, portfolio)
        database.creatIndex("Portfolio", collectionName, "Key")
        database.creatIndex("Portfolio", collectionName, "StdDateTime")
        kkwood = 1


class Portfolio(object):
    def __init__(self, name):
        #print("Create Portfolio " + name)
        self.name = name
        # Equity / Margin / Position Value
        self._equity = 0
        # notional / exposure / net exposure
        # Future exposure = position.asset.price_multiplier
        self._notional = 0
        #
        self._cash = 0
        # Cash + Equity
        self._value = 0
        #
        self._positionPL = 0
        #
        self._closedPL = 0
        #
        self._longEquity = 0
        self._longNotional = 0
        self._shortEquity = 0
        self._shortNotional = 0
        self._grossNotional = 0
        #
        self._returns = 0
        self._unitNetValue = 1
        #
        self._deposit = 0
        self._withdrawal = 0
        self._latestWeekly = 0
        self._latestMonthly = 0
        self._compoundAnnualReturn = 0
        #
        self._tradingCost = 0
        #
        # self.positions = []
        self.positionsBySymbol = {}
        self.trades = []
        self.accounts = []
        self._datetime1 = None
        self._datetime2 = None
        #
        self._createDateTime = None
        #self._updateDateTime = None

        #if database != None:
        #    self.Save()

    @property
    def DateTime1(self):
        return self._datetime1

    @property
    def DateTime2(self):
        return self._datetime2

    @property
    def Equity(self):
        return self._equity

    @property
    def Cash(self):
        return self._cash

    @property
    def PositionValue(self):
        return self._equity

    @property
    def PositionProfitLoss(self):
        return self._positionPL

    @property
    def Notional(self):
        return self._notional

    @property
    def Exposure(self):
        return self._notional

    @property
    def Value(self):
        return self._value

    @property
    def GrossNotional(self):
        return self._grossNotional

    @property
    def UnitNetValue(self):
        return self._unitNetValue

    @UnitNetValue.setter
    def UnitNetValue(self, value):
        self._unitNetValue = value

    def Deposit(self, money, tradeDateTime):
        #
        qty = money
        if qty >= 0:
            self._deposit += qty
        else:
            self._withdrawal += abs(qty)
        #
        self._cash = self._cash + qty
        self._value = self._equity + self._cash
        self._datetime2 = tradeDateTime


    def Buy(self, symbol, price, qty, tradeDateTime, adjFactor=1):
        self.AddTrade(symbol, price, qty, "Buy", tradeDateTime, adjFactor)


    def Sell(self, symbol, price, qty, tradeDateTime, adjFactor=1):
        self.AddTrade(symbol, price, qty, "Sell", tradeDateTime, adjFactor)


    def Short(self, symbol, price, qty, tradeDateTime, adjFactor=1):
        self.AddTrade(symbol, price, qty, "Short", tradeDateTime, adjFactor)


    def Cover(self, symbol, price, qty, tradeDateTime, adjFactor=1):
        self.AddTrade(symbol, price, qty, "Cover", tradeDateTime, adjFactor)


    # ---Add Mutiples Trade/Fills---
    # fills[{Symbol，Price，Qty，Side}] Side=Buy / Sell / Short / Cover / Deposit / Withdraw
    def AddTrades(self, trades, tradeDateTime):
        # stdDateTime = Core.Gadget.ToUTCDateTime(tradeDateTime)
        # tradeDateTime = Core.Gadget.ToLocalDateTime(stdDateTime)
        for trade in trades:
            self.AddTrade(trade["Symbol"], trade["Price"], trade["Qty"], trade["Side"],
                          tradeDateTime, adjFactor=trade["AdjFactor"])

    # ---Qty -> Positive
    # ---Amount  ->Can be Negtive
    def AddTrade(self, symbol, price, qty, side, tradeDateTime, adjFactor=1, saveHistory=True):
        #
        print(symbol + " " + side + " Price:" + str(price) + " Qty:" + str(qty) + " @" + str(tradeDateTime))
        #
        if self._datetime1 == None:
            self._datetime1 = tradeDateTime
        #
        tradeDoc = {}
        tradeDoc["Symbol"] = symbol
        tradeDoc["Portfolio"] = self.name
        tradeDoc["Price"] = price
        tradeDoc["Qty"] = qty
        tradeDoc["Side"] = side
        tradeDoc["StdDateTime"] = tradeDateTime
        #
        localDateTime = Gadget.ToLocalDateTime(tradeDateTime)
        tradeDoc["Key"] = self.name + "_" + symbol + "_" + side + "_" + Gadget.ToDateTimeString(localDateTime)

        if saveHistory:
            self.trades.append(tradeDoc)

        #
        tradingCost = self._tradingCost

        if side == "Deposit":
            if qty >= 0:
                self._deposit += qty
            else:
                self._withdrawal += abs(qty)

            self._cash = self._cash + qty
            self._value = self._equity + self._cash
            tradeDoc["CashFlow"] = qty

        else:
            # ---if new Position, Create Cache---
            if symbol not in self.positionsBySymbol:
                position = {}
                position["Symbol"] = symbol
                position["Cost"] = 0
                position["Qty"] = 0
                position["Amount"] = 0
                position["Price"] = 0 # LastTrade
                position["Value"] = 0  # new
                position["CashFlow"] = 0  # new
                position["PL"] = 0  # new
                position["Avaliable"] = 0
                position["Portfolio"] = self.name
                position["AdjFactor"] = adjFactor
                self.positionsBySymbol[symbol] = position

            position = self.positionsBySymbol[symbol]
            position["StdDateTime"] = tradeDateTime

            # Find Adj Factor
            # closingDateTime = Gadget.ToClosingDateTime(tradeDateTime)
            # quote = GetQuote(self.database, symbol, closingDateTime)
            # if quote == None:
            #     print("Portfolio::Update Can't Value Position " + symbol + " No Quote")

            # ---update adjusted factor && position qty BEFORE trade---
            # self.CorrectAdjFactor(quote["AdjFactor"], position)

            # ---Amount and Cost Price---
            if side == "Buy":
                # Update Cost
                position["Cost"] = (position["Cost"] * position["Qty"] + price * qty) / (position["Qty"] + qty)

                # Update Position---
                position["Amount"] += qty
                # position["Qty"] = position["Qty"] + qty
                cashflow = -1 * price * (1 + tradingCost) * qty

            elif side == "Sell":
                # qty = math.fabs(qty)
                # if qty > position["Qty"]:
                #    qty = position["Qty"]

                position["Amount"] -= qty
                # position["Qty"] = position["Qty"] - qty
                cashflow = price * (1 - tradingCost) * qty

            elif side == "Short":
                #
                position["Amount"] -= qty
                position["Cost"] = (position["Cost"] * position["Qty"] + price * qty) / ( position["Qty"] + qty)
                cashflow = price * (1 - tradingCost) * qty

            elif side == "Cover":
                position["Amount"] += qty
                cashflow = -1 * price * (1 + tradingCost) * qty

            # ---Update Cashflow---
            # cashflow = price * (1 - tradingCost) * qty
            # if fill["Side"] == "Buy" or "Cover":
            #    cashflow = -1 * price * (1 + tradingCost) * qty
            self._cash = self._cash + cashflow
            tradeDoc["CashFlow"] = cashflow

            # ---Amount To Position---
            position["Qty"] = abs(position["Amount"])
            if position["Amount"] >= 0:
                position["Side"] = "Long"
            elif position["Amount"] < 0:
                position["Side"] = "Short"

            # cumulative cashflow
            position["CashFlow"] = position["CashFlow"] + cashflow

            # update position value
            # position["Value"] = position["Qty"] * price
            position["Equity"] = position["Qty"] * price  # Position Value
            position["Notional"] = position["Equity"]

            # update pl = position value + cum cashflow
            position["PL"] = position["Value"] + position["CashFlow"]

            #
            position["Price"] = price

            # Remove if No Position
            if position["Qty"] == 0:
                self.positionsBySymbol.pop(symbol)

            # self.updateDatetime = tradeDateTime
            # ---Finish to add/remove position, not Re-Valuation whole Portfolio yet---
        #
        self.ReCalculate(tradeDateTime)


    # ---Summation of the Positions---
    def ReCalculate(self, updateDateTime):
        #
        positionEquity = 0
        positionNotional = 0
        longEquity = 0
        longNotional = 0
        shortEquity = 0
        shortNotional = 0
        profitloss = 0

        # ---Loop Positions---
        for position in self.positionsBySymbol.values():
            price = position["Price"]

            # ---Equity and Notional---
            position["Equity"] = position["Qty"] * price  # Position Value

            # ---Stock: Equity==Notional---
            position["Notional"] = position["Equity"]

            #
            if position["Amount"] >= 0:
                longEquity += position["Equity"]
                longNotional += position["Notional"]
                positionNotional += position["Notional"]
            else:
                shortEquity += position["Equity"]
                shortNotional += position["Notional"]
                positionNotional -= position["Notional"]

            positionEquity += position["Equity"]

            # ---Profit Loss---
            position["ProfitLoss"] = (price - position["Cost"]) * position["Amount"]
            profitloss += position["ProfitLoss"]

            # ---backup---
            # position value
            # position["Value"] = price * position["Qty"]
            # backup: position value + cum Cashflow
            # position["PL"] = position["Value"] + position["CashFlow"]
            # profitloss2 += position["PL"]

            # ---datetime---
            # position["StdDateTime"] = updateDateTime
            # print(symbol + "," + str(endDateTime) + "," + str(position["Qty"]) + "," +  str(position["Price"]) + "," + str(position["Equity"]))

        #
        self._notional = positionNotional
        self._equity = positionEquity
        self._positionPL = profitloss
        self._value = self._equity + self._cash
        #
        self._longEquity = longEquity
        self._longNotional = longNotional
        self._shortEquity = shortEquity
        self._shortNotional = shortNotional
        #
        self._grossNotional = self._longNotional + self._shortNotional
        gross_leverage = self._grossNotional / self._value
        net_leverage = self._notional / self._value
        #
        self._datetime2 = updateDateTime


    # ---Re Valuate the whole Portfolio, (vluate each position)---
    def Valuate(self, database, updateDateTime, realtimeView=None):
        #  daily value position
        # ---Valuation: Use Position to Calc Portfolio Value(Account)---
        ValuatePositions(database, self.positionsBySymbol.values(), updateDateTime, realtimeView)
        self.ReCalculate(updateDateTime)

        # ---Add to Accounts---
        # accountDoc = self.GenerateAccountDocument()
        # self.accounts.append(accountDoc)


    def GenerateAccountDocument(self):
        accountDoc = ToAccount(self)
        return accountDoc

    # ---Sell Old/ Buy new-->Cast to Target Postitions---
    # targetPositions[{Symbol，Money}] #Side=Long/Short
    # 有权重按照权重，没有权重，平均持仓
    def Rebalance(self, database, targetPositions, tradeDateTime, realtimeView=None):
        print("Rebalence: " + self.name + " @" + str(tradeDateTime))

        # closingDateTime = Gadget.ToClosingDateTime(tradeDateTime)
        trades = RebalancePosition(database, self, targetPositions, tradeDateTime, realtimeView)
        self.AddTrades(trades, tradeDateTime)


    def Summary(self, postions=False, accounts=False, trades=False):
        print("Pf Summary", self.name + " " + str(self._datetime2)
              + " Unit: %.4f, Value: %.4f, Cash: %.4f, Equity: %.4f, PosiPL: %.4f, ClosePL: %.4f"
              %(self._unitNetValue,self._value, self._cash,self._equity,self._positionPL,self._closedPL)
              )
        #
        if postions:
            self.PrintPositions()
        # print("***")

    def Performance(self):
        pass


    def PrintPositions(self):
        print("Symbol,Qty,Price,Value,CashFlow,PL")
        for k, v in self.positionsBySymbol.items():
            print(k + " Qty " + str(v["Qty"]) + " Price " + str(v["Price"]) + " Value " + str(
                v["Value"]) + " CashFlow " + str(v["CashFlow"]) + " PL " + str(v["ProfitLoss"]))


    def PrintAccounts(self):
        for account in self.accounts:
            print(str(account["StdDateTime"])\
                  + " Unit:" + str(account["UnitNetValue"]) + " Value:" + str(account["Value"]) + " Cash:" + str(account["Cash"]) + " Equity:" + str(account["Equity"])\
                  + " Profitloss:" + str(account["ProfitLoss"]) +" Profitloss2:" + str(account["ProfitLoss2"]))


    def Save(self):
        pass
        #account = self.GenerateAccountDocument()
        #account["Key"] = self.name
        #account["DateTime1"] = self._datetime1
        #account["DateTime2"] = self._datetime2
        #self.database.upsert("Portfolio", "Portfolio", {"Key": self.name}, account)

        #---Save Portfolio---
        #p = self.database.find("Portfolio", "Portfolio", query={"Name":self.name})

        #count = len(self.accounts)
        #if count == 0:
        #    print("Not Calculate Account Yet")

        #portfolioDoc = self.accounts[count-1]
        #portfolioDoc["Name"] = self.name
        #portfolioDoc["Key"] = self.name

        # ---Update Portfolio---
        #self.database.upsert("Portfolio", "Portfolio", {"Key":self.name}, portfolioDoc)

        # ---Save Trades---
        #self.database.saveCollection(self.trades, "Portfolio", "Trade")

        # ---Positions---
        # ---Save Account---
        # self.database.saveCollection(self.accounts, "Portfolio", "Account")