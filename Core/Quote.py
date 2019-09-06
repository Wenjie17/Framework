import datetime
import json
import re
import copy
import pandas as pd
import Core.Gadget as Gadget
global quotes
quotes = {}
quoteUsedCount = pd.DataFrame(columns=["Symbol","Count"])
Max_Instrument_Limit = 100


#
def CacheDailyBar(database, realtime, datetime1=None, datetime2=None, asHash=False, startBatch=0):

    #
    filter = {}
    if datetime1 != None:
        filter["StdDateTime1"] = {"$gte": datetime1}
    if datetime2 !=None:
        filter["StdDateTime2"] = {"$lte": datetime1}

    #
    count = database.Count("Stock", "DailyBar", filter)
    batchSize = 100000
    batchCount = int(count / batchSize) + 1
    filter["limit"] = batchSize

    #
    for i in range(batchCount):
        if i < startBatch:
            continue
        #
        dataDic = {}
        filter["skip"] = i * batchSize
        bars = database.Find("Stock", "DailyBar", filter)
        for bar in bars:
            symbol = bar["Symbol"]
            stdDateTime = bar["StdDateTime"]
            localDateTime = Gadget.ToLocalDateTime(stdDateTime)
            #
            document = {}
            # document["Symbol"] = instrument["Symbol"]
            # document["DateTime"] = bar["DateTime"]
            document["DateTime"] = bar["DateTime"]
            document["OpenDateTime"] = bar["OpenDateTime"]
            #
            document["Open"] = bar["Open"]
            document["High"] = bar["High"]
            document["Low"] = bar["Low"]
            document["Close"] = bar["Close"]
            document["Volume"] = bar["Volume"]
            document["Money"] = bar["Money"]

            #
            if "AdjFactor" in bar:
                document["AdjFactor"] = bar["AdjFactor"]
            if "TradeStatus" in bar:
                document["TradeStatus"] = bar["TradeStatus"]

            #
            if "BOpen" in bar["Values"]:
                document["BOpen"] = bar["Values"]["BOpen"]
            if "BHigh" in bar["Values"]:
                document["BHigh"] = bar["Values"]["BHigh"]
            if "BLow" in bar["Values"]:
                document["BLow"] = bar["Values"]["BLow"]
            if "BClose" in bar["Values"]:
                document["BClose"] = bar["Values"]["BClose"]
            if "TotalShares" in bar["Values"]:
                document["TotalShares"] = bar["Values"]["TotalShares"]
            if "FreeFloatShares" in bar["Values"]:
                document["FreeFloatShares"] = bar["Values"]["FreeFloatShares"]
            #
            key = symbol + "_" + Gadget.ToDateString(localDateTime)

            # realtime.SetDocument(key, document)
            # s = json.dumps(document)
            dataDic[key] = document

        # 多层存储
        if asHash:
            realtime.SetHashObjects("DailyBar", dataDic)
        else: # 单层存储
            # 以每个Batch为单位，set一次
            realtime.MultiSetDocuments(dataDic)

        print("Cached BatchCount", i, "of", batchCount, "Progress", round(i/batchCount,2)*100, "%", datetime.datetime.now())
        pass

#---Get Quote, put to Cache---
def GetQuote(database, symbol, stdDateTime, realtimeView=None):
    #tradeDateTime = Core.Gadget.ToUTCDateTime(tradeDateTime)
    #global quotes

    if realtimeView != None:
        return GetQuoteRealTimeView(realtimeView, symbol, stdDateTime, isHash=False)

    # ---Add to Table---
    if symbol not in quoteUsedCount.index:
        quoteUsedCount.loc[symbol] = [symbol, 0]
    # print(quoteUsedCount)

    #
    reload = False
    if symbol not in quotes:# not in cache, to reload
        reload = True
    else:    # out of time range, to reload
        barSeries = quotes[symbol]
        try:
            if stdDateTime < barSeries.DateTime1() or stdDateTime > barSeries.DateTime2():
                reload = True
        except:
            dt0 = stdDateTime
            dt1 = barSeries.DateTime1()
            dt2 = barSeries.DateTime2()
            return None

    if reload and len(quotes) >= Max_Instrument_Limit:
        # numToRelease = len(quotes) - Max_Instrument_Limit
        numToRelease = int(0.1 * Max_Instrument_Limit)
        #
        quoteUsedCount.sort_values(by=["Count"], ascending=True, inplace=True)
        #
        i = 0
        for index, row in quoteUsedCount.iterrows():
            if row["Symbol"] in quotes:
                print("Try to delete Quote " + row["Symbol"] + " Count " + str(row["Count"]))
                del quotes[row["Symbol"]]
                i += 1
            if i >=numToRelease:
                break

    if reload:
        #datetime2 = stdDateTime +
        datetime1 = stdDateTime + datetime.timedelta(days=-365)
        print("Load Quote " + symbol + " Since " + str(datetime1))
        barSeries = database.getDataSeries(symbol + "_Time_86400_Bar", datetime1=datetime1)
        quotes[symbol] = barSeries
        #
        quoteUsedCount.loc[symbol, "Count"] += 1

    quote = barSeries.Get(stdDateTime, "Previous")
    return quote


# ---Get Quote from Realtimes view---
def GetQuoteRealTimeView(realtimeView, symbol, stdDateTime, isHash=True):
    #
    localDateTime = Gadget.ToLocalDateTime(stdDateTime)
    #global quotes

    #
    key = symbol + "_" + Gadget.ToDateString(localDateTime)

    quote = None
    if isHash:
        quote = realtimeView.GetHashDocument("DailyBar", key)
    else:
        try:
            quote = realtimeView.GetDocument(key)
        except Exception as e:
            print(repr(e))

    # quote = barSeries.Get(stdDateTime, "Previous")
    return quote