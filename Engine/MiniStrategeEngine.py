import pandas as pd
import datetime
import Core.Gadget as Gadget
import Core.IO as IO
from Core.Config import *


dfData = pd.DataFrame()

def OnMonthly():
    pass


def BackTest(datetime1, datetime2, instruments, callBack):
    # Load Monthly Data
    bmSymbol = "000001.SH"
    dfData = IO.LoadFactorsAsDataFrame(database,bmSymbol,datetime1,datetime2,factors=["MonthlyReturn"])
    dfData.rename(columns={'MonthlyReturn': bmSymbol}, inplace=True)

    # Align to Benchmark
    count = 0
    for instrument in instruments:
        count += 1
        symbol = instrument["Symbol"]
        if count % 100 == 0:
            print("Process " + str(count) + " / " + str(len(instruments)))
        #print("Merge " + symbol)
        dfTemp = IO.LoadFactorsAsDataFrame(database, symbol, datetime1, datetime2, factors=["MonthlyReturn"])
        if dfTemp.empty:
            print("Skip " + symbol)
            continue
        dfTemp.rename(columns={'MonthlyReturn': symbol}, inplace=True)
        dfData = pd.merge(dfData, dfTemp, on='StdDateTime', how='left')

    print(dfData.head())
    dfData.to_csv("d:/data/strategy/" + "Test" + ".csv")
    pass

    # Loop
    for index, row in dfData.iterrows():
        #
        # value = row[factorName]
        # period = row["Period"]
        pass


datetime1 = datetime.datetime(2010,1,1)
stdDateTime1 = Gadget.ToUTCDateTime(datetime1)
datetime2 = datetime.datetime(2018,9,1)
stdDateTime2 = Gadget.ToUTCDateTime(datetime2)

#instruments = Gadget.FindListedInstrument(database, datetime1, datetime2)
instruments = database.find("Instruments","Stock",query={"limit": 1000})

BackTest(stdDateTime1, stdDateTime2, instruments, OnMonthly)

