import os
import xlrd
import re
#from WindPy import *
import pandas as pd
import copy
from Core.Gadget import *
import Core.DataSeries as DataSeries


def DataSeriesToDataFrame(dataSeries, keepFields=[]):
    pass


def DataListToDataFrame(dataList, keepFields=[]):
    fields = keepFields
    data = []
    for d in dataList:
        entry = []
        for field in fields:
            if field in d:
                entry.append(d[field])
            elif field in d["Values"]:
                entry.append(d["Values"][field])
        data.append(entry)
    #
    df = pd.DataFrame(data, columns=fields)
    return df


def LoadAsDataFrame(database, databaseName, collectionName, filter={}, fields={}):

    #
    sort = [("StdDateTime", pymongo.ASCENDING)]

    #
    data = database.findWithFilter(databaseName, collectionName, filter, sort)

    #
    for i in range(len(data)):
        # ---Init---
        if i == 0:
            if len(fields) == 0:
                fields = data[i].keys()
            df = pd.DataFrame(columns=fields)

        # ---Generte Data---
        entry = []
        for field in fields:
            value = None
            if field == "StdDateTime":
                value = str(data[i][field])
            else:
                value = data[i][field]

            entry.append(value)

        # ---
        df.loc[i] = entry

    k = 0

    return df

# ---Bar to Pandas---
# Specific Symbol
#           Field1 Field2 Field3
# DateTime1
# DateTime2
# DateTime3
# 可以自动去 values 字段下寻找
def LoadBarsAsDataFrame(database, symbol, datetime1=None, datetime2=None, fields=None, instrumentType="Stock"):

    sort = [("StdDateTime", 1)]
    filter = {}
    filter["Symbol"] = symbol
    filter["StdDateTime"] = {}
    if datetime1 != None:
        filter["StdDateTime"]["$gte"] = datetime1
    if datetime2 != None:
        filter["StdDateTime"]["$lte"] = datetime2
    if not filter["StdDateTime"]:
        del filter["StdDateTime"]

    #
    # instrumentType = instrument["InstrumentType"]
    databaseName = instrumentType
    collectionName = "DailyBar"
    barSeries = database.Find(databaseName, collectionName, filter, sort)

    if fields == None:
        if instrumentType == "Stock":
            newFields = ["StdDateTime", 'DateTime', 'BOpen', 'BHigh','BLow','BClose','Volume','Money']
        else:
            newFields = ["StdDateTime", 'DateTime', 'Open', 'High', 'Low', 'Close']
    else:
        newFields = ["StdDateTime", "DateTime"] + fields

    data = []
    for bar in barSeries:
        entry = []
        entry.append(bar["StdDateTime"])
        localDateTime = ToLocalDateTime(bar["StdDateTime"])
        entry.append(localDateTime.date())
        # ---默认字段 不同资产，略有不同---
        if fields == None:
            if instrumentType == "Stock":
                entry.append(bar["Values"]["BOpen"])
                entry.append(bar["Values"]["BHigh"])
                entry.append(bar["Values"]["BLow"])
                entry.append(bar["Values"]["BClose"])
                entry.append(bar["Volume"])
                entry.append(bar["Money"])
            else:
                entry.append(bar["Open"])
                entry.append(bar["High"])
                entry.append(bar["Low"])
                entry.append(bar["Close"])

        # ---指定字段---
        else:
            for field in fields:
                if field in bar:
                    entry.append(bar[field])
                elif field in bar["Values"]: #---自动寻找---
                    entry.append(bar["Values"][field])
                else:
                    entry.append(np.nan)
        data.append(entry)
    #
    df = pd.DataFrame(data, columns=newFields)

    #print(df.head())
    #print(df[1:3])
    #print(df.loc[1:3])
    #df.iloc[5, 3] = np.nan
    #print(df)
    #print(df[df.isnull().values==True])
    #df.at[1,"BClose"] = 2.1111
    #a = df.iloc[1]["BClose"]
    #a = df.at[1,"BClose"]
    return df


# ---MultiSymbol--> Row:"Close" X Col:"Symbols"---
# Specific Field(Close)
#           Symbol1 Symbol2 Symbol3
# DateTime1 Close1  Close1  Close1
# DateTime1 Close2  Close2  Close2
# DateTime1 Close3  Close3  Close3
def LoadMultiInstrumentsBarsAsDataFrame(database, datetime1, datetime2, instruments, databaseName = "Quote"):

    #print("Load Multi-Instruments Bars As DataFrame")
    df = pd.DataFrame()
    totalCount = len(instruments)
    count = 0
    for instrument in instruments:
        count += 1
        symbol = instrument["Symbol"]
        if count % 100 == 0:
            print("Load Multi-Instruments Bars As DataFrame " + str(count) + " / " + str(totalCount))

        if databaseName == "Index":
            tempDf = LoadBarsAsDataFrame(database, symbol, datetime1, datetime2, ["Close"], databaseName)
            tempDf.rename(columns={'Close': symbol}, inplace=True)
        else:
            tempDf = LoadBarsAsDataFrame(database, symbol, datetime1, datetime2, ["BClose"], databaseName)
            tempDf.rename(columns={'BClose': symbol}, inplace=True)
        # print(tempDf)
        #---drop useless column---
        # tempDf = tempDf.drop(["DataSeries", "Key", "Symbol"], axis=1)
        #---
        # tempDf.rename(columns={'BClose': symbol}, inplace=True)
        #
        # ---First time loop---
        if tempDf.empty:
            continue

        if df.empty:
            df = tempDf
            continue
        #
        # print("Df")
        # print(df)
        # print("tempDf")
        # print(tempDf)
        # ---Temp merge to Mother---
        df = pd.merge(df, tempDf, on='DateTime', how='outer')

    #---After Merge All---
    df['DateTime'] = pd.to_datetime(df['DateTime'])
    #df.set_index("DateTime", inplace=True)
    #df.sort_index(inplace=True)
    df.sort_values("DateTime", inplace=True)
    df.reset_index(drop=True, inplace=True)
    #print(df.head())
    return df


# ---Single Symbol--> Row:"DateTime" X Col:"Fundamental Fields"
# DateTime1 Field1 Field2 Field3
# DateTime2 Field1 Field2 Field3
# DateTime3 Field1 Field2 Field3
def LoadFundamentalsAsDataFrame(database, symbol, datetime1, datetime2, fields=[]):

    sort = [("StdReportDate", pymongo.ASCENDING)]
    #
    filter = {}
    filter["Symbol"] = symbol

    filter["StdDateTime"] = {}
    if datetime1 != None:
        filter["StdDateTime"]["$gte"] = datetime1
    if datetime2 != None:
        filter["StdDateTime"]["$lte"] = datetime2
    if not filter["StdDateTime"]:
        del filter["StdDateTime"]
    #
    statements = database.Find("Stock", "Fundamental", filter, sort)
    fieldNames = ['ReportDate', 'ReleaseDate', 'Period']
    fieldNames = fieldNames + fields

    data = []
    for statement in statements:
        entry = []
        entry.append(statement["ReportDate"].date())
        entry.append(statement["ReleaseDate"].date())
        entry.append(statement["Period"])
        for field in fields:
            if field in statement["Values"]:
                entry.append(statement["Values"][field])
            else:
                entry.append(np.nan)
        data.append(entry)

    df = pd.DataFrame(data, columns=fieldNames)
    #
    # df['ReportDate'] = pd.to_datetime(df['ReportDate'] )
    #df['ReleaseDate'] = pd.to_datetime(df['ReleaseDate'])
    return df


# ---Dependent of LoadFactor---
# ---Load single factors---
def LoadFactor(database, symbol, factorName, filter, datetime1, datetime2):
    #
    sort = [("StdReportDate", pymongo.ASCENDING)]
    filter["Symbol"] = symbol
    values = database.findWithFilter("Factor", factorName, filter, sort)
    return values


# --- Specific Symbol, Muti DateTime X Muti Factors ---
# --- Params: Specific(Symbol) , Row(DateTime) , Col(Factors) ---
# DateTime1: Factor1, Factor2, Factor3
# DateTime2: Factor1, Factor2, Factor3
# DateTime3: Factor1, Factor2, Factor3
def LoadFactorsAsDataFrame(database, symbol, datetime1=None, datetime2=None, factors=[], filter={}):

    df = pd.DataFrame()
    for factor in factors:
        #filter["StdDateTime"] = {"$gte": datetime1, "$lte": datetime2}

        filter["StdDateTime"] = {}
        if datetime1 != None:
            filter["StdDateTime"]["$gte"] = datetime1
        if datetime2 != None:
            filter["StdDateTime"]["$lte"] = datetime2
        if not filter["StdDateTime"]:
            del filter["StdDateTime"]

        # filter["Name"] = factor
        # filter["Params"] = "Daily"
        filter["Symbol"] = symbol
        sort = [("StdDateTime", pymongo.ASCENDING)]
        projection={"StdDateTime":1,"Value":1}
        values = database.findWithFilter("Factor", factor, filter=filter, sort=sort, projection=projection)
        if len(values) == 0:
            continue

        #---Generate DataFRame---
        tempDf = pd.DataFrame(values)
        # print(tempDf.head())
        #---drop useless column---
        # tempDf = tempDf.drop(["DataSeries", "Key", "Symbol"], axis=1)
        tempDf = tempDf.drop(["_id"], axis=1)
        #---
        tempDf.rename(columns={'Value': factor}, inplace=True)
        # ---First time loop---
        if df.empty:
            df = tempDf
            continue

        # ---Temp merge to Mother---
        df = pd.merge(df, tempDf, on='StdDateTime',how='left')

    # ---用前一个数据代替NaN---
    df = df.fillna(method='pad')
    # print(df.head())
    return df


# --- Profile Factor Data ---
# --- Muti Instruments X Mutl Factors @ Specific DateTime ---
# --- Params: Specific(DateTime) , Row(Symbols) , Col(Factors) ---
# Symbol1: Factor1 Factor2 Factor3---
# Symbol2: Factor1 Factor2 Factor3---
# Symbol3: Factor1 Factor2 Factor3---
def LoadFactorsProfileAsDataFrame(database, datetime2, instruments, factors, filter={}):

    #if rename == "":
    #    rename = factorName

    # print("LoadFactor " + factorName + " as " + rename + " @Time " + str(datetime2))

    data = []
    i = 0
    countInstrumnts = len(instruments)
    for instrument in instruments:
        symbol = instrument["Symbol"]
        i += 1
        #
        if i % 100 == 0:
            print("LoadFactorsAsDataFrame " + symbol + " " + str(i) + " / " + str(countInstrumnts) + " " + str(datetime.now()))
        #
        filter["StdDateTime"] = {"$lte": datetime2}
        filter["Symbol"] = symbol
        # values = LoadFactor(database,symbol, factorName, datetime2)
        sort = [("StdDateTime", pymongo.ASCENDING)]
        #
        entry = [symbol]
        # Check if All element are None
        invalid = True
        for factor in factors:
            factorValue = None
            values = database.findWithFilter("Factor", factor, filter, sort)
            count = len(values)
            if count != 0:
                factorValue = values[count-1]["Value"]
            #
            if factorValue != None:
                invalid = False
            entry.append(factorValue)

        if invalid:
            print(symbol + " All Factor Invalid")
        data.append(entry)

    # ---Header---
    fields = ['Symbol']
    for factor in factors:
        fields.append(factor)

    dfData = pd.DataFrame(data, columns=fields)
    return dfData


#--- Specific Factor---
# --- Params: Specific(Factor) , Row(DateTimes) , Col(Symbols) ---
#           Symbo1 Symbol2 Symbol3
# DateTime1 Factor  Factor  Factor
# DateTime2 Factor  Factor  Factor
# DateTime3 Factor  Factor  Factor
def LoadMultiInstrumentsFactorsAsDataFrame(database, factor, datetime1, datetime2, instruments, filter={}):

    df = pd.DataFrame()
    n = len(instruments)
    i = 0
    for instrument in instruments:
        symbol = instrument["Symbol"]
        tempDf = LoadFactorsAsDataFrame(database, symbol, factors=[factor], datetime1=datetime1, datetime2=datetime2)
        tempDf.rename(columns={factor: symbol}, inplace=True)
        # print(tempDf)
        # ---drop useless column---
        # tempDf = tempDf.drop(["DataSeries", "Key", "Symbol"], axis=1)
        # ---
        # tempDf.rename(columns={'BClose': symbol}, inplace=True)
        #
        # ---First time loop---
        if tempDf.empty:
            continue

        if df.empty:
            df = tempDf
            continue
        #
        # print("Df")
        # print(df)
        # print("tempDf")
        # print(tempDf)
        # ---Temp merge to Mother---
        df = pd.merge(df, tempDf, on='StdDateTime', how='outer')

        # ---Print info---
        i += 1
        if i == n:
            print("LoadMultiInstrumentsFactorsAsDataFrame", "100", "%")
        elif i % 100 == 0:
            progress = i / n
            print("LoadMultiInstrumentsFactorsAsDataFrame", round(progress * 100, 2), "%", datetime.now())




    # ---After Merge All---
    df['StdDateTime'] = pd.to_datetime(df['StdDateTime'])
    # print(df)
    # df.set_index("DateTime", inplace=True)
    # df.sort_index(inplace=True)
    df.sort_values("StdDateTime", inplace=True)
    df.reset_index(drop=True, inplace=True)
    # print(df.head())
    return df


# ---Only Used in DIYFinancialAdvisors---
def LoadFactorFast(database, instruments, factorName, filter, rename=""):

    if rename == "":
        rename = factorName

    #print("LoadFactor " + factorName + " as " + rename)

    #instruments2 = []
    #for instrument in instruments:
    #    instruments2.append(instrument)

    dfInstruments = pd.DataFrame(instruments, columns = ["Symbol"])
    dfSymbols = dfInstruments[["Symbol"]]
    #print(dfSymbols)

    values = database.findWithFilter("Factor", factorName, filter)
    dfTemp = pd.DataFrame(values)
    #print(dfTemp)
    dfData = pd.merge(dfSymbols, dfTemp, on='Symbol', how='left')
    return dfData



#################################################
#---Read TextFile---

def WriteToDataBase_HuaBaoPositionFile(portfolioName, datetime1, database, pathFilename):

    instruments = database.findAll("Instruments","Instruments")

    dataObjects = DataSeries(portfolioName + "_Portfolio")
    dataObject = {}
    dataObject["DataSeries"] = dataObjects.name

    #---Load Text File---
    positions = []
    file = open(pathFilename, 'r')
    i = 0;
    headerIndexByName = {}
    while True:
        s = file.readline()
        i = i+1
        if s == '':
            break

        content = s.split(',')
        #Process Headers
        if i == 1:
            headerCount = 0
            for header in content:
                headerIndexByName[header] = headerCount
                headerCount = headerCount + 1
            continue
        #Process Header Done

        # Position 的必填字段
        position = {}
        position["DateTime"] = ToDateTimeString(datetime1)
        position["StdDateTime"] = ToUTCDateTime(datetime1)
        position["Portfolio"] = portfolioName
        position["Instrument"] = ""
        position["Side"] = ""
        position["Amount"] = 0.0 # 总持仓 可以是负数
        position["Qty"] = 0.0 #总持仓 只能为正
        position["Avaliable"] =0.0 #可平仓
        position["CostPrice"] = 0.0
        position["LastTrade"] = 0.0
        position["PositionValue"] = 0.0
        position["PositionEquity"] = 0.0
        position["ProfitLoss"] = 0.0

        symbol1 = content[headerIndexByName["证券代码"]] #symbol read from Text File 600000, without Exchange info
        name = content[headerIndexByName["证券名称"]]
        #---Find Instrument in DataBase---
        curInstrument = None
        for instrument in instruments:
            symbol2 = instrument["Symbol"] #symbol in Database 600000.SH, with Exchange info
            symbol2 = symbol2[:6] #取前六位
            if(symbol2 == "600988"):
                kkwood = 1
            if symbol1 == symbol2:
                position["Instrument"] = instrument["Symbol"]
                position["Key"] = instrument["Symbol"]
                curInstrument = instrument
                break
        if  curInstrument == None:
            print("Can't Found Instrument Symbol: " + symbol1 + " " + name)
            continue

        position["Qty"] = float(content[headerIndexByName["总持仓"]])
        side = content[headerIndexByName["方向"]]
        if side == "" or side == "Long":
            position["Side"] = "Long"
            position["Amount"] = position["Qty"]
        elif side == "Short":
            position["Side"] = "Short"
            position["Amount"] = -1.0 * position["Qty"]
        else:
            print("Can't Parse 方向: " + side)
            continue

        position["Avaliable"] = float(content[headerIndexByName["可平仓"]])
        position["CostPrice"] = float(content[headerIndexByName["持仓均价"]])
        position["LastTrade"] = float(content[headerIndexByName["最新价"]])
        position["ProfitLoss"] = float(content[headerIndexByName["浮动盈亏"]])
        position["PositionValue"] = float(content[headerIndexByName["市值"]])
        if curInstrument["InstrumentType"] == "Future" or curInstrument["InstrumentType"] == "Option":
            position["PositionEquity"] = float(content[headerIndexByName["保证金"]])
        else:
            position["PositionEquity"] = position["PositionValue"]

        #添加“一个Position”到“Positions列表”
        positions.append(position)

    dataObject["DateTime"] = ToDateTimeString(datetime1)
    dataObject["StdDateTime"] = ToUTCDateTime(datetime1)
    dataObject["Account"] = portfolioName
    dataObject["Cash"] = 0.0
    dataObject["Value"] = 0.0
    dataObject["Positions"] = positions

    positionEquity = 0
    positionValue = 0
    pofitLoss = 0
    for position in positions:
        positionEquity = positionEquity + position["PositionEquity"]
        positionValue = positionValue + position["PositionValue"]
        pofitLoss = pofitLoss + position["ProfitLoss"]

    dataObject["PositionEquity"] = positionEquity
    dataObject["PositionValue"] = positionValue
    dataObject["PofitLoss"] = pofitLoss

    dataObjects.add(dataObject)
    database.saveDataSeries(dataObjects, isUpSert=True)
    #---Loop Finished---
    file.close()

def WriteToDataBase_CTPPositionFile(portfolioName, datetime1, database, pathFilename):

    instruments = database.findAll("Instruments","Future")

    dataObjects = DataSeries(portfolioName + "_Portfolio")
    dataObject = {}
    dataObject["DataSeries"] = dataObjects.name

    #---Load Text File---
    positions = []
    file = open(pathFilename, 'r')
    i = 0;
    headerIndexByName = {}
    while True:
        s = file.readline()
        i = i+1
        if s == '':
            break

        content = s.split(',')
        tmpList = []
        tmp = ""
        for s in content:
            if s[0] == '"':
                tmp = s #Begin
            elif s[-1] == '"':
                tmp = tmp + s
                tmpList.append(tmp)
                tmp = "" #End
            else:
                if tmp != "": # Maight Middle
                    tmp = tmp + s
                else:
                    tmpList.append(s)
        content = tmpList

        #Process Headers
        if i == 1:
            headerCount = 0
            for header in content:
                headerIndexByName[header] = headerCount
                headerCount = headerCount + 1
            continue
        #Process Header Done

        # Position 的必填字段
        position = {}
        position["DateTime"] = ToDateTimeString(datetime1)
        position["StdDateTime"] = ToUTCDateTime(datetime1)
        position["Portfolio"] = portfolioName
        position["Instrument"] = ""
        position["Side"] = ""
        position["Amount"] = 0.0 # 总持仓 可以是负数
        position["Qty"] = 0.0 #总持仓 只能为正
        position["Avaliable"] =0.0 #可平仓
        position["CostPrice"] = 0.0
        position["LastTrade"] = 0.0
        position["PositionValue"] = 0.0
        position["PositionEquity"] = 0.0
        position["ProfitLoss"] = 0.0

        symbol1 = content[headerIndexByName["合约"]] #symbol read from Text File 600000, without Exchange info
        symbol1 = symbol1.upper()
        #name = content[headerIndexByName["证券名称"]]
        #---Find Instrument in DataBase---
        curInstrument = None
        for instrument in instruments:
            symbol2 = instrument["Symbol"] #symbol in Database 600000.SH, with Exchange info
            symbol2 = symbol2[:-4] #舍弃后四位
            if(symbol2 == "IF1612"):
                kkwood = 1
            if symbol1 == symbol2:
                position["Instrument"] = instrument["Symbol"]
                position["Key"] = instrument["Symbol"]
                curInstrument = instrument
                break
        if  curInstrument == None:
            print("Can't Found Instrument Symbol: " + symbol1)
            continue

        position["Qty"] = float(content[headerIndexByName["总持仓"]])
        side = content[headerIndexByName["买卖"]]
        side = side.strip()
        if side == "买":
            position["Side"] = "Long"
            position["Amount"] = position["Qty"]
        elif side == "卖":
            position["Side"] = "Short"
            position["Amount"] = -1.0 * position["Qty"]
        else:
            print("Can't Parse 方向: " + side)
            continue

        position["Avaliable"] = float(content[headerIndexByName["可平量"]])
        position["CostPrice"] = float(content[headerIndexByName["持仓均价"]])
        #position["LastTrade"] = float(content[headerIndexByName["最新价"]])#没有最新价字段
        position["ProfitLoss"] = float(content[headerIndexByName["持仓盈亏"]].strip('"'))
        #position["PositionValue"] = float(content[headerIndexByName["市值"]])#自然算不出市值
        if curInstrument["InstrumentType"] == "Future" or curInstrument["InstrumentType"] == "Option":
            s = content[headerIndexByName["占用保证金"]] # 原文带着分号和逗号 "49,719.60"
            s = s.strip('"')
            position["PositionEquity"] = float(s)
        else:
            position["PositionEquity"] = position["PositionValue"]

        #添加“一个Position”到“Positions列表”
        positions.append(position)

    dataObject["DateTime"] = ToDateTimeString(datetime1)
    dataObject["StdDateTime"] = ToUTCDateTime(datetime1)
    dataObject["Account"] = portfolioName
    dataObject["Cash"] = 0.0
    dataObject["Value"] = 0.0
    dataObject["Positions"] = positions

    positionEquity = 0
    positionValue = 0
    pofitLoss = 0
    for position in positions:
        positionEquity = positionEquity + position["PositionEquity"]
        positionValue = positionValue + position["PositionValue"]
        pofitLoss = pofitLoss + position["ProfitLoss"]

    dataObject["PositionEquity"] = positionEquity
    dataObject["PositionValue"] = positionValue
    dataObject["PofitLoss"] = pofitLoss

    dataObjects.add(dataObject)
    database.saveDataSeries(dataObjects, isUpSert=True)
    #---Loop Finished---
    file.close()

def WriteToDataBase_WindPortfolioFile(portfolioName, datetime1, database, excelPathfilename):

    instruments = database.findAll("Instruments","Instruments")

    dataObjects = DataSeries(portfolioName + "_Portfolio")
    dataObject = {}
    dataObject["DataSeries"] = dataObjects.name

    #---Load Text File---
    book = xlrd.open_workbook(excelPathfilename)
    sheet = book.sheet_by_name("Wind资讯")

    nrows = sheet.nrows
    ncols = sheet.ncols
    positions = []
    i = 0;
    headerIndexByName = {}
    for i in range(0,nrows):
        row_data = sheet.row_values(i)
        content = row_data
        if content[2] == "":
            continue

        #Process Headers
        if i == 0:
            headerCount = 0
            for header in content:
                headerIndexByName[header] = headerCount
                headerCount = headerCount + 1
            continue
        #Process Header Done

        # Position 的必填字段
        position = {}
        position["DateTime"] = ToDateTimeString(datetime1)
        position["StdDateTime"] = ToUTCDateTime(datetime1)
        position["Portfolio"] = portfolioName
        position["Instrument"] = ""
        position["Side"] = ""
        position["Amount"] = 0.0 # 总持仓 可以是负数
        position["Qty"] = 0.0 #总持仓 只能为正
        position["Avaliable"] =0.0 #可平仓
        position["CostPrice"] = 0.0
        position["LastTrade"] = 0.0
        position["PositionValue"] = 0.0
        position["PositionEquity"] = 0.0
        position["ProfitLoss"] = 0.0

        symbol1 = content[headerIndexByName["证券代码"]] #symbol read from Text File 600000, without Exchange info
        name = content[headerIndexByName["证券简称"]]
        #---Find Instrument in DataBase---
        curInstrument = None
        for instrument in instruments:
            symbol2 = instrument["Symbol"] #symbol in Database 600000.SH, with Exchange info
            if symbol1 == symbol2:
                position["Instrument"] = instrument["Symbol"]
                position["Key"] = instrument["Symbol"]
                curInstrument = instrument
                break
        if  curInstrument == None:
            print("Can't Found Instrument Symbol: " + symbol1 + " " + name)
            continue

        index = headerIndexByName["持仓数量"]
        value = content[index]
        position["Amount"] = float(content[headerIndexByName["持仓数量"]])
        amount =  position["Amount"]
        position["Qty"] = np.absolute(amount)
        if amount >= 0:
            position["Side"] = "Long"
        elif amount < 0:
            position["Side"] = "Short"

        position["Avaliable"] = position["Qty"]
        position["CostPrice"] = float(content[headerIndexByName["成本价格"]])
        position["LastTrade"] = float(content[headerIndexByName["最新价"]])
        position["ProfitLoss"] = float(content[headerIndexByName["浮动盈亏(元)"]])
        position["PositionValue"] = float(content[headerIndexByName["持仓市值(元)"]])
        if curInstrument["InstrumentType"] == "Future" or curInstrument["InstrumentType"] == "Option":
            position["PositionEquity"] = 0 # Wind文件没法计算保证金
        else:
            position["PositionEquity"] = position["PositionValue"]

        #添加“一个Position”到“Positions列表”
        positions.append(position)

    dataObject["DateTime"] = ToDateTimeString(datetime1)
    dataObject["StdDateTime"] = ToUTCDateTime(datetime1)
    dataObject["Account"] = portfolioName
    dataObject["Cash"] = 0.0
    dataObject["Value"] = 0.0
    dataObject["Positions"] = positions

    positionEquity = 0
    positionValue = 0
    pofitLoss = 0
    for position in positions:
        positionEquity = positionEquity + position["PositionEquity"]
        positionValue = positionValue + position["PositionValue"]
        pofitLoss = pofitLoss + position["ProfitLoss"]

    dataObject["PositionEquity"] = positionEquity
    dataObject["PositionValue"] = positionValue
    dataObject["PofitLoss"] = pofitLoss

    dataObjects.add(dataObject)
    database.saveDataSeries(dataObjects, isUpSert=True)
    #---Loop Finished---
    #book.close()

def WriteToDataBase_555PositionFile(portfolioName, datetime1, database, pathFilename):

    instruments = database.findAll("Instruments","Instruments")

    dataObjects = DataSeries(portfolioName + "_Portfolio")
    dataObject = {}
    dataObject["DataSeries"] = dataObjects.name

    #---Load Text File---
    positions = []
    file = open(pathFilename, 'r')
    i = 0;
    headerIndexByName = {}
    while True:
        s = file.readline()
        i = i+1
        if s == '':
            break
        if i < 4:
            continue

        s = s.replace('"',"")
        s = s.replace('=',"")
        content = s.split('\t')
        #Process Headers
        if i == 4:
            headerCount = 0
            for header in content:
                headerIndexByName[header] = headerCount
                headerCount = headerCount + 1
            continue
        #Process Header Done

        # Position 的必填字段
        position = {}
        position["DateTime"] = ToDateTimeString(datetime1)
        position["StdDateTime"] = ToUTCDateTime(datetime1)
        position["Portfolio"] = portfolioName
        position["Instrument"] = ""
        position["Side"] = ""
        position["Amount"] = 0.0 # 总持仓 可以是负数
        position["Qty"] = 0.0 #总持仓 只能为正
        position["Avaliable"] =0.0 #可平仓
        position["CostPrice"] = 0.0
        position["LastTrade"] = 0.0
        position["PositionValue"] = 0.0
        position["PositionEquity"] = 0.0
        position["ProfitLoss"] = 0.0

        symbol1 = content[headerIndexByName["证券代码"]] #symbol read from Text File 600000, without Exchange info
        name = content[headerIndexByName["证券名称"]]
        #---Find Instrument in DataBase---
        curInstrument = None
        for instrument in instruments:
            symbol2 = instrument["Symbol"] #symbol in Database 600000.SH, with Exchange info
            symbol2 = symbol2[:6] #取前六位
            if(symbol2 == "600988"):
                kkwood = 1
            if symbol1 == symbol2:
                position["Instrument"] = instrument["Symbol"]
                position["Key"] = instrument["Symbol"]
                curInstrument = instrument
                break
        if  curInstrument == None:
            print("Can't Found Instrument Symbol: " + symbol1 + " " + name)
            continue

        position["Qty"] = float(content[headerIndexByName["证券数量"]])
        position["Side"] = "Long"
        position["Amount"] = position["Qty"]
        position["Avaliable"] = float(content[headerIndexByName["可卖数量"]])
        position["CostPrice"] = float(content[headerIndexByName["买入均价"]])
        position["LastTrade"] = float(content[headerIndexByName["当前价"]])
        position["ProfitLoss"] = float(content[headerIndexByName["持仓盈亏"]])
        position["PositionValue"] = float(content[headerIndexByName["最新市值"]])
        position["PositionEquity"] = position["PositionValue"]

        #添加“一个Position”到“Positions列表”
        positions.append(position)

    dataObject["DateTime"] = ToDateTimeString(datetime1)
    dataObject["StdDateTime"] = ToUTCDateTime(datetime1)
    dataObject["Account"] = portfolioName
    dataObject["Cash"] = 0.0
    dataObject["Value"] = 0.0
    dataObject["Positions"] = positions

    positionEquity = 0
    positionValue = 0
    pofitLoss = 0
    for position in positions:
        positionEquity = positionEquity + position["PositionEquity"]
        positionValue = positionValue + position["PositionValue"]
        pofitLoss = pofitLoss + position["ProfitLoss"]

    dataObject["PositionEquity"] = positionEquity
    dataObject["PositionValue"] = positionValue
    dataObject["PofitLoss"] = pofitLoss

    dataObjects.add(dataObject)
    database.saveDataSeries(dataObjects, isUpSert=True)
    #---Loop Finished---
    file.close()

def WriteToDataBase_ArbiPositionFile(portfolioName, datetime1, database, pathFilename):

    instruments = database.findAll("Instruments","Instruments")

    dataObjects = DataSeries(portfolioName + "_Portfolio")
    dataObject = {}
    dataObject["DataSeries"] = dataObjects.name

    #---Load Text File---
    positions = []
    file = open(pathFilename, 'r')
    i = 0;
    headerIndexByName = {}
    while True:
        s = file.readline()
        i = i+1
        if s == '':
            break

        content = s.split('\t')
        #Process Headers
        if i == 1:
            headerCount = 0
            for header in content:
                headerIndexByName[header] = headerCount
                headerCount = headerCount + 1
            continue
        #Process Header Done

        # Position 的必填字段
        position = {}
        position["DateTime"] = ToDateTimeString(datetime1)
        position["StdDateTime"] = ToUTCDateTime(datetime1)
        position["Portfolio"] = portfolioName
        position["Instrument"] = ""
        position["Side"] = ""
        position["Amount"] = 0.0 # 总持仓 可以是负数
        position["Qty"] = 0.0 #总持仓 只能为正
        position["Avaliable"] =0.0 #可平仓
        position["CostPrice"] = 0.0
        position["LastTrade"] = 0.0
        position["PositionValue"] = 0.0
        position["PositionEquity"] = 0.0
        position["ProfitLoss"] = 0.0

        symbol1 = content[headerIndexByName["证券代码"]] #symbol read from Text File 600000, without Exchange info
        name = content[headerIndexByName["证券名称"]]
        #---Find Instrument in DataBase---
        curInstrument = None
        for instrument in instruments:
            symbol2 = instrument["Symbol"] #symbol in Database 600000.SH, with Exchange info
            symbol2 = symbol2[:6] #取前六位
            if(symbol2 == "600988"):
                kkwood = 1
            if symbol1 == symbol2:
                position["Instrument"] = instrument["Symbol"]
                position["Key"] = instrument["Symbol"]
                curInstrument = instrument
                break
        if  curInstrument == None:
            print("Can't Found Instrument Symbol: " + symbol1 + " " + name)
            continue

        position["Qty"] = float(content[headerIndexByName["股票余额"]])
        position["Side"] = "Long"
        position["Amount"] = position["Qty"]
        position["Avaliable"] = float(content[headerIndexByName["可用余额"]])
        position["CostPrice"] = float(content[headerIndexByName["成本价"]])
        position["LastTrade"] = float(content[headerIndexByName["市价"]])
        position["ProfitLoss"] = float(content[headerIndexByName["盈亏"]])
        position["PositionValue"] = float(content[headerIndexByName["市值"]])
        position["PositionEquity"] = position["PositionValue"]

        #添加“一个Position”到“Positions列表”
        positions.append(position)

    dataObject["DateTime"] = ToDateTimeString(datetime1)
    dataObject["StdDateTime"] = ToUTCDateTime(datetime1)
    dataObject["Account"] = portfolioName
    dataObject["Cash"] = 0.0
    dataObject["Value"] = 0.0
    dataObject["Positions"] = positions

    positionEquity = 0
    positionValue = 0
    pofitLoss = 0
    for position in positions:
        positionEquity = positionEquity + position["PositionEquity"]
        positionValue = positionValue + position["PositionValue"]
        pofitLoss = pofitLoss + position["ProfitLoss"]

    dataObject["PositionEquity"] = positionEquity
    dataObject["PositionValue"] = positionValue
    dataObject["PofitLoss"] = pofitLoss

    dataObjects.add(dataObject)
    database.saveDataSeries(dataObjects, isUpSert=True)
    #---Loop Finished---
    file.close()

def WriteToDataBase_JingChaoExcelFile(portfolioName, datetime1,datetime2, database, excelPathfilename):

    dataObjects = DataSeries(portfolioName + "_Portfolio")

    #---Load Text File---
    book = xlrd.open_workbook(excelPathfilename)
    sheet = book.sheet_by_name("资产")

    nrows = sheet.nrows
    ncols = sheet.ncols
    positions = []
    i = 0;
    headerIndexByName = {}
    for i in range(0,nrows):
        row_data = sheet.row_values(i)
        content = row_data
        if content[0] == "":
            continue

        #Process Headers
        if i == 0:
            headerCount = 0
            for header in content:
                headerIndexByName[header] = headerCount
                headerCount = headerCount + 1
            continue
        #Process Header Done

        #---Portfolio Basic Info---
        portfolio = {}
        portfolio["DataSeries"] = dataObjects.name
        portfolio["Name"] = portfolioName
        portfolio["Account"] = portfolioName

        #---Datatime---
        daysAfter1900 = content[headerIndexByName["日期"]]
        datetime0 = datetime(1900,1,1) + timedelta(days=daysAfter1900-2)
        #datetime0 = ParseDateTime(sDate)
        datetime0 = datetime0 + timedelta(hours=15)
        portfolio["DateTime"] = ToDateTimeString(datetime0)
        portfolio["StdDateTime"] = ToUTCDateTime(datetime0)

        #---Stock Account---
        portfolio["StockAccountValue"] = float(content[headerIndexByName["总资产"]])

        f0 = 0
        f1 = 0
        f2 = 0
        f3 = 0
        f4 = 0
        f5 = 0
        s0 = content[headerIndexByName["现金"]]
        s1 = content[headerIndexByName["证券持仓资金"]]
        s2 = content[headerIndexByName["拆借资金"]]
        s3 = content[headerIndexByName["新股申购/冻结款"]]
        s4 = content[headerIndexByName["证券出入金"]]
        s5 = content[headerIndexByName["期货出入金"]]
        if s0 != "":
            f0 = float(s0)
        if s1 != "":
            f1 = float(s1)
        if s2 != "":
            f2 = float(s2)
        if s3 != "":
            f3 = float(s3)
        if s4 != "":
            f4 = float(s4)
        if s5 != "":
            f5 = float(s5)
        portfolio["StockAccountCash"] = f0 + f2
        portfolio["StockAccountPositionValue"] = f1 + f3
        portfolio["StockAccountPositionEquity"] = portfolio["StockAccountPositionValue"]
        portfolio["StockAccountDeposit"] = f4

        #---Future Account---
        portfolio["FutureAccountValue"] = float(content[headerIndexByName["期货权益"]])
        portfolio["FutureAccountCash"] = 0
        portfolio["FutureAccountPositionValue"] = 0
        portfolio["FutureAccountPositionEquity"] = 0
        portfolio["FurureAccountDeposit"] = f5

        #---Total---
        portfolio["Value"] = float(content[headerIndexByName["计划总市值"]])
        portfolio["Cash"] = 0.0
        portfolio["PositionValue"] = 0.0
        portfolio["PositionEquity"] = 0.0
        portfolio["ProfitLoss"] = 0.0
        portfolio["Deposit"] = f4 + f5

        children = []
        children.append(portfolioName + "-Stock")
        children.append(portfolioName + "-Future")
        portfolio["Chidren"] = children

        #添加“Portfolio”
        dataObjects.add(portfolio)

    database.saveDataSeries(dataObjects, isUpSert=True)
    #---Loop Finished---
    #book.close()

def ReadTDXTextFile_WriteToDataBase(pathFolderName,database, minutePeriod ,startDatetime):

    list = []
    instruments = database.findAll("Instruments","Stock")
    for instrument in instruments:
        list.append(instrument["Symbol"])

    count = 0
    files = os.listdir(pathFolderName)
    for filename in files:
        count = count + 1
        if count <= 1:
            continue

        #symbol = filename[3:-4] + "." + filename[0:2]
        symbol = filename[:6] + "." + filename[7:9]
        if symbol not in list:
            print(symbol + " Not in Instruments")
            continue
        #if symbol != "000006.SZ":
        #    continue
        print("Reading TDX Text File " + str(symbol) + " Count " + str(count))
        ReadTDXTextFile_WriteToDataBase2(symbol, pathFolderName + "/" + filename, database, minutePeriod, startDatetime)
        kkwood = 1
    kkwood = 0

def ReadTDXTextFile_WriteToDataBase2(symbol, pathFilename, database, minutePeriod, startDatetime):

    size = int(minutePeriod * 60)
    dataObjects = DataSeries.DataSeries(symbol + "_Time_" + str(size) + "_Bar")

    #---Load Text File---
    positions = []
    file = open(pathFilename, 'r')
    i = 0
    headerIndexByName = {}
    while True:
        s = file.readline()
        i = i+1
        if s == '':
            break

        if i == 1:
            continue

        #Process Headers
        if i == 2:
            content = s.split('\t')
            headerCount = 0
            for header in content:
                header = header.strip(" ")
                header = header.strip("\n")
                headerIndexByName[header] = headerCount
                headerCount = headerCount + 1
            continue
        #Process Header Done

        content = s.split(',')
        if content.__len__() < 2:
            break

        dataObject = {}

        sDate = content[headerIndexByName["日期"]]
        sTime = content[headerIndexByName["时间"]]
        sDateTime = sDate + " " + sTime[0:2] + ":" + sTime[2:4] + ":00.000"
        datetime1 = ParseDateTime(sDateTime)

        if datetime1 < startDatetime:#不计入那些太早的时间，加快效率
            continue

        datetime0 = datetime1 + timedelta(minutes = -minutePeriod)
        datetime2 = ToUTCDateTime(datetime1)

        #dataObject["DataSeries"] = name
        dataObject["OpenDateTime"] =  ToDateTimeString(datetime0)
        dataObject["DateTime"] = ToDateTimeString(datetime1)
        dataObject["StdDateTime"] = datetime2
        dataObject["DataSeries"] = dataObjects.name
        dataObject["Key"] = dataObjects.name + "_" + dataObject["DateTime"]

        dataObject["Symbol"] = symbol
        dataObject["Size"] = size
        dataObject["BarType"] = "Time"

        dataObject["Open"] =  float(content[headerIndexByName["开盘"]])
        dataObject["High"] = float(content[headerIndexByName["最高"]])
        dataObject["Low"] = float(content[headerIndexByName["最低"]])
        dataObject["Close"] = float(content[headerIndexByName["收盘"]])
        dataObject["Volume"] = float(content[headerIndexByName["成交量"]])
        dataObject["Money"] = float(content[headerIndexByName["成交额"]])

        if "持仓量" in headerIndexByName:
            dataObject["OpenInt"] =  float(content[headerIndexByName["持仓量"]])

        dataObjects.add(dataObject)

    #---把DataSeries储存到数据库---
    database.saveDataSeries(dataObjects, isUpSert=True)
    kkwood = 1

def WriteToPortfolioDataBase(portfolioName, symbols, factorValues, rebalanceDate, database):

    dataObjects = DataSeries(portfolioName + "_Portfolio")

    #---Portfolio Basic Info---
    portfolio = {}
    portfolio["Rebalance"] = True
    portfolio["DataSeries"] = dataObjects.name
    portfolio["Name"] = portfolioName
    portfolio["Account"] = portfolioName

    #---Datatime---
    portfolio["DateTime"] = ToDateTimeString(rebalanceDate)
    portfolio["StdDateTime"] = ToUTCDateTime(rebalanceDate)

    #---Stock Account---
    weight = 1 / symbols.__len__()
    positions = []
    for i in range(symbols.__len__()):
        positions.append({"Instrument": symbols[i], "Factor":factorValues[i], "Weight":weight})
    portfolio["Positions"] = positions

    #---Total---
    portfolio["Value"] = 0.0
    portfolio["Cash"] = 0.0
    portfolio["PositionValue"] = 0.0
    portfolio["PositionEquity"] = 0.0
    portfolio["ProfitLoss"] = 0.0
    portfolio["Deposit"] = 0.0

    #添加“Portfolio”
    dataObjects.add(portfolio)
    database.saveDataSeries(dataObjects, isUpSert=True)
    #---Loop Finished---
    #book.close()


def ReadExcelFile(excelPathfilename, sheetName = "Sheet1"):

    # ---Load Text File---
    book = xlrd.open_workbook(excelPathfilename)
    sheet = book.sheet_by_name(sheetName)

    nrows = sheet.nrows
    ncols = sheet.ncols
    i = 0;
    headerIndexByName = {}
    table = []
    for i in range(0, nrows):
        row_data = sheet.row_values(i)
        content = row_data
        # if content[0] == "":
        #     continue

        # Process Headers
        if i == 0:
            headerCount = 0
            for header in content:
                headerIndexByName[header] = headerCount
                headerCount = headerCount + 1
            continue
        # Process Header Done

        #Process Contents
        entry = []
        for data in content:
            #data = data.strip("\n")
            entry.append(data)
        table.append(entry)

    return headerIndexByName, table


def ReadCSVFile(pathFilename,spliter = ","):
    file = open(pathFilename, 'r')
    i = 0
    headerIndexByName = {}
    table = []
    while True:
        s = file.readline()
        i = i+1
        if s == '':
            break

        content = s.split(spliter)
        #Process Headers
        if i == 1:
            headerCount = 0
            for header in content:
                header = header.strip(" ")
                header = header.strip("\n")
                headerIndexByName[header] = headerCount
                headerCount = headerCount + 1
            continue

        #Process Contents
        entry = []
        for data in content:
            data = data.strip("\n")
            entry.append(data)
        table.append(entry)

    return headerIndexByName,table

#####################

def ExportWindTextFile(pathFilename, symbols, datetime2):
    file = open(pathFilename, 'w', encoding= 'UTF-8')
    weight = 1 / symbols.__len__()
    asset = 1000000
    content = "证券代码,持仓权重,成本价格,调整日期,证券类型\n"
    content = content + "Asset," + str(asset) + ",1," + ToDateString(datetime2)
    for symbol in symbols:
        s = "\n" + symbol + "," + str("%.2f"%(weight*100)) + "%," + "0.00" + "," + ToDateString(datetime2) + ",股票"
        content = content + s
    file.write(content)
    file.close()
    print("Write File "+ pathFilename)

def AppendListToFile(pathFilename, list):
    file = open(pathFilename, 'a', encoding= 'UTF-8')
    content = ""
    for value in list:
        content = content + str(value) + ","
    content = content + "\n"
    file.write(content)
    file.close()
    print("Append to File "+ pathFilename)

def WriteListToFile(pathFilename, list):
    file = open(pathFilename, 'w', encoding= 'UTF-8')
    content = ""
    for entry in list:
        content = content + str(entry) + "\n"
    file.write(content)
    file.close()
    #print("Write File "+ pathFilename)

def WriteList2ToFile(pathFilename, list):
    file = open(pathFilename, 'w', encoding= 'UTF-8')
    content = ""
    for entry in list:
        for value in entry:
            content = content + str(value) + ","
        content = content + "\n"
    file.write(content)
    file.close()
    print("Write File "+ pathFilename)

#---将Instruments输出到文件---
def WriteInsrumentsToFile(filename,instruments):
    f = open(filename, 'w')
    for instrument in instruments:
        f.write(instrument.symbol + "," + instrument.description + "\n")
    f.close()

def WriteDictToFile(pathFilename,dict,mode = "w"):
    content = ""
    for k,v in dict.items():
        content = content + k + "," + str(v) + "\n"

    file = open(pathFilename, mode, encoding='UTF-8')
    file.write(content + "\n")
    file.close()

def WriteToFile(pathFilename,content):
    file = open(pathFilename,mode="w", encoding= 'UTF-8')
    file.write(content)
    file.close()

def AppendToFile(pathFilename,content,mode = "a"):
    file = open(pathFilename, mode, encoding= 'UTF-8')
    file.write(content + "\n")
    file.close()

#--- Use MongoDB.SaveDataSeries Please ---
'''
def WriteFactorToDataBase(database,symbol,name,stdDateTime,value,params = ""):

    #if isinstance(params, dict):
    #    paramStr = ""
    #    for k,v in params.items():
    #        str += v + ";"
    #else:
    #    paramStr = params

    #target = {"Symbol":symbol,"Name":"LogReturn","Params":"Daily","StdDateTime":bar["StdDateTime"]}
    target = {"Symbol":symbol,"Name":name,"StdDateTime":stdDateTime ,"Params":params}
    element = copy.deepcopy(target)
    element["Value"] = value

    collectionName = name
    database.upsert("Factor", collectionName, target ,element)
'''

def WriteJsonFile(pathFileName, data):
    with open(pathFileName, 'w') as json_file:
        json_file.write(json.dumps(data))
        #json.dump(new_dict, f)

#---field =[]
def ExportDataBaseResult(database, databaseName, collectionName, filter, fields, pathFileName):
    #
    results = database.findWithFilter(databaseName, collectionName, filter)
    content = ""
    for entry in results:
        row = ""
        for field in fields:
            if field in entry:
                row += str(entry[field]) + ","

        content += row + "\n"
    WriteToFile(pathFileName,content)