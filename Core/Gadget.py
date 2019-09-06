#coding=utf-8

from __future__ import print_function
from sys import getsizeof, stderr
from itertools import chain
from collections import deque
from reprlib import repr
#try:
#    from reprlib import repr
#except ImportError:
#    pass

import winsound
from datetime import *
import numpy as np
import pytz
import pymongo
import pandas as pd
import calendar as calendar
import pandas as pd
from Core.Instrument import *
import json
import os

class MyEncoder(json.JSONEncoder):
  def default(self, obj):
      # if isinstance(obj, datetime.datetime):
      #     return int(mktime(obj.timetuple()))
      if isinstance(obj, datetime):
          return obj.strftime('%Y-%m-%d %H:%M:%S.%f ')
      elif isinstance(obj, date):
          return obj.strftime('%Y-%m-%d')
      else:
          return json.JSONEncoder.default(self, obj)


def DeleteTimeZone(datetime1):
    return datetime1.replace(tzinfo = None)


def FilterWindDateTimeDirty(dt):
    return datetime(dt.year, dt.month, dt.day, dt.hour,dt.minute, dt.second)


def FutureSymbolToProduct(symbol):
    out = ""
    for s in symbol:
        if s.isdigit():
            return out
        else:
            out = out + s


def FindElement(list, target):
    for element in list:
        found = True
        for k,v in target.items():
            if element[k] != v:
                found = False
                break
        if found == True:
            return element
    return None


def FindIndex(dataSeries, datetime, option="Previous", datetimeField="StdDateTime"):
    if len(dataSeries) == 0:
        return None
    index = GetIndex(dataSeries, datetime, 0, len(dataSeries) - 1, 0, option, datetimeField)
    if index == None:
        return None
    if index == -1:
        return None
    if index >= len(dataSeries):
        return None
    return index


def Find(dataSeries, datetime, option="Previous", datetimeField="StdDateTime"):
    index = FindIndex(dataSeries, datetime, option=option, datetimeField=datetimeField)
    #
    if index == None:
        return None
    #
    return dataSeries[index]

#--- Listed before datetime1 ----
def FindListedInstrument(database, datetime1, datetime2=None, InstrumentType="Stock"):

    # 退市不早于 datetime1
    notDelisted = {"DateTime2": {">=": datetime1}} #$gte

    if datetime2 == None:
        datetime2 = datetime1

    # 上市早于 datetime2, 如果有datetime2
    alreadayListed = {"DateTime1": {"<=": datetime2}} #$lte

    #
    filter = {}

    # ---MongoStyle---
    # if datetime2:
    #     filter["$and"] = [notDelisted, alreadayListed]
    # else:
    #     filter = notDelisted
    #

    if datetime2:
        filter = {"DateTime2": {">=": datetime1}, "DateTime1": {"<=": datetime2}}
    else:
        filter = notDelisted
    #
    filteredInstruments = database.Find("Instruments", InstrumentType, filter=filter)
    return filteredInstruments

# ---Fix bug, no "Excat" situation 2018-4-10 ---
def GetIndex(dataSeries, datetime, beginIndex, endIndex, recursive, option, datetimeField):
    length = endIndex - beginIndex + 1
    beginDatetime = dataSeries[beginIndex][datetimeField]
    endDatetime = dataSeries[endIndex][datetimeField]

    #-------------------------------
    #---Special Case---
    if datetime < beginDatetime:
        if option == "Previous":
            return beginIndex - 1
        if option == "Next":
            return beginIndex
        if option == "Exact":
            return -1
    if datetime > endDatetime:
        if option == "Previous":
            return endIndex
        if option == "Next":
            return endIndex + 1
        if option == "Exact":
            return -1

    if datetime == beginDatetime:
        return beginIndex

    if datetime == endDatetime:
        return endIndex

    #-------------------------------
    #if length == 1:
    #    if option == "Previous":
    #        return beginIndex - 1
    #    if option == "Next":
    #        return beginIndex
    #    #other "Exect"
    #    return None

    if length == 2:
        #---只剩下中间状态了---
        if option == "Previous":
            return beginIndex
        if option == "Next":
            return endIndex
        return -1

    else:
        midIndex = beginIndex + int((length * 0.5))
        tmpDateTime = dataSeries[midIndex][datetimeField]
        # tmpDateTime = tmpDateTime.replace(tzinfo=None)
        if datetime <= tmpDateTime:
            return GetIndex(dataSeries, datetime, beginIndex, midIndex, recursive+1, option, datetimeField)
        else:
            return GetIndex(dataSeries, datetime,midIndex, endIndex, recursive+1, option, datetimeField)

    return -1


def GenerateReportDate(year, period, isUTC=True):
    #
    if period == 1:
        reportDate = datetime(year, 3, 31)
    elif period == 2:
        reportDate = datetime(year, 6, 30)
    elif period == 3:
        reportDate = datetime(year, 9, 30)
    else:
        reportDate = datetime(year, 12, 31)
    #
    if isUTC:
        return ToUTCDateTime(reportDate)
    else:
        return reportDate


#---给定起始日期，获取中间包括的报告日日期，report date
def GenerateReportDates(datetime1, datetime2):
    reportDateTimes = []
    reportDate = datetime1
    while reportDate <= datetime2:
        if ((reportDate.month == 3 and reportDate.day == 31)
            or
            (reportDate.month == 6 and reportDate.day == 30)
            or
            (reportDate.month == 9 and reportDate.day == 30)
            or
            (reportDate.month == 12 and reportDate.day == 31)):
            newReportDate = datetime(reportDate.year, reportDate.month, reportDate.day)
            reportDateTimes.append(newReportDate)
        reportDate = reportDate + timedelta(days=1)

    return reportDateTimes


# ---获取每月第一天---
def GenerateMonthDates(datetime1, datetime2):
    #
    if datetime1.tzinfo != None:
        isUTC = True
    else:
        isUTC = False
    #
    if isUTC:
        localDateTime1 = ToLocalDateTime(datetime1)
        localDateTime2 = ToLocalDateTime(datetime2)
    else:
        localDateTime1 = datetime1
        localDateTime2 = datetime2
    #
    datetimes = []
    for year in range(localDateTime1.year, localDateTime2.year + 1):
        for month in range(1, 13):
            beginDayofMonth = datetime(year, month, 1)
            #
            if isUTC:
                utcBeginDayofMonth = ToUTCDateTime(beginDayofMonth)
            else:
                utcBeginDayofMonth = beginDayofMonth
            #
            if datetime1 > utcBeginDayofMonth:
                continue
            if datetime2 < utcBeginDayofMonth:
                continue
            datetimes.append(utcBeginDayofMonth)
    #
    return datetimes


def GenerateEndDayofMonth(datetime1, datetime2):
    localDateTime1 = ToLocalDateTime(datetime1)
    localDateTime2 = ToLocalDateTime(datetime2)
    datetimes = []
    for year in range(localDateTime1.year, localDateTime2.year + 1):
        for month in range(1, 13):
            days = calendar.monthrange(year, month)
            endDayofMonth = datetime(year, month, days[1])
            endDayofMonth = endDayofMonth + timedelta(days=1)
            utcEndMonthDate = ToUTCDateTime(endDayofMonth)
            if datetime1 > utcEndMonthDate:
                continue
            if datetime2 < utcEndMonthDate:
                continue
            datetimes.append(utcEndMonthDate)
    return datetimes


def GenerateEndDateofMonth(datetime1, datetime2, asDate=False):
    # localDateTime1 = ToLocalDateTime(datetime1)
    # localDateTime2 = ToLocalDateTime(datetime2)
    localDateTime1 = datetime1
    localDateTime2 = datetime2
    if asDate:
        localDateTime1 = datetime1.date()
        localDateTime2 = datetime2.date()
    #
    datetimes = []
    for year in range(localDateTime1.year, localDateTime2.year + 1):
        for month in range(1, 13):
            days = calendar.monthrange(year, month)
            if asDate:
                endDayofMonth = date(year, month, days[1])
            else:
                endDayofMonth = datetime(year, month, days[1])
            endDayofMonth = endDayofMonth + timedelta(days=-1)
            if localDateTime1 > endDayofMonth:
                continue
            if localDateTime2 < endDayofMonth:
                continue
            datetimes.append(endDayofMonth)
    return datetimes


# ---5.1, 9.1 和 11.1(12.1)?
def GenerateReleaseDates(datetime1, datetime2):
    reportDateTimes = []
    reportDate = datetime1
    while reportDate <= datetime2:
        if ((reportDate.month == 5 and reportDate.day == 1)
            or
            (reportDate.month == 9 and reportDate.day == 1)
            or
            (reportDate.month == 11 and reportDate.day == 1)):
            reportDateTimes.append(reportDate)
        reportDate = reportDate + timedelta(days = 1)
    return reportDateTimes


def DateTimeToReportDate(datetime2):

    reportDate = None
    if datetime2 < datetime(datetime2.year, 5, 1):
        #
        reportDate = datetime(datetime2.year-1, 9, 31) # last year Q3
    elif datetime2 < datetime(datetime2.year, 9, 1):
        #
        reportDate = datetime(datetime2.year, 3, 31)
    elif datetime2 < datetime(datetime2.year, 11, 1):
        #
        reportDate = datetime(datetime2.year, 6, 30)
    else:
        reportDate = datetime(datetime2.year, 9, 30)
    #
    return reportDate





def GenerateCloseTime(year, month, day):
    datetime1 = datetime(year, month, day)
    datetime1 = datetime1 + timedelta(hours=15)
    return ToUTCDateTime(datetime1)


def GenerateDateRange(datetime1, datetime2):
    datetimes = []
    dt = datetime1
    while dt <= datetime2:
        datetimes.append(dt)
        dt += timedelta(days=1)

    return datetimes



def LoadConstitutes(database, indexSymbol, datetime2=None):

    instruments = []
    instrumentList_TimeSeries = database.getDataSeries(indexSymbol + "_InstrumentList")

    # --- Old Fashion---
    # instrumentsList = instrumentSeries[instrumentList_TimeSeries.count()-1]

    if datetime2 == None:
        datetime2 = datetime.now()
    datetime2 = ToUTCDateTime(datetime2)

    instrumentsList = instrumentList_TimeSeries.Get(datetime2)

    for content in instrumentsList["Values"]:
        symbol = content["Symbol"]
        weight = content["Weight"]
        instruments.append({"Symbol": symbol, "Weight": weight})

    return instruments


#---读取证券列表---
def LoadInstruments(database, instrumentsByName, maxNum):
    instruments = []
    instrumentSeries = database.findAll("Instruments","Stock")
    i = -1
    for obj in instrumentSeries:
        i = i+1
        if maxNum != 0:
            if(i >= maxNum):
                break
        instrument = Instrument(obj["Symbol"], obj["Description"], obj["Type"]) #建立一个Instrument对象
        if "Industry" in obj["Properties"]:
            instrument.industry = obj["Properties"]["Industry"] #补充一个行业信息进去
        else:
            print("Symbol " + obj["Symbol"] + " Not Include Industry Info!!!")
        instrumentsByName[instrument.symbol] = instrument #把instrument们放进一个大容器（字典）
        instruments.append(instrument)
    return instruments


def LoadInstruments2(database, maxNum):
    instruments = []
    instrumentSeries = database.findAll("Instruments","Stock")
    i = -1
    for obj in instrumentSeries:
        i = i+1
        if maxNum != 0:
            if(i >= maxNum):
                break
        instruments.append(obj["Symbol"])
    return instruments


def MinDateTime():
    minDateTime = datetime(1900, 1, 1)
    # minDateTime = ToUTCDateTime(minDateTime)
    return minDateTime


def MaxDateTime():
    maxDateTime = datetime(2100, 1, 1)
    maxDateTime = ToUTCDateTime(maxDateTime)
    return maxDateTime


# --- 2010-05-04 14:20:01.500 ---
# --- "%Y/%m/%d %H:%M:%S.%f" ---
# --- "%Y-%m-%d-%H" ---
def ParseDateTime(strDateTime, format = "%Y-%m-%d %H:%M:%S.%f"):
     return datetime.strptime(strDateTime, format)


def PlaySound():
    soundFile = 'd:\Sound\BeepLoop.wav'

    while(1):
        winsound.PlaySound(soundFile,winsound.SND_FILENAME)
        #winsound.PlaySound("SystemExit", winsound.SND_ALIAS)


def PageNavigation(page,button,maxLength):

    #page = parseInt(page)
    pageSize = 20
    maxPage = int(maxLength / pageSize) + 1

    if(button == "nextPage"):
        page += 1
    elif(button == "prePage"):
        page-=1
    elif(button == "lastPage"):
        page = maxPage
    elif(button == "firstPage"):
        page = 1

    if(page<1):
        page = 1
    if(page > maxPage):
        page = maxPage

    begin = (page - 1) * pageSize
    end = page * pageSize
    if (end > maxLength):
        end = maxLength - 1

    pageNavigation = {
        "Page":page,
        "Begin":begin,
        "End": end,
        "Size": pageSize,
        "MaxPage":maxPage}

    return pageNavigation


def StdDateTimeToTradeDate(stdDateTime):
    localDateTime = ToLocalDateTime(stdDateTime)
    return ToDate(localDateTime)


# ---DateTime convert to String of "2016-01-01"---
def ToDateString(date):
        #dt = datetime(int(date[0,3]),int(date[4,5]),int(date[6,7]))
        return date.strftime('%Y-%m-%d')


# ---DateTime convert to String of "20160101"---
def ToDateString2(date):
        #dt = datetime(int(date[0,3]),int(date[4,5]),int(date[6,7]))
        return date.strftime('%Y%m%d')

def ToDateString3(date):
 	#dt = datetime(int(date[0,3]),int(date[4,5]),int(date[6,7]))
    return str(date.year)+ "/" + str(date.month) + "/" + str(date.day)


def ToDateTimeString(date):
    s = date.strftime('%Y-%m-%d %H:%M:%S.%f')
    return s[:-3]


def ToDateTimeString2(date):
    return date.strftime('%Y-%m-%d %H:%M:%S.%f')


#---过滤Wind中毫秒脏数据现象---
def ToDateTimeString3(date):
    return date.strftime('%Y-%m-%d %H:%M:%S' + ".000")


def ToUTCDateTime(datetime1):
    #datetime2 = datetime1 + timedelta(hours=-8)
    #datetime2 = datetime2.replace(tzinfo=pytz.utc)

    # min = datetime(1950, 1, 1)
    # if datetime1 < min:
    #    datetime1 = min

    utc = pytz.utc
    datetime2 = datetime1.astimezone(utc)
    return datetime2


def ToLocalDateTime(datetime1):
    #datetime2 = datetime1 + timedelta(hours=+8)
    #datetime2 = datetime2.replace(tzinfo = pytz.timezone("Asia/Shanghai"))

    #min = datetime(1970, 1, 3)
    #if datetime1 < min:
    #    datetime1 = min

    local_Timezone = pytz.timezone("Asia/Shanghai")
    datetime2 = datetime1.astimezone(local_Timezone)
    return datetime2


def ToDate(datetime1):
    return datetime(datetime1.year, datetime1.month, datetime1.day)


def ToClosingDateTime(datetime1):
    datetime1 = ToLocalDateTime(datetime1)
    datetime2 = datetime(datetime1.year, datetime1.month, datetime1.day, 15, 0, 0)
    return ToUTCDateTime(datetime2)


def DateToDateTime(date1):
    return datetime.combine(date1, datetime.min.time())


#---Return 报告期 1,2,3,4---
def ReportPeriod(reportDate):
    if (reportDate.month == 3 and reportDate.day == 31):
        return 1;
        #modifiedDateTime = releaseDateTime + timedelta(seconds = 1)#防止第一季度报告和年报时间重合
    elif (reportDate.month == 6 and reportDate.day == 30):
        return 2;
    elif (reportDate.month == 9 and reportDate.day == 30):
        return 3;
    elif (reportDate.month == 12 and reportDate.day == 31):
        return 4;
    else:
        print("Unresolve Period");
        #print("Unresolve Period : " + symbol);
        return None


def ResampledDataSeries(dataSeries, datetimeSeries):
    resampledSeries = []
    i = 0
    for datetime0 in datetimeSeries:
        element = Find(dataSeries, datetime0)
        i = i + 1
        if element == None:
            continue
        resampledSeries.append(element)
    return resampledSeries


def ReadFolder(filepath):
    filenames = []
    pathDir =  os.listdir(filepath)
    for allDir in pathDir:
        child = os.path.join('%s%s' % (filepath, allDir))
        #print child.decode('gbk')
        filenames.append(allDir)

    return filenames


def TotalSize(o, handlers={}, verbose=False):
    """ Returns the approximate memory footprint an object and all of its contents.

    Automatically finds the contents of the following builtin containers and
    their subclasses:  tuple, list, deque, dict, set and frozenset.
    To search other containers, add handlers to iterate over their contents:

        handlers = {SomeContainerClass: iter,
                    OtherContainerClass: OtherContainerClass.get_elements}

    """
    dict_handler = lambda d: chain.from_iterable(d.items())
    all_handlers = {tuple: iter,
                    list: iter,
                    deque: iter,
                    dict: dict_handler,
                    set: iter,
                    frozenset: iter,
                   }
    all_handlers.update(handlers)     # user handlers take precedence
    seen = set()                      # track which object id's have already been seen
    default_size = getsizeof(0)       # estimate sizeof object without __sizeof__

    def sizeof(o):
        if id(o) in seen:       # do not double count the same object
            return 0
        seen.add(id(o))
        s = getsizeof(o, default_size)

        if verbose:
            print(s, type(o), repr(o), file=stderr)

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)


def SortDict(dict, reverse=False):
    sorted(dict.items(), key=lambda x: x[1], reverse=reverse)


def CreateFolder(fullPathname):
    existed = os.path.exists(fullPathname)
    if not existed:
        os.makedirs(fullPathname)
        print("Create Folder: " + fullPathname)
    else:
        #print("Folder Existed: " + fullPathname)
        pass


def GenerateTimeRange_Yearly(datetime1, datetime2, baseDate=datetime(2000, 5, 1)):
    datatimeRanges = []
    for year in range(datetime1.year, datetime2.year + 1):
        begin = datetime(year, baseDate.month, baseDate.day)
        end = datetime(year + 1, baseDate.month, baseDate.day)
        #
        if begin < datetime1 or end > datetime2:
            continue
        #
        datatimeRanges.append([begin, end])
    #
    return datatimeRanges


def GenerateTimeRange_Monthly(datetime1, datetime2):
    pass