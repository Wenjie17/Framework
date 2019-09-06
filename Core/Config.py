import json
import sys
import os
import logging
import logging.handlers
import Core.MongoDB as MongoDB
import Core.MySQLDB as MySQLDB
import Core.Realtimeview as RealTimeView


def CreateFolder(fullPathname):
    existed = os.path.exists(fullPathname)
    if not existed:
        os.makedirs(fullPathname)
        print("Create Folder: " + fullPathname)
    else:
        #print("Folder Existed: " + fullPathname)
        pass


# cfg_file = os.getcwd() + "\config_simulation.json"
# print("Init Config with " + cfg_file)
# config = json.load(open(cfg_file, 'r', encoding='utf-8'))


class Config(object):

    __initialized = False
    cfgFile = None
    __loggers__ = {}
    __database = None
    __realtimeViews = {}
    testnum = 0


    def __init__(self, pathFilename=""):
        if not Config.__initialized:
            if pathFilename == "":
                pathFilename = os.getcwd() + "\..\config.json"
            print("Init Config with " + pathFilename)
            Config.cfgFile = json.load(open(pathFilename, 'r', encoding='utf-8'))
            Config.__initialized = True


    def Logger(self, loggerName, consoleOutput=True):
        #
        if loggerName not in Config.__loggers__:
            #
            logger = logging.getLogger(loggerName)
            logger.setLevel(logging.INFO)  # Log等级总开关

            # ---File Name---
            logDir = Config.cfgFile["LogDir"]
            folder = logDir + "\\" + loggerName
            CreateFolder(folder)

            # fh = logging.FileHandler(folder + "\\" + loggerName , mode='a')
            fh = logging.handlers.TimedRotatingFileHandler(folder + "\\" + loggerName, when='D', interval=1, encoding="utf-8")

            # 指定logger输出格式
            formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s: %(message)s')
            fh.setFormatter(formatter)
            logger.addHandler(fh)

            # 往屏幕上输出
            if consoleOutput:
                sh = logging.StreamHandler()  # 往屏幕上输出
                sh.setFormatter(formatter)  # 设置屏幕上显示的格式
                logger.addHandler(sh)

            #
            Config.__loggers__[loggerName] = logger

        return Config.__loggers__[loggerName]


    def DataBase(self, type="Mongo"):
        #
        if Config.__database == None:
            print("Create Database Connection")
            if type == "Mongo":
                addressPort = Config.cfgFile["MongoDBAddressPort"].split(":")
                if Config.cfgFile["MongoDBAuth"] == "Yes":
                    Config.__database = MongoDB.MongoDB(addressPort[0], addressPort[1], Config.cfgFile["MongoDBUsername"], Config.cfgFile["MongoDBPassword"])
                else:
                    Config.__database = MongoDB.MongoDB(addressPort[0], addressPort[1])
            elif type == "MySQL":
                # ---connect database---
                addressPort = Config.cfgFile["MySQLDBAddressPort"].split(":")
                Config.__database = MySQLDB.MySQLDB(addressPort[0], addressPort[1],
                                                    username=Config.cfgFile["MySQLDBUsername"],
                                                    password=Config.cfgFile["MySQLDBPassword"])

        return Config.__database


    def RealTime(self, db=0):
        #
        if db not in Config.__realtimeViews:
            print("Create RealTimeView Connection")
            addressPort = Config.cfgFile["RedisAddressPort"].split(":")
            Config.__realtimeViews[db] = RealTimeView.RealTimeView(address=addressPort[0], port=addressPort[1], db=db)
        return Config.__realtimeViews[db]


# ---Instanced Here---
# config = Config()




