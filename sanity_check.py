from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import inspect
from pyspark.sql.functions import col,size,length,split,regexp_replace
from pyspark.sql import SparkSession
import json
import sys
import logging
import os

def getConfig(filename):
    with open(filename) as configFile:
        data = json.load(configFile)
    return data

def getSource(spark, log, config):
    if(config['input']['isDirectHive']):
        log.info("Hive Direct Data Source")
        sourceData = spark.sql(config['input']['query'])
    elif (config['input']['isFile']):
        sourceData = spark.read.format("csv") \
            .option("header", config['input']['fileWithSchema']) \
            .load(config['input']['fileName'])
    else:
        log.info("JDBC Data Source")
        sourceData = spark.read.format("jdbc") \
            .option("url", config['input']['databaseUrl']) \
            .option("driver", config['input']['driverName']) \
            .option("dbtable", config['input']['query']) \
            .option("user", config['input']['databaseUserName']) \
            .option("password", config['input']['databasePassword']) \
            .load()
    return sourceData

def getLogger(logFilePath):

    #function_name = inspect.stack()[1][3]
    logger = logging.getLogger("Framework")
    logger.setLevel(logging.DEBUG)
    file_handler = logging.FileHandler(logFilePath)
    file_handler.setLevel(logging.DEBUG)
    file_handler_format = logging.Formatter('%(asctime)s - %(lineno)d - %(levelname)-8s - %(message)s')
    file_handler.setFormatter(file_handler_format)
    logger.addHandler(file_handler)

    return logger

def getSparkSession():

    #conf = SparkConf().setAppName("sanity_check_framework").setMaster("yarn-client")
	#sc = SparkContext(conf=conf)
	#spark = SQLContext(sc)
    spark = SparkSession.Builder().master("yarn-client").appName("Data Validation").enableHiveSupport().getOrCreate()
    return spark


def writeToFile(dataFrame, path, format):
    dataFrame.write.save(path=path, format=format, mode='overwrite')

def absoluteDuplicate(sourceData,config,log):
    log.info("Absolute Duplicate Check Started!!!")
    columnList = sourceData.columns
    resultData = sourceData.groupBy(columnList).count().filter("count>1")
    if(resultData.count() > 0):
        log.error("Absolute Duplicate records found, data placed at: "+config['test']['absoluteDuplicateCheckFileName'])
        writeToFile(resultData, config['test']['absoluteDuplicateCheckFileName'], 'csv')
    log.info("Absolute Duplicate Check Completed!!!")


def duplicateCheck(sourceData, config, log):
    log.info("Duplicate Check Started!!!")
    inputColumns = config['test']['duplicateCheckColumns']
    columnsList = inputColumns.split(",")
    resultData = sourceData.groupBy(columnsList).count().filter("count>1")
    if (resultData.count() > 0):
        log.error("Duplicate records found, data placed at: " + config['test']['duplicateCheckFileName'])
        writeToFile(resultData, config['test']['duplicateCheckFileName'], 'csv')
    log.info("Duplicate Check Completed!!!")

def nullCheck(sourceData, config, log):
    log.info("Null Check Started!!!")
    columnsList = sourceData.columns
    row_count= sourceData.count()
    for column in columnsList:
        # if(sourceData.count() == sourceData.where(sourceData[x].isNull()).count()):
        # if(sourceData.filter( (length(col(x))==0) & (col(x).isNull()) ).count() == sourceData.count() ):
        columnNullCount = sourceData.filter((length(col(column)).isNull())).count()
        if (columnNullCount == row_count):
            log.error(column + " is null")
        elif config['test']['nullCheck'] and columnNullCount > 0:
            log.error(column + " contains " + columnNullCount + " null records")

    log.info("Null Check Completed!!!")

def alphaNumericCheck(sourceData, config, log):
    log.info("Alphanumeric Check Started!!!")
    inputColumns = config['test']['alphaNumericCheckColumns']
    columnsList = inputColumns.split(",")
    for column in columnsList :
        alphaNumericColumnData = sourceData.filter(length(regexp_replace(col(column), "\d+", "")) > 0)
        if alphaNumericColumnData.count() > 0:
            log.error(column + " contains non numeric data")
    log.info("Alphanumeric Check Completed!!!")

def dateFormatCheck(sourceData, config, log):
    log.info("Date Format Check Started!!!")
    inputColumns = config['test']['dateFormatCheckColumns']
    columnsList = inputColumns.split(",")
    for column in columnsList:
        dateFormatColumnData = sourceData.filter(split(col(column), '-')[0] > 12)
        if dateFormatColumnData.count() > 0:
            log.error(column + " has data which is not in format MM-DD-YYYY format")
    log.info("Date Format Check Completed!!!")


if __name__ =='__main__':
    configFileName = sys.argv[1]
    #print(configFileName)
    config = getConfig(configFileName)
    log = getLogger(config['log']['fileName'])
    log.info("**************************************Start*************************************************************")
    log.info("Data Sanity Check Started, executed by: "+os.getlogin())
    log.info("Query: "+config["input"]["query"])
    spark = getSparkSession()
    sourceData = getSource(spark, log, config)

    if config['test']['absoluteNullCheck']:
        nullCheck(sourceData, config, log)

    if config['test']['duplicateCheck']:
        duplicateCheck(sourceData, config, log)

    if config['test']['absoluteDuplicateCheck']:
        absoluteDuplicate(sourceData, config, log)

    if config['test']['alphaNumericCheck']:
        alphaNumericCheck(sourceData, config, log)

    if config['test']['dateFormatCheck']:
        dateFormatCheck(sourceData, config, log)

    log.info("Data Sanity check completed")
    log.info("#######################################END###############################################################")
