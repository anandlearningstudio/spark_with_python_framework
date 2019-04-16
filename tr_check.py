__auth__= "Bibhuti Anand and Ashrumochan Behera"
__copyright__ = "Tetrasoft Inc."
__version__ = "1.0"
#Date : 03-April-2019
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import logging
import os
import json
import sys


def getConfig(fileName):
    with open(fileName) as f:
        data = json.load(f)
    return data

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

def getTestSource(spark, log, config):

    if config['source']["isDirectHive"] :
        log.info("The Source table is Hive.")
        query = spark.sparkContext.wholeTextFiles(config['source']["queryFile"]).take(2)[0][1]
        sourceData = spark.sql(query)

    elif config["source"]["isdatabase"]:
        log.info("The Source is a database")
        sourceData = spark.read.format("jdbc") \
            .option("url", config['source']['databaseUrl']) \
            .option("driver", config['source']['driverName']) \
            .option("dbtable", config['source']['query']) \
            .option("user", config['source']['databaseUserName']) \
            .option("password", config['source']['databasePassword']) \
            .load()
    elif config["source"]["isFile"]:
        log.info("Source is file.")
        sourceData = spark.read.format("csv") \
            .option("header", config['source']['fileWithSchema']) \
            .load(config['source']['fileName'])
    return sourceData


def getProdSource(spark, log, config):

    if config['target']["isDirectHive"]:
        log.info("The target table is Hive.")
        query=spark.sparkContext.wholeTextFiles(config['target']["queryFile"]).take(2)[0][1]
        targetData = spark.sql(query)

    elif config["target"]["isdatabase"]:
        log.info("The target is a database")
        targetData = spark.read.format("jdbc") \
            .option("url", config['target']['databaseUrl']) \
            .option("driver", config['target']['driverName']) \
            .option("dbtable", config['target']['query']) \
            .option("user", config['target']['databaseUserName']) \
            .option("password", config['target']['databasePassword']) \
            .load()
    elif config["target"]["isFile"]:
        log.info("Target is file.")
        targetData = spark.read.format("csv") \
            .option("header", config['target']['fileWithSchema']) \
            .load(config['target']['fileName'])
    return targetData

def getsparkContext():
    #conf = SparkConf().setAppName("Transformation_check").setMaster("yarn-client").set("spark.yarn.queue","cdl_yarn").set("spark.executor.instances","50").set("spark.executor.cores","4")
    #conf = SparkConf().setAppName("Transformation_Check").setMaster("yarn-client")
    #sc = SparkContext(conf=conf)
    #spark = SQLContext(sc)
    spark = SparkSession.Builder().master("yarn-client").appName("Data Validation").enableHiveSupport().getOrCreate()
    return spark

def writeToFile(dataFrame, path, format):
    dataFrame.write.save(path=path,format=format,mode='overwrite')

def schemaComparisonWithConstraints(sourceData, targetData, log):

    log.info("Schema comparision Started!!!")

    if sourceData.schema == targetData.schema:
        log.info("Schema comparison Successful.")
    else:
        log.error("Schema comparison Unsuccessful.")
        log.error(sourceData.schema.simpleString())
        log.error(targetData.schema.simpleString())
        log.error("Schema of Test dataFrame doesn't matches with the prod dataFrame. ")

    log.info("Schema comparision Completed!!!")

def schemaComparison(sourceData, targetData, log):

    log.info("Schema comparision Started!!!")

    if sourceData.dtypes == targetData.dtypes:
        log.info("Schema comparison Successful.")
    else:
        log.error("Schema comparison Unsuccessful.")
        log.error("Source DataFrame: ",sourceData.dtypes)
        log.error("Target DataFrame: ",targetData.dtypes)
        log.error("Schema of Test dataFrame doesn't matches with the prod dataFrame. ")

    log.info("Schema comparision Completed!!!")

def rowComparison(sourceData, targetData, log):

    log.info("Row count comparision Started!!!")

    sourceDataCount = sourceData.count()
    targetDataCount = targetData.count()
    log.info("Test Data Count: ", sourceDataCount)
    log.info("Prod Data Count: ", targetDataCount)

    if sourceDataCount == 0:
        log.error("Row count Unsuccessful.")
        log.error("Zero rows found in Test")
    elif sourceDataCount == targetDataCount:
        log.info("Row count Successful.")
    else:
        log.error("Row count Unsuccessful.")
        log.error("Row count Difference: ", (sourceDataCount - targetDataCount))
        log.error("Row count of source dataFrame doesn't match with the target dataFrame.")

    log.info("Row count comparision Completed!!!")


def dataComparison(sourceData, targetData, log, config):

    log.info("Data comparision Started!!!")

    resultData = sourceData.subtract(targetData)
    log.info(resultData.count())
    #resultData = resultData.unionAll(targetData.subtract(sourceData))
    #log.info(resultData.count())

    if resultData.count() == 0:
        log.info("Data comparison successful.")
    else:
        log.error("Data comparison Unsuccessful.")
        writeToFile(resultData,config["test"]["dataCompareFileName"],"csv")
        log.error("There is a mismatch in the data between source and target, data placed at: "+config["test"]["dataCompareFileName"])

    log.info("Data comparision Started!!!")

if __name__ == "__main__":

    config = getConfig(sys.argv[1])
    path = config["log"]["fileName"]
    log =  getLogger(path)
    log.info("**************************************Start*************************************************************")
    log.info("Transformation check started.")
    log.info("User logged in is: " + os.getlogin())
    spark = getsparkContext()
    #spark = getSparkSession()
    sourceData = getTestSource(spark, log, config)
    targetData = getProdSource(spark, log, config)

    if config['test']['schemaCompareWithConstraints']:
        schemaComparisonWithConstraints(sourceData, targetData, log)

    if config['test']['schemaCompare']:
        schemaComparison(sourceData, targetData, log)

    if config['test']['rowCountCompare']:
        rowComparison(sourceData, targetData, log)

    if config['test']['dataCompare']:
        dataComparison(sourceData, targetData, log, config)

    log.info("Transformation check completed.")
    log.info("#######################################END###############################################################")