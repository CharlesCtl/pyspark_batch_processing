import configparser
from pyspark import SparkConf
#import os

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("/opt/spark/apps/spark.conf")
    #print(os.getcwd())
    #print(config.get("SPARK_APP_CONFIGS","spark.app.name"))
    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf