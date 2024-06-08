import sys
from pyspark.sql import *
from pyspark.sql.functions import col,current_timestamp,to_timestamp,concat,lit,spark_partition_id
from lib.logger import Log4j
from lib.utils import get_spark_app_config,load_races_df


if __name__ == "__main__":
    conf = get_spark_app_config()
    warehouse_location = 'spark-warehouse'
#Enable HIVE support to allow connectivity to a persistent Hive Metastore
    spark = SparkSession.builder \
        .config(conf=conf) \
        .appName("SparkSQLTableDemo")\
        .config("spark.sql.warehouse.dir", warehouse_location)\
        .enableHiveSupport() \
        .getOrCreate()
    
    logger = Log4j(spark)
    logger.info("¡¡¡ STARTING APP  !!!")

    parquetFile_df = spark.read.parquet("/opt/spark/data/processed/circuits")
    
    spark.sql("CREATE DATABASE IF NOT EXISTS CIRCUITS_DB")
    spark.catalog.setCurrentDatabase("CIRCUITS_DB")

    parquetFile_df.write \
    .mode("overwrite")\
    .saveAsTable("circuits_table")

    logger.info(spark.catalog.listTables("CIRCUITS_DB"))
    #---------------------------------
    logger.info("¡¡¡ FINISHING APP !!!")
    spark.stop() 