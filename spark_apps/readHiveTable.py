from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import get_spark_app_config


if __name__ == "__main__":
    conf = get_spark_app_config()
    warehouse_location = 'spark-warehouse'
#Enable HIVE support to allow connectivity to a persistent Hive Metastore
    spark = SparkSession.builder \
        .config(conf=conf) \
        .appName("SparkSQL_ReadFromHive")\
        .config("spark.sql.warehouse.dir", warehouse_location)\
        .enableHiveSupport() \
        .getOrCreate()
    
    logger = Log4j(spark)
    logger.info("¡¡¡ STARTING APP  !!!")
    
    spark.catalog.setCurrentDatabase("CIRCUITS_DB")
    spark.sql("Select count(*) from circuits_table").show()

    logger.info(spark.catalog.listTables("CIRCUITS_DB"))
    #---------------------------------
    logger.info("¡¡¡ FINISHING APP !!!")
    spark.stop() 