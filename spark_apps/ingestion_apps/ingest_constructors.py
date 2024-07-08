import sys
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import get_spark_app_config,load_constructors_df,transform_constructors_df

def ingest_and_transform_constructors(spark,file_path):
    constructors_df = load_constructors_df(spark,file_path)
    constructors_final_df = transform_constructors_df(constructors_df)
    constructors_final_df.write.mode("overwrite").parquet("/opt/spark/data/processed/constructors")
    return True

if __name__ == "__main__":
    conf = get_spark_app_config()
    #-------------------------------
    #conf = SparkConf()
    #conf.set("spark.app.name","My spark app")
    #conf.set("spark.master","")
    #--------------------------------
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    logger = Log4j(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: App <filename>")
        sys.exit(-1)
    
    logger.info("Starting App!!!")
    ingest_and_transform_constructors(spark,sys.argv[1])
    #---------------------------------
    logger.info("Finished Hello Spark")
    spark.stop()