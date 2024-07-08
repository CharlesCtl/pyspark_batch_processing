import sys
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import get_spark_app_config,load_results_df,transform_results_df
def ingest_and_transform_results(spark,file_path):
    results_df = load_results_df(spark,file_path)  
    results_final_df = transform_results_df(results_df)
    results_final_df.write.mode("overwrite").parquet("/opt/spark/data/processed/results")
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
    ingest_and_transform_results(spark,sys.argv[1])
    #This is used to print conf parameters
    #conf_out = spark.sparkContext.getConf()
    #logger.info(conf_out.toDebugString())
    #---------------------------------
    logger.info("Finished Hello Spark")
    spark.stop()