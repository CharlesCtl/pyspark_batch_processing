import sys
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import get_spark_app_config,load_circuits_df,transform_circuits_df


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
    #This is used to print conf parameters
    #conf_out = spark.sparkContext.getConf()
    #logger.info(conf_out.toDebugString())
    circuits_df = load_circuits_df(spark,sys.argv[1])
    #df.show()
    #df.printSchema()    

    circuits_final_df = transform_circuits_df(circuits_df)

    circuits_final_df.write.mode("overwrite").parquet("/opt/spark/data/processed/circuits")
    
    #---------------------------------
    logger.info("Finished Hello Spark")
    spark.stop()