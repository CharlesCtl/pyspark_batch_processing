from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import get_spark_app_config

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
        
    """
    print("Hello from pyspark")
    sample_data = [{"name": "John    D.", "age": 30},
        {"name": "Alice   G.", "age": 25},
        {"name": "Bob  T.", "age": 35},
        {"name": "Eve   A.", "age": 28}]
    
    df = spark.createDataFrame(sample_data)
    df.show()
    df.printSchema()
    """
    logger = Log4j(spark)
    logger.info("It works from Hello Spark!!!")

    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())

    logger.info("Finished Hello Spark")
    spark.stop()