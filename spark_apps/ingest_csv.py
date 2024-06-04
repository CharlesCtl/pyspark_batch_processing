from pyspark.sql import *
from lib.logger import Log4j


if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("Hello Spark")\
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

    logger.info("Finished Hello Spark")
    spark.stop()