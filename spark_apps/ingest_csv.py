from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("Hello Spark")\
        .getOrCreate()
    print("Hello from pyspark")
    sample_data = [{"name": "John    D.", "age": 30},
        {"name": "Alice   G.", "age": 25},
        {"name": "Bob  T.", "age": 35},
        {"name": "Eve   A.", "age": 28}]

    df = spark.createDataFrame(sample_data)
    df.show()
    df.printSchema()
    spark.stop()