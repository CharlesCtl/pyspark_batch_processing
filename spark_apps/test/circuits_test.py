from unittest import TestCase
import unittest
import sys
import datetime
from pyspark.sql import SparkSession
#The pkg lib and test dont see between them because their arent inside a pkg
sys.path.append('/opt/spark/apps/lib')
from utils import load_circuits_df,transform_circuits_df


class RowDemoTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
        .master("local[3]") \
        .appName("Demo Test")\
        .getOrCreate()

        cls.df = load_circuits_df(cls.spark,"/opt/spark/data/circuits.csv")
        cls.test_df = transform_circuits_df(cls.df)
    
    def test_data_type(self):
        rows = self.test_df.collect()
        for row in rows:
            self.assertIsInstance(row["ingestion_date"],datetime.datetime)

if __name__=="__main__":
    unittest.main(verbosity=1)