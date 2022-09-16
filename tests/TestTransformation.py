import os
import sys
dirname = os.getcwd()
if dirname not in sys.path:
    sys.path.append(dirname)
import unittest
import sys
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from src.RevenueAnalyzer import RevenueAnalyzer


class PysparkETLTestCase(unittest.TestCase, RevenueAnalyzer):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder.appName("Unit-tests").getOrCreate()
    
    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_etl_pipeline(self):
       test_obj = RevenueAnalyzer()
       # Preparing output dataframe
       input_df = test_obj.create_input_df(file_path='s3://revenue-analyzer-file-store/input/data_1.tsv')
       output_df = test_obj.create_final_df(input_df)
       print(output_df.show())
       print(output_df.collect())
       output_df.printSchema()

       # Preparing expected dataframe
       expected_schema = StructType([
        StructField('Search Engine Domain', StringType(), True),
        StructField('Search Keyword', StringType(), True),
        StructField('Revenue', DecimalType(), True)
       ])

       expected_data = [("google", "ipod", Decimal(5120)),
                        ("bing", "zune", Decimal(2860))]
       expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
       print(expected_df.show())
       print(expected_df.collect())

       # comparing schema
       schema_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
       schema1 = [*map(schema_list, output_df.schema.fields) ]
       schema2 = [*map(schema_list, expected_df.schema.fields)]
       res = set(schema1) == set(schema2)

       # assert schema
       self.assertTrue(res)
       
       # compare data
       self.assertEqual(sorted(expected_df.collect()), sorted(output_df.collect()))

if __name__=="__main__":
    unittest.main()

       
