import os
import sys
dirname = os.getcwd()
if dirname + '/src' not in sys.path:
    sys.path.append(dirname + '/src')
import argparse
from logging import exception
import re
import shutil
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from core import AwsOperations
from core import AppConfig

class RevenueAnalyzer:
  """
  Class which contains logic to analyze revenue of a client based on
  search engines, search key words for a given client's data file
  """
  def __init__(self):
    self.output = []
    self.client_name = "esshopzilla"
    self.spark = SparkSession.builder.getOrCreate()
    self.configuration = AppConfig.AppConfig()
    self.config_data = self.configuration.load_configuration()
    self.aws_ops_obj = AwsOperations.AwsOperations()
    self.input_file_path_local = None
    self.output_file_path_local = None

  def get_search_engine_name(self, url:str) -> str:
    """
    This module is used to extract search engine name given the referrer URL
    """
    domain_name = re.search("\/\/.*\/", url ).group()
    search_engine = domain_name.split('.')[1]
    return search_engine

  def get_search_key_word(self, url:str) -> str:
    """
    This module is used to derive the search key word given the URL
    """
    search_query = re.search("[&|?][p|q]=(\w|\+|%20)*&", url).group()
    search_keyword = search_query.split('=')[1]
    space_delimited_search_keyword = re.sub('\+|%20|&', ' ',  search_keyword).strip()
    return space_delimited_search_keyword

  def get_revenue(self, product_list):
    """
    This is used to derive revenue given the product list
    """
    products = product_list.split(',')
    revenue = 0
    for product in products:
      revenue_per_product = product.split(';')[3]
      if (revenue_per_product != ''):
        revenue += int(revenue_per_product)
    return revenue


  def create_base_df(self,row):
    """
    This module is used to construct a base dataframe which contains data
    related to external search engines, their search keywords and
    revenue of the product
    """
    space_delimited_search_keyword = ''
    search_engine = ''
    revenue = 0
    if (row["pagename"] == "Order Complete"):
      search_engine = self.get_search_engine_name(row["referrer"])
      if search_engine != self.client_name:
        space_delimited_search_keyword = self.get_search_key_word(row["referrer"])
        revenue = self.get_revenue(row["product_list"])
        output_row = (search_engine.lower(), space_delimited_search_keyword.lower(), revenue)
        self.output.append(output_row)

  def create_final_df(self, df):
    """
    This module is used to create final data frame as per business requirements
    """
    #creating a temp table "revenue_data"
    df.createOrReplaceTempView("revenue_data")
    #Constructing a query to get revenue based on search keyword
    query = '''SELECT search_engine_domain, search_keyword, sum(revenue) as Revenue  from revenue_data
                group by search_engine_domain, search_keyword order by Revenue desc'''
    revenue_result = self.spark.sql(query)
    revenue_result = revenue_result.withColumn("Revenue", revenue_result["Revenue"].cast(DecimalType()))
    revenue_result = revenue_result.toDF(*("Search Engine Domain", "Search Keyword", "Revenue"))
    return revenue_result
  
  def output_result_to_s3(self, revenue_result):
    """
    This module takes input a df, creates a file and uploads it to AWS S3 
    """
    revenue_result.write.options(header=True, delimiter="\t").csv(self.config_data['dev']['temp_local_output_location_part_files'], mode='overwrite')
    listFiles = os.listdir(self.config_data['dev']['temp_local_output_location_part_files'])
    print(listFiles)
    for subFiles in listFiles:
      if subFiles[-4:] == ".csv":
        utc_date = datetime.utcnow().strftime("%Y-%m-%d")
        self.output_file_path_local = self.config_data['dev']['temp_local_output_location_final_output']+ '/' + str(utc_date) + '_' + self.config_data['dev']['output_file_name']
        shutil.copyfile(self.config_data['dev']['temp_local_output_location_part_files']+'/'+subFiles, self.output_file_path_local)
        print(os.listdir(self.config_data['dev']['temp_local_output_location_final_output']))
        output_file_name = str(utc_date) + '_' + self.config_data['dev']['output_file_name']
        self.aws_ops_obj.get_put_file_s3(source_filepath=self.output_file_path_local, dest_file_path=self.config_data["dev"]["s3_output_location"] + output_file_name)
        self.clean_local_files(self.output_file_path_local)
      
  
  def clean_local_files(self, file_path: str):
    """
    This module removes files locally after execution
    """
    if os.path.exists(file_path):
      try:
        os.remove(file_path)
        print(f"Deleted file {file_path}")
      except OSError as e:
        print("Error: %s - %s." % (e.filename,e.strerror))
    else:
      print(f"Cannot find file in {file_path}")
  
  def create_input_df(self, file_path: str):
    """
    This module take input a file path, derives necessary fields required for computation into DF
    """
    if(self.aws_ops_obj.validate_s3_file_path(file_path)):
      str_lst = file_path.split('/')
      file_list = str_lst[-1:]
      file_name = file_list[0]
      self.aws_ops_obj.get_put_file_s3(source_filepath=file_path, dest_file_path=self.config_data['dev']['source_file_local_path'])
      # Reading the file
      self.input_file_path_local = self.config_data['dev']['source_file_local_path'] + '/' + file_name
      temp_input_df = self.spark.read.options(header='True', Inferschema=True, delimiter='\t') \
          .csv(self.input_file_path_local)
      data_collect = temp_input_df.collect()
      output_columns = ["search_engine_domain", "search_keyword", "revenue"]
      for row in data_collect:
        self.create_base_df(row)
      final_input_df = self.spark.createDataFrame(self.output, output_columns)
      return final_input_df
    else:
      print("Not valid file path")
      self.spark.stop()


  def main(self, input_file_path=None):
    """
    Main function which calculates and outputs the revenue file
    """
    # Checking if the given s3 path is valid
    try:
      final_input_df = self.create_input_df(file_path=input_file_path)
      revenue_result = self.create_final_df(final_input_df)
      self.output_result_to_s3(revenue_result)

      # Deleting the input file in local
      self.clean_local_files(self.input_file_path_local)

    except exception as e:
      print(e)

    finally:
      # Ending spark job
      print("Ending spark session")
      self.spark.stop()

      
  
if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description='Enter file path')
    arg_parser.add_argument('file_path', help='S3 filename')
    args = arg_parser.parse_args()
    file_path = args.file_path
    obj = RevenueAnalyzer()
    obj.main(file_path)
