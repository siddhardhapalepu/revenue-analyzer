import argparse
from email.policy import strict
from logging import exception
import re
import os
import json
import shutil
import datetime
from statistics import mode
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from core import AwsOperations
from core import AppConfig

class RevenueAnalyzer:
  """
  Class which contains logic to analyze revenue given a client's data file
  """
  def __init__(self):
    self.output = []
    self.client_name = "esshopzilla"
    self.spark = SparkSession.builder.getOrCreate()
    self.configuration = AppConfig.AppConfig()
    self.config_data = self.configuration.load_configuration()

  def get_search_engine_name(self, url):
    """
    This is used to derive search engine name given the URL
    """
    domain_name = re.search("\/\/.*\/", url ).group()
    search_engine = domain_name.split('.')[1]
    return search_engine

  def get_search_key_word(self, url):
    """
    This is used to derive the search key word given the URL
    """
    search_query = re.search("[&|?][p|q]=(\w|\+|%20)*&", url).group()
    search_keyword = search_query.split('=')[1]
    space_delimited_search_keyword = re.sub('\+|%20|&', ' ',  search_keyword)
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
    This is used to create a base dataframe which contains data
    related to external search engines, their search keywords and
    revenue of the product bought
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
    This module is used to create final data frame which will be then converted into output file
    Input:
    Output:
    """
    #creating a temp table "revenue_data"
    df.createOrReplaceTempView("revenue_data")
    #Constructing a query to get revenue based on search keyword
    query = '''SELECT search_engine_domain, search_keyword, sum(revenue) as Revenue  from revenue_data
                group by search_engine_domain, search_keyword order by Revenue desc'''
    revenue_result = self.spark.sql(query)
    return revenue_result
  
  def output_result_to_file(self, revenue_result):
    """
    define 
    """
    revenue_result.write.options(header=True, delimiter="\t").csv(self.config_data['dev']['temp_local_output_location_part_files'], mode='overwrite')
    listFiles = os.listdir(self.config_data['dev']['temp_local_output_location_part_files'])
    print(listFiles)
    for subFiles in listFiles:
      if subFiles[-4:] == ".csv":
        current_date = datetime.date.today()
        shutil.copyfile(self.config_data['dev']['temp_local_output_location_part_files']+'/'+subFiles, self.config_data['dev']['temp_local_output_location_final_output']+ '/' + str(current_date) + '_' + self.config_data['dev']['output_file_name'])
        print(os.listdir(self.config_data['dev']['temp_local_output_location_final_output']))
    

  def main(self, input_file_path=None):
    """
    Main function which calculates and outputs the revenue file
    """
    aws_ops_obj = AwsOperations.AwsOperations()
    return_value = aws_ops_obj.validate_s3_file_path(input_file_path)
    print(return_value)
    if(aws_ops_obj.validate_s3_file_path(input_file_path)):
      str_lst = input_file_path.split('/')
      file_list = str_lst[-1:]
      file_name = file_list[0]
      aws_ops_obj.get_put_file_s3(source_filepath=input_file_path, dest_file_path=self.config_data['dev']['/home/ubuntu/input'])
      input_df = self.spark.read.options(header='True', Inferschema=True, delimiter='\t') \
          .csv(self.config_data['dev']['/home/ubuntu/input'] + '/' + file_name)
      data_collect = input_df.collect()
      output_columns = ["search_engine_domain", "search_keyword", "revenue"]
      for row in data_collect:
        self.create_base_df(row)
      new_df = self.spark.createDataFrame(self.output, output_columns)
      #new_df.show()
      revenue_result = self.create_final_df(new_df)
      self.output_result_to_file(revenue_result)
    else:
      print("Not valid file path")
    

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description='Enter file path')
    arg_parser.add_argument('file_name', help='S3 filename')
    #args = arg_parser.parse_args()
    #file = args.file_name
    obj = RevenueAnalyzer()
    obj.main('s3://revenue-analyzer-file-store/input/data_1.tsv')
    # implement sanitizer for path
