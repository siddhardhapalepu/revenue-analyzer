import argparse
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import *

class RevenueAnalyzer:
  """
  Class which contains logic to analyze revenue given a client's data file
  """
  def __init__(self):
    self.output = []
    self.client_name = "esshopzilla"

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
    define
    """
  
  def output_result_to_file(self, revenue_result):
    """
    define 
    """
    
    #revenue_result.write.options(header=True, delimiter="\t").csv("C:\\Users\\swsee\\Documents\\Projects\\revenue-analyzer\\data\\output.tsv")
    #revenue_result.write.csv("C:\\Users\\swsee\\Documents\\Projects\\revenue-analyzer\\data\\output.csv")

  def main(self):
    """
    Main function which calculates and outputs the revenue file
    """
    spark = SparkSession.builder.getOrCreate()
    input_df = spark.read.options(header='True', Inferschema=True, delimiter='\t') \
        .csv(r"C:\Users\swsee\Documents\Projects\revenue-analyzer\data\data_1.tsv")
    data_collect = input_df.collect()
    output_columns = ["search_engine_domain", "search_keyword", "revenue"]
    # output_schema = StructType([
    #     StructField("search_engine_domain", StringType(), False),
    #     StructField("search_keyword", StringType(), False),
    #     StructField("revenue", IntegerType(), True)
    # ])
    #output_df = spark.createDataFrame(data=[], schema=output_schema)
    for row in data_collect:
      self.create_base_df(row)

    new_df = spark.createDataFrame(self.output, output_columns)
   # new_df = output_df.union(newDf)
    new_df.show()
    
    #creating a temp table "revenue_data"
    new_df.createOrReplaceTempView("revenue_data")
    #Constructing a query to get revenue based on search keyword
    query = '''SELECT search_engine_domain, search_keyword, sum(revenue) as Revenue  from revenue_data
                group by search_engine_domain, search_keyword order by Revenue desc'''
    revenue_result = spark.sql(query)
    revenue_result.show()
    self.output_result_to_file(revenue_result)
    

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description='Enter file path')
    arg_parser.add_argument(
        'File path', help='Path of the data file to be processed')
    obj = RevenueAnalyzer()
    obj.main()
    # implement sanitizer for path
