#%%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, regexp_replace
import pandas as pd
import os
from dotenv import load_dotenv
from utils import MySQLUtils
import numpy as np
from multiprocessing import Process, current_process, Queue

# Connect to database
load_dotenv()
mysql_conn = MySQLUtils(
    driver = os.getenv("DRIVER_NAME"),
    db_user = os.getenv("DB_USERNAME"),
    db_password = os.getenv("DB_PASSWORD"),
    host = os.getenv("HOST"),
    port = os.getenv("PORT"),
    given_db = os.getenv("DATABASE")
)
mysql_conn.connect_to_mysql()

#%% using pyspark to process the data 
# Initialize Spark session with
spark = SparkSession.builder \
    .appName("MySQL Data Insertion") \
    .master("local[*]") \
    .getOrCreate()

# Read DMOP Excel files using pandas
dmop_cols = ["CompanyCode", "DocumentNo", "SAPInoviceNo", "SAPBillingDate", "SAPPositingDate", 
    "SAPTaxCode", "SAPOrderNo", "SAPDispatchNoteNo", "ProductLine", "SAPStorageLocation", "SAPRoute", 
    "Plant", "SAPCustomerTaxClassification", "SAPMaterialTaxClassification", "SAPIncoterm", "ShipToName", 
    "ShipToCountry", "CountryKey", "SoldToName", "CurrencyCode", "UM_ST", "UM_EURO", "UM_WAE", 
    "UM_TAX", "UM_TAX_WAE", "SAPTaxDepartCountry", "CostBaseSGD","GST_VAT_SGD"]

dmop_pd = pd.read_excel("./asset/DMOP_IFAP_DEC24.xlsx",
                        usecols='B,E:F,H:K,M,Q:W,Y:AA,AD,AF:AK,AN:AP')
dmop_pd = dmop_pd.set_axis(dmop_cols, axis=1)

# Convert pandas DataFrames to Spark DataFrames
dmop_spark = spark.createDataFrame(dmop_pd)

# step 1: aggregate the values
dmop_spark_agg = dmop_spark.groupBy("DocumentNo").agg(
    sum("UM_EURO").alias("UM_EURO"), 
    sum("UM_WAE").alias('UM_WAE'), 
    sum("UM_TAX").alias("UM_TAX"), 
    sum("UM_TAX_WAE").alias("UM_TAX_WAE")
)

# step 2: join back rest columns to the aggregated data frame to ensure DocumentNo is unique
dmop_spark_drop = dmop_spark.drop("UM_EURO", "UM_WAE", "UM_TAX", "UM_TAX_WAE", "CountryKey")
dmop_spark_unique = dmop_spark_agg \
    .join(dmop_spark_drop, on="DocumentNo", how="left") \
    .replace(float('nan'), None) \
    .withColumn("SoldToName", regexp_replace("SoldToName", "'", "-")) \
    .withColumn("ShipToName", regexp_replace("ShipToName", "'", "-")) \

# step 3: seperate the countryCode table from the main table
# get columns for countryCode table
dmop_country_code = dmop_spark.select(col("ShipToCountry").alias('CountryName'), col("CountryKey").alias("CountryKey"))
# Remove duplicates based on 'ShipToCountry'
dmop_country_code_no_duplicates = dmop_country_code.dropDuplicates(["CountryName"])

# convert spark df back to pandas df, prepare for data upload
dmop_spark_unique_df = dmop_spark_unique.toPandas()
dmop_spark_unique_df = dmop_spark_unique_df.replace(['nan', np.nan], "null")
dmop_country_code_no_duplicates_df = dmop_country_code_no_duplicates.toPandas()
spark.stop()

#%% DMOP data upload
def upload_date(mysql_conn, df, *target_table_info):
    result = mysql_conn.insert_dataframe_to_sql(df, *target_table_info)
    return result

def worker_process(q):
    process_name = current_process().name
    print(f"{process_name} started")

    country_code_db_info = ('Country', ['CountryName']) #(table_name, primary key list)
    dmop_main_table_info = ('DMOP', ['DocumentNo'])
    upload_date(mysql_conn, dmop_country_code_no_duplicates_df, *country_code_db_info)
    upload_date(mysql_conn, dmop_spark_unique_df, *dmop_main_table_info)
    q.put(f"{process_name} finished")

def main():
    q = Queue()
    processes = []

    for _ in range(4):  # create 4 processes
        p = Process(target=worker_process, args=(q,))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    while not q.empty():
        print(q.get())

if __name__ == "__main__":
    main()