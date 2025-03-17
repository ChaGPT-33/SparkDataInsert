#%%
import pandas as pd
import os
from dotenv import load_dotenv
from utils import MySQLUtils
import numpy as np

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

#%%
# Read tax reporting excel files 
tax_report_cols = ['CoCode', 'FIDocNo', 'CustomerNo', 'PostingDate', 'TxRptgDate', 'DocDate', 'DocType', 'InvoiceNo', 
                   'TaxCode', 'TaxRate', 'GLAccount', 'Currency', 'DocNetValue', 'TaxDocValue', 'ExchRate', 'NetLC', 
                   'LCTax', 'LocalCurrency', 'CustomerID', 'CustomerName', 'Plant', 'ShipFrom', 'ShipTo', 'CountrySR', 
                   'ServiceRend', 'DeliveryNote', 'CustomerTaxClassification', 'FIDocNo']
text_columns = [
    "CoCode", "FIDocNo", "CustomerNo", "DocType", "InvoiceNo", "TaxCode", 
    "GLAccount", "Currency", "LocalCurrency", "Plant", "ShipFrom", "ShipTo", 
    "CountrySR", "ServiceRend", "DeliveryNote", "CustomerTaxClassification"
]
decimal_columns = ["DocNetValue", "TaxDocValue", "NetLC", "LCTax"]


tax_report_pd = pd.read_excel('./asset/IFAP-PIO-DEC24(1-5).xls',
                              usecols='A:B,D,F:M,O:AA,AD:AE,AG,AI')
tax_report_pd = tax_report_pd.set_axis(tax_report_cols, axis=1)
tax_report_pd[text_columns].astype(str)
tax_report_pd[decimal_columns].astype(float)

# data processing
# step 1: remove the '/' at currency rate column, remove "'" in customerName column
tax_report_pd["TaxRate"] = tax_report_pd["TaxRate"].apply(lambda x: str(x).strip().replace('/', ''))
tax_report_pd["CustomerName"] = tax_report_pd["CustomerName"].apply(lambda x: str(x).strip().replace("'", '-'))
tax_report_pd["CustomerID"] = tax_report_pd["CustomerID"].apply(lambda x: str(x).strip().replace(".0", ''))

# step 2: standardize date format
date_columns = ["PostingDate", "TxRptgDate", "DocDate"]
for col in date_columns:
    tax_report_pd[col] = pd.to_datetime(tax_report_pd[col], format='%d.%m.%Y').dt.strftime('%Y-%m-%d')

# step 3: replace null value
tax_report_pd = tax_report_pd.replace(['nan', np.nan], "null")

# select out customerNo and customerName for 'Customer' table
customer_code = tax_report_pd[['CustomerID', 'CustomerName']]
customer_code_unique = customer_code.drop_duplicates(subset=['CustomerID'])

#%% Tax Report data upload
customer_code_db_info = ('Customer', ['CustomerID'])
tax_report_db_info = ('TaxReporting', ['FIDocNo', 'DocNetValue'])

customer_code_table_result = mysql_conn.insert_dataframe_to_sql(customer_code_unique, *customer_code_db_info)
print(customer_code_table_result)

# tax_reporting_table_result = mysql_conn.insert_dataframe_to_sql(tax_report_pd, *tax_report_db_info)
# print(tax_reporting_table_result)