#%%
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

#%%
# Read tax reporting excel files 
tax_report_cols = ['CoCode', 'FIDocNo', 'CustomerNo', 'PostingDate', 'TxRptgDate', 'DocDate', 'DocType', 'InvoiceNo', 
                   'TaxCode', 'TaxRate', 'GLAccount', 'Currency', 'DocNetValue', 'TaxDocValue', 'ExchRate', 'NetLC', 
                   'LCTax', 'LocalCurrency', 'CustomerID', 'CustomerName', 'Plant', 'ShipFrom', 'ShipTo', 'CountrySR', 
                   'ServiceRend', 'DeliveryNote', 'CustomerTaxClassification', 'MaterialTaxClassification']
text_columns = [
    "CoCode", "FIDocNo", "CustomerNo", "DocType", "InvoiceNo", "TaxCode", 
    "GLAccount", "Currency", "LocalCurrency", "Plant", "ShipFrom", "ShipTo", 
    "CountrySR", "ServiceRend", "DeliveryNote", "CustomerTaxClassification", "MaterialTaxClassification"
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
tax_report_pd["ExchRate"] = tax_report_pd["ExchRate"].apply(lambda x: str(x).strip().replace('/', ''))
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

# drop the unused cols from main table
tax_report_pd = tax_report_pd.drop(columns=["CustomerName"])

#%% Tax Report data upload
def upload_date(mysql_conn, df, *target_table_info):
    result = mysql_conn.insert_dataframe_to_sql(df, *target_table_info)
    return result

def worker_process(q):
    process_name = current_process().name
    print(f"{process_name} started")

    customer_code_db_info = ('Customer', ['CustomerID'])
    tax_report_db_info = ('TaxReporting', ['FIDocNo', 'DocNetValue'])
    # upload_date(mysql_conn, customer_code_unique, *customer_code_db_info)
    upload_date(mysql_conn, tax_report_pd, *tax_report_db_info)
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