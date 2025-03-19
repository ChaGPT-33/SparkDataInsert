#%%
import pandas as pd
import os
from dotenv import load_dotenv
from utils import MySQLUtils
import numpy as np
from multiprocessing import Pool, freeze_support
#%%
def load_file(file_path):
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

    # load the file under folder
    tax_report_pd = pd.read_excel(file_path, usecols='A:B,D,F:M,O:AA,AD:AE,AG,AI')
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

    return customer_code_unique, tax_report_pd

#%% Tax Report data upload
# Connect to database
def upload_data(chunk, *target_table_info):
    mysql_conn = MySQLUtils(
        driver = "mysql+mysqlconnector",
        db_user = "", # change to your own 
        db_password = "", # change to your own
        host ="", # change to your own
        port = "3306",
        given_db ="", # change to your own 
    )
    mysql_conn.connect_to_mysql()
    mysql_conn.insert_dataframe_to_sql(chunk, *target_table_info)

def upload_data_wrapper(args):
    return upload_data(*args)

def main():
    num_processes = 4  # Number of parallel processes
    customer_code_db_info = ('Customer', ['CustomerID'])
    tax_report_db_info = ('TaxReporting', ['FIDocNo', 'DocNetValue'])

    # load folder
    tax_report_folder = './asset/TaxReport'
    for file_name in os.listdir(tax_report_folder):
        file_path = os.path.join(tax_report_folder, file_name)

        customer_df, tax_report_df = load_file(file_path)
        upload_data(customer_df, *customer_code_db_info) # no need to use multi-thread as the data size always small for this

        tax_report_chunks = np.array_split(tax_report_df, num_processes)
        freeze_support()
        args = [(chunk, *tax_report_db_info) for chunk in tax_report_chunks]
        with Pool(num_processes) as pool:
            pool.map(upload_data_wrapper, args)

if __name__ == "__main__":
    main()