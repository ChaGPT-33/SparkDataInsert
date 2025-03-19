#%%
import pandas as pd
import os
from dotenv import load_dotenv
from utils import MySQLUtils
import numpy as np
from multiprocessing import Pool, freeze_support
#%% using pandas to process the data 
def load_file(file_path):
    # Read DMOP Excel files using pandas
    dmop_cols = ["CompanyCode", "DocumentNo", "SAPInoviceNo", "SAPBillingDate", "SAPPositingDate", 
        "SAPTaxCode", "SAPOrderNo", "SAPDispatchNoteNo", "ProductLine", "SAPStorageLocation", "SAPRoute", 
        "Plant", "SAPCustomerTaxClassification", "SAPMaterialTaxClassification", "SAPIncoterm", "ShipToName", 
        "ShipToCountry", "CountryKey", "SoldToName", "CurrencyCode", "UM_ST", "UM_EURO", "UM_WAE", 
        "UM_TAX", "UM_TAX_WAE", "SAPTaxDepartCountry", "CostBaseSGD","GST_VAT_SGD"]

    dmop_pd = pd.read_excel(file_path, usecols='B,E:F,H:K,M,Q:W,Y:AA,AD,AF:AK,AN:AP')
    dmop_pd = dmop_pd.set_axis(dmop_cols, axis=1)

    # step 1: aggregate the values
    dmop_agg = dmop_pd.groupby("DocumentNo", as_index=False).agg({
            "UM_EURO": "sum",
            "UM_WAE": "sum",
            "UM_TAX": "sum",
            "UM_TAX_WAE": "sum"
        }).rename(columns={
            "UM_EURO": "UM_EURO",
                "UM_WAE": "UM_WAE",
                "UM_TAX": "UM_TAX",
                "UM_TAX_WAE": "UM_TAX_WAE"
            })

    # step 2: join back rest columns to the aggregated data frame to ensure DocumentNo is unique
    dmop_drop = dmop_pd.drop(columns=["UM_EURO", "UM_WAE", "UM_TAX", "UM_TAX_WAE"])
    dmop_unique = pd.merge(dmop_agg, dmop_drop, on="DocumentNo", how="left").drop_duplicates(subset=['DocumentNo'])

    # step 3: Replace NaN and clean text columns
    dmop_unique = dmop_unique.replace(['nan', np.nan], "null")
    dmop_unique["SoldToName"] = dmop_unique["SoldToName"].str.replace("'", "-")
    dmop_unique["ShipToName"] = dmop_unique["ShipToName"].str.replace("'", "-")

    # step 4: seperate the countryCode table from the main table
    # get columns for countryCode table
    dmop_country_code = dmop_unique[["ShipToCountry", "CountryKey"]].rename(
        columns={"ShipToCountry": "CountryName", "CountryKey": "CountryKey"}
    ).drop_duplicates()
    dmop_unique = dmop_unique.drop(columns=['CountryKey'])
    return dmop_unique, dmop_country_code

#%% DMOP data upload
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
    country_code_db_info = ('Country', ['CountryName']) #(table_name, primary key list)
    dmop_main_table_info = ('DMOP', ['DocumentNo'])

    # load folder
    dmop_folder = './asset/DMOP'
    for file_name in os.listdir(dmop_folder):
        file_path = os.path.join(dmop_folder, file_name)

        dmop_main_table_df, country_code_df = load_file(file_path)
        upload_data(country_code_df, *country_code_db_info) # no need to use multi-thread as the data size always small for this

        dmop_chunks = np.array_split(dmop_main_table_df, num_processes)
        freeze_support()
        args = [(chunk, *dmop_main_table_info) for chunk in dmop_chunks]
        with Pool(num_processes) as pool:
            pool.map(upload_data_wrapper, args)


if __name__ == "__main__":
    main()