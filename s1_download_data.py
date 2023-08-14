import pandas as pd
import numpy as np
import datetime as dt
import pymssql
import pyodbc
import tomli
from pprint import pprint as pp

# Customize the display of the table
pd.set_option('chained_assignment',None)
pd.set_option('display.precision', 3)

# ignore warnings
import warnings
warnings.filterwarnings('ignore')


# Get configuration to access database
print("Setting configuration for connecting to SQL Database...")
with open(r"F:\Tùng\Tung\BSC_Config\CONFIG_SQL.toml", mode="rb") as fp:
    config_sql = tomli.load(fp)

# Set up confiq for SQL Server Management
sv = config_sql["sql_acc"]["sv"]
user = config_sql["sql_acc"]["user"]
pwd = config_sql["sql_acc"]["pwd"]
db = config_sql["sql_acc"]["db"]

# Get query from TOML files
print("Creating queries...")
with open(r"F:\Tùng\Tung\BSC_Config\RANKING_FIN_STOCKS_SQL.toml", mode="rb") as fp:
    query_sql = tomli.load(fp)

# Assign query to variables
is_insurance = query_sql["insurance"]["income_statement_insurance"]
bs_insurance = query_sql["insurance"]["balance_sheet_insurance"]
is_securities = query_sql["securties"]["income_statement_securities"]
bs_securities = query_sql["securties"]["balance_sheet_securities"]
list_banks = query_sql["list_banks"]["list_banks"]

query = [is_insurance, bs_insurance, is_securities, bs_securities]

save_csv = [
    r"F:\Tùng\Tung\Python\BSC_DataRankingStocks\cache\is_insurance.csv",
    r"F:\Tùng\Tung\Python\BSC_DataRankingStocks\cache\bs_insurance.csv",
    r"F:\Tùng\Tung\Python\BSC_DataRankingStocks\cache\is_securities.csv",
    r"F:\Tùng\Tung\Python\BSC_DataRankingStocks\cache\bs_securities.csv",
]

# Connect to SQL Server
conn = pymssql.connect(
    server=sv, 
    user=user, 
    password=pwd, 
    database=db
)

# Download data from financial statement: Insurance & Securities
print("Start to download data from financial statement: Insurance & Securities")
for i in range(0,4):
    data = pd.read_sql(
        sql=query[i], 
        con=conn
    )
    data.to_csv(save_csv[i])
    print(f"{i+1}: Downloaded file to {save_csv[i]}")
    

# Download a list of banks
print("Start to download a list of banks")
data = pd.read_sql(list_banks, con=conn)
data[['Symbol', 'CompanyType', 'ICBIndustry']].to_csv(r"F:\Tùng\Tung\Python\BSC_DataRankingStocks\cache\list_banks.csv")

print("Downloaded a list of banks")

# Commit and close connection to SQL database
conn.commit()
conn.close()
print("Finished.")