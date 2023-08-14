# # Ranking: Banking Sector
""" 
1. Profit: 
    - ROE, 
    - ROA, 
    - NIM
2. Health: 
    - Loan-to-provision; 
    - Loan-to-deposit; 
    - Non-performing Loan; 
    - NPL Coverage """

## 1. Import 
### 1.1 Library
import pandas as pd
import numpy as np
import datetime as dt
import pyodbc
import pymssql
import sys

# ignore warnings
import warnings
warnings.filterwarnings('ignore')

# Customize the display of the table
pd.set_option('chained_assignment', None)

# Get today
now = dt.datetime.today().strftime("%Y%m%d")

### 1.2 Get the result from `ptsp_stock_fundamental_score`
# Get raw final result
conn = pyodbc.connect(
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=V:\iBroker\stock_database.accdb;'
)
df_raw = pd.read_sql('select * from ptsp_stock_fundamental_score', conn)

conn.close()


### 1.3 Process data following new model
df = pd.read_csv(r"F:\Tùng\Tung\Python\BSC_DataRankingStocks\cache\list_banks.csv")
list_banks = df['Symbol']
df_bank = df_raw.loc[df_raw['Symbol'].isin(list_banks)]

# Get data based on selected columns
df_bank = df_bank[['Symbol', 'Year', 'Quarter', 'score_ROE_sector',
       'score_ROA_sector', 'score_NIM_sector', 'score_profit', 'rank_profit', 
       'z_LoanProvisionRatio', 'z_Deposit2Loan', 'z_NPL_ratio_inv',
       'z_NPL_coverage', 'score_health', 'rank_health',
       'score_EPS_above_average', 'score_EPS_growth', 'score_EPS_above_sector',
       'score_EPS_above_group', 'score_growth', 'rank_growth', 'score_PE_5Y',
       'score_PB_5Y', 'score_PE_sector', 'score_PB_sector', 'score_valuation',
       'rank_valuation', 'score_final', 'rank_final', 'Update']]

# Change type of data
df_bank[[
    'score_ROE_sector', 'score_ROA_sector', 'score_NIM_sector', 'score_profit',
    'z_LoanProvisionRatio', 'z_Deposit2Loan', 'z_NPL_ratio_inv',
    'z_NPL_coverage', 'score_health', 'score_EPS_above_average',
    'score_EPS_growth', 'score_EPS_above_sector', 'score_EPS_above_group',
    'score_growth', 'score_PE_5Y', 'score_PB_5Y', 'score_PE_sector',
    'score_PB_sector', 'score_valuation', 'score_final'
]] = df_bank[[
    'score_ROE_sector', 'score_ROA_sector', 'score_NIM_sector', 'score_profit',
    'z_LoanProvisionRatio', 'z_Deposit2Loan', 'z_NPL_ratio_inv',
    'z_NPL_coverage', 'score_health', 'score_EPS_above_average',
    'score_EPS_growth', 'score_EPS_above_sector', 'score_EPS_above_group',
    'score_growth', 'score_PE_5Y', 'score_PB_5Y', 'score_PE_sector',
    'score_PB_sector', 'score_valuation', 'score_final'
]].astype(float)

# Calculate score_profit
df_bank['score_profit'] = round((df_bank['score_ROE_sector'] + df_bank['score_ROA_sector'] + df_bank['score_NIM_sector'])*4/3,2)

# Ranking based on score_profit
for _, items in df_bank.iterrows():
    if items['score_profit'] < 1:
        items['rank_profit'] = 'D'
    elif items['score_profit'] < 2:
        items['rank_profit'] = 'C'
    elif items['score_profit'] < 3:
        items['rank_profit'] = 'B'
    else:
        items['rank_profit'] = 'A'
        
# Calculate score_final
df_bank['score_final'] = round(np.mean(df_bank[['score_profit', 'score_health', 'score_growth', 'score_valuation']], axis=1),2)

# Ranking based on score_final
for _, items in df_bank.iterrows():
    if items['score_final'] < 1:
        items['rank_final'] = 'D'
    elif items['score_final'] < 2:
        items['rank_final'] = 'C'
    elif items['score_final'] < 3:
        items['rank_final'] = 'B'
    else:
        items['rank_final'] = 'A'
        
# Change type of data before save to database
df_bank = df_bank.astype(str)

# Assign variable for selected columns
select_column = df_bank.columns[:-1].to_list()


## 2. Save data to DB Access
### 2.1 Get data fields in new table
""" - `ptsp_stock_fundamental_score_financial` """
conn = pyodbc.connect(
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=V:\iBroker\stock_database.accdb;'
)
df_db = pd.read_sql(
    sql="SELECT * FROM ptsp_stock_fundamental_score_financial", 
    con=conn
)
conn.commit()

# Rename columns to match with format in database
df_db.rename(
    columns={
        "score_roe_sector": "score_ROE_sector",
        "score_roa_sector": "score_ROA_sector",
        "score_nim_sector": "score_NIM_sector"
    },
    inplace=True
)

# To insert new values by dropping duplicate values
df_saving = pd.concat(
    [
        df_db[select_column],
        df_bank[select_column].astype(str)
    ]
).drop_duplicates(keep=False)

# Implement update day
df_saving['Update'] = now

col_df_db = '[Symbol],[Year],[Quarter],[score_roe_sector],[score_roa_sector],[score_nim_sector],[score_profit],[rank_profit],[z_LoanProvisionRatio],[z_Deposit2Loan],[z_NPL_ratio_inv],[z_NPL_coverage],[score_health],[rank_health],[score_EPS_above_average],[score_EPS_growth],[score_EPS_above_sector],[score_EPS_above_group],[score_growth],[rank_growth],[score_PE_5Y],[score_PB_5Y],[score_PE_sector],[score_PB_sector],[score_valuation],[rank_valuation],[score_final],[rank_final],[update]'


### 2.2 Save data to new table
conn = pyodbc.connect(
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=V:\iBroker\stock_database.accdb;'
)
cursor = conn.cursor()
for _, row in df_bank.iterrows():
    sql = "INSERT INTO ptsp_stock_fundamental_score_financial ("+col_df_db+") VALUES "+ str(tuple(row))
    cursor.execute(sql)
    conn.commit()
    
print("Successfully saved data")