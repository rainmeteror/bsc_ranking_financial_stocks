""" 
# RANKING: INSURANCE SECTOR

Profit:
- ROE
- ROA
- Combined Ratio

Health:
- Net Premium Written to Equity
- Net Leverage
- Gross Reserve to Equity
- Net Premium Written to Gross Premium Written
"""

"""
This Script:
    1. Using input data:
        - From SQL Database
        - From External source (VNDirect) for serveral fields.
    2. Processing data:
        - Calculate ratios
        - Calculate score
    3. Store data
        - Drop duplicate data: By comparing the lastest data to the data which already exists in the database
        - Then insert new data into the database
        - Import the day which is updated
        - Directly store data into the database.
"""

# ================================================
# ================================================
## 1. Import
### 1.1 Library
import pandas as pd
import numpy as np
import datetime as dt
import pyodbc
import pymssql
import sys
from src import functions_insurance as fi

sys.path.append(r"F:\Tùng\Tung\Python\DashBoard\vnd_data")
import get_vnd_data as vnd

# ignore warnings
import warnings
warnings.filterwarnings('ignore')

# Customize the display of the table
pd.set_option('chained_assignment', None)

# Get today
now = dt.datetime.today().strftime("%Y%m%d")


# ================================================
### 1.2 Import Data
# Assign pathlink
path_income_insurance = r"F:\Tùng\Tung\Python\BSC_DataRankingStocks\cache\is_insurance.csv"
path_bs_insurance = r"F:\Tùng\Tung\Python\BSC_DataRankingStocks\cache\bs_insurance.csv"


df_bs = pd.read_csv(path_bs_insurance)
df_bs.drop(['Unnamed: 0'], axis=1, inplace=True)

df_is = pd.read_csv(path_income_insurance)
df_is.drop(['Unnamed: 0'], axis=1, inplace=True)

# Preprocess data
df_is = df_is.loc[df_is['Quarter'] != 0]
df_bs = df_bs.loc[df_bs['Quarter'] != 0]
df_is.fillna(0, inplace=True)
df_bs.fillna(0, inplace=True)

# Sort data
df_bs.sort_values(by=['Symbol', 'Year', 'Quarter'], ascending=[True, True, True], inplace=True)
df_is.sort_values(by=['Symbol', 'Year', 'Quarter'], ascending=[True, True, True], inplace=True)

# Get stocks of this sectors
list_stocks = df_bs['Symbol'].unique()


#### 1.2.1 Check Null data
# df_is.isnull().any()
# df_bs.isnull().any()
# df_is[df_is.isna().any(axis=1)]
# df_bs[df_bs.isna().any(axis=1)]


#### 1.2.2 Import External Data
# - Ceded Reserves: From VND's source due to SQL Server doesn't have this type of data
ceded_reserves = []
print("Get Ceded Reserve or 'Provision for claim from outward insurance' from the balance sheet ")
for i in list_stocks:
    print(f"Stock: {i}")
    df_i = vnd.get_balance_sheet(i)
    df_i['fiscalDate'] = pd.to_datetime(df_i['fiscalDate'])
    df_i['Year'] = df_i['fiscalDate'].dt.year
    df_i['Quarter'] = df_i['fiscalDate'].dt.quarter
    df_i = df_i.loc[df_i['itemCode'] == 411920]
    
    ceded_reserves.append(df_i)
    
print("Finish: Successfully get the data")
ceded_reserves = pd.concat(ceded_reserves)

# Process and Remove unnecessary columns
ceded_reserves.drop(
    [
        'reportType', 'modelType', 'fiscalDate', 
        'createdDate', 'modifiedDate', 'itemCode'
    ],
    axis=1,
    inplace=True
)
ceded_reserves.rename(
    columns={
        "code": "Symbol",
        "numericValue": "CededReserves"
    },
    inplace=True
)

# Merge Ceded_reserves to Balance Sheet
df_bs = df_bs.merge(ceded_reserves, how="inner", on=['Symbol', 'Year', 'Quarter'])

# Get Revenues
# - Net Revenues: From VND's source due to SQL Server has some errors in this type of data
net_revenue = []
print("Get Net revenue data from the income statement")
for i in list_stocks:
    print(f"Stock: {i}")
    df_i = vnd.get_income_statement(i)
    df_i['fiscalDate'] = pd.to_datetime(df_i['fiscalDate'])
    df_i['Year'] = df_i['fiscalDate'].dt.year
    df_i['Quarter'] = df_i['fiscalDate'].dt.quarter
    df_i = df_i.loc[df_i['itemCode'] == 21001]
    
    net_revenue.append(df_i)
    
print("Finish: Successfully get the data")
net_revenue = pd.concat(net_revenue)

# Process and Remove unnecessary columns
net_revenue.drop(
    [
        'reportType', 'modelType', 'fiscalDate', 
        'createdDate', 'modifiedDate','itemCode'
    ],
    axis=1,
    inplace=True
)
net_revenue.rename(
    columns={
        "code": "Symbol",
        "numericValue": "Revenues"
    },
    inplace=True
)

# Merge Net Revenue to Income Statement
df_is = df_is.merge(
    right=net_revenue, 
    how="inner", 
    on=['Symbol', 'Year', 'Quarter']
)


#### 1.2.3 Preprocess data
# - Combined Ratio (TTM)
df_is = fi.combined_ratio_ttm(panel_data=df_is, window=4)
df_is.fillna(0, inplace=True)

# Connect to database and get the corresponding data 
conn = pyodbc.connect(
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=V:\iBroker\stock_database.accdb;'
)
is_insurance = pd.read_sql("SELECT * FROM income_statement_insurance", con=conn)
conn.commit()

# Assign variable for selected coloumns
select_column = [
    'Symbol', 
    'Year', 
    'Quarter', 
    'incurred_losses_ttm', 
    'expenses_ttm', 
    'revenues_ttm'
]

# To insert new values by dropping duplicate values
df_is_saving = pd.concat(
    [
        df_is[select_column].astype(str), 
        is_insurance[select_column]
    ]
).drop_duplicates(keep=False)

# Implement update day
df_is_saving['Update'] = now

# Set up variable for inserting data to the database
col_is_insurance = "],[".join(i for i in is_insurance.columns.to_list())

# Save data
conn = pyodbc.connect(
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=V:\iBroker\stock_database.accdb;'
)
cursor = conn.cursor()
for _, row in df_is_saving[['Symbol', 'Year', 'Quarter', 'incurred_losses_ttm', 'expenses_ttm', 'revenues_ttm', 'Update']].iterrows():
    sql = "INSERT INTO income_statement_insurance (["+col_is_insurance+"]) VALUES "+ str(tuple(row))
    cursor.execute(sql)
    conn.commit()
    
print("Successfully saved data")


# ================================================
# ================================================
## 2. Process data for Ranking
### 2.1 Profit Rank
# - ROE: `score_roe_sector`
# - ROA: `score_roa_sector`
# - Combined Ratio: `score_combined_ratio_sector`
# - `score_profit`
# - `rank_profit`

# Merge balance sheet and income statement
df_profit = pd.merge(
    df_bs[['Symbol', 'Year', 'Quarter', 'Equity', 'Assets']],
    df_is[['Symbol', 'Year', 'Quarter', 'NetIncome2', 'combined_ratio_ttm']],
    how='inner',
    on=['Symbol', 'Year', 'Quarter'])


#### 2.1.1 Calculate ratios
df_profit['Equity_m'] = df_profit.groupby('Symbol')['Equity'].shift(4).to_list()
df_profit['Assets_m'] = df_profit.groupby('Symbol')['Assets'].shift(4).to_list()


df_profit['Equity_m'] = df_profit[['Equity', 'Equity_m']].mean(axis=1)
df_profit['Assets_m'] = df_profit[['Assets', 'Assets_m']].mean(axis=1)

df_profit['NetIncome2_ttm'] = df_profit.groupby('Symbol')['NetIncome2'].rolling(4).sum().to_list()

df_profit['ROE_ttm'] = df_profit['NetIncome2_ttm']/df_profit['Equity_m']
df_profit['ROA_ttm'] = df_profit['NetIncome2_ttm']/df_profit['Assets_m']


#### 2.1.2 Calculate ratios of the Insurance sector
# Calculate ratios of the insurance sector
# # Median approach
df_sector = df_profit.groupby(["Year", "Quarter"]).agg({
    "NetIncome2_ttm": "sum",
    "Equity_m": "sum",
    "Assets_m": "sum",
    'combined_ratio_ttm': "median"
}).reset_index()
df_sector['ROE_sector_ttm'] = df_sector['NetIncome2_ttm']/df_sector['Equity_m']
df_sector['ROA_sector_ttm'] = df_sector['NetIncome2_ttm']/df_sector['Assets_m']
df_sector.rename(
    columns={"combined_ratio_ttm": "combined_ratio_sector_ttm_median"}, 
    inplace=True
)

# Merge data from individual stocks and their sector
df_profit = pd.merge(
    df_profit,
    df_sector[[
        'Year', 'Quarter', 'ROE_sector_ttm', 
        'ROA_sector_ttm', 'combined_ratio_sector_ttm_median'  
    ]],
    how='inner',
    on=['Year', 'Quarter']
)

# Calculate ratios of the insurance sector
# # Average approach
df_sector_avg_a = df_is.groupby(["Year", "Quarter"]).agg({
    "incurred_losses_ttm": "sum",
    "expenses_ttm": "sum",
    "revenues_ttm": "sum",
}).reset_index()

df_sector_avg_a['combined_ratio_sector_ttm_avg'] = (df_sector_avg_a['incurred_losses_ttm']+df_sector_avg_a['expenses_ttm'])/df_sector_avg_a['revenues_ttm']

# Merge data from individual stocks and their sector
df_profit = pd.merge(
    df_profit,
    df_sector_avg_a[[
        'Year', 'Quarter', 'combined_ratio_sector_ttm_avg'             
    ]],
    how='inner',
    on=['Year', 'Quarter']
)

#### 2.1.3 Scoring profit criteria
# Rank based on median approach (No use average approach)
""" 
If you want to use average approach instead of median approach. 
Replace combined_ratio_sector_ttm_median by combined_ratio_sector_ttm_avg 
"""

df_profit['score_roe_sector'] = np.where(df_profit['ROE_ttm'] > df_profit['ROE_sector_ttm'], 1, 0)
df_profit['score_roa_sector'] = np.where(df_profit['ROA_ttm'] > df_profit['ROA_sector_ttm'], 1, 0)
df_profit['score_combined_ratio_sector'] = np.where(df_profit['combined_ratio_ttm'] < df_profit['combined_ratio_sector_ttm_median'], 1, 0)
df_profit['score_profit'] = round((df_profit['score_roe_sector']+df_profit['score_roa_sector']+df_profit['score_combined_ratio_sector'])*4/3,2)

# Create an empty list to store data
rank_profit = []

# Ranking based on score_profit
for _, items in df_profit.iterrows():
    if items['score_profit'] < 1:
        rank_profit.append("D")
    elif items['score_profit'] < 2:
        rank_profit.append("C")
    elif items['score_profit'] < 3:
        rank_profit.append("B")
    else:
        rank_profit.append("A")

df_profit['rank_profit'] = rank_profit
df_profit.sort_values(
    by=['Symbol', 'Year', 'Quarter'], 
    ascending=[True, True, True], 
    inplace=True
)


# ================================================================
### 2.2 Health Rank
# - `score_npw2equity`
# - `score_net_leverage`
# - `score_grossreserve2equity`
# - `score_npw2gpw`
# - `score_health`
# - `rank_health`

#### 2.2.1 Calculate ratios
df_health = pd.merge(
    df_is[['Symbol', 'Year', 'Quarter', 'GrossPremiumWritten','NetPremiumWritten']],
    df_bs[['Symbol', 'Year', 'Quarter', 'Equity', 'Provisions', 'CededReserves']], 
    how='inner',
    on=['Symbol', 'Year', 'Quarter']
)
df_health.sort_values(
    by=['Symbol', 'Year', 'Quarter'], 
    ascending=[True, True, True], 
    inplace=True
)

# Calculate ratios
df_health['npw_to_equity'] = df_health['NetPremiumWritten']/df_health['Equity']
df_health['net_leverage'] = (df_health['NetPremiumWritten'] + df_health['Provisions'] - df_health['CededReserves'])/df_health['Equity']
df_health['gross_reserves_to_equity'] = df_health['Provisions']/df_health['Equity']
df_health['npw_gpw']= df_health['NetPremiumWritten']/df_health['GrossPremiumWritten']

#### 2.2.2 Function to score based on metrics
# - Net Premium Written to Equity Score
# - Net Leverage Score
# - Gross Reserves to Equity Score
# - Net Premium Written to Gross Premium Written Score

#### 2.2.3 Scoring health criteria by using functions
df_health = fi.npw_to_equity_score(
    panel_data=df_health
)
df_health = fi.net_leverage_score(
    panel_data=df_health
)
df_health = fi.gross_reserves_to_equity_score(
    panel_data=df_health
)
df_health = fi.npw_gpw_score(
    panel_data=df_health
)
df_health['score_health'] = round(
    number=np.mean(
        df_health[[ 
            'score_npw2equity', 'score_net_leverage',
            'score_grossreserve2equity', 'score_npw2gpw'
        ]],
        axis=1
    ), 
    ndigits=2
)

# Create an empty list to store data
rank_health = []

# Rankiing based on score_health
for _, items in df_health.iterrows():
    if items['score_health'] > 3:
        rank_health.append("Safe +")
    elif items['score_health'] > 2:
        rank_health.append("Safe")
    elif items['score_health'] > 1:
        rank_health.append("Warning")
    else:
        rank_health.append("Danger")
        
df_health['rank_health'] = rank_health
df_health.tail(3)



# ================================================================
# ================================================================
## 3. Merge data
### 3.1 Merge profit & health
# - `df_profit`: Profit
# - `df_health`: Health

# Merge data with aming at saving data to stock_financial_ratio_insurance
df_sfri = pd.merge(
    left=df_profit[[
        'Symbol', 'Year', 'Quarter', 
        'combined_ratio_ttm'
    ]],
    right=df_health[[
        'Symbol', 'Year', 'Quarter', 
        'npw_to_equity', 'net_leverage', 
        'gross_reserves_to_equity', 'npw_gpw'
    ]],
    how='inner',
    on=['Symbol', 'Year', 'Quarter']
)

# Connect to database and get the corresponding data 
conn = pyodbc.connect(
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=V:\iBroker\stock_database.accdb;'
)
df_ratio_insurance = pd.read_sql(
    sql="SELECT * FROM stock_financial_ratio_insurance", 
    con=conn
)
conn.commit()

# Assign variable for selected coloumns
select_column_ratio = [
    'Symbol', 'Year', 'Quarter', 
    'combined_ratio_ttm', 'npw_to_equity',
    'net_leverage', 'gross_reserves_to_equity', 'npw_gpw'
]

# To insert new values by dropping duplicate values
df_ratio_saving = pd.concat(
    [
        df_sfri[select_column_ratio].astype(str), 
        df_ratio_insurance[select_column_ratio]
    ]
).drop_duplicates(keep=False)

# Implement update day
df_ratio_saving['Update'] = now

# Set up variable for inserting data to the database
select_column_ratio.append("Update")
col_df_sfri = "],[".join(i for i in df_ratio_insurance.columns.to_list())

# Save data
conn = pyodbc.connect(
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=V:\iBroker\stock_database.accdb;'
)
cursor = conn.cursor()
for _, row in df_ratio_saving[select_column_ratio].astype(str).iterrows():
    sql = "INSERT INTO stock_financial_ratio_insurance (["+col_df_sfri+"]) VALUES "+ str(tuple(row))
    cursor.execute(sql)
    conn.commit()
    
print("Successfully saved data")

# Create df_final by merging df_profit and df_health
df_final = pd.merge(
    df_profit[[
        'Symbol', 'Year', 'Quarter', 
        'score_roe_sector', 'score_roa_sector', 'score_combined_ratio_sector', 
        'score_profit', 'rank_profit'
    ]],
    df_health[[
        'Symbol', 'Year', 'Quarter', 
        'score_npw2equity', 'score_net_leverage',
        'score_grossreserve2equity', 'score_npw2gpw', 
        'score_health', 'rank_health'
    ]],
    how='inner',
    on=['Symbol', 'Year', 'Quarter']
)


# ================================================================
### 3.2 Import Raw data
# To get growth score and valuation score

#### 3.2.1 Get the final result 
# From table: `ptsp_stock_fundamental_score`
# Get raw final result from table: ptsp_stock_fundamental_score
conn = pyodbc.connect(
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=V:\iBroker\stock_database.accdb;'
)
df_raw = pd.read_sql(
    sql='select * from ptsp_stock_fundamental_score', 
    con=conn
)
conn.close()

# Get the data of insurance sector
df_raw = df_raw.loc[df_raw['Symbol'].isin(list_stocks)]
df_raw[['Year', 'Quarter']] = df_raw[['Year', 'Quarter']].astype(int)

#### 3.2.2 Merge data
# It includes:
# - New profit rank
# - New health rank
# - Current growth rank
# - Current Valuation rank

# Merge df_final and df_raw
df_final = pd.merge(
    df_final,
    df_raw[[
        'Symbol', 'Year', 'Quarter', 'score_EPS_above_average',
        'score_EPS_growth', 'score_EPS_above_sector',
        'score_EPS_above_group', 'score_growth', 'rank_growth',
        'score_PE_5Y', 'score_PB_5Y', 'score_PE_sector',
        'score_PB_sector', 'score_valuation', 'rank_valuation',
        'score_final', 'rank_final', 'Update'            
    ]],
    how='inner',
    on=['Symbol', 'Year', 'Quarter']
)

# Change type of data in order to calculate
list_col = [
    'score_roe_sector',
    'score_roa_sector',
    'score_combined_ratio_sector',
    'score_profit',
    'score_npw2equity',
    'score_net_leverage',
    'score_grossreserve2equity',
    'score_npw2gpw',
    'score_health',
    'score_EPS_above_average',
    'score_EPS_growth',
    'score_EPS_above_sector',
    'score_EPS_above_group',
    'score_growth',
    'score_PE_5Y',
    'score_PB_5Y',
    'score_PE_sector',
    'score_PB_sector',
    'score_valuation',
]

for i in list_col:
    df_final[i] = df_final[i].astype(float)


### 3.3 Calculate Final Score
df_final['score_final'] = round(
    number=np.mean(
        df_final[[
            'score_profit', 
            'score_health', 
            'score_growth', 
            'score_valuation'
        ]],
        axis=1
    ), 
    ndigits=2
)

# Ranking based on score_final
for _, items in df_final.iterrows():
    if items['score_final'] < 1:
        items['rank_final'] = "D"
    elif items['score_final'] < 2:
        items['rank_final'] = "C"
    elif items['score_final'] < 3:
        items['rank_final'] = "B" 
    else:
        items['rank_final'] = "A"


# ================================================================
# ================================================================
## 4. Save to DB Access
### 4.1 Get data fields in new table
# - `ptsp_stock_fundamental_score_financial`

# Connect to database and get the corresponding data 
conn = pyodbc.connect(
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=V:\iBroker\stock_database.accdb;'
)
df_db = pd.read_sql(
    sql="SELECT * FROM ptsp_stock_fundamental_score_financial", 
    con=conn
)
conn.commit()

# Filter to get data of insurance
df_db = df_db.loc[
    df_db['score_combined_ratio_sector'].notna()
]

# Assign variable for selected columns
select_column_final = df_final.columns[:-1].to_list()

# To insert new values by dropping duplicate values
df_final_saving = pd.concat(
    [
        df_final[select_column_final].astype(str),
        df_db[select_column_final]
    ]
).drop_duplicates(keep=False)

# Implement update day
df_final_saving['Update'] = now

# Set up variable for inserting data to the database
col_df_db = '[Symbol],[Year],[Quarter],[score_roe_sector],[score_roa_sector],[score_combined_ratio_sector],[score_profit],[rank_profit],[score_npw2equity],[score_net_leverage],[score_grossreserve2equity],[score_npw2gpw],[score_health],[rank_health],[score_EPS_above_average],[score_EPS_growth],[score_EPS_above_sector],[score_EPS_above_group],[score_growth],[rank_growth],[score_PE_5Y],[score_PB_5Y],[score_PE_sector],[score_PB_sector],[score_valuation],[rank_valuation],[score_final],[rank_final],[update]'

# Save data
conn = pyodbc.connect(
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=V:\iBroker\stock_database.accdb;'
)
cursor = conn.cursor()
for _, row in df_final_saving.astype(str).iterrows():
    sql = "INSERT INTO ptsp_stock_fundamental_score_financial ("+col_df_db+") VALUES "+ str(tuple(row))
    cursor.execute(sql)
    conn.commit()
    
print("Successfully saved data")
print("Finish!")
