""" 
Ranking the securities sector based on Profit and Health

Profit:
- ROE
- ROA
- NIM

Health:
- Loans-to-Equity
- Debt-to-Equity
- Top % Share
- Coefficient Variation of FVTPL
"""


# ================================================
# 1. Import
### 1.1 Library
import pandas as pd
import numpy as np
import datetime as dt
import pyodbc
import pymssql
import sys
from src import functions_securities as fs

sys.path.append(r"F:\Tùng\Tung\Python\DashBoard\vnd_data")
import get_vnd_data as vnd

# ignore warnings
import warnings
warnings.filterwarnings('ignore')

# Customize the display of the table
pd.set_option('chained_assignment', None)

# Get today
now = dt.datetime.today().strftime("%Y%m%d")


### 1.2 Import data
#### 1.2.1 Raw data
# Assign pathlink
path_income_securities = r"F:\Tùng\Tung\Python\BSC_DataRankingStocks\cache\is_securities.csv"
path_bs_securities = r"F:\Tùng\Tung\Python\BSC_DataRankingStocks\cache\bs_securities.csv"

# Import data, it includes Income statement and Balance Sheet
df_is = pd.read_csv(path_income_securities)
df_is.drop(['Unnamed: 0'], axis=1, inplace=True)
df_bs = pd.read_csv(path_bs_securities)
df_bs.drop(['Unnamed: 0'], axis=1, inplace=True)

# Preprocess data
df_is = df_is.loc[df_is['Quarter'] != 0]
df_bs = df_bs.loc[df_bs['Quarter'] != 0]
df_is.fillna(0, inplace=True)
df_bs.fillna(0, inplace=True)

# Assign the list of stocks
list_sec = df_is['Symbol'].unique()


#### 1.2.2 Provision data 
# Due to SQL lacks this field
""" 
Due to BSC SQL Server does not have 
the data about provision for losses 
from mortgage assets, uncollectible receivables 
and borrowing expenses in the Income Statement. 
Therefore, this step is to implement the data. 
Specifically, the data is collected from VND's resources.
"""

# Create an empty list to store collecting data
provision_for_losses = []

for i in list_sec:
    print(f"Stock: {i}")
    df_i = vnd.get_income_statement(i)
    df_i['fiscalDate'] = pd.to_datetime(df_i['fiscalDate'])
    df_i['Year'] = df_i['fiscalDate'].dt.year
    df_i['Quarter'] = df_i['fiscalDate'].dt.quarter
    df_i = df_i.loc[df_i['itemCode'] == 700053]
    
    provision_for_losses.append(df_i)

print("Finish: Successfully get the data")
provision_for_losses = pd.concat(provision_for_losses)

# Process and Remove unnecessary columns
provision_for_losses.drop(
    [
        'reportType', 'modelType', 'fiscalDate', 
        'createdDate', 'modifiedDate','itemCode'
    ],
    axis=1,
    inplace=True
)
provision_for_losses.rename(
    columns={
        "code": "Symbol",
        "numericValue": "ProvisionForLosses"
    },
    inplace=True
)
provision_for_losses.sort_values(
    by=['Symbol', 'Year', 'Quarter'],
    ascending=[True, True, True],
    inplace=True
)


#### 1.2.3 Add data for WSS
# This step is to manually add data for some stocks 
# in case these stocks do not have data
# """ The data lacks WSS from Quarter 4 - Year 2022 up to now. Therefore, this step is to add related data.
# When there's no problem about the data, may be, this step could be removed."""

# provision_for_losses = provision_for_losses.append(
#     [{
#         "Symbol": 'WSS',
#         'ProvisionForLosses': 0,
#         'Year': 2022,
#         'Quarter': 4
#     },
#     {
#         "Symbol": 'WSS',
#         'ProvisionForLosses': 0,
#         'Year': 2023,
#         'Quarter': 1
#     }],
#     ignore_index=True)


#### 1.2.3 Preprocess data
# Merge all the data belonging to the Income Statement
df_is = df_is.merge(
    provision_for_losses, 
    how="inner", 
    on=['Symbol', 'Year', 'Quarter']
)

# Sort all the data by Symbol, Year and Quarter for df_is and df_bs
df_is.sort_values(
    by=['Symbol', 'Year', 'Quarter'], 
    ascending=[True, True, True], 
    inplace=True
)
df_bs.sort_values(
    by=['Symbol', 'Year', 'Quarter'], 
    ascending=[True, True, True], 
    inplace=True
)


# ================================================================
## 2. Process data
### 2.1 Profit Rank
# Get the suitable columns from the balance sheet and the income statement
df_profit = pd.merge(
    left=df_bs[[
        'Symbol', 'Year', 'Quarter', 'Assets', 'Equity', 'Loans'
    ]],
    right=df_is[[
        'Symbol', 'Year', 'Quarter', 'IncomeLoansReceivables',
        'InterestExpenses', 'ProvisionForLosses', 'NetIncome2'
    ]],
    on=['Symbol', 'Year', 'Quarter']
)


#### 2.1.1 Calculate ratios
# Calculate ratios of Individual Stocks
df_profit['Equity_m'] = df_profit.groupby('Symbol')['Equity'].shift(4).to_list()
df_profit['Assets_m'] = df_profit.groupby('Symbol')['Assets'].shift(4).to_list()

df_profit['NetIncome2_ttm'] = df_profit.groupby('Symbol')['NetIncome2'].rolling(4).sum().to_list()

df_profit['Equity_m'] = df_profit[['Equity', 'Equity_m']].mean(axis=1)
df_profit['Assets_m'] = df_profit[['Assets_m', 'Assets_m']].mean(axis=1)

df_profit['ROE_ttm'] = df_profit['NetIncome2_ttm']/df_profit['Equity_m']
df_profit['ROA_ttm'] = df_profit['NetIncome2_ttm']/df_profit['Assets_m']
df_profit['nim_securities'] = (df_profit['IncomeLoansReceivables'] - df_profit['InterestExpenses'] - df_profit['ProvisionForLosses'])/df_profit['Loans']

# Calculate ratios of the securities sector
df_sector = df_profit.groupby(["Year", "Quarter"]).agg({
    "NetIncome2": "sum",
    "Equity_m": "sum",
    "Assets_m": "sum",
    "IncomeLoansReceivables": "sum",
    "InterestExpenses": "sum",
    "ProvisionForLosses": "sum",
    "Loans": "sum"
}).reset_index()

df_sector['ROE_sector_ttm'] = df_sector['NetIncome2']/df_sector['Equity_m']
df_sector['ROA_sector_ttm'] = df_sector['NetIncome2']/df_sector['Assets_m']
df_sector['NIM_sector_securities'] = (
    df_sector['IncomeLoansReceivables']-df_sector['InterestExpenses'] -
    df_sector['ProvisionForLosses'])/df_sector['Loans']

# Merge data from individual stocks and their sector
df_profit = pd.merge(
    df_profit,
    df_sector[[
        'Year', 'Quarter', 'ROE_sector_ttm', 'ROA_sector_ttm',
        'NIM_sector_securities'
    ]],
    how='outer',
    on=['Year', 'Quarter']
)


#### 2.1.2 Scoring profit criteria
# - Rank and score for profit criteria
# Rank
df_profit['score_roe_sector'] = np.where(df_profit['ROE_ttm'] > df_profit['ROE_sector_ttm'], 1, 0)
df_profit['score_roa_sector'] = np.where(df_profit['ROA_ttm'] > df_profit['ROA_sector_ttm'], 1, 0)
df_profit['score_nim_sector'] = np.where(df_profit['nim_securities'] > df_profit['NIM_sector_securities'], 1, 0)
df_profit['score_profit'] = round((df_profit['score_roe_sector']+df_profit['score_roa_sector']+df_profit['score_nim_sector'])*4/3,2)

# Create an empty list to store collecting data
rank_profit = []

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


### 2.2 Health Rank
#### 2.2.1 Merge data
# Get the suitable columns from the balance sheet and the income statement
df_health = pd.merge(
    df_bs[['Symbol', 'Year', 'Quarter', 'Loans', 'Debt', 'Equity']],
    df_is[[
        'Symbol', 'Year', 'Quarter', 'Sales', 'IncomeFVTPL', 'IncomeHTM',
        'IncomeLoansReceivables', 'IncomeAFS', 'IncomeDerivatives',
        'RevenueBrokerageServices', 'RevenueUnderwritingIssuuanceServices',
        'RevenueAdvisoryServices', 'RevenueAuctionTrustServices',
        'RevenueCustodyServices', 'OtherRevenues', 'FVTPL'
    ]],
    on=['Symbol', 'Year', 'Quarter']
)


#### 2.2.2 Calculate ratios
# - Loans-to-equity
# Calculate Loans-to-equity
df_health['Loans_8Q'] = df_health.groupby('Symbol')['Loans'].rolling(8).sum().to_list()
df_health['Equity_8Q'] = df_health.groupby('Symbol')['Equity'].rolling(8).sum().to_list()
# df_health['Equity_8Q'] = df_health[['Equity', 'Equity_8Q']].mean(axis=1)
df_health['lte_8q'] = df_health['Loans_8Q']/df_health['Equity_8Q']
df_health['lte'] = df_health['Loans']/df_health['Equity']


df_health = fs.score_lte(panel_data=df_health)


# - Debt-to-Equity
# Calculate Debt-to-equity
df_health['debt_to_equity'] = df_health['Debt']/df_health['Equity']

# Calculate and compare based on median value of the sector
dte_median = df_health.groupby(
    ['Year',
     'Quarter'])['debt_to_equity'].median().reset_index(name='dte_median')
df_health = pd.merge(
    df_health,
    dte_median[['Year', 'Quarter', 'dte_median']],
    how='outer',
    on=['Year', 'Quarter']
)
score_dte = []
for _, item in df_health.iterrows():
    if item['debt_to_equity'] > item['dte_median']:
        score_dte.append(0)
    elif item['debt_to_equity'] == item['dte_median']:
        score_dte.append(0.5)
    else:
        score_dte.append(1)

df_health['score_dte'] = score_dte


# - Diversified sales
# Calculate the ratios of each lines which contribute to the sales
for i in range(7, 18):
    print("Calculating: " + df_health.columns[i])
    df_health[f"{df_health.columns[i]}_%"] = df_health[df_health.columns[i]]/df_health[df_health.columns[6]]

dict1 = df_health[[
    'IncomeFVTPL_%', 'IncomeHTM_%', 
    'IncomeLoansReceivables_%', 'IncomeAFS_%',
    'IncomeDerivatives_%', 'RevenueBrokerageServices_%',
    'RevenueUnderwritingIssuuanceServices_%', 'RevenueAdvisoryServices_%',
    'RevenueAuctionTrustServices_%', 'RevenueCustodyServices_%',
    'OtherRevenues_%'
]].to_dict('records')

# Scoring top_share
for i in range(0, len(dict1)):
    a = fs.top_share(dict1[i], percent_sales=0.8)
    score = fs.score_share(a)
    print(score)
    dict1[i]['score_diversified_sale'] = score
    
df_health['score_diversified_sale'] = pd.DataFrame(data=dict1)['score_diversified_sale']


# - Coefficient Variation: FVTPL
df_health = fs.coef_variation_fvtpl(panel_data=df_health, window=12)
df_health = fs.score_coef(panel_data=df_health)


#### 2.2.3 Scoring health criteria
df_health['score_health'] = (
    df_health['score_lte'] +
    df_health['score_dte'] + 
    df_health['score_diversified_sale'] +
    df_health['score_coef_variation']
)

# Create an empty list to store collecting data
rank_health = []

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


### 2.3 Merge data
#### 2.3.1 Merge data
# - New profit rank: `df_proft`
# - New health rank: `df_health`

# TABLE: income_statement_securities
# Connect to database and get the corresponding data 
conn = pyodbc.connect(
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=V:\iBroker\stock_database.accdb;'
)
df_isr = pd.read_sql(
    sql="SELECT * FROM income_statement_securities", 
    con=conn
)
conn.commit()

# Assign variable for selected columns
list_col_isr = [
    'Symbol', 'Year', 'Quarter', 
    'IncomeFVTPL_%', 'IncomeHTM_%',
    'IncomeLoansReceivables_%', 'IncomeAFS_%', 
    'IncomeDerivatives_%', 'RevenueBrokerageServices_%', 
    'RevenueUnderwritingIssuuanceServices_%',
    'RevenueAdvisoryServices_%', 'RevenueAuctionTrustServices_%',
    'RevenueCustodyServices_%', 'OtherRevenues_%', 
    'FVTPL_m', 'FVTPL_std',
]

# To insert new values by dropping duplicate values
df_isr_new = pd.concat(
    [
        df_isr[list_col_isr],
        df_health[list_col_isr].astype(str)
    ]
).drop_duplicates(keep=False)

# Implement update day
df_isr_new['Update'] = now

# Set up variable for inserting data to the database
list_col_isr.append('Update')
col_df_isr = "],[".join(i for i in df_isr.columns.to_list())

# Save data
conn = pyodbc.connect(
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=V:\iBroker\stock_database.accdb;'
)
cursor = conn.cursor()
for _, row in df_isr_new[list_col_isr].astype(str).iterrows():
    sql = "INSERT INTO income_statement_securities (["+col_df_isr+"]) VALUES "+ str(tuple(row))
    cursor.execute(sql)
    conn.commit()
    
print("Successfully saved data")


# TABLE: stock_financial_ratio_securities
# Create sfri (table) by merging df_profit and df_health
sfri = pd.merge(
    df_profit[[
        'Symbol', 'Year', 'Quarter', 
        'nim_securities'
    ]],
    df_health[[
        'Symbol', 'Year', 'Quarter', 
        'lte_8q', 'lte', 'debt_to_equity', 
        'coef_var_12q'
    ]],
    how='inner',
    on=['Symbol', 'Year', 'Quarter']
)

# Connect to database and get the corresponding data 
conn = pyodbc.connect(
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=V:\iBroker\stock_database.accdb;'
)
df_sfri = pd.read_sql(
    sql="SELECT * FROM stock_financial_ratio_securities", 
    con=conn
)

# Assign variable for selected columns
sfri_column = df_sfri.columns[:-1].to_list()

# To insert new values by dropping duplicate values
df_sfri_new = pd.concat(
    [
        df_sfri[sfri_column],
        sfri.astype(str)
    ]
).drop_duplicates(keep=False)

# Implement update day
df_sfri_new['Update'] = now

# Set up variable for inserting data to the database
col_df_sfri = "],[".join(i for i in df_sfri.columns.to_list())

# Save data
cursor = conn.cursor()
for _, row in df_sfri_new.astype(str).iterrows():
    sql = "INSERT INTO stock_financial_ratio_securities (["+col_df_sfri+"]) VALUES "+ str(tuple(row))
    cursor.execute(sql)
    conn.commit()
    
print("Successfully saved data")


# TABLE: ptsp_stock_fundamental_score
# Create df_final (table) to store final result by mergin df_profit and df_health
df_final = pd.merge(
    df_profit[['Symbol', 'Year', 'Quarter', 
               'score_roe_sector', 'score_roa_sector', 'score_nim_sector', 
               'score_profit', 'rank_profit']], 
    df_health[['Symbol', 'Year', 'Quarter', 
               'score_lte', 'score_dte', 
               'score_diversified_sale', 'score_coef_variation', 
               'score_health', 'rank_health']], 
    how='inner', 
    on=['Symbol', 'Year', 'Quarter']
)

# Sort values
df_final.sort_values(
    by=['Symbol', 'Year', 'Quarter'], 
    ascending=[True, True, True], 
    inplace=True
)

# Connect to database and get the corresponding data 
conn = pyodbc.connect(
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=V:\iBroker\stock_database.accdb;'
)
df_raw = pd.read_sql(
    sql='select * from ptsp_stock_fundamental_score', 
    con=conn
)

conn.commit()
conn.close()

# Filter to get data of securities
df_raw = df_raw.loc[df_raw['Symbol'].isin(list_sec)]
df_raw[['Year', 'Quarter']] = df_raw[['Year', 'Quarter']].astype(int)


#### 2.3.3 Merge all data
# - New profit rank
# - New health rank
# - Current growth rank
# - Currnet valuation rank
df_final = pd.merge(
    df_final,
    df_raw[[
        'Symbol', 'Year', 'Quarter', 
        'score_EPS_above_average', 'score_EPS_growth', 
        'score_EPS_above_sector', 'score_EPS_above_group', 
        'score_growth', 'rank_growth',
        'score_PE_5Y', 'score_PB_5Y', 
        'score_PE_sector', 'score_PB_sector', 
        'score_valuation', 'rank_valuation',
        'score_final', 'rank_final', 
        'Update'
    ]],
    how='inner',
    on=['Symbol', 'Year', 'Quarter']
)

# Change type of data to calculate final score
list_col = [
    'score_roe_sector',
    'score_roa_sector',
    'score_nim_sector',
    'score_profit',
    'score_lte',
    'score_dte',
    'score_diversified_sale',
    'score_coef_variation',
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

# Calculate score_final
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
## 3. Save to DB Access
### 3.1 Get data fields in new table
# - `ptsp_stock_fundamental_score_financial`

# Connect to database and get the corresponding data 
conn = pyodbc.connect(
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=V:\iBroker\stock_database.accdb;'
)
df_final_prev = pd.read_sql(
    sql='select * from ptsp_stock_fundamental_score_financial', 
    con=conn
)

conn.close()

# Filter to get data of securities
df_final_prev = df_final_prev.loc[
    df_final_prev['score_lte'].notna()
]

# Assign variable for selected columns
final_column = df_final.columns[:-1].to_list()

# To insert new values by dropping duplicate values
df_final_new = pd.concat(
    [
        df_final_prev[final_column],
        df_final[final_column].astype(str)
    ]
).drop_duplicates(keep=False)

# Implement update day
df_final_new['Update'] = now

# Set up variable for inserting data to the database
final_column.append('Update')
col_df_db = '[Symbol],[Year],[Quarter],[score_roe_sector],[score_roa_sector],[score_nim_sector],[score_profit],[rank_profit],[score_lte],[score_dte],[score_diversified_sale],[score_coef_variation],[score_health],[rank_health],[score_EPS_above_average],[score_EPS_growth],[score_EPS_above_sector],[score_EPS_above_group],[score_growth],[rank_growth],[score_PE_5Y],[score_PB_5Y],[score_PE_sector],[score_PB_sector],[score_valuation],[rank_valuation],[score_final],[rank_final],[update]'

# Save data
conn = pyodbc.connect(
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=V:\iBroker\stock_database.accdb;'
)
cursor = conn.cursor()
for _, row in df_final_new[final_column].astype(str).iterrows():
    sql = "INSERT INTO ptsp_stock_fundamental_score_financial ("+col_df_db+") VALUES "+ str(tuple(row))
    cursor.execute(sql)
    conn.commit()

conn.close()
    
print("Successfully saved data")
