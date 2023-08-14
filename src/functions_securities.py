import pandas as pd
import numpy as np


# Function for scoring based on loans-to-equity
def score_lte(panel_data) -> pd.DataFrame():    
    score_lte = []
    for _, item in panel_data.iterrows():
        if item['lte'] >= item['lte_8q']*1.6:
            score_lte.append(0)
        elif item['lte'] >= item['lte_8q']*1.3:
            score_lte.append(0.5)
        elif item['lte'] >= item['lte_8q']*0.7:
            score_lte.append(1)
        else:
            score_lte.append(0)
    
    panel_data['score_lte'] = score_lte
    
    
    return panel_data


def top_share(dictionary: dict, percent_sales: float):
    """ Count the number of top lines which contribute to the given percentage of sales
    ================================================================
    Parameters:
        dictionary: dict
        percent_sales: float
            Defaults equal to 0.8
    """
    df1 = pd.DataFrame.from_dict(
        data=dictionary, 
        orient='index'
    )
    df1.sort_values(
        by=[0],
        ascending=False, 
        inplace=True
    )
    df1['position_score'] = np.where(df1[0].cumsum()>=percent_sales, 1, 0)
    a = df1.loc[df1['position_score'] == 0].count()['position_score']
    
    return a


def score_share(a: int):
    """ Ranking based on top lines
    ================================================================
    Meanings:
        If there are at least 3 lines contributing to the given percentage of sales. Score = 1
        If there are at least 2 lines contributing to the given percentage of sales. Score = 0.5
        If there are at least 1 lines contributing to the given percentage of sales. Score = 0
    """
    if a>2:
        score = 1
    elif a>1:
        score = 0.5
    else:
        score = 0
    
    return score


# Calculate the coefficient variation the income from financial assets recognized through profit/loss
def coef_variation_fvtpl(panel_data, window=12) -> pd.DataFrame():
    """ Calculate the coefficient variation the income from financial assets recognized through profit/loss (FVTPL)
    ================================================================
    Parameters:
        panel_data: pd.DataFrame()
        window: int
            The number of period to calculate coefficient variation for FVTPL
    """
    panel_data['FVTPL_m'] = panel_data.groupby('Symbol')['FVTPL'].rolling(window=window).mean().to_list()
    panel_data['FVTPL_std'] = panel_data.groupby('Symbol')['FVTPL'].rolling(window=window).std().to_list()
    panel_data[f'coef_var_{window}q'] = panel_data['FVTPL_m']/panel_data['FVTPL_std']
    
    # del panel_data['FVTPL_m']
    # del panel_data['FVTPL_std']
    
    return panel_data


# Score based on coeffiecient variation of FVTPL
def score_coef(panel_data) -> pd.DataFrame():
    """ Score based on coeffiecient variation of FVTPL
    ================================================================
    Parameters:
        panel_data: pd.DataFrame()
    
    """
    coef_var_12_med = panel_data.groupby([
        'Year', 'Quarter'
    ])['coef_var_12q'].median().reset_index(name='coef_var_12_med')
    panel_data = pd.merge(panel_data, coef_var_12_med, how='outer', on=['Year', 'Quarter'])
    
    score_coef_var = []
    for _, item in panel_data.iterrows():
        if item['coef_var_12q'] > item['coef_var_12_med']:
            score_coef_var.append(0)
        elif item['coef_var_12q'] == item['coef_var_12_med']:
            score_coef_var.append(0.5)
        else:
            score_coef_var.append(1)
    panel_data['score_coef_variation'] = score_coef_var
    
    # del panel_data['coef_var_12_med']
    
    
    return panel_data


if __name__ == '__main__':
    print("Hello world!")