import pandas as pd
import numpy as np


def combined_ratio_ttm(panel_data, window=4) -> pd.DataFrame():
    """ This function is to calculate combined ratio TTM
    ================================================================
    panel_data: pd.DataFrame
    window: int
        Default value is 4
    """
    incurred_losses_ttm = panel_data.groupby('Symbol')['IncurredLosses'].rolling(window=window).sum().to_list()
    expenses_ttm = panel_data.groupby('Symbol')['Expenses'].rolling(window=window).sum().to_list()
    revenues_ttm = panel_data.groupby('Symbol')['Revenues'].rolling(window=window).sum().to_list()

    panel_data['incurred_losses_ttm'] = incurred_losses_ttm
    panel_data['expenses_ttm'] = expenses_ttm
    panel_data['revenues_ttm'] = revenues_ttm
    panel_data['combined_ratio_ttm'] = (panel_data['incurred_losses_ttm'] + panel_data['expenses_ttm'])/panel_data['revenues_ttm']
        
    
    return panel_data


def npw_to_equity_score(panel_data) -> pd.DataFrame:
    """ 
    This fuction is to calculate quantiles of Net Premium Written to Equity for each period of time, 
    after that, to score based on the quantitles.
    
    
    ================================================================
    panel_data: pd.DataFrame()
    """
    npw_to_equity = panel_data.groupby(['Year', 'Quarter'])['npw_to_equity'].quantile(0.25).reset_index(name='npw_to_equity_25')
    npw_to_equity['npw_to_equity_50'] = panel_data.groupby(['Year', 'Quarter'])['npw_to_equity'].quantile(0.50).to_list()
    npw_to_equity['npw_to_equity_75'] = panel_data.groupby(['Year', 'Quarter'])['npw_to_equity'].quantile(0.75).to_list()

    panel_data = pd.merge(
        panel_data,
        npw_to_equity,
        how='outer',
        on=['Year', 'Quarter']
    )

    npw_to_equity_score = []

    for _, items in panel_data.iterrows():
        if items['npw_to_equity'] < items['npw_to_equity_25']:
            npw_to_equity_score.append(4)
        elif items['npw_to_equity'] < items['npw_to_equity_50']:
            npw_to_equity_score.append(3)
        elif items['npw_to_equity'] < items['npw_to_equity_75']:
            npw_to_equity_score.append(2)
        else:
            npw_to_equity_score.append(1)
    panel_data['score_npw2equity'] = npw_to_equity_score
    
    del panel_data['npw_to_equity_25']
    del panel_data['npw_to_equity_50'] 
    del panel_data['npw_to_equity_75']
    
    
    return panel_data


def net_leverage_score(panel_data) -> pd.DataFrame:
    """ 
    This fuction is to calculate quantiles of Net Leverage for each period of time, 
    after that, to score based on the quantitles.
    ================================================================
    panel_data: pd.DataFrame()
    """
    net_leverage = panel_data.groupby(['Year', 'Quarter'])['net_leverage'].quantile(0.25).reset_index(name='net_leverage_25')
    net_leverage['net_leverage_50'] = panel_data.groupby(['Year', 'Quarter'])['net_leverage'].quantile(0.50).to_list()
    net_leverage['net_leverage_75'] = panel_data.groupby(['Year', 'Quarter'])['net_leverage'].quantile(0.75).to_list()

    panel_data = pd.merge(
        panel_data,
        net_leverage,
        how='outer',
        on=['Year', 'Quarter']
    )

    net_leverage_score = []

    for _, items in panel_data.iterrows():
        if items['net_leverage'] < items['net_leverage_25']:
            net_leverage_score.append(4)
        elif items['net_leverage'] < items['net_leverage_50']:
            net_leverage_score.append(3)
        elif items['net_leverage'] < items['net_leverage_75']:
            net_leverage_score.append(2)
        else:
            net_leverage_score.append(1)
    panel_data['score_net_leverage'] = net_leverage_score
    
    del panel_data['net_leverage_25']
    del panel_data['net_leverage_50'] 
    del panel_data['net_leverage_75']
    
    
    return panel_data


def gross_reserves_to_equity_score(panel_data) -> pd.DataFrame:
    """ 
    This fuction is to calculate quantiles of Gross Reserve to Equity for each period of time, 
    after that, to score based on the quantitles.
    ================================================================
    panel_data: pd.DataFrame()
    """
    gross_reserves_to_equity = panel_data.groupby(['Year', 'Quarter'])['gross_reserves_to_equity'].quantile(0.25).reset_index(name='gross_reserves_to_equity_25')
    gross_reserves_to_equity['gross_reserves_to_equity_50'] = panel_data.groupby(['Year', 'Quarter'])['gross_reserves_to_equity'].quantile(0.50).to_list()
    gross_reserves_to_equity['gross_reserves_to_equity_75'] = panel_data.groupby(['Year', 'Quarter'])['gross_reserves_to_equity'].quantile(0.75).to_list()

    panel_data = pd.merge(
        panel_data,
        gross_reserves_to_equity,
        how='outer',
        on=['Year', 'Quarter']
    )

    gross_reserves_to_equity_score = []

    for _, items in panel_data.iterrows():
        if items['gross_reserves_to_equity'] < items['gross_reserves_to_equity_25']:
            gross_reserves_to_equity_score.append(4)
        elif items['gross_reserves_to_equity'] < items['gross_reserves_to_equity_50']:
            gross_reserves_to_equity_score.append(3)
        elif items['gross_reserves_to_equity'] < items['gross_reserves_to_equity_75']:
            gross_reserves_to_equity_score.append(2)
        else:
            gross_reserves_to_equity_score.append(1)
    panel_data['score_grossreserve2equity'] = gross_reserves_to_equity_score
    
    del panel_data['gross_reserves_to_equity_25']
    del panel_data['gross_reserves_to_equity_50'] 
    del panel_data['gross_reserves_to_equity_75']
    
    
    return panel_data


def npw_gpw_score(panel_data) -> pd.DataFrame:
    """ 
    This fuction is to calculate quantiles of Net Premium Written to Gross Premium Written for each period of time, 
    after that, to score based on the quantitles.
    ================================================================
    panel_data: pd.DataFrame()
    """
    npw_gpw = panel_data.groupby(['Year', 'Quarter'])['npw_gpw'].quantile(0.25).reset_index(name='npw_gpw_25')
    npw_gpw['npw_gpw_50'] = panel_data.groupby(['Year', 'Quarter'])['npw_gpw'].quantile(0.50).to_list()
    npw_gpw['npw_gpw_75'] = panel_data.groupby(['Year', 'Quarter'])['npw_gpw'].quantile(0.75).to_list()

    panel_data = pd.merge(
        panel_data,
        npw_gpw,
        how='outer',
        on=['Year', 'Quarter']
    )

    npw_gpw_score = []

    for _, items in panel_data.iterrows():
        if items['npw_gpw'] < items['npw_gpw_25']:
            npw_gpw_score.append(4)
        elif items['npw_gpw'] < items['npw_gpw_50']:
            npw_gpw_score.append(3)
        elif items['npw_gpw'] < items['npw_gpw_75']:
            npw_gpw_score.append(2)
        else:
            npw_gpw_score.append(1)
    panel_data['score_npw2gpw'] = npw_gpw_score
    
    del panel_data['npw_gpw_25']
    del panel_data['npw_gpw_50'] 
    del panel_data['npw_gpw_75']
    
    
    return panel_data


if __name__ == '__main__':
    print("Hello World")