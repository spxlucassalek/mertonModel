# -*- coding: utf-8 -*-
"""
Created on Mon May 18 22:09:12 2020

@author: jardiel.junior
"""

# -*- coding: utf-8 -*-
"""
Created on Sat May 16 12:14:12 2020

@author: jardiel.junior
"""


"""
The Merton model takes an overly simple debt structure, and assumes that the total value
At of a firm’s assets follows a geometric Brownian motion under the physical measure
dAt = µAtdt + σAtdWt, A0 > 0, (4.1)
where µ is the mean rate of return on the assets and σ is the asset volatility. We also
need further assumptions: there are no bankruptcy charges, meaning the liquidation value
equals the firm value; the debt and equity are frictionless tradeable assets.
Large and medium cap firms are funded by shares (“equity”) and bonds (“debt”). The
Merton model assumes that debt consists of a single outstanding bond with face value K
and maturity T. At maturity, if the total value of the assets is greater than the debt, the
latter is paid in full and the remainder is distributed among shareholders.

Default: AT < K 

dAt = µAtdt + σAtdWt, A0 > 0,

µ = mean rate of return on the assets 
σ = asset volatility
At = total value of a firm's assets

"""
from scipy.stats import norm
import numpy as np
import readData
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
from scipy.stats import norm

def calculatePDKMV(dataFrame):
    tickers = dataFrame['ticker'].unique()
    final = pd.DataFrame()
    for t in tickers:
        ewma_df = dataFrame.loc[dataFrame['ticker']==t]
        ewma_df.sort_values('date', ascending=True, inplace=True)           
        ewma_df['bs_tot_liab2'] = ewma_df['bs_tot_liab2'].ffill().bfill()
        ewma_df['bs_cur_liab'] = ewma_df['bs_cur_liab'].ffill().bfill()
        ewma_df['px_last'] = ewma_df['px_last'].ffill().bfill()
        ewma_df['ret'] = ewma_df['px_last'].pct_change(1).fillna(0)
        ewma_df['var recursivo'] = None 
        ewma_df.loc[ewma_df['date'] == ewma_df['date'].min(), 'var recursivo'] = (0.4**2)/252
        ewma_df.reset_index(inplace=True, drop=True)
        for i in range(len(ewma_df)):
            if i>=1:
                ewma_df.loc[i, 'var recursivo'] =0.98*ewma_df.loc[i-1, 'var recursivo']+(1-0.98)*ewma_df.loc[i, 'ret']**2
        ewma_df.sort_values('date', inplace=True, ascending=True)
        ewma_df['EWMA'] = ewma_df['var recursivo'].apply(lambda x: np.sqrt(x)*np.sqrt(252))
        ewma_df['z score'] = ewma_df.apply(insertKMVModel, axis=1)
        ewma_df['PD'] = ewma_df['z score'].apply(lambda x: norm.cdf(-x))
        final = final.append(ewma_df)
    
    return final
   
# mediana de todas as empresas 
     
def insertKMVModel(x):    
    return ( np.log((x['cur_mkt_cap'] + x['bs_cur_liab'] + 0.5*(x['bs_tot_liab2']-x['bs_cur_liab'])  ) / ( x['bs_cur_liab'] + 0.5*(x['bs_tot_liab2']-x['bs_cur_liab']))) + x['cdi'] - 0.5*x['EWMA']**2)/x['EWMA']
    
    
        
    
        
        
        
        