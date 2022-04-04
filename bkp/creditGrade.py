# -*- coding: utf-8 -*-
"""
Created on Tue May 19 16:09:12 2020

@author: jardiel.junior
"""

import pandas as pd
import numpy as np
from scipy.stats import norm
import utils

def creditGradeModel(dataFrame):
    tickers = dataFrame['ticker'].unique()
    final = pd.DataFrame()
    for t in tickers:
        ewma_df = dataFrame.loc[dataFrame['ticker']==t]
        ewma_df.sort_values('date', ascending=True, inplace=True)           
        ewma_df['bs_tot_liab2'] = ewma_df['bs_tot_liab2'].ffill().bfill()
        ewma_df['px_last'] = ewma_df['px_last'].ffill().bfill()
        ewma_df['bs_cur_liab'] = ewma_df['bs_cur_liab'].ffill().bfill()
        ewma_df['cur_mkt_cap'] = ewma_df['cur_mkt_cap'].ffill().bfill()
        ewma_df['ret'] = ewma_df['cur_mkt_cap'].pct_change(1).fillna(0)
        ewma_df['ret - 12 months'] = ewma_df['cur_mkt_cap'].pct_change(252).fillna(0)
        ewma_df['def - 60pct'] = None 
        ewma_df.loc[ewma_df['ret - 12 months']<-0.6, 'def - 60pct'] = 1
        ewma_df['def - 60pct'] = ewma_df['def - 60pct'].fillna(0) 
        ewma_df['def - 70pct'] = None 
        ewma_df.loc[ewma_df['ret - 12 months']<-0.7, 'def - 70pct'] = 1
        ewma_df['def - 70pct'] = ewma_df['def - 70pct'].fillna(0) 
        ewma_df['def - 80pct'] = None 
        ewma_df.loc[ewma_df['ret - 12 months']<-0.8, 'def - 80pct'] = 1
        ewma_df['def - 80pct'] = ewma_df['def - 80pct'].fillna(0) 
        ewma_df['var recursivo'] = None 
        ewma_df.loc[ewma_df['date'] == ewma_df['date'].min(), 'var recursivo'] = (0.4**2)/252
        ewma_df['ini_stock'] = None
        ewma_df['ini_stock'] = ewma_df.loc[ewma_df['date'] == ewma_df['date'].min(), 'px_last'].values[0]
        ewma_df.reset_index(inplace=True, drop=True)
        for i in range(len(ewma_df)):
            if i>=1:
                ewma_df.loc[i, 'var recursivo'] =0.98*ewma_df.loc[i-1, 'var recursivo']+(1-0.98)*ewma_df.loc[i, 'ret']**2
        ewma_df.sort_values('date', inplace=True, ascending=True)
        ewma_df['EWMA'] = ewma_df['var recursivo'].apply(lambda x: np.sqrt(x)*np.sqrt(252))
        ewma_df['d'] = ewma_df.apply(insertD, axis=1)
        ewma_df['Aquadrado'] = ewma_df.apply(insertAquadrado, axis=1)
        ewma_df['p1'] = ewma_df.apply(insertP1, axis=1)
        ewma_df['p2'] = ewma_df.apply(insertP2, axis=1)
        ewma_df['PD Credit Grade'] = ewma_df.apply(insertPD, axis=1)
        final = final.append(ewma_df)
#        utils.insertCreditGradeModelMySQL(final)
    return final

def insertD(x):
    lgd = 0.5
    lambda_ = 0.3
    return ((x['cur_mkt_cap']+ lgd*x['bs_tot_liab2'])/(lgd*x['bs_tot_liab2']))*np.exp(lambda_**2)

def insertAquadrado(x):
    T = 1
    lgd = 0.5
    lambda_ = 0.3    
    return T*(x['EWMA'] * x['cur_mkt_cap']/(x['cur_mkt_cap']+lgd*x['bs_tot_liab2']))**2+(lambda_**2)

def insertP1(x):
    return -np.sqrt(x['Aquadrado'])/2 + np.log(x['d'])/np.sqrt(x['Aquadrado'])

def insertP2(x):
    return -np.sqrt(x['Aquadrado'])/2  -np.log(x['d'])/np.sqrt(x['Aquadrado'])
    
def insertPD(x):
    return 1-norm.cdf(x['p1'])+x['d']*norm.cdf(x['p2'])
    