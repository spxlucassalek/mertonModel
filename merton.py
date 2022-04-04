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
import utils

def calculatePD(dataFrame):
   
    tickers = dataFrame['ticker'].unique()
    
    final = pd.DataFrame()
    
    dataFrame.loc[dataFrame['ticker'] == 'LTM CI Equity', 'bs_tot_liab2'] = dataFrame['bs_tot_liab2']*1000
    
    for t in tickers:
        print(t)
        ewma_df = dataFrame.loc[dataFrame['ticker']==t]
        ewma_df.sort_values('date', ascending=True, inplace=True)           
        ewma_df['bs_tot_liab2'] = ewma_df['bs_tot_liab2'].ffill().bfill()
        ewma_df['px_last'] = ewma_df['px_last'].ffill().bfill()
       
        ewma_df['bs_cur_liab'] = ewma_df['bs_cur_liab'].ffill().bfill()
        
        ewma_df['cur_mkt_cap'] = ewma_df['cur_mkt_cap'].ffill().bfill()
        
        ewma_df['ret'] = ewma_df['px_last'].pct_change(1).fillna(0)
        ewma_df['ret - 12 months'] = ewma_df['px_last'].pct_change(252).fillna(0)
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
        ewma_df.reset_index(inplace=True, drop=True)
        
        # se o retorno tiver negativo - pergar retorno
        
        # se o retorno tiver positivo - media dos retornos positivos
        
        for i in range(len(ewma_df)):
            if i>=1:
                ewma_df.loc[i, 'var recursivo'] =0.98*ewma_df.loc[i-1, 'var recursivo']+(1-0.98)*ewma_df.loc[i, 'ret']**2
        ewma_df.sort_values('date', inplace=True, ascending=True)
        ewma_df['EWMA'] = ewma_df['var recursivo'].apply(lambda x: np.sqrt(x)*np.sqrt(252))
        ewma_df['z score Merton'] = ewma_df.apply(insertMertonModel, axis=1)
        ewma_df['PD Merton'] = ewma_df['z score Merton'].apply(lambda x: norm.cdf(-x))
        ewma_df['z score KMV'] = ewma_df.apply(insertKMVModel, axis=1)
        ewma_df.loc[ewma_df['z score KMV']>5,'z score KMV'] = 5 
        ewma_df.loc[ewma_df['z score KMV']<-2,'z score KMV'] = -2
        ewma_df.loc[ewma_df['z score Merton']>5,'z score Merton'] = 5 
        ewma_df.loc[ewma_df['z score Merton']<-2,'z score Merton'] = -2
        ewma_df['PD KMV'] = ewma_df['z score KMV'].apply(lambda x: norm.cdf(-x))
        ewma_df['d'] = ewma_df.apply(insertD, axis=1)
        ewma_df['Aquadrado'] = ewma_df.apply(insertAquadrado, axis=1)
        ewma_df['p1'] = ewma_df.apply(insertP1, axis=1)
        ewma_df['p2'] = ewma_df.apply(insertP2, axis=1)
        ewma_df['PD Credit Grade'] = ewma_df.apply(insertPD, axis=1)
        final = final.append(ewma_df)
#        utils.insertMertonModelMySQL(final)
#        utils.insertKMVModelMySQL(final)
        
    return final
   
# mediana de todas as empresas 
     
def insertMertonModel(x):    
    with open('duration.txt', 'r') as file_:
        duration_value = file_.read()
        duration = float(duration_value)
    
    return ( np.log((x['cur_mkt_cap'] + x['bs_tot_liab2']) / x['bs_tot_liab2'] ) + (x['cdi'] - 0.5*x['EWMA']**2)*duration)/x['EWMA']*np.sqrt(duration)
    
    
def insertKMVModel(x):
    with open('duration.txt', 'r') as file_:
        duration_value = file_.read()
        duration = float(duration_value)    
    
    return ( np.log((x['cur_mkt_cap'] + x['bs_cur_liab'] + 0.5*(x['bs_tot_liab2']-x['bs_cur_liab'])  ) / ( x['bs_cur_liab'] + 0.5*(x['bs_tot_liab2']-x['bs_cur_liab']))) + (x['cdi'] - 0.5*x['EWMA']**2)*duration/x['EWMA']*np.sqrt(duration))
    

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
    

