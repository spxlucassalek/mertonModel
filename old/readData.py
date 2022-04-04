# -*- coding: utf-8 -*-
"""
Created on Sat May 16 20:09:05 2020

@author: jardiel.junior
"""

import pandas as pd
import dataFeeder
from scipy.interpolate import interp1d
import datetime as dt

def getMarketData():
    
    with open('queries/market_data.txt', 'r') as file_:
        queryStr = file_.read()
        queryStr = queryStr.replace('\n', ' ')
        
    df = dataFeeder.MySQLQuery(queryStr).queryResult
    
    df['date'] = pd.to_datetime(df['date'])
    df = df.loc[df['date']>=pd.to_datetime(dt.date(2008,1,2))]
    
#    df = pd.merge(df, getCDI(), how = 'left', left_on = 'date', right_on = 'date')
#    ,
#    df.rename(columns={'yield':'cdi'}, inplace=True)
    short_term = getShortTermLiab()
    
    df = pd.merge(df, short_term, left_on=['date', 'ticker'], right_on=['date','asset'], how='left')
    df.drop('asset', axis = 1, inplace=True)
    
    interp =  getCDI()
    
    df['cdi'] = df['date'].apply(lambda x: interp(x.toordinal()))
    df['cdi'] = df['cdi']/100    
    
    return df






def getShortTermLiab():
    
    with open('queries/short_term.txt', 'r') as file_:
        queryStr = file_.read()
        queryStr = queryStr.replace('\n', ' ')

    df = dataFeeder.MySQLQuery(queryStr).queryResult
    return df



def getCDI():
    
    with open('queries/cdi_rate.txt', 'r') as file_:
        queryStr = file_.read()
        queryStr = queryStr.replace('\n', ' ')
    df = dataFeeder.MySQLQuery(queryStr).queryResult
    
    
    final = pd.DataFrame()
    
    for date in df['date'].unique():
        aux = df.loc[df['date'] == date]
        aux = aux.loc[aux['ndc'] == aux['ndc'].min()]
        final = final.append(aux)    
    final['date'] = final['date'].apply(lambda x: x.toordinal())
    
    f = interp1d(final['date'], final['yield']) 
           
    
    return f