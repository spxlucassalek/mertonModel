# -*- coding: utf-8 -*-
"""
Created on Sat May 16 20:09:05 2020

@author: jardiel.junior
"""

import pandas as pd
import dataFeeder
from scipy.interpolate import interp1d
import datetime as dt
import utils


missing = \
['AAL US Equity']

inserted = list(pd.read_excel('inserted.xlsx')['ticker'])

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


def generateDatesAndTickers():
    
    query = \
        """
          SELECT DISTINCT
            cq_prices.dd_bala.date, cq_prices.dd_bala.ticker
        FROM
            cq_prices.dd_bala
        WHERE
            cq_prices.dd_bala.date >= '20200101' 
        UNION SELECT DISTINCT
            cq_prices.dd_equity.date, cq_prices.dd_equity.ticker
        FROM
            cq_prices.dd_equity
        WHERE
            cq_prices.dd_equity.date >= '20200101'
        
        """
        
    df = utils.MySQLQueryHom(query).queryResult

    return df


def getMarketDataNew():
    
    padding = generateDatesAndTickers()
    
    eq = getEquityData()
    
    bala = getBalaData()
    
    df = pd.merge(padding, eq, left_on=['date', 'ticker'], 
                  right_on=['date', 'ticker'], how = 'left')
    
    
    
    df = pd.merge(df, bala, left_on=['date', 'ticker'], 
                  right_on=['date', 'ticker'], how = 'left')
    
    interp =  getCDI()

    df['cdi'] = df['date'].apply(lambda x: interp(x.toordinal()))
    df['cdi'] = df['cdi']/100    
    
    
    # df = df.loc[df['ticker'].isin(missing)]
    
    return df
    



def getEquityData():

    with open('queries/equity_data.txt', 'r') as file_:
        queryStr = file_.read()
        queryStr = queryStr.replace('\n', ' ')
    
    df = utils.MySQLQueryHom(queryStr).queryResult
    
    df['date'] = pd.to_datetime(df['date'])
    df = df.loc[df['date']>=pd.to_datetime(dt.date(2008,1,2))]
    
    return df

def getBalaData(): 
    
    with open('queries/bala_data.txt', 'r') as file_:
        queryStr = file_.read()
        queryStr = queryStr.replace('\n', ' ')
    
    df = utils.MySQLQueryHom(queryStr).queryResult
    
    df['date'] = pd.to_datetime(df['date'])
    df = df.loc[df['date']>=pd.to_datetime(dt.date(2008,1,2))]
    
    return df


def getShortTermLiab():
    
    with open('queries/short_term.txt', 'r') as file_:
        queryStr = file_.read()
        queryStr = queryStr.replace('\n', ' ')

    df = utils.MySQLQueryHom(queryStr).queryResult
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
    
    f = interp1d(final['date'], final['yield'],
                 fill_value='extrapolate') 
           
    
    return f