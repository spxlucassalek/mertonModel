# -*- coding: utf-8 -*-
"""
Created on Sat May 16 22:56:57 2020

@author: jardiel.junior
"""

import readData
import merton
import creditGrade
import utils
import pandas as pd
import numpy as np
from pathos.multiprocessing import ProcessingPool
from functools import partial



if __name__ == '__main__':
    
    
    def getInsertedValuesCreditGrade():
        
        query = 'select * from credquant_model.default_credit_grade'
        inserted = utils.MySQLQueryHom(query).queryResult
        inserted['key'] = inserted['date'].apply(str) + inserted['ticker']
        return inserted
    
    
    def getInsertedValuesKMV():
        query = 'select * from credquant_model.default_kmv'
        inserted = utils.MySQLQueryHom(query).queryResult
        inserted['key'] = inserted['date'].apply(str) + inserted['ticker']
        return inserted
    
    def getInsertedValuesMerton():
        
        query = 'select * from credquant_model.default_merton'
        inserted = utils.MySQLQueryHom(query).queryResult
        inserted['key'] = inserted['date'].apply(str) + inserted['ticker']
        return inserted
    
    
    # inserted_merton = list(getInsertedValuesMerton()['key'].unique())
    
    
    # inserted_credit_grade = list(getInsertedValuesCreditGrade()['key'].unique())
    
    # inserted_kmv = list(getInsertedValuesKMV()['key'].unique())
    
    df_unfiltered = readData.getMarketDataNew()
     
    total = len(df_unfiltered['ticker'].unique())
   
    print(total)
    
    i=0
    for ticker in df_unfiltered['ticker'].unique():
    
        print(ticker)
        
        df = df_unfiltered.loc[df_unfiltered['ticker']==ticker]
        
        pd_table_Merton = merton.calculatePD(df)
          
        # print(pd_table_Merton.head())
          
        # # pd_table_CreditGrade = creditGrade.creditGradeModel(df)
        # # pd_table_CreditGrade['key'] = pd_table_CreditGrade['date'].apply(str) + pd_table_CreditGrade['ticker']
        # # pd_table_CreditGrade.sort_values('date', ascending=True, inplace=True)
        # # pd_table_CreditGrade = pd_table_CreditGrade.drop_duplicates('key', keep='last')  
          
        # # pd_table_CreditGrade = pd_table_CreditGrade.loc[~pd_table_CreditGrade['key'].isin(inserted_credit_grade)]
    
        # # pd_table_CreditGrade.dropna(inplace=True)
        
        # # pd_table_CreditGrade.drop('key', axis=1, inplace=True)
        
        
        
        # pd_table_Merton['key'] = pd_table_Merton['date'].apply(str) + pd_table_Merton['ticker']
        # pd_table_Merton.sort_values('date', ascending=True, inplace=True)
        # pd_table_Merton = pd_table_Merton.drop_duplicates('key', keep='last')  
          
        # # pd_table_Merton = pd_table_Merton.loc[~pd_table_Merton['key'].isin(inserted_merton)]
    
        # pd_table_Merton.dropna(inplace=True)
        
        # pd_table_Merton.drop('key', axis=1, inplace=True)
        
        
        # pd_table_Merton['key'] = pd_table_Merton['date'].apply(str) + pd_table_Merton['ticker']
        # pd_table_Merton.sort_values('date', ascending=True, inplace=True)
        # pd_table_Merton = pd_table_Merton.drop_duplicates('key', keep='last')  
          
        
        # # pd_table_Merton = pd_table_Merton.loc[~pd_table_Merton['key'].isin(inserted_kmv)]
    
        # pd_table_Merton.dropna(inplace=True)
        
        # pd_table_Merton.drop('key', axis=1, inplace=True)
        
        
        # # 
        
        
        
        
#        try:
        utils.insertMertonModelMySQL(pd_table_Merton)
#        except:
#            pass
#        
#        try:
        utils.insertKMVModelMySQL(pd_table_Merton)
#        except:
#            pass
#        try:
            
        utils.insertCreditGradeModelMySQL(pd_table_Merton)
#        except:
#            pass
        i=i+1
        
        print('Percentual: '+ str(np.round((i/total)*100,2)) + '%')

    
    
    
    
#    filter_ = list(pd.read_excel('us_tickers.xlsx')['ticker'].unique())

#    df_unfiltered_us = df_unfiltered.loc[~df_unfiltered['ticker'].isin(filter_)].sort_values(by='ticker', ascending=False)
#    list_dfs = []
#
#    for ticker in df_unfiltered['ticker'].unique():
#
#        list_dfs.append(df_unfiltered.loc[df_unfiltered['ticker']==ticker])
#
#
#    with ProcessingPool() as pool:
#        r_merton = pool.map(merton.calculatePD, list_dfs)
#
#
#    with ProcessingPool() as pool:
#        r_credit_grade = pool.map(creditGrade.creditGradeModel, list_dfs)
#
#
#    for r_m in r_merton:
#        utils.insertMertonModelMySQL(r_m)
#        utils.insertKMVModelMySQL(r_m)
#
#    for r_cred in r_credit_grade:
#        utils.insertCreditGradeModelMySQL(r_cred)
#        
        
    #
#     df_unfiltered_us = df_unfiltered.loc[df_unfiltered['ticker'].isin(filter_)].sort_values(by='ticker', ascending=True)
    