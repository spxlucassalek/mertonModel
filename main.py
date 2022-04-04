# -*- coding: utf-8 -*-
"""
Created on Sat May 16 22:56:57 2020

@author: jardiel.junior
"""

pass
import readData
import merton
import creditGrade
import utils
import pandas as pd
import numpy as np
from pathos.multiprocessing import ProcessingPool
from functools import partial

if __name__ == '__main__':

    df_unfiltered = readData.getMarketDataNew()
     
    total = len(df_unfiltered['ticker'].unique())
    
    print(total)
    # df_unfiltered = df_unfiltered.sort_values('ticker', ascending=True).reset_index(drop=True)
    
    # cut = df_unfiltered.loc[df_unfiltered['ticker']=='PBI US Equity']
    
    # df_unfiltered = df_unfiltered.loc[df_unfiltered.index>9680112]
    
    # ticker = 'MSFT US Equity'
    i=0
    # list_data = []
    # for ticker in df_unfiltered['ticker'].unique():
    #     list_data.append(df_unfiltered.loc[df_unfiltered['ticker']==ticker])
    
    
    # with ProcessingPool() as pool:
    #     r = pool.map(merton.calculatePD, list_data)
    
    for ticker in df_unfiltered['ticker'].unique():
    
        print(ticker)
        
        df = df_unfiltered.loc[df_unfiltered['ticker']==ticker]
        
        pd_table_Merton = merton.calculatePD(df)
         
        utils.insertMertonModelMySQL(pd_table_Merton)
        
        utils.insertKMVModelMySQL(pd_table_Merton)
        
        utils.insertCreditGradeModelMySQL(pd_table_Merton)
        
        i=i+1
        
        print('Percentual: '+ str(np.round((i/total)*100,2)) + '%')




