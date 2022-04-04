# -*- coding: utf-8 -*-
"""
Created on Thu Sep  3 12:52:18 2020

@author: jardiel.junior
"""
import pandas as pd
import utils
from scipy.stats import norm
import numpy as np
import db

class CreateTable(utils.InsertDataFrameIntoMySQLHom):
    
  
    def __init__(self):
        
        merton_df = utils.MySQLQueryHom("select * from credquant_model.default_merton").queryResult
        
        credit_grade_df = utils.MySQLQueryHom("select * from credquant_model.default_credit_grade").queryResult
        
        kmv_df = utils.MySQLQueryHom("select * from credquant_model.default_kmv").queryResult
        
        merton_df['key'] = merton_df['date'].apply(str)  + merton_df['ticker'].apply(str)
        
        credit_grade_df['key'] = credit_grade_df['date'].apply(str)  + credit_grade_df['ticker'].apply(str)
        
        kmv_df['key'] = kmv_df['date'].apply(str)  + kmv_df['ticker'].apply(str)
        
        # self.getInsertedKeys()
        
#        self.merton_df = self.deleteInsertedData(merton_df)
        
        self.merton_df = merton_df
        
        self.merton_df.rename(columns={'distance_to_default': 'dd_merton',
                                        'pd': 'pd_merton'}, inplace=True)
        
        
        self.merton_df['ticker'] = self.merton_df['ticker'].apply(str.strip)
        
#        self.credit_grade_df = self.deleteInsertedData(credit_grade_df)
        
        self.credit_grade_df = credit_grade_df
        
        self.credit_grade_df.rename(columns={'pd': 'pd_credit_grade'},inplace=True)
        
        self.credit_grade_df['dd_credit_grade'] = self.credit_grade_df['pd_credit_grade'].apply(lambda x: -norm.ppf(x))
        
        
        self.credit_grade_df['ticker'] = self.credit_grade_df['ticker'].apply(str.strip)
        
        self.kmv_df = kmv_df
        
#        self.kmv_df = self.deleteInsertedData(kmv_df)
        
        

        self.kmv_df.rename(columns={'distance_to_default': 'dd_kmv',
                                              'pd': 'pd_kmv'},inplace=True)
    
        
        
        self.kmv_df['ticker'] = self.kmv_df['ticker'].apply(str.strip)
        
        self.getEquityTable()
    
        self.getCompaniesData()
        
        
        self.final_table = pd.merge(self.merton_df, self.credit_grade_df, how='inner', 
                                    left_on = ['date', 'ticker'], right_on = ['date', 'ticker'])
        try:
            self.kmv_df.drop('ewma', axis=1, inplace=True)
        except:
            pass
        
        self.final_table = pd.merge(self.final_table, self.kmv_df, how='inner', 
                                    left_on = ['date', 'ticker'], right_on = ['date', 'ticker'])
        
        self.final_table = pd.merge(self.final_table, self.companies_data,
                                    how='left', left_on = ['date', 'ticker'],
                                    right_on = ['date', 'ticker'])
      
    
        self.final_table.rename(columns={'long_comp_name': 'long_name',
                                          'industry_group': 'sector_group',
                                          'industry_sector': 'sector',
                                        'cntry_of_risk': 'country',
                                        'bs_tot_liab2': 'tot_liab',
                                        'cur_mkt_cap': 'mkt_cap'}, inplace=True)
    
        self.final_table['pd_adj'] = self.final_table['dd_merton'].apply(lambda x: self.calculateAdjPD(x))
    
    
        # utils.InsertDataFrameIntoMySQLHom.__init__(self)
        
        
        self.final_table['key'] = self.final_table['date'].apply(str) + self.final_table['ticker'].apply(str)
        
        self.final_table = self.final_table.drop_duplicates(subset=['date', 'ticker'], keep='first')
        
        # self.final_table = self.final_table[~self.final_table['key'].isin( self.inserted_list)]
        
        self.final_table.drop('key',axis=1, inplace=True)
        
        self.final_table.replace(np.inf, np.nan, inplace=True)
        
        self.final_table.replace(-np.inf, np.nan, inplace=True)
        
        
        
    
    
    @staticmethod
    def calculateAdjPD(x):
        
        if -3.290526714895756<(-3.290526714895756+((-x-(-4.26489079392283)))*abs(0-(-3.290526714895756))/(3.71901648545571-(-4.26489079392283))):
    
            return norm.cdf((-3.290526714895756+((-x-(-4.26489079392283)))*abs(0-(-3.290526714895756))/(3.71901648545571-(-4.26489079392283))))
    
        else:
            return norm.cdf(-3.290526714895756)
            
        
    
    
    def deleteInsertedData(self, dataFrame):
        dataFrame = dataFrame[~dataFrame['key'].isin(self.inserted_list)]
        
        dataFrame.drop('key', axis=1, inplace=True)
        return dataFrame
        
    
    def getInsertedKeys(self):
        
        query = 'select * from credquant_model.pd_overview'
        
        inserted = utils.MySQLQueryHom(query).queryResult
        
        inserted['key'] = inserted['date'].apply(str)  + inserted['ticker'].apply(str)
        
        self.inserted_list = list(inserted['key'].unique())
        
    
    def getEquityTable(self):
        
        query = \
            """
            SELECT * FROM cq_prices.dd_equity;
            """
            
        self.equity_data = utils.MySQLQueryHom(query).queryResult
    
        self.equity_data  = self.equity_data[['cur_mkt_cap', 'ticker', 'date']]
    
        self.equity_data['ticker'] = self.equity_data['ticker'].apply(str.strip)
        
        
    def getCompaniesData(self):
        query = \
            """
                      SELECT 
                cq_prices.dd_bala.date,
                cq_prices.equity.ticker,
                cq_prices.company.long_comp_name,
                cq_prices.company.industry_group,
                cq_prices.company.industry_sector,
                cq_prices.company.cntry_of_risk,
                cq_prices.dd_bala.bs_tot_liab2
            FROM
                cq_prices.company
                    LEFT JOIN
                cq_prices.equity ON `cq_prices`.`company`.`id_bb_company` = `cq_prices`.`equity`.`id_bb_company`
                    LEFT JOIN
                cq_prices.dd_bala ON cq_prices.dd_bala.ticker = cq_prices.equity.ticker
            """
        self.companies_data_all = utils.MySQLQueryHom(query).queryResult
        
        self.companies_data_all = self.companies_data_all.loc[~pd.isna(self.companies_data_all['ticker'])]
        self.companies_data_all = self.companies_data_all[['date', 'long_comp_name', 'industry_group', 
                            'industry_sector','cntry_of_risk', 'bs_tot_liab2',
                         'ticker']]
        
        
        self.companies_data_name = self.companies_data_all[['long_comp_name', 'industry_group', 
                            'industry_sector','cntry_of_risk', 'ticker']]
        
        self.companies_data_name = self.companies_data_name.drop_duplicates(keep='first')
        
        
        self.companies_data_name = self.companies_data_name[~pd.isna(self.companies_data_name['ticker'])]
        
        self.companies_data_name['ticker'] = self.companies_data_name['ticker'].apply(str.strip)
        
        
        self.companies_data_aux = pd.merge(self.equity_data, self.companies_data_name , how='left',
                 left_on = 'ticker', right_on='ticker')
        
        self.companies_data = pd.merge(self.companies_data_aux, self.companies_data_all[['date',  'bs_tot_liab2','ticker']], how='left',
                 left_on = ['date', 'ticker'], right_on=['date', 'ticker'])
        
        
        
        # self.companies_data = pd.merge(self.companies_data, self.equity_data, how='left',
        #          left_on = ['date', 'ticker'], right_on=['date', 'ticker'])
              
        
        
if __name__ == '__main__':
    
    
    obj = CreateTable()
    
    c = obj.final_table.columns 
    
    cols = [x for x in c if 'key' not in x]
    
    df =  obj.final_table[cols]
    
    # db.sql_upsert(obj.final_table, 'pd_overview')
    
    total = len(df['ticker'].unique())
    i=0
    for ticker in obj.final_table['ticker'].unique():
        
        db.sql_upsert(df.loc[df['ticker']==ticker], 'pd_overview')
        i=i+1
        print(i/total)
        
        
        
        
        