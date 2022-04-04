# -*- coding: utf-8 -*-
"""
Created on Thu Aug  6 11:02:24 2020

@author: jardiel.junior
"""

import sqlalchemy
import pandas as pd
# _config_dict = {'drivername':'mysql',
#                'host': '192.168.1.62',
#                "username": 'federicofavero',
#                'password': 'risk@001',
#                'database': 'credquant_model',}
import db

def insertMertonModelMySQL(dataFrame):
    
    df = dataFrame[['date', 'ticker','PD Merton', 'z score Merton', 'EWMA' ]]
    df.rename(columns={'PD Merton': 'pd', 'z score Merton': 'distance_to_default',
                       'EWMA': 'ewma'}, inplace=True)
    # df['key'] = df['date'].apply(str) + df['ticker']
    # df.sort_values('date', ascending=True, inplace=True)
#    df = df.drop_duplicates('key', keep='last')
    
    # def getInsertedValues():
        
    #     query = 'select * from credquant_model.default_merton'
    #     inserted = MySQLQueryHom(query).queryResult
    #     inserted['key'] = inserted['date'].apply(str) + inserted['ticker']
    #     return inserted
    
    # df = df.loc[~df['key'].isin(list(getInsertedValues()['key'].unique()))]
    
    df.dropna(inplace=True)
    
    # df.drop('key', axis=1, inplace=True)
    
    
    db.sql_upsert(df,'default_merton' )
    
    # insertObj = InsertDataFrameIntoMySQLHom()
    
    
    # insertObj.insert_table(df, 'default_merton')
    
    

def insertKMVModelMySQL(dataFrame):
    
    df = dataFrame[['date', 'ticker','PD KMV','z score KMV',  'EWMA' ]]
    df.rename(columns={'PD KMV': 'pd', 'z score KMV': 'distance_to_default',
                        'EWMA': 'ewma'}, inplace=True)
    # df['key'] = df['date'].apply(str) + df['ticker']
    # df.sort_values('date', ascending=True, inplace=True)
#    df = df.drop_duplicates('key', keep='last')
    
    # def getInsertedValues():
        
    #     query = 'select * from credquant_model.default_kmv'
    #     inserted = MySQLQueryHom(query).queryResult
    #     inserted['key'] = inserted['date'].apply(str) + inserted['ticker']
    #     return inserted
    
    # df = df.loc[~df['key'].isin(list(getInsertedValues()['key'].unique()))]
    
    # df.dropna(inplace=True)
    
    # df.drop('key', axis=1, inplace=True)
    
    db.sql_upsert(df,'default_kmv')
    
    # insertObj = InsertDataFrameIntoMySQLHom()
    
    # insertObj.insert_table(df, 'default_kmv')
    
    

def insertCreditGradeModelMySQL(dataFrame):
    
    df = dataFrame[['date', 'ticker','PD Credit Grade']]
    df.rename(columns={'PD Credit Grade': 'pd'}, inplace=True)
    # df['key'] = df['date'].apply(str) + df['ticker']
    # df.sort_values('date', ascending=True, inplace=True)
#    df = df.drop_duplicates('key', keep='last')
    
    # def getInsertedValues():
        
    #     query = 'select * from credquant_model.default_credit_grade'
    #     inserted = MySQLQueryHom(query).queryResult
    #     inserted['key'] = inserted['date'].apply(str) + inserted['ticker']
        
    #     return inserted
    
    # df = df.loc[~df['key'].isin(list(getInsertedValues()['key'].unique()))]
    
    # df.dropna(inplace=True)
    
    # df.drop('key', axis=1, inplace=True)
    
    db.sql_upsert(df,'default_credit_grade')    
    # insertObj = InsertDataFrameIntoMySQLHom()
    
    # insertObj.insert_table(df, 'default_credit_grade')


class InsertDataFrameIntoMySQL:

    def __init__(self):
        # MYSQL CONNECTION
        try:
            self.engine = sqlalchemy.create_engine("mysql://jardieljunior:1234@192.168.1.189/credquant_model")
            self.db_connection = self.engine.connect()
        except:
            print('ERROR: Mysql connection')

    def insert_table(self, table, table_name):
        self.table_name = table_name
        self.table = table
        self.table.to_sql(con=self.db_connection, name=self.table_name, if_exists='append', index=False)



class InsertDataFrameIntoMySQLHom:

    def __init__(self):
        # MYSQL CONNECTION
        try:
            # self.engine = sqlalchemy.create_engine("mysql://jardieljunior:1234@192.168.1.189/investment_committee")
            self.engine = sqlalchemy.create_engine("mysql://jardieljunior:spx@123@192.168.1.62/credquant_model")
            self.db_connection = self.engine.connect()
        except:
            print('ERROR: Mysql connection')

    def insert_table(self, table, table_name):
        self.table_name = table_name
        self.table = table
        self.table.to_sql(con=self.db_connection, name=self.table_name, if_exists='append', index=False)




class MySQLQuery:
    
    def __init__(self, query):
        self.query = query 
        
        try:
            self.engine = sqlalchemy.create_engine("mysql://jardieljunior:1234@192.168.1.189/credquant_model")
            self.db_connection = self.engine.connect()
            
        except:
            print('ERROR: Mysql connection')

        self.querying = self.db_connection.execute(query)     
        
        try:
            self.queryResult = pd.DataFrame(self.querying.fetchall())
            if len(self.queryResult)!=0:            
                self.queryResult.columns = self.querying.keys()
            else:
                self.queryResult = pd.DataFrame(columns = self.querying.keys())
        except:
            self.queryResult = pd.DataFrame()
     
        
        

class MySQLQueryHom:
    
    def __init__(self, query):
        
        self.query = query 
        
        try:
            self.engine = sqlalchemy.create_engine("mysql://jardieljunior:spx@123@192.168.1.62/investment_committee")
            self.db_connection = self.engine.connect()
            
        except:
            print('ERROR: Mysql connection')

        self.querying = self.db_connection.execute(query)     
        
        try:
            self.queryResult = pd.DataFrame(self.querying.fetchall())
            if len(self.queryResult)!=0:            
                self.queryResult.columns = self.querying.keys()
            else:
                self.queryResult = pd.DataFrame(columns = self.querying.keys())
        except:
            self.queryResult = pd.DataFrame()