import pandas as pd
import sqlalchemy


class ReadTxt:

    def __init__(self, path):
        data = pd.read_csv(path, sep='\t')
        self.textResult = data
        
        
class ReadExcelDataFrame:

    def __init__(self, path, sheet):
        data = pd.read_excel(path, sheet)
        self.excelResult = data


class InsertDataFrameIntoMySQL:

    def __init__(self):
        # MYSQL CONNECTION
        try:
            self.engine = sqlalchemy.create_engine("mysql://jardieljunior:1234@192.168.1.189/credquant_prices")
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
            self.engine = sqlalchemy.create_engine("mysql://jardieljunior:1234@192.168.1.189/credquant_prices")
            self.db_connection = self.engine.connect()
            
        except:
            print('ERROR: Mysql connection')

        self.querying = self.db_connection.execute(query)     
       
        self.queryResult = pd.DataFrame(self.querying.fetchall())
        if len(self.queryResult)!=0:            
            self.queryResult.columns = self.querying.keys()
        else:
            self.queryResult = pd.DataFrame(columns = self.querying.keys())