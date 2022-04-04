# -*- coding: utf-8 -*-
"""
@author:
    leonardo.sarmanho
@Description:
    Module to work with DB
    Engine: SQL Server
"""
import yaml
import numpy as np
import pandas as pd

import sqlalchemy
import MySQLdb


def build_engine(interface_type="engine",db_name=None,config_dict={},dbcredential="dbcredential.conf"):
    """
    Create types of connections
    Inputs:
        conf_dict - connection dictonary
        cred_file - credential file
            "dbcredential.conf" (default)
        interface_type - conn types
            "engine" (default), "conn", "cursor"
    Outputs:
        sql engine
        sql connection
    """
    try:
        if(config_dict=={}):

            dbconffile=yaml.safe_load(open(dbcredential))#read_conf(dbcredential)
            config_dict = {
                    'drivername':'mysql',
                    'username': dbconffile['db_user'],
                    'password': dbconffile['db_password'],
                    'host': dbconffile['db_host'],
                    'database': dbconffile['db_name'],
                }

        if(db_name!=None):
            config_dict['database']=db_name

        ENGINE = sqlalchemy.create_engine(sqlalchemy.engine.url.URL(**config_dict))
        if(interface_type=="engine"):
            return ENGINE

        elif(interface_type=="conn"):
            return MySQLdb.connect(config_dict['host'],config_dict['username'],config_dict['password'],config_dict['database'])

        elif(interface_type=="cursor"):
            return ENGINE.raw_connection().cursor()

    except Exception as e:
        print(f"Fail to create connection: {e}")

def get_pk(table_name,CURSOR):
    pk_str = """SHOW COLUMNS FROM {TABLENAME} where `Key` = 'PRI';""".format(TABLENAME="`"+table_name+"`")
    CURSOR.execute(pk_str)
    pk_result = CURSOR.fetchall()

    return [tup[0] for tup in pk_result]


def sql_raw_exec(query_string,db_name=None,config_dict={},dbcredential='dbcredential.conf'):
    """
    execute raw queries. If is a select, return pandas dataframe
    Inputs:
        query_string='SELECT * FROM tablename'
        db_name (optional)
            if none, read
        dbcredential='dbcredential.conf' (default)
    """
    output_table = pd.DataFrame()
    try:
        #Open Connection
        # CONN = build_engine("conn",db_name,config_dict,dbcredential)
        # query_result = CONN.execute(query_string)
        ENGINE = build_engine("engine",db_name,config_dict,dbcredential)
        CONN = ENGINE.connect()
        query_result = CONN.execute(query_string)

        # Check execution type
        if(query_result.returns_rows):
            output_table=pd.DataFrame(query_result.fetchall(),columns=query_result.keys())

        CONN.close()
    except Exception as error :
        print("Erro exec query: "+ str(error))

    finally:
        return output_table


def sql_insert(insert_df, table_name, cols_df=[], db_name=None, dbcredential='dbcredential.conf',config_dict={}):
    try:
        # List all columns
        if (cols_df==[]):
            cols_df = list(insert_df.columns)

        insert_str = """INSERT INTO {MyTable} ({DF_COLS}) VALUES ({INSERT_COLS});
        """.format(MyTable = "`"+table_name+"`",
            DF_COLS = ", ".join(["`"+x+"`" for x in cols_df]),
            INSERT_COLS =','.join(['%s']*len(cols_df)))


        # Build CURSOR
        CONN = build_engine("conn", db_name=db_name,config_dict=config_dict, dbcredential=dbcredential)
        CURSOR = CONN.cursor()

        # old inserting
        #insert_df = insert_df.where(pd.notnull(insert_df), None)
        #rows_to_insert = list(insert_df[cols_df].replace({np.nan: None}).itertuples(index=False,name=None))

        # Build rows to insert
        df_to_tup = list(insert_df[cols_df].itertuples(index=False,name=None))
        rows_to_insert = [tuple(None if pd.isna(i) else i for i in t)for t in df_to_tup]

        # Execute insert
        CURSOR.executemany(insert_str,rows_to_insert)
        CONN.commit()
        CONN.close()
        return True

    except Exception as e:
        print("Fail to insert: {}".format(str(e)))
        #CONN.rollback()
        CONN.close()

        return False


def sql_upsert(upsert_df, table_name, cols_df=[], db_name=None, dbcredential='dbcredential.conf',config_dict={}):
    try:
        # List all columns
        if (cols_df==[]):
            cols_df = list(upsert_df.columns)


        update_str = " , ".join(["`{a}` = VALUES(`{a}`)".format(a=i) for i in cols_df])

        upsert_str = """INSERT INTO {MyTable} ({Cols}) VALUES ({IntNum})
        ON DUPLICATE KEY UPDATE {UPDATE_STR}""".format(MyTable = "`"+table_name+"`",
            Cols = ", ".join(["`"+x+"`" for x in cols_df]),
            IntNum =','.join(['%s']*len(cols_df)),
            UPDATE_STR=update_str
            )
        upsert_str = upsert_str +";"

        # Build CURSOR
        CONN = build_engine("conn", db_name=db_name,config_dict=config_dict, dbcredential=dbcredential)
        CURSOR = CONN.cursor()

        # old inserging
        #upsert_df = upsert_df.where(pd.notnull(upsert_df), None)
        #rows_to_upsert = list(upsert_df[cols_df].replace({np.nan: None}).itertuples(index=False,name=None))

        # Build rows to insert
        df_to_tup = list(upsert_df[cols_df].itertuples(index=False,name=None))
        rows_to_upsert = [tuple(None if pd.isna(i) else i for i in t)for t in df_to_tup]

        #Execute upsert
        CURSOR.executemany(upsert_str,rows_to_upsert)
        CONN.commit()
        CURSOR.close()
        CONN.close()
        return True

    except Exception as e:
        print("Fail to upsert: {}".format(str(e)))
        #CONN.rollback()
        CURSOR.close()
        CONN.close()

        return False

def sql_replace(replace_df, table_name, cols_df=[], db_name=None, dbcredential='dbcredential.conf',config_dict={}):

    try:
        # List all columns
        if (cols_df==[]):
            cols_df = list(replace_df.columns)

        insert_str = """REPLACE INTO {MyTable} ({Cols}) VALUES ({IntNum});
        """.format(MyTable ="`"+table_name+"`",
            Cols = ", ".join(["`"+x+"`" for x in cols_df]),
            IntNum =','.join(['%s']*len(cols_df)))

        # Build CURSOR
        CONN = build_engine("conn", db_name=db_name, config_dict=config_dict, dbcredential=dbcredential)
        CURSOR = CONN.cursor()


        # old replacing
        #replace_df = replace_df.where(pd.notnull(replace_df), None)
        #replace_df = list(replace_df[cols_df].itertuples(index=False,name=None))

        # Build  rows to insert
        df_to_tup = list(replace_df[cols_df].itertuples(index=False,name=None))
        rows_to_insert = [tuple(None if pd.isna(i) else i for i in t)for t in df_to_tup]

        # Execute replace
        CURSOR.executemany(insert_str,rows_to_insert)
        CONN.commit()
        CONN.close()
        return True

    except Exception as e:
        print("Fail to replace: {}".format(str(e)))
        #CONN.rollback()
        CONN.close()

        return False

def sql_update(update_df, table_name,db_name=None, dbcredential='dbcredential.conf',config_dict={}):

    try:
        # Build CURSOR
        CONN = build_engine("conn", db_name=db_name, config_dict=config_dict, dbcredential=dbcredential)
        CURSOR = CONN.cursor()

        # Get Columns
        df_cols = list(update_df.columns)
        pk_cols = get_pk(table_name,CURSOR)
        update_cols = list(set(df_cols) - set(pk_cols))
        update_df = update_df[update_cols+pk_cols]

        # build str
        update_str = " , ".join(["`{a}` = %s".format(a=i) for i in update_cols])
        pk_str = " AND ".join(["`{a}` = %s".format(a=i) for i in pk_cols])

        update_str = """UPDATE {MyTable} SET {UPDATE_COLS} WHERE ({PK_COLS});
        """.format(MyTable = "`"+table_name+"`",
        UPDATE_COLS = update_str,
        PK_COLS = pk_str)

        # old updating
        #update_df = update_df.where(pd.notnull(update_df), None)
        #rows_to_update = list(update_df.replace({np.nan: None}).itertuples(index=False,name=None))

        # Build  rows to insert
        df_to_tup = list(update_df.itertuples(index=False,name=None))
        rows_to_update = [tuple(None if pd.isna(i) else i for i in t)for t in df_to_tup]

        #Execute update
        CURSOR.executemany(update_str,rows_to_update)
        CONN.commit()
        CONN.close()
        return True

    except Exception as e:
        print("Fail to update: {}".format(str(e)))
        #CONN.rollback()
        CONN.close()

        return False