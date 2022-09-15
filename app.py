#!/usr/bin/python3

import os
import json
import sqlparse

import pandas as pd
import numpy as np

import connection
import conn_warehouse

if __name__ == '__main__':
    print(f"[INFO] Service ETL is Starting .....")

    #connect to db warehouse
    conn_dwh, engine_dwh  = conn_warehouse.conn()
    cursor_dwh = conn_dwh.cursor()

    #connect to db source
    conf = connection.config('postgresql')
    conn, engine = connection.psql_conn(conf)
    cursor = conn.cursor()

    # #connect hadoop
    conf = connection.config('hadoop')
    client = connection.hadoop_conn(conf)
    
     
    
    #query extract db source 
    path_query = os.getcwd()+'/query/'
    query = sqlparse.format(
        open(
            path_query+'query.sql','r'
            ).read(), strip_comments=True).strip()
    #query load db warehouse
    query_dwh = sqlparse.format(
        open(
            path_query+'dwh_design.sql','r'
            ).read(), strip_comments=True).strip()
    #transform etl
    try:
        print(f"[INFO] Service ETL is Running .....")
        df = pd.read_sql(query, engine)

        # #upload hadoop
        # with client.write('/digitalskola/test.csv', encoding='utf-8') as writer:
        #     df.to_csv(writer, index=False)

        path = os.getcwd()
        directory = path+'/'+'tmp_hadoop'+'/'
        if not os.path.exists(directory):
            os.makedirs(directory)
        df.to_csv(directory+'test.csv', index=False)
        
    #     cursor_dwh.execute(query_dwh)
    #     conn_dwh.commit()

    #     df.to_sql('dim_orders', engine_dwh, if_exists='append', index=False)
        print(f"[INFO] Service ETL is Success .....")
    except:
        print(f"[INFO] Service ETL is Failed .....")
    

    

    