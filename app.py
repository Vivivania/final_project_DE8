#!/usr/bin/python3

from configparser import ConfigParser
from datetime import datetime
from kafka import KafkaConsumer

import os
import json
import sqlparse

import pandas as pd
import numpy as np

import connection
import conn_warehouse
import consumer

if __name__ == '__main__':
    filetime = datetime.now().strftime('%Y%m%d')
    print(f"[INFO] Service ETL is Starting .....")

    #connect db warehouse
    conn_dwh, engine_dwh  = conn_warehouse.conn()
    cursor_dwh = conn_dwh.cursor()

    #connect db source
    conf = connection.config('postgresql')
    conn, engine = connection.psql_conn(conf)
    cursor = conn.cursor()

    #connect kafka
    #conf = connection.kafka_conn()
    #consumer = KafkaConsumer("final-project", bootstrap_servers='localhost')

    #connect spark
    conf = connection.config('spark')
    spark = connection.spark_conn(app="etl",config=conf)

    # # #connect hadoop
    # conf = connection.config('hadoop')
    # client = connection.hadoop_conn(conf)

    #query extract db source
    path_query = os.getcwd()+'/queries/'
    query = sqlparse.format(
        open(
            path_query+'join_query.sql','r'
            ).read(), strip_comments=True).strip()

    #query load db warehouse
    query_dwh = sqlparse.format(
        open(
            path_query+'dwh_design.sql','r'
            ).read(), strip_comments=True).strip()

    try:
        print(f"[INFO] Service ETL is Running .....")
        df = pd.read_sql(query, engine)

        # #upload local
        # path = os.getcwd()
        # directory = path+'/'+'local'+'/'
        # if not os.path.exists(directory):
        #     os.makedirs(directory)
        # df.to_csv(f"{directory}tb_orders_{filetime}.csv", index=False)
        # print(f"[INFO] Upload Data in LOCAL Success .....")

        #insert dwh
        cursor_dwh.execute(query_dwh)
        conn_dwh.commit()
        df.to_sql('tb_orders', engine_dwh, if_exists='append', index=False)
        print(f"[INFO] Update DWH Success .....")


        #spark processing

        TransformDF = spark.createDataFrame(df)
        TransformDF.createOrReplaceTempView("queryResult")

        #spark transform
        #Mendapatkan informasi top produk terbanyak yang terbeli
        SparkTopSold = spark.sql("SELECT distinct Product , Type ,sum(amount_transaction) as amount_transaction FROM queryResult group by Product, Type order by amount_transaction desc limit 1").show()
        print(f"***Note: Mendapatkan hasil informasi top produk terbanyak yang terbeli***")

        #Mendapatkan informasi trend jumlah transaksi yang terjadi pada tiap wilayah
        SparkTransPerCountry = spark.sql("SELECT distinct Product, Type , country_customer, sum(amount_transaction) as amount_transaction FROM queryResult group by Product, Type, country_customer").show()
        print(f"***Note: Mendapatkan informasi trend jumlah transaksi yang terjadi pada tiap wilayah***")

        #Mendapatkan informasi distribusi usia dan jenis kelamin pelanggan


        # TotalTrans = spark.createDataFrame(df2)
        # TotalTrans.groupBy("order_date").sum("order_total") \
        #      .toPandas() \
        #          .to_csv(f"TotalTransactionPerMonth.csv", index=False)

        print(f"[INFO] Service ETL is Success .....")
    except:
        print(f"[INFO] Service ETL is Failed .....")