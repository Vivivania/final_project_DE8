#!/usr/bin/python3

import os
import json
import psycopg2

from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark import SparkContext
from kafka import KafkaConsumer

def config(param):
    path = os.getcwd()
    with open(path+'/'+'config.json') as file:
        conf = json.load(file)[param]
    return conf

def psql_conn(conf):
    try:
        conn = psycopg2.connect(host=conf['host'], 
                                database=conf['db'], 
                                user=conf['user'], 
                                password=conf['pwd']
                                )
        print(f"[INFO] Success connect PostgreSQL .....")
        engine = create_engine(f"postgresql+psycopg2://{conf['user']}:{conf['pwd']}@{conf['host']}/{conf['db']}")
        return conn, engine
    except:
        print(f"[INFO] Can't connect PostgreSQL .....")

# def hadoop_conn(conf):
#     client = conf['client']
#     try:
#         conn = hdfs.InsecureClient(client)
#         print(f"[INFO] Success connect HADOOP .....")
#         return conn
#     except:
#         print(f"[INFO] Can't connect HADOOP .....")

def spark_conn(app, config):
    master = config['ip']
    try:
        spark = SparkSession.builder \
            .master(master) \
                .appName(app) \
                    .getOrCreate()
                    
        print(f"[INFO] Success connect SPARK ENGINE .....")
        return spark
    except:
        print(f"[INFO] Can't connect SPARK ENGINE .....")

def kafka_conn():

    if __name__ == "__main__":
        print("starting the consumer")
        path = os.getcwd()+"/"

    #connect kafka server
    try:
        consumer = KafkaConsumer("final-project", bootstrap_servers='localhost')
        print(f"[INFO] Successfully Connect Kafka Server .....")
    except:
        print(f"[INFO] Error Connect Kafka Server .....")