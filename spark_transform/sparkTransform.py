from configparser import ConfigParser
from datetime import datetime

import os
import json
import sqlparse
import psycopg2

import pandas as pd
import numpy as np

from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from app import TransformDF, spark

def spark_trans():

#Mendapatkan informasi top produk terbanyak yang terbeli
    SparkTopSold = spark.sql("SELECT distinct Product , Type ,sum(amount_transaction) as amount_transaction FROM queryResult group by Product, Type order by amount_transaction desc limit 1").show()
    print(f"[INFO] hasil informasi top produk terbanyak yang terbeli")
    
#Mendapatkan informasi trend jumlah transaksi yang terjadi pada tiap wilayah
    SparkTransPerCountry = spark.sql("SELECT distinct Product, Type , country_customer, sum(amount_transaction) as amount_transaction FROM queryResult group by Product, Type, country_customer").show()
    print(f"***Note: Mendapatkan informasi distribusi usia dan jenis kelamin pelanggan***")
    
    return SparkTopSold, SparkTransPerCountry