#!python3

import pandas as pd
import numpy as np

from sqlalchemy import create_engine

if __name__ == "__main__":
    
    username = 'postgres'
    password = 'postgres'
    database = 'postgres'
    ip       = 'localhost'


    try:

        engine = create_engine(f"postgresql://{username}:{password}@{ip}:5432/{database}")
        print(f"[INFO] Sucess Connect PostgreSQL....")
    except:
        print(f"[INFO] Error Connect PostgreSQL....")

    listf_filename =['customer','product','transaction']
    for file in listf_filename:
        pd.read_csv(f"bigdata_{file}.csv").to_sql(f"bigdata_{file}", con=engine)
        print(f"[INFO] Sucess Dump File {file}......")