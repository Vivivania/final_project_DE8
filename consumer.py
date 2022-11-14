#!python3

# import os
# import json
# import pandas

# from kafka import KafkaConsumer
# from sqlalchemy import create_engine

# def transformStream(df):
#     df = df \
#             .groupby(['Id','devideId','logActivity']) \
#             .agg({'logTimestamp':['sum','count']}) \
#             .reset_index()
#     df.columns = ['Id','newbalanceDest','device', 'timeformat1', 'timeformat2']
    
#     return df.head(1)

# if __name__ == "__main__":
#     print("starting the consumer")
#     path = os.getcwd()+"/"

    # #connect database
    # try:
    #     engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')
    #     print(f"[INFO] Successfully Connect Database .....")
    # except:
    #     print(f"[INFO] Error Connect Database .....")

    #connect kafka server
    # try:
    #     consumer = KafkaConsumer("final-project", bootstrap_servers='localhost')
    #     print(f"[INFO] Successfully Connect Kafka Server .....")
    # except:
    #     print(f"[INFO] Error Connect Kafka Server .....")

    #read message from topic kafka server
    # for msg in consumer:
    #     data = json.loads(msg.value)
    #     print(f"Records = {json.loads(msg.value)}")
        
        # try:
        #     #checking data
        #     df = pandas.read_sql_query(f"""
        #                                 select * 
        #                                 from user_activity 
        #                                 where user_activity."Id" = {data['Id']};""", engine)
        # except:
        #     #create dataframe 
        #     df = pandas.DataFrame(data, index=[0])

        #insert database   
        # df = pandas.DataFrame(data, index=[0])
        # df.to_sql('user_activity', engine, if_exists='append', index=False)

        # #transfrom "Fraud Detection"
        # status = FraudModel.runModel(transformStream(df), path)
        # print(f"User Predict: {status}")

        # #insert predictioni to database
        # pandas \
        #     .DataFrame({'userId':[data['Id']], 'userFlag':[status]})  \
        #     .to_sql('user_fraud',  engine, if_exists='append', index=False)