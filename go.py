from kafka import KafkaConsumer
import ssl
import csv
import json
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, select

host = 'rc1a-b5e65f36lm3an1d5.mdb.yandexcloud.net:9091'
topic = 'zsmk-9433-dev-01'
user = '9433_reader'
password = 'eUIpgWu0PWTJaTrjhjQD3.hoyhntiK'

SASL_MECHANISM = 'SCRAM-SHA-512'
SASL_SSL = True

ssl_context = ssl.create_default_context(cafile='CA.pem')
sasl_mechanism = SASL_MECHANISM
sasl_plain_username = user
sasl_plain_password = password

consumer = KafkaConsumer(
    topic,
    # auto_offset_reset='earliest',
    bootstrap_servers=[host],
    security_protocol='SASL_SSL',
    ssl_context=ssl_context,
    sasl_mechanism=sasl_mechanism,
    sasl_plain_username=sasl_plain_username,
    sasl_plain_password=sasl_plain_password
)


for message in consumer:

    message_str = message.value.decode('utf-8').strip('[]')
    print(message_str)
    data = json.loads(message_str)
    df = pd.DataFrame(columns=['art', 'данные'])
    for key, value in data.items():
        df = df.append({'art': key, 'данные': value}, ignore_index=True)
    print(df)
    writer = pd.ExcelWriter('output.xlsx', engine='xlsxwriter')
    writer2 = pd.ExcelWriter('graph.xlsx', engine='xlsxwriter')
    df.to_excel(writer, sheet_name='Sheet1', index=False)
    for i, col in enumerate(df.columns):
        worksheet = writer.sheets['Sheet1']
        sheet = writer2.sheets['XLS Worksheet']
        sheet.write(0, i, col)
        worksheet.write(0, i, col)
        for h, value in enumerate(df[col]):
            sheet.write(h + 1, i, value)
            worksheet.write(h + 1, i, value)
    writer2.save()

    writer.save()
    engine = create_engine('postgresql://postgres:2312@localhost/hackaton')
    data = pd.read_excel("output.xlsx")
    data2 = pd.read_excel("graph.xls")
    data.to_csv('file.csv', index=False)
    data2.to_csv('file2.csv', index=False)
    data.to_sql('exg6', engine, if_exists='replace', index=False)
    data2.to_sql('gpaph', engine, if_exists='append', index=False)



















    # DROP TABLE IF EXISTS real_time;
    # CREATE TABLE real_time AS
    # SELECT mapping6.art, Сигнал1, Сигнал2, Сигнал3, Сигнал4, Название, "Тип сигнала", exg6.данные
    # FROM public.mapping6 JOIN public.exg6
    # ON mapping6.art = exg6.art;
    # это код для обновления таблицы sql
# код для загрузки таблицы из бд в csv формате
# metadata = MetaData()
# metadata.reflect(bind=engine, schema='public')
# your_table = Table('public.real_time', metadata)
#
# conn = engine.connect()
#
# query = select(your_table)
# result = conn.execute(query)
#
# with open('data.csv', 'w') as f:
#     for row in result:
#         f.write(','.join(str(cell) for cell in row) + '\n')
