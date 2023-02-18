from kafka import KafkaConsumer
import ssl
import json
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy


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

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:2312@localhost/hackaton'
db = SQLAlchemy(app)


class RealTime(db.Model):
    art = db.Column(db.String, primary_key=True)
    Сигнал1 = db.Column(db.String)
    Сигнал2 = db.Column(db.String)
    Сигнал3 = db.Column(db.String)
    Сигнал4 = db.Column(db.String)
    Название = db.Column(db.String)
    данные = db.Column(db.String)


def fetch_data(self):
    return {
        'art': self.art,
        'value': self.данные
    }

@app.route('/')
def get_first_real_time_row():
    row = RealTime.query.first()
    data = fetch_data(row)
    json_data = json.dumps(data)

    # Return the JSON response
    return jsonify(json_data)




for message in consumer:

    message_str = message.value.decode('utf-8').strip('[]')
    print(message_str)
    data = json.loads(message_str)
    df = pd.DataFrame(columns=['art', 'данные'])
    for key, value in data.items():
        df = df.append({'art': key, 'данные': value}, ignore_index=True)
    print(df)
    writer = pd.ExcelWriter('output.xlsx', engine='xlsxwriter')
    df.to_excel(writer, sheet_name='Sheet1', index=False)
    for i, col in enumerate(df.columns):
        worksheet = writer.sheets['Sheet1']
        worksheet.write(0, i, col)
        for h, value in enumerate(df[col]):
            worksheet.write(h + 1, i, value)
    writer.save()
    engine = create_engine('postgresql://postgres:2312@localhost/hackaton')
    data = pd.read_excel("output.xlsx")
    data2 = pd.read_excel("output.xlsx")
    data.to_csv('file.csv', index=False)
    data2.to_csv('file2.csv', index=False)
    data.to_sql('exg6', engine, if_exists='replace', index=False)
    data2.to_sql('gpaph', engine, if_exists='append', index=False)


    # Соединяемся с базой данных
    conn = psycopg2.connect(
        dbname="hackaton",
        user="postgres",
        password="2312",
        host="localhost",
        port="5432"
    )

    # Создаем курсор
    cur = conn.cursor()

    # Выполняем запрос
    cur.execute("""
        DROP TABLE IF EXISTS real_time;
        CREATE TABLE real_time AS
        SELECT mapping_all.art, Сигнал1, Сигнал2, Сигнал3, Сигнал4, Название, "Тип сигнала", exg6.данные
        FROM public.mapping_all JOIN public.exg6
        ON mapping_all.art = exg6.art;
    """)

    # Сохраняем изменения
    conn.commit()

    # Закрываем соединение и курсор
    cur.close()
    conn.close()












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
