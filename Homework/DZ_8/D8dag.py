import datetime
import os
import requests
import pendulum
from airflow.decorators import dag, task
from airflow.providers.telegram.operators.telegram import TelegramOperator

os.environ["no_proxy"]="*"

@dag(
    dag_id="wether-tlegram",
    schedule="@once",
    start_date=pendulum.datetime(2024, 1, 9, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)

def WetherETL():

    send_message_telegram_task = TelegramOperator(
        task_id='send_message_telegram',
        telegram_conn_id='telegram_default',
        token='7448418409:AAEkoiNVv5sUatk4VaNf-w4KoQ96ltAM3LI',
        chat_id='7448418409',
        text='Wether in Moscow \nYandex: ' + "{{ ti.xcom_pull(task_ids=['yandex_wether'],key='wether')[0]}}" + " degrees" +
        "\nOpen wether: " + "{{ ti.xcom_pull(task_ids=['open_wether'],key='open_wether')[0]}}" + " degrees",
    )

    @task(task_id='yandex_wether')
    def get_yandex_wether(**kwargs):
        ti = kwargs['ti']
        url = "https://api.weather.yandex.ru/v2/informers/?lat=55.75396&lon=37.620393"

        payload={}
        headers = {
        'X-Yandex-API-Key': '33f45b91-bcd4-46e4-adc2-33cfdbbdd88e'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        print("test")
        a=response.json()['fact']['temp']
        print(a)
        ti.xcom_push(key='wether', value=response.json()['fact']['temp'])
#        return str(a)
    @task(task_id='open_wether')
    def get_open_wether(**kwargs):
        ti = kwargs['ti']
        url = "https://api.openweathermap.org/data/2.5/weather?lat=55.749013596652574&lon=37.61622153253021&appid=2cd78e55c423fc81cebc1487134a6300"

        payload={}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)
        print("test")
        a=round(float(response.json()['main']['temp']) - 273.15, 2)
        print(a)
        ti.xcom_push(key='open_wether', value=round(float(response.json()['main']['temp']) - 273.15, 2))
#        return str(a)

    @task(task_id='save_weather')
    def get_save_weather(**kwargs):
        yandex_data = kwargs['ti'].xcom_pull(task_ids='yandex_wether', key='wether')
        open_weather_data = kwargs['ti'].xcom_pull(task_ids='open_wether', key='open_wether')

        temperature_yandex = yandex_data['temperature']
        datetime_yandex = yandex_data['datetime']
        service_yandex = 'Yandex'

        temperature_open_weather = open_weather_data['temperature']
        datetime_open_weather = open_weather_data['datetime']
        service_open_weather = 'OpenWeather'

        engine = create_engine("mysql://root:1@localhost:33061/spark")

        with engine.connect() as connection:
            connection.execute("""DROP TABLE IF EXISTS spark.`Temperature_Weather`""")
            connection.execute("""CREATE TABLE IF NOT EXISTS spark.`Temperature_Weather` (
                Service VARCHAR(255),
                Date_time TIMESTAMP,
                City VARCHAR(255),
                Temperature FLOAT,
                PRIMARY KEY (Date_time, Service)
            )COLLATE='utf8mb4_general_ci' ENGINE=InnoDB""")
            connection.execute(f"""INSERT INTO spark.`Temperature_Weather` (Date_time, City, Temperature, Service) VALUES ('{datetime_yandex}', 'Moscow', {temperature_yandex}, '{service_yandex}')""")
            connection.execute(f"""INSERT INTO spark.`Temperature_Weather` (Date_time, City, Temperature, Service) VALUES ('{datetime_open_weather}', 'Moscow', {temperature_open_weather}, '{service_open_weather}')""")
            
    @task(task_id='python_wether')
    def get_wether(**kwargs):
        print("Yandex "+str(kwargs['ti'].xcom_pull(task_ids=['yandex_wether'],key='wether')[0])+" Open "+str(kwargs['ti'].xcom_pull(task_ids=['open_wether'],key='open_wether')[0]))
    
    yandex_wether = get_yandex_wether()
    open_wether = get_open_wether()
    python_wether = get_wether()
    save_weather = get_save_weather()
    
    
    yandex_wether >> open_wether >> python_wether >> send_message_telegram_task >> save_weather

dag = WetherETL()