from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pendulum
import requests
import openpyxl
import pandas as pd
from sqlalchemy import create_engine

default_args = {
'owner': 'Anna',
'depends_on_past': False,
'start_date': pendulum.datetime(year=2022, month=6, day=1).in_timezone('Europe/Moscow'),
'email': ['anna@anna.ru'],
'email_on_failure': False,
'email_on_retry': False,
'retries': 0,
'retry_delay': timedelta(minutes=5)

}

#DAG1
dag1 = DAG('DZ_t1',
default_args=default_args,
description="DZ",
catchup=False,
schedule_interval='0 6 * * *')

DZ_t1  = BashOperator(
task_id='pyspark',
bash_command='export SPARK_HOME=/home/spark && export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && python3 /home/anna/DZ_7.py',
dag=dag1)

DZ_t1


#DAG2
dag2 = DAG('DZ',
default_args=default_args,
description="DZ",
catchup=False,
schedule_interval='0 */6 * * *')
def hello(**kwargs):
    print('Hello from {kw}'.format(kw=kwargs['my_keyword']))
    
    # Читаем данные
    DZ_7 = pd.read_excel('/home/anna/DZ_7.xlsx')
    con = create_engine("mysql://Airflow:1@localhost:33061/spark")
    
    # Генерация данных для трех случаев выплат
    # 1. 360 месяцев без долгосрочного погашения
    data_case_1 = {
        'number': range(1, 361),  # 360 месяцев
        'Month': [f'Month {i}' for i in range(1, 361)],
        'Payment amount': 10000,  # Примерная сумма платежа
        'Payment of the principal debt': 0,
        'Payment of interest': 10000 * 0.05,  # 5% от платежа как процент
        'Balance of debt': 3600000,  # Исходный долг
        'interest': 0.05,  # Ставка процента
        'debt': 3600000,
    }
    
    # 2. 120 месяцев с погашением 150 тысяч рублей
    data_case_2 = {
        'number': range(1, 121),  # 120 месяцев
        'Month': [f'Month {i}' for i in range(1, 121)],
        'Payment amount': 150000,
        'Payment of the principal debt': 150000,
        'Payment of interest': (3600000 * 0.05) / 12,  # Процент на оставшийся долг
        'Balance of debt': [3600000 - (i * 150000) for i in range(1, 121)],
        'interest': 0.05,
        'debt': [3600000 - (i * 150000) for i in range(1, 121)],
    }
    
    # 3. 120 месяцев с погашением 120 тысяч рублей
    data_case_3 = {
        'number': range(1, 121),  # 120 месяцев
        'Month': [f'Month {i}' for i in range(1, 121)],
        'Payment amount': 120000,
        'Payment of the principal debt': 120000,
        'Payment of interest': (3600000 * 0.05) / 12,  # Процент на оставшийся долг
        'Balance of debt': [3600000 - (i * 120000) for i in range(1, 121)],
        'interest': 0.05,
        'debt': [3600000 - (i * 120000) for i in range(1, 121)],
    }
    
    # Создание DataFrame для каждого случая
    df_case_1 = pd.DataFrame(data_case_1)
    df_case_2 = pd.DataFrame(data_case_2)
    df_case_3 = pd.DataFrame(data_case_3)
    
    # Объединение всех DataFrame в один
    all_cases = pd.concat([df_case_1, df_case_2, df_case_3], ignore_index=True)

    # Обработка данных
    all_cases['Payment of the principal debt'] = all_cases['Payment of the principal debt'].cumsum() 
    all_cases['Payment of interest'] = all_cases['Payment of interest'].cumsum()  
    
    # Округление накопленных значений
    all_cases['Payment of interest'] = all_cases['Payment of interest'].round()
    all_cases['Payment of the principal debt'] = all_cases['Payment of the principal debt'].round()
    
    # Сохранение обновлённого DataFrame в таблицу 'DZ_7' в базе данных
    all_cases.to_sql('DZ_7', con, schema='spark', if_exists='replace', index=False)
  
  
t2 = PythonOperator(
    task_id='python3',
    dag=dag2,
    python_callable=hello,
    op_kwargs={'my_keyword': 'Airflow 1234'}
)



hello_operator = BashOperator(task_id='hello_task', bash_command='echo Hello from Airflow', dag=dag2)
hello_file_operator = BashOperator(task_id='hello_file_task', bash_command='sh /home/anna/s6.sh', dag=dag2)
skip_operator = BashOperator(task_id='skip_task', bash_command='exit 99', dag=dag2)

hello_operator >> hello_file_operator >> skip_operator

