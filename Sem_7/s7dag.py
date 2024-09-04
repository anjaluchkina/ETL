from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pendulum
import requests
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
dag1 = DAG('My_1',
default_args=default_args,
description="seminar_6",
catchup=False,
schedule_interval='0 6 * * *')
task1 = BashOperator(
task_id='pyspark',
bash_command='export SPARK_HOME=/home/spark && export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && python3 /home/anna/s6.py',
dag=dag1)
task2 = BashOperator(
task_id='spark',
bash_command='export SPARK_HOME=/home/spark && export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && spark-shell -i /home/anna/s6s1.scala',
dag=dag1)
task2 >> task1
#DAG3
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import inspect,create_engine
from dateutil.relativedelta import relativedelta
from datetime import datetime
from pandas.io import sql
import time
#pip install openpyxl
dag3 = DAG('My_3',
default_args=default_args,
description="seminar_6",
catchup=False,
schedule_interval='0 8 * * *')
def hello(**kwargs):
    encoding = "ISO-8859-1"
    print('Hello from {kw}'.format(kw=kwargs['my_keyword']))
    
    df = 5 + 5
    print(df)
    
    df = pd.read_excel('/home/anna/s4_2.xlsx')
    con = create_engine("mysql://Airflow:1@localhost:33061/spark")
    
    print(df)
    df['долг'] = df['Платеж по основному долгу'].cumsum()
    df['проценты'] = df['Платеж по процентам'].cumsum()
    df.to_sql('credit', con, schema='spark', if_exists='replace', index=False)
t2 = PythonOperator(
task_id='python3',
dag=dag3,
python_callable=hello,
op_kwargs={'my_keyword': 'Airflow 1234'}
)
dag11 = DAG( 'hello_world' , description= 'Hello World DAG' , 
          schedule_interval= '0 12 * * *' , 
          start_date=datetime( 2023 , 1 , 1
          ), catchup= False ) 

hello_operator = BashOperator(task_id= 'hello_task' , bash_command='echo Hello from Airflow', dag=dag11)
hello_file_operator = BashOperator(task_id= 'hello_file_task' , bash_command='sh /home/anna/s6.sh ', dag=dag11) 
skipp_operator = BashOperator(task_id= 'skip_task' , bash_command='exit 99', dag=dag11) 

hello_operator >> hello_file_operator >> skipp_operator

