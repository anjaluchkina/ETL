from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum
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
bash_command='export SPARK_HOME=/home/spark && export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && spark-shell -i /home/anna/s6s1.scala'
dag=dag1
)

#DAG2
dag2 = DAG('My_2',
default_args=default_args,
description="seminar_6",
catchup=False,
schedule_interval='0 */6 * * *')
task2 = BashOperator(
task_id='pyspark',
bash_command='python3 /home/anna/s6.py',
dag=dag2
)


task2 = BashOperator(
task_id='spark',
bash_command='export SPARK_HOME=/home/spark && export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && spark-shell -i /home/anna/s6s1.scala',
dag=dag1)
