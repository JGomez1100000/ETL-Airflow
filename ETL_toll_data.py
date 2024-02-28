import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

# DAG arguments

default_args={'owner' : 'Javi',
                'start_date' : days_ago(0),
                'email' : 'javiergomezbiedma@gmail.com',
                'email_on_failure' : True,
                'email_on_retry' : True,
                'retries' : 1,
                'retry_delay' : timedelta(minutes=5)}


# DAG definition

dag = DAG(
    
    'ETL_toll_data',

    description='Apache Airflow Final Assignment',

    default_args=default_args,

    schedule_interval=timedelta(days=1)
)


# Tasks
'''
change_directory = BashOperator(
    task_id='change_directory',
    bash_command= 'cd /mnt/c/Users/Javi/workspace/airflow/dags/finalassignment/staging',
    dag=dag
)
'''



unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command= 'tar -xvzf tolldata.tgz ',
    dag=dag,
    cwd='/opt/airflow/dags/finalassignment/staging'
)
# C:\Users\Javi\workspace\airflow\dags\tolldata.tgz

# Extract data from different sources and consolidate it in one file

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command= 'cut -d"," -f1-4 vehicle-data.csv > csv_data.csv',
    dag=dag,
    cwd='/opt/airflow/dags/finalassignment/staging'
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command= 'cat tollplaza-data.tsv | tr "\t" "," | cut -d"," -f5-7 > tsv_data.csv',
    dag=dag,
    cwd='/opt/airflow/dags/finalassignment/staging'
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command= 'cat payment-data.txt | tr -s "[:space:]" | tr " " "," | cut -d"," -f11-12 > fixed_width_data.csv',
    dag=dag,
    cwd='/opt/airflow/dags/finalassignment/staging'
)

consolidate = BashOperator(
    task_id='consolidate_data_from_different_sources',
    bash_command= 'paste -d"," csv_data.csv fixed_width_data.csv tsv_data.csv > extracted_data.csv',
    dag=dag,
    cwd='/opt/airflow/dags/finalassignment/staging'
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command= 'tr "[a-z]" "[A-Z]" < extracted_data.csv > transformed_data.csv',
    dag=dag,
    cwd='/opt/airflow/dags/finalassignment/staging'
)


unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate >> transform_data