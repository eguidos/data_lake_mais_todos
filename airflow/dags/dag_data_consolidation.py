from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from src.raw.loading_file import raw_loading
from src.integration.integration import integration_loading
from src.bussiness.data_consolidation import business_loading

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(0,0,0,0,0),
    "email": ['airflow@example.com'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retires": 1,
    "retray_delay": timedelta(minutes=1)

}

with DAG(dag_id="californian_consolidated_data", default_args=default_args) as dag:
    with TaskGroup("RAW") as raw:
        raw = PythonOperator(
        task_id='RAW_DATA',
        python_callable=raw_loading,
        op_kwargs = {'url': 'https://raw.githubusercontent.com/MaisTodos/challenge-data-engineering/main/california_housing_train.csv', 
            'file': 'src/data/california_housing_train.csv', 'mode': 'wb', 'table': 'california_housing_train'},
        dag=dag) 


    with TaskGroup("integration") as integration_data:
        integration_data = PythonOperator(
        task_id='INTEGRATION_DATA',
        python_callable=integration_loading,
        op_kwargs = {'url': 'https://raw.githubusercontent.com/MaisTodos/challenge-data-engineering/main/california_housing_train.csv', 
            'file': 'src/data/california_housing_train.csv', 'mode': 'wb', 'table': 'california_housing_train'},
        dag=dag)

    with TaskGroup("business") as business_data:
        business_data = PythonOperator(
        task_id='BUSINESS_DATA',
        python_callable=business_loading,
        op_kwargs = {'url': 'https://raw.githubusercontent.com/MaisTodos/challenge-data-engineering/main/california_housing_train.csv', 
            'file': 'src/data/california_housing_train.csv', 'mode': 'wb', 'table': 'californians_consolidated_data'},
        dag=dag) 


    raw >> integration_data >> business_data