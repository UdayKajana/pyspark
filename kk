from datetime import datetime, timedelta
from functools import partial
import pendulum
from airflow import DAG, AirflowException
import os
import sys
import yaml
import logging
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator

# Constants
BASE_DIR = "/home/airflow/gcs/dags/vz-it-gudv-dtwndo-0/bq_spanner/config"
sys.path.append(f"{BASE_DIR}")

trans_date = '{{ (data_interval_end - macros.timedelta(days=1)).strftime("%Y-%m-%d") }}'

def load_configs():
    try:
        project = os.environ.get('GCP_PROJECT')
        if not project:
            raise AirflowException("GCP_PROJECT environment variable not set")
        logging.info(f"Loading configurations for project: {project}")
        with open(f"{BASE_DIR}/base_config.yaml", 'r') as file:
            base_config = yaml.safe_load(file)
            logging.info(f"Loaded base config: {base_config}")

        # Load DAG config
        with open(f"{BASE_DIR}/ug_imsi_level_ericsson_raw_v1.yaml", 'r') as file:
            dag_config = yaml.safe_load(file)
            logging.info(f"Loaded DAG config: {dag_config}")

        config_values = {}
        
        filtered_base_dict = dict(filter(lambda elem: elem[0] == project, base_config.items()))
        if not filtered_base_dict:
            raise AirflowException(f"No base configuration found for project {project}")
        
        filtered_dict = dict(filter(lambda elem: elem[0] == project, dag_config.items()))
        if not filtered_dict:
            raise AirflowException(f"No DAG configuration found for project {project}")

        base_value = filtered_base_dict[project][0]
        app_value = filtered_dict[project][0]
        
        config_values = {**config_values, **base_value, **app_value}
        logging.info(f"Final merged config: {config_values}")
        return config_values
    except Exception as e:
        logging.error(f"Error loading configurations: {str(e)}")
        raise AirflowException(f"Configuration loading failed: {str(e)}")

config_values = load_configs()
# query = str(config_values['query']).replace("current_date_value",trans_date)
# logging.info((query+"\n")*10)

default_args = {
    'owner': 'dtwin',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2023, 2, 16, 23, tz="UTC"),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}
def print_query():
    logging.info( f"""{config_values['query']} DATE('{{ (data_interval_end - macros.timedelta(days=1)).strftime("%Y-%m-%d") }}')""")

def params_print_fun(**kwargs):
    print("---------Printing All Job Params---------------")
    print(kwargs)

dt_val = datetime.strptime('{{(data_interval_end - macros.timedelta(days=2, hours=1)).strftime("%Y-%m-%d %H:%M:%S")}}','%Y-%m-%d %H:%M:%S')
str_query = str(config_values['query']).replace('CURRENT_DATE_VALUE', f"""'{dt_val.strftime("%Y-%m-%d")}'""") 
str_query = str(config_values['query']).replace('CURRENT_DT_HR_VALUE', f"""'{dt_val.strftime("%Y%m%d%H")}'""")
job_params = {
     "query": str_query,
}
# "query2": f"""{config_values['query']} DATE('{{ data_interval_end.strftime("%Y-%m-%d") }}')""",
with DAG(
    dag_id=config_values['dag_id'],
    schedule_interval= None if config_values['schedule_interval']=='None' else config_values['schedule_interval'],
    catchup=False,
    default_args=default_args,
    description='Wireline Model Scores Processing DAG',
    concurrency=1,
    max_active_runs=1,
    tags=["dtwin","bq_spanner"]
) as dag:
    start = PythonOperator(task_id='start',
                           python_callable=params_print_fun,
                           op_kwargs= job_params,
                           provide_context=True)
    
    trigger_template_task = DataflowTemplatedJobStartOperator(
        task_id=config_values['task_id'],
        template=config_values['template_path'],
        parameters=job_params,
        location=config_values['region'],
        project_id=config_values['project_id'],
        gcp_conn_id=config_values.get('google_cloud_conn_id', 'google_cloud_default'),
        job_name=config_values['job_name'],
        wait_until_finished=True,
        environment={
            "numWorkers": str(config_values.get('num_workers', 1)),
            "maxWorkers": str(config_values.get('max_workers', 1)),
            "tempLocation": config_values['temp_location'],
            # "machineType": config_values.get('worker_machine_type', 'n1-standard-2'),
            "network": config_values['network'],
            'subnetwork': config_values['subnetwork'],
            "kmsKeyName": config_values['kmsKeyName'],
            "workerRegion": config_values['region'],
            "enableStreamingEngine": True,
            "ipConfiguration": "WORKER_IP_PRIVATE",
            "serviceAccountEmail": config_values['service_account_email'],
        },
        dag=dag
    )
    end = DummyOperator(task_id='end')
    start >> trigger_template_task >> end
raise ValueError("time data %r does not match format %r" %
ValueError: time data '{{(data_interval_end - macros.timedelta(days=2, hours=1)).strftime("%Y-%m-%d %H:%M:%S")}}' does not match format '%Y-%m-%d %H:%M:%S'
