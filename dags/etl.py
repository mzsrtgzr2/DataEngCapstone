import os
import logging

from datetime import datetime, timedelta
from helpers.task_group_emr_step import scaffold_emr_step
from operators import UploadSourceOperator

from settings import (
    BUCKET_NAME,
    JOB_FLOW_OVERRIDES,
    DATA_SOURCES_MAP)

from airflow import DAG
from airflow.utils.task_group import TaskGroup

from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)

# Configurations
LOCAL_SCRIPTS_PATH = "./dags/scripts/spark"

AWS_CONN_ID = 'aws_default'
JOB_FLOW_ID = "{{ task_instance.xcom_pull(task_ids='create_emr_cluster_safely.create_emr_cluster', key='return_value') }}"


def _check_connectivity_to_s3(*args, **kwargs):
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    logging.info(f"Listing Keys from {BUCKET_NAME}")
    keys = hook.list_keys(BUCKET_NAME)
    for key in keys:
        logging.info(f"- s3://{BUCKET_NAME}/{key}")

    
def _local_scripts_to_s3(path, key_prefix, bucket_name=BUCKET_NAME):
    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    for filename in os.listdir(path):
        s3.load_file(
            filename=os.path.join(LOCAL_SCRIPTS_PATH, filename),
            bucket_name=bucket_name,
            replace=True,
            key=os.path.join(key_prefix, filename)
        )


default_args = {
    "owner": "Moshe Roth",
    'start_date': datetime(2019,1,1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    "spark_submit_airflow",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)

check_connectivity_to_s3 = PythonOperator(
    task_id="check_connectivity_to_s3",
    python_callable=_check_connectivity_to_s3,
    dag=dag
)

# upload_scripts_to_s3 = PythonOperator(
#     dag=dag,
#     task_id="upload_scripts_to_s3",
#     python_callable=_local_scripts_to_s3,
#     op_kwargs={"path": LOCAL_SCRIPTS_PATH, "key_prefix": 'scripts',},
# )


# upload_data_to_s3 = tuple(
#     UploadSourceOperator(
#         dag=dag,
#         task_id=f'upload-source-{name}',
#         aws_conn_id=AWS_CONN_ID,
#         source_url=src_url,
#         bucket_name=BUCKET_NAME,
#         key=f'data/{dest_key}',
#     )
#     for name, (src_url, dest_key) in DATA_SOURCES_MAP.items())


# Create an EMR cluster
with TaskGroup('create_emr_cluster_safely', dag=dag) as create_emr_cluster_safely:
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id=AWS_CONN_ID,
        dag=dag,
    )

    wait_cluster_completion = EmrJobFlowSensor(
        task_id='wait_cluster_completion',
        job_flow_id=JOB_FLOW_ID,
        aws_conn_id=AWS_CONN_ID,
        target_states=["RUNNING", "WAITING"],
        dag=dag
    )

    create_emr_cluster >> wait_cluster_completion

load_to_hadoop = scaffold_emr_step(
    dag,
    'load_to_hadoop',
    JOB_FLOW_ID,
    AWS_CONN_ID,
    [
        "s3-dist-cp",
        f"--src=s3://{BUCKET_NAME}/data",
        "--dest=/data",
    ]
)

# transform_activity = scaffold_emr_step(
#     dag,
#     'transform_activity',
#     JOB_FLOW_ID,
#     AWS_CONN_ID,
#     [
#         "spark-submit",
#         "--deploy-mode",
#         "client",
#         f"s3://{BUCKET_NAME}/scripts/job.py"
#     ]
# )


load_results_to_s3 = scaffold_emr_step(
    dag,
    'load_results_to_s3',
    JOB_FLOW_ID,
    AWS_CONN_ID,
    [
        "s3-dist-cp",
        "--src=/output",
        f"--dest=s3://{BUCKET_NAME}/output",
    ]
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id=JOB_FLOW_ID,
    aws_conn_id=AWS_CONN_ID,
    dag=dag,
)

end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)

(
start_data_pipeline >> 
    check_connectivity_to_s3 >>
    # upload_scripts_to_s3 >> 
    # upload_data_to_s3 >>
    create_emr_cluster_safely >>
    load_to_hadoop >> 
    # transform_activity >>
    load_results_to_s3 >>
    terminate_emr_cluster >> 
    end_data_pipeline
)
