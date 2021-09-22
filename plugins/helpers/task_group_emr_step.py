import logging
from typing import Sequence
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

def scaffold_emr_step(
    dag: DAG,
    step_name: str,
    job_flow_id: str,
    aws_conn_id: str,
    command_args: Sequence[str],
) -> TaskGroup:
    """
    Generate an Airflow Task Group
    To run an EMR step (on an existing EMR cluster)
    and wait for it to finish
    
    example for command_args:
    [
        "s3-dist-cp",
        "--src=s3://{{ params.BUCKET_NAME }}/data",
        "--dest=/movie",
    ]
    another example
    [
        "spark-submit",
        "--deploy-mode",
        "client",
        "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
    ]
    """
    run_task_id = f'step-{step_name}-run'
    group_id = f'step-{step_name}'
    with TaskGroup(group_id=group_id, dag=dag) as tg:
        
        # Add your steps to the EMR cluster
        step_adder = EmrAddStepsOperator(
            dag=dag,
            task_id=run_task_id,
            job_flow_id=job_flow_id,
            aws_conn_id=aws_conn_id,
            steps=[{
                "Name": step_name,
                "ActionOnFailure": "CANCEL_AND_WAIT",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": command_args,
                },
            }],
        )

        step_id = f"{{{{ task_instance.xcom_pull(task_ids='{group_id}.{run_task_id}', key='return_value')[0] }}}}"
        
        logging.info('checking step %s', step_id)

        # wait for the steps to complete
        step_checker = EmrStepSensor(
            dag=dag,
            task_id=f'step-{step_name}-checker',
            job_flow_id=job_flow_id,
            step_id=step_id,
            aws_conn_id=aws_conn_id,
        )

        step_adder >> step_checker
    return tg