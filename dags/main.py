from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from src.model_development import (
    load_data,
    data_preprocessing,
    separate_data_outputs,
    build_model,
    evaluate_model,
    load_model,
)
from src.quality_gate import check_model_quality, notify_rejection

default_args = {
    "start_date": pendulum.datetime(2024, 1, 1, tz="UTC"),
    "retries": 0,
}

dag = DAG(
    dag_id="Airflow_Lab2",
    default_args=default_args,
    description="Airflow-Lab2 DAG Description, with a model quality gate",
    schedule="@daily",
    catchup=False,
    tags=["example"],
    owner_links={"Ramin Mohammadi": "https://github.com/raminmohammadi/MLOps/"},
    max_active_runs=1,
)

owner_task = BashOperator(
    task_id="task_using_linked_owner",
    bash_command="echo 1",
    owner="Ramin Mohammadi",
    dag=dag,
)

load_data_task = PythonOperator(
    task_id="load_data_task",
    python_callable=load_data,
    dag=dag,
)

data_preprocessing_task = PythonOperator(
    task_id="data_preprocessing_task",
    python_callable=data_preprocessing,
    op_args=[load_data_task.output],
    dag=dag,
)

separate_data_outputs_task = PythonOperator(
    task_id="separate_data_outputs_task",
    python_callable=separate_data_outputs,
    op_args=[data_preprocessing_task.output],
    dag=dag,
)

build_save_model_task = PythonOperator(
    task_id="build_save_model_task",
    python_callable=build_model,
    op_args=[separate_data_outputs_task.output, "model.sav"],
    dag=dag,
)

evaluate_model_task = PythonOperator(
    task_id="evaluate_model_task",
    python_callable=evaluate_model,
    op_args=[separate_data_outputs_task.output, "model.sav"],
    dag=dag,
)

check_quality_branch = BranchPythonOperator(
    task_id="check_quality_branch",
    python_callable=check_model_quality,
    dag=dag,
)

load_model_task = PythonOperator(
    task_id="load_model_task",
    python_callable=load_model,
    op_args=[separate_data_outputs_task.output, "model.sav"],
    dag=dag,
)

model_rejected_task = PythonOperator(
    task_id="model_rejected_task",
    python_callable=notify_rejection,
    dag=dag,
)

trigger_dag_task = TriggerDagRunOperator(
    task_id="my_trigger_task",
    trigger_dag_id="Airflow_Lab2_Flask",
    conf={"message": "Data from upstream DAG"},
    reset_dag_run=False,
    wait_for_completion=False,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

owner_task >> load_data_task >> data_preprocessing_task >> \
    separate_data_outputs_task >> build_save_model_task >> \
    evaluate_model_task >> check_quality_branch

check_quality_branch >> load_model_task >> trigger_dag_task
check_quality_branch >> model_rejected_task