from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

###################################
#        DAG CONFIGURATION        #
###################################

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2), 
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=6),
}

###################################
#          DAG DEFINITION         #
###################################

with DAG(
    dag_id="0_ExecuteAllPipeline", 
    default_args=default_args,
    description="Global pipeline orchestrating DataIngestion, TrustedZone, and ExploitationZone DAGs.",
    schedule_interval="@daily",
    catchup=False,
    tags=["global", "orchestration", "pipeline"],
) as dag:

    # Task to trigger the first DAG: 1_DataIngestionToLandingZone
    trigger_dag1_ingestion = TriggerDagRunOperator(
        task_id="trigger_data_ingestion_to_landing_zone",
        trigger_dag_id="1_DataIngestionToLandingZone",  # This MUST match the dag_id of your first DAG
        wait_for_completion=True,  # Wait for DAG 1 to complete before proceeding
        poke_interval=10,          # How often to check for completion (seconds)
        execution_date="{{ dag_run.logical_date }}",
        reset_dag_run=True,
       
    )

    # Task to trigger the second DAG: 2_TrustedZone
    trigger_dag2_trusted_zone = TriggerDagRunOperator(
        task_id="trigger_trusted_zone_processing",
        trigger_dag_id="2_TrustedZone",  
        wait_for_completion=True,
        poke_interval=10,
        execution_date="{{ dag_run.logical_date }}",
        reset_dag_run=True,
    )

    # Task to trigger the third DAG: 3_ExploitationZone
    trigger_dag3_exploitation_zone = TriggerDagRunOperator(
        task_id="trigger_exploitation_zone_processing",
        trigger_dag_id="3_ExploitationZone", 
        wait_for_completion=True,
        poke_interval=10,
        execution_date="{{ dag_run.logical_date }}",
        reset_dag_run=True,
    )

    # Define the execution order for the triggered DAGs
    trigger_dag1_ingestion >> trigger_dag2_trusted_zone >> trigger_dag3_exploitation_zone