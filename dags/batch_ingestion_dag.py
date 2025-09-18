from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.batch_ingestion.batch_ingestion import BatchIngestion
from utils.batch_ingestion.delta_manager import DeltaLakeManager


###################################
#        METHODS AND CONFIG       #
###################################

def create_batch_ingestion() -> BatchIngestion:
    """Factory that returns a configured BatchIngestion object."""
    dl_manager = DeltaLakeManager()
    return BatchIngestion(
        update_frequency=3600,
        delta_manager=dl_manager,
        delta_lake_batch_dir="./landing_zone/batch",
    )


def ingest_static_data() -> None:
    ingestion = create_batch_ingestion()
    ingestion.ingest_static_sources()


def ingest_api_data_openmeteo() -> None:
    ingestion = create_batch_ingestion()
    ingestion.ingest_openmeteo_historical()


def ingest_api_data_airquality() -> None:
    ingestion = create_batch_ingestion()
    ingestion.ingest_airquality()


###################################
#        DAG CONFIGURATION        #
###################################

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 29),
    "retries": 100,
    "retry_delay": timedelta(seconds=10),
    "execution_timeout": timedelta(hours=2),
}

###################################
#          DAG DEFINITION         #
###################################

with DAG(
    dag_id="1_DataIngestionToLandingZone",
    default_args=default_args,
    description=(
        "Runs static-source ingestion first, then Open-Meteo and Air-Quality "
        "API ingestions, in that order."
    ),
    schedule_interval="@hourly",   
    catchup=False,
    tags=["ingestion", "static", "api"],
) as dag:

    # 1) Static ingestion
    task_static_ingestion = PythonOperator(
        task_id="ingest_static_data",
        python_callable=ingest_static_data,
    )

    # 2) Open-Meteo API ingestion
    task_api_openmeteo = PythonOperator(
        task_id="ingest_api_openmeteo",
        python_callable=ingest_api_data_openmeteo,
    )

    # 3) Air-Quality API ingestion
    task_api_airquality = PythonOperator(
        task_id="ingest_api_airquality",
        python_callable=ingest_api_data_airquality,
    )

    # Execution order: static → openmeteo → airquality
    task_static_ingestion >> task_api_openmeteo >> task_api_airquality
