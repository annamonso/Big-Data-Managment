import duckdb
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from trusted_zone.trustedManager import TrustedZone


# --- Configuration ---
DUCKDB_FILE_PATH = '/opt/airflow/duckdb_data/trusted/trusted_zone.duckdb'

###################################
#        METHODS FOR DAG          #
###################################

def run_trusted_zone_pipeline():
    """
    Airflow task function to:
    1. Initialize TrustedZone manager.
    2. Read data from Delta Lake (Landing Zone).
    3. Save the data to DuckDB (Trusted Zone).
    4. Display the saved data from DuckDB.
    """
    print("Starting Trusted Zone Pipeline...")
    try:

        # 1. Initialize TrustedZone manager
        trusted_manager = TrustedZone(
            duckdb_file_path=DUCKDB_FILE_PATH,
        )
        print("TrustedZone Manager Initialized.")

        # 2. Read data from Delta Lake
        print("Attempting to get data from Landing Zone (Delta Lake)...")
        trusted_manager.get_landing_zone_data()
        print("Data loaded from Delta Lake into TrustedZone manager.")

        # 3. Perform the transformations to the trusted zone 
        print("Performing trusted zone data transformations")
        trusted_manager.perform_transformations()
        print("Completed trusted zone data transformations")

        # 4. Save the data to DuckDB
        print("Attempting to save data to DuckDB (Trusted Zone)...")
        trusted_manager.save_df_to_duckdb()
        print("Data saved successfully to DuckDB.")

        # 5. Display the saved data from DuckDB
        print("Attempting to display data from DuckDB...")
        trusted_manager.display_dataframes_from_duckdb()
        print("Data display finished.")

        print("Trusted Zone Pipeline finished successfully.")

    except ValueError as ve:
        print(f"ValueError during Trusted Zone pipeline: {ve}")
        raise
    except ImportError as ie:
         print(f"ImportError during Trusted Zone pipeline (check dependencies like 'deltalake' or 'pyspark'): {ie}")
         raise
    except Exception as e:
        print(f"An unexpected error occurred in the Trusted Zone pipeline: {e}")
        raise

###################################
#        DAG CONFIGURATION        #
###################################

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1, 
    "retry_delay": timedelta(minutes=1), 
    "execution_timeout": timedelta(hours=1), 
}

###################################
#              DAG DEFINITION     #
###################################

# Use with statement for DAG definition (best practice)
with DAG(
    dag_id="2_TrustedZone", 
    default_args=default_args,
    description="Loads data from Delta Lake Landing Zone to DuckDB Trusted Zone",
    schedule_interval="@daily", 
    catchup=False, 
    tags=['trusted-zone', 'duckdb', 'delta-lake'], 
) as dag:

###################################
#           TASKS                 #
###################################

    process_landing_to_trusted_task = PythonOperator(
        task_id="process_landing_to_trusted",
        python_callable=run_trusted_zone_pipeline,
    )