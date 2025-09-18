import pyspark
from delta import *
import os
import sys
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from typing import List, Optional
from delta import configure_spark_with_delta_pip
from datetime import datetime
from pyspark.sql import SparkSession

class DeltaLakeManager:
    """
    Manages Delta Lake operations with a persistent SparkSession.
    """

    def __init__(self):
        self.spark = self._get_spark_session()
        self.delta_lake_path_batch = './landing_zone/batch'

    def _get_spark_session(self):
        """Initializes and returns a SparkSession with Delta Lake support."""

        builder = pyspark.sql.SparkSession.builder \
            .appName("DeltaLakeManager") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")

        return configure_spark_with_delta_pip(builder).getOrCreate()

    def save_to_delta_with_timestamp(self, df, path= "./landing_zone", mode="overwrite"):
        """Saves a DataFrame to Delta Lake in append or overwrite mode."""
        try:
            # Get the current date
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # Create the path with the date
            final_path = f"{path}_{timestamp}"
            final_path = f"{path}"
            df.write.format("delta").mode(mode).save(final_path)
            print(f"💾 Saved `{path}` to Delta Lake at `{final_path}`")
        except Exception as e:
            raise ValueError(f"❌ Failed to save DataFrame to Delta Lake: {e}")

    def merge_into_delta(self, new_data, path, key_column="id"):
        """Performs an UPSERT (Merge) into a Delta table."""
        delta_table = DeltaTable.forPath(self.spark, path)

        delta_table.alias("target").merge(
            new_data.alias("source"),
            f"target.{key_column} = source.{key_column}"
        ).whenMatchedUpdate(set={"*": "source.*"}) \
         .whenNotMatchedInsertAll() \
         .execute()

    def optimize_and_vacuum(self, path, retention_hours=168):
        """Optimizes and cleans up Delta Table storage (removes old versions)."""
        delta_table = DeltaTable.forPath(self.spark, path)

        print("Optimizing Delta Table...")
        delta_table.optimize().executeCompaction()

        print(f"Vacuuming old versions (Keeping last {retention_hours//24} days)...")
        delta_table.vacuum(retentionHours=retention_hours)

    def reset_delta_lake(self):
        """Deletes all Delta Lake data and metadata in the configured path."""
        os.system(f"rm -rf {self.delta_lake_path_batch}")

    
    def display_existing_data(self):
        """Returns the head of the dataframes stores as Delta Tables."""
        # Check if the delta_lake_path exists
        if not os.path.exists(self.delta_lake_path_batch):
            return "Data lake already not initialized! Please ingest data first."
        # For each folder in the delta_lake_path, load the Delta Table and display the head
        dataframe_heads = {}
        for folder in os.listdir(self.delta_lake_path_batch):
            df = pd.read_parquet(os.path.join(self.delta_lake_path_batch, folder))
            dataframe_heads[folder] = df.head()
        if dataframe_heads == {}:
            return "Empty Data lake."
        return dataframe_heads
    
    def get_all_data(self):
        """Returns all dataframes stored as Delta Tables as pandas dataframes."""
        # Check if the delta_lake_path exists
        if not os.path.exists(self.delta_lake_path_batch):
            return "Data lake already not initialized! Please ingest data first."
        # For each folder in the delta_lake_path, load the Delta Table and display the head
        dataframes = []
        for folder in os.listdir(self.delta_lake_path_batch):
            try:
                table_path = os.path.join(self.delta_lake_path_batch, folder)
                if os.path.exists(os.path.join(table_path, "_delta_log")):
                    spark_df = self.spark.read.format("delta").load(table_path)
                    df = spark_df.toPandas()
                    df.name = folder
                    dataframes.append(df) 
                else:
                    print(f"Skipping '{folder}', does not appear to be a Delta table.")
            except Exception as e:
                print(f"Error reading Delta table '{folder}': {e}")
        if dataframes == []:
            return "Empty Data lake."
        return dataframes
