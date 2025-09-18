import os
import pandas as pd
import sys
import json
import gdown
import re
from pyspark.sql.functions import col
# Add the root directory to sys.path to allow imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.batch_ingestion.api_manager import open_meteo_api, air_quality_api

class BatchIngestion:
    """
    This class is responsible for ingesting data from different sources in a batch manner.
    The data is ingested from different data sources and properly organized in a delta lake (landing zone).
    """
    def __init__(self, update_frequency, delta_manager, static_data_dir="./static_data",
                 delta_lake_batch_dir="./landing_zone/batch"):
        """
        Initialize the batch ingestion class with the update frequency.
        :param update_frequency: The frequency at which the data is ingested (in seconds).
        :param static_data_dir: Directory where static CSV files are located.
        :param delta_lake_dir: Directory where Delta Lake data will be stored.
        """
        self._delta_manager = delta_manager
        self._update_frequency = update_frequency
        self.static_data_dir = static_data_dir
        self.delta_lake_batch_dir = delta_lake_batch_dir
        self.temp_directory = "./utils/temp"
        self.cities_path = "./utils/cities.json"
        self.static_ids = {
            "air_quality_index":"1ft08YYJ4dYY_kAgPwcuUGOGEGAksifod",
            "GlobalEnvironmentalTemperatureTrends":"1tLKRyz4GdXA6zN0Dt0koc-HiYbT-zgIM",
            "GlobalLandTemperaturesByMajorCity":"1BoKkcsDlWqHga9s2HdUSGZDSaR_4BkX-",
            "natural_disaster_database":"1ELCe4Bm6CUYRz19vkCV_yvxgQZsB6jTp",
            "world_hapiness_scores":"10bhhjz2h0s7l_F7YXW79ZZK8wgB8d9oe"
        }



    def ingest_static_sources(self) -> None:
        """
        Ingest data from static sources from a remote server to the landing zone (delta lake).
        """

        if len(self.static_ids) == 0:
            print("No static file IDs found.")
            return

        for dataset_name, file_id in self.static_ids.items():
            print(f"⬇️ Downloading `{dataset_name}` from the server...")

            # Define download destination path
            csv_filename = f"{dataset_name}.csv"
            file_path = os.path.join(self.temp_directory, csv_filename)

            # Construct gdown download URL
            gdrive_url = f"https://drive.google.com/uc?id={file_id}"

            try:
                # Download file
                gdown.download(gdrive_url, file_path, quiet=False)
                print(f"✅ Downloaded `{csv_filename}` to `{file_path}`")
            except Exception as e:
                self._delete_temp_folder()
                raise ValueError(f"❌ Failed to download {dataset_name}: {e}")

            try: 
                spark_df = self._delta_manager.spark.read.option("header", "true").csv(file_path)
                # Clean column names (Spark equivalent)
                for col_name in spark_df.columns:
                    new_name = re.sub(r'[^a-zA-Z0-9]', '_', col_name)
                    spark_df = spark_df.withColumnRenamed(col_name, new_name)
            except Exception as e: 
                self._delete_temp_folder()
                raise ValueError(f"❌ Failed to read `{csv_filename}` into Spark DataFrame: {e}")

            # Define Delta Lake save path
            delta_path = os.path.join(self.delta_lake_batch_dir, dataset_name)

            try:
                # Save Spark DataFrame to Delta Lake
                self._delta_manager.save_to_delta_with_timestamp(spark_df, path=delta_path)
                
            except Exception as e:
                self._delete_temp_folder()
                raise ValueError(f"❌ Failed to save `{dataset_name}` to Delta Lake: {e}")
            
            # Delete all contents in temp folder (self.temp)
            self._delete_temp_folder()

    
    def ingest_openmeteo_historical(self, cities: list = None, start_date: str = "1940-12-10", end_date: str = "2024-12-10") -> None:
        """
        Ingest historical weather data from OpenMeteo API. If API fails, download backup data from Google Drive.
        Merges both datasets (if applicable), cleans, and saves to Delta Lake.
        """
        open_manager = open_meteo_api()
        collected_df = pd.DataFrame()

        # Load cities from JSON if not provided
        if cities is None:
            with open(self.cities_path, 'r') as f:
                cities = list(json.load(f).keys())

        api_failed = False

        for city in cities:
            try:
                data = open_manager.get_historical_temperatures(city, start_date, end_date)
                if data is not None:
                    collected_df = pd.concat([collected_df, data], ignore_index=True)
                else:
                    print(f"No data returned for {city}.")
            except Exception as e:
                print(f"⚠️ API Error for city '{city}': {e}")
                api_failed = True
                break  # Stop querying more cities if rate-limited or API fails

        # Backup dataset download if API failed or returned no data
        if api_failed or collected_df.empty:
            print("📥 Downloading backup dataset from Google Drive...")
            backup_file_path = os.path.join(self.temp_directory, "openmeteo_backup.csv")
            gdown.download("https://drive.google.com/uc?id=1vh1FbHqXMmwgksaaWSkQEGpIvTwAaDIo", backup_file_path, quiet=True)

            try:
                backup_df = pd.read_csv(backup_file_path)
                print(f"✅ Backup dataset loaded: {backup_df.shape[0]} rows")
            except Exception as e:
                raise ValueError(f"❌ Failed to read backup CSV: {e}")

            # Keep backup dataset as the definitive , since the API failed.
            full_df = backup_df
        else:
            full_df = collected_df

        # Clean and convert to Spark DataFrame
        try:
            full_df.columns = full_df.columns.str.replace(r'[^a-zA-Z0-9]', '_', regex=True)
            spark_df = self._delta_manager.spark.createDataFrame(full_df)
        except Exception as e:
            raise ValueError(f"❌ Failed to convert merged data to Spark DataFrame: {e}")

        # Save to Delta Lake
        table_name = "historical_weather"
        delta_path = os.path.join(self.delta_lake_batch_dir, table_name)

        try:
            self._delta_manager.save_to_delta_with_timestamp(spark_df, path=delta_path)
        except Exception as e:
            print(f"❌ Failed to save to Delta Lake: {e}")

        # Delete all contents in temp folder (self.temp)
        self._delete_temp_folder()


    def _delete_temp_folder(self):
        if os.path.exists(self.temp_directory):
            for item in os.listdir(self.temp_directory):
                item_path = os.path.join(self.temp_directory, item)
                if os.path.isfile(item_path):
                    #if it is not a readme.md file, delete it
                    if item != "README.md":
                        try:
                            os.remove(item_path)
                            print(f"Deleted file: {item_path}")
                        except Exception as e:
                            print(f"⚠️ Failed to delete {item_path}: {e}")
            print(f"✅ Cleared all files in {self.temp_directory}")
        else:
            print(f"⚠️ Temp folder does not exist: {self.temp_directory}")
            
    
    def ingest_airquality(self, cities: list = None, start_date: str = "1940-12-10", end_date: str = "2024-12-10") -> None:
        """
        Ingest historical weather data from Airquality API. If API fails, download backup data from Google Drive.
        Merges both datasets (if applicable), cleans, and saves to Delta Lake.
        """
        air_quality_manager = air_quality_api()
        collected = []
        collected_df = pd.DataFrame()

        # Load cities from JSON if not provided
        if cities is None:
            with open(self.cities_path, 'r') as f:
                cities = list(json.load(f).keys())

        api_failed = False

        for city in cities:
            try:
                data = air_quality_manager.get_air_quality_data(city)
                if data is not None:
                    collected.append(data)
                    print(f"✅ Data collected for {city}")
                else:
                    print(f"No data returned for {city}.")
            except Exception as e:
                print(f"⚠️ API Error for city '{city}': {e}")
                api_failed = True
                break  # Stop querying more cities if rate-limited or API fails
        
        
        collected_df = pd.DataFrame(collected)
        
        # Backup dataset download if API failed or returned no data
        if api_failed or collected_df.empty:
            print("📥 Downloading backup dataset from Google Drive...")
            backup_file_path = os.path.join(self.temp_directory, "airquality_backup.csv")
            gdown.download("https://drive.google.com/uc?id=169iGB_oQRT3GIlvfbRRLJ_jXiczAZb1t", backup_file_path, quiet=True)

            try:
                backup_df = pd.read_csv(backup_file_path)
                print(f"✅ Backup dataset loaded: {backup_df.shape[0]} rows")
            except Exception as e:
                raise ValueError(f"❌ Failed to read backup CSV: {e}")

            # Keep backup dataset as the definitive , since the API failed.
            full_df = backup_df
        else:
            full_df = collected_df

        # Clean and convert to Spark DataFrame
        try:
            
            full_df.columns = full_df.columns.str.replace(r'[^a-zA-Z0-9]', '_', regex=True)
            # Clean invalid numeric entries
            numeric_columns = ['aqi', 'pm25', 'pm10', 'no2']
            for col in numeric_columns:
                full_df[col] = pd.to_numeric(full_df[col], errors='coerce')
            spark_df = self._delta_manager.spark.createDataFrame(full_df)
        except Exception as e:
            raise ValueError(f"❌ Failed to convert merged data to Spark DataFrame: {e}")

        # Save to Delta Lake
        table_name = "Current_air_quality_data"
        delta_path = os.path.join(self.delta_lake_batch_dir, table_name)

        try:
            self._delta_manager.save_to_delta_with_timestamp(spark_df, path=delta_path)
        except Exception as e:
            print(f"❌ Failed to save to Delta Lake: {e}")

        # Delete all contents in temp folder (self.temp)
        self._delete_temp_folder()