import pandas as pd
import numpy as np
import sys
import os
import duckdb
from delta import *
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.batch_ingestion.delta_manager import DeltaLakeManager
from utils.BDMLibrary import BDMLibrary


class TrustedZone():
    """
    Contains all functions required for the trusted zone.
    """

    def __init__(self, duckdb_file_path='/opt/airflow/duckdb_data/trusted_zone.duckdb'):
        """Initialize the Trusted Zone"""
        self.dataframes = []
        self.duckdb_path = duckdb_file_path
        self.trusted_path = "./trusted_zone"

    def get_landing_zone_data(self):
        """ 
        Obtains all tables in the landing zone and saves them as pandas dataframes inside the class.
        """
        dlm = DeltaLakeManager()
        data = dlm.get_all_data()

        if data == "Empty Data lake.":
            raise ValueError("The Data LakeHouse is empty. Could not extract any table.")

        self.dataframes = []
        for pdf in data:
            if not hasattr(pdf, "name"):
                raise ValueError("Missing 'name' attribute on dataframe — cannot identify table name.")
            self.dataframes.append(pdf)

    def save_df_to_duckdb(self):
        """
        Saves all pandas dataframes to a DuckDB database.
        """
        if not self.dataframes: # More Pythonic check for empty list
            raise ValueError("No dataframes to save. Please load dataframes first.")

        con = None 
        try:
            print("Starting DuckDB connection for saving...")
            db_dir = os.path.dirname(self.duckdb_path)
            if db_dir:
                 os.makedirs(db_dir, exist_ok=True)
            con = duckdb.connect(database=self.duckdb_path, read_only=False)
            print(f"Connected to DuckDB at {self.duckdb_path}")

            if not isinstance(self.dataframes, list):
                raise TypeError(f"Expected self.dataframes to be a list, but got {type(self.dataframes)}")

            for df in self.dataframes:
                if not isinstance(df, pd.DataFrame):
                    raise TypeError(f"Expected object in self.dataframes to be a pandas DataFrame, but got {type(df)}. Make sure DeltaLakeManager.get_all_data() converts Spark DFs to Pandas.")

                if not hasattr(df, 'name') or not df.name:
                    print(f"Warning: Skipping DataFrame because it lacks a valid 'name' attribute.")
                    continue
                table_name = df.name
                print(f"Saving DataFrame to DuckDB table: '{table_name}'...")
                con.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM df')
                print(f"Successfully saved DataFrame to DuckDB table: '{table_name}'.")

        except Exception as e:
            print(f"Error saving DataFrames to DuckDB: {e}")
            raise ValueError(f"Failed to save DataFrames to DuckDB: {e}") from e 
        finally:
            if con:
                con.close()
                print("Closed DuckDB connection after saving.")
        
    def display_dataframes_from_duckdb(self):

        """
        Displays the first few rows of tables in DuckDB corresponding to loaded dataframes.
        Ensures table names are quoted correctly.
        """
        if not self.dataframes:
            raise ValueError("No dataframes loaded to determine which tables to display.")

        con = None
        try:
            print("Starting DuckDB connection for displaying...")
            if not os.path.exists(self.duckdb_path):
                 print(f"DuckDB file not found at {self.duckdb_path}. Cannot display.")
                 return

            con = duckdb.connect(database=self.duckdb_path, read_only=True) # Read-only is safer
            print(f"Connected to DuckDB at {self.duckdb_path} (Read-Only)")

            print("\n--- Displaying Data from DuckDB (First 10 Rows) ---")
            for df_info in self.dataframes: # Use a different variable name to avoid confusion
                if not hasattr(df_info, 'name') or not df_info.name:
                    print(f"Warning: Skipping display for a DataFrame without a valid 'name' attribute.")
                    continue

                table_name = df_info.name
                try:
                    print(f"\n-- Table: {table_name} --")
                    # --- FIX: Quote the table name ---
                    # Use single quotes for f-string to easily use double quotes inside
                    query = f'SELECT * FROM "{table_name}" LIMIT 10'
                    # ---------------------------------
                    print(f"Executing query: {query}")
                    result = con.execute(query).fetchall()

                    if result:
                        # Fetch column names for better printing
                        col_names = [desc[0] for desc in con.description]
                        print(f"Columns: {col_names}")
                        print("First 10 rows:")
                        for row in result:
                            print(row)
                        # Optionally fetch total row count
                        count_result = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
                        if count_result:
                            print(f"Total rows in table: {count_result[0]}")
                    else:
                        print("Table is empty.")

                except duckdb.CatalogException:
                     print(f"Table `{table_name}` not found in DuckDB.")
                except duckdb.ParserException as pe:
                     print(f"Parser Error querying table `{table_name}`: {pe}. Query was: {query}")
                     # This might indicate an issue even with quoting, e.g. invalid characters
                except Exception as table_e:
                     print(f"Error querying table `{table_name}`: {table_e}")

            print("\n--- End of Data Display ---")

        except Exception as e:
            # Catch broader errors during connection or iteration
            print(f"An error occurred while displaying data from DuckDB: {e}")
            # Decide whether to raise or just log
            raise ValueError(f"Failed to display DataFrames from DuckDB: {e}") from e
        finally:
            if con:
                con.close()
                print("Closed DuckDB connection after displaying.")

    def preprocess_air_quality_data(self, df):
        bdmlib = BDMLibrary()
        df['city'] = df['city'].str.strip()
        
        df['CountryISO'] = df['city'].map(bdmlib.city_to_iso)

        df = bdmlib.cast_date_column(df, column_name='date')
        if 'DateTime' not in df.columns:
            print("Error: 'DateTime' column was not created by cast_date_column. Please check bdmlib.")
            if 'date' in df.columns:
                df['DateTime'] = pd.to_datetime(df['date'], errors='coerce')
            else:
                sys.exit()


        df['Year'] = df['DateTime'].dt.year


        numeric_cols = ['aqi', 'pm25', 'pm10', 'no2']
        for col in numeric_cols:
            df[col] = df[col].replace('-', np.nan)
            df[col] = pd.to_numeric(df[col], errors='coerce')

        if 'DateTime' in df.columns:
            initial_rows = len(df)
            df.drop_duplicates(subset=['city', 'DateTime'], keep='first', inplace=True)
            deduplicated_rows = initial_rows - len(df)
            if deduplicated_rows > 0:
                print(f"Removed {deduplicated_rows} duplicate rows based on 'city' and 'DateTime'.")
        else:
            print("Warning: 'DateTime' column not found, skipping deduplication based on date.")

        df = df[df['aqi'].notna() & (df['aqi']>=0)]
        df = df[df['pm25'].notna() & (df['pm25']>=0)]
        df = df[df['pm10'].notna() & (df['pm10']>=0)]
        df = df[df['no2'].notna() & (df['no2']>=0)]
        df.drop(columns=['date'], inplace=True, errors='ignore')
        df.rename(columns={'city': 'City'}, inplace=True)

        return df

    def preprocess_historical_temperatures(self, df):
       
        original_date_col = 'date'
        city_col = 'city'
        country_col = 'Country'
        lat_col_original = 'latitude'
        lon_col_original = 'longitude'
        temp_col = 'AverageTemperature'
        temp_uncertainty_col = 'AverageTemperatureUncertainty'


        standardized_date_col = 'DateTime'
        standardized_lat_col = 'Latitude'
        standardized_lon_col = 'Longitude'
        iso_country_col = 'CountryISO'


        
        if city_col in df.columns:
            df[city_col] = df[city_col].astype(str).str.strip()
            
        else:
            print(f"Warning: City column '{city_col}' not found.")

        if country_col in df.columns:
            df[country_col] = df[country_col].astype(str).str.strip()
        else:
            print(f"Warning: Country column '{country_col}' not found. ISO mapping might be affected if relying on it.")

        bdmlib = BDMLibrary()
        df[iso_country_col] = df[city_col].map(bdmlib.city_to_iso)

        if city_col in df.columns and hasattr(bdmlib, 'city_to_iso'):
            df[iso_country_col] = df[city_col].map(bdmlib.city_to_iso)

        elif not hasattr(BDMLibrary, 'get_country_iso_from_api'):
            print(f"Warning: bdmlib.get_country_iso_from_api not found. Skipping API-based ISO mapping for '{city_col}'.")
            df[iso_country_col] = None
        else:
            print(f"Warning: City column '{city_col}' not found. Skipping API-based ISO mapping.")
            df[iso_country_col] = None


        '''    
            def validate_api_iso_code(iso_value):
                if isinstance(iso_value, str) and len(iso_value) == 3 and iso_value.isupper():
                    return iso_value
                
                return None if pd.isna(iso_value) or not (isinstance(iso_value, str) and len(iso_value) == 3 and iso_value.isupper()) else iso_value
                
                
            df[iso_country_col] = df[iso_country_col].apply(validate_api_iso_code)
            print(f"Finished ISO code mapping. Check '{iso_country_col}' for results.")
        '''
        
        if original_date_col in df.columns:
            if hasattr(bdmlib, 'cast_date_column'):
                df = bdmlib.cast_date_column(df,
                                                column_name=original_date_col,
                                                new_column_name=standardized_date_col)
                
                if standardized_date_col not in df.columns or df[standardized_date_col].isnull().all() or not pd.api.types.is_datetime64_any_dtype(df[standardized_date_col][df[standardized_date_col].notnull()].head()):
                    print(f"Warning: bdmlib.cast_date_column did not create or correctly populate '{standardized_date_col}'. Falling back to direct pd.to_datetime.")
                    df[standardized_date_col] = pd.to_datetime(df[original_date_col].astype(str).str.strip(), errors='coerce')
            else:
                print("Warning: bdmlib.cast_date_column not found. Using direct pd.to_datetime.")
                df[standardized_date_col] = pd.to_datetime(df[original_date_col].astype(str).str.strip(), errors='coerce')
            
            if standardized_date_col in df.columns and df[standardized_date_col].isnull().any():
        
                print(f"Warning: Some dates from '{original_date_col}' could not be parsed and are now NaT in '{standardized_date_col}'.")
                if original_date_col in df.columns:
                    problematic_original_values = df[df[standardized_date_col].isnull()][original_date_col].unique()
                    print(f"Sample of unique original values from '{original_date_col}' that resulted in NaT (up to 10):")
                    for val in problematic_original_values[:10]:
                        print(f"  - '{val}' (type: {type(val)})")
                else:
                    print(f"Original date column '{original_date_col}' no longer exists for inspection of problematic values.")
                    
            elif standardized_date_col not in df.columns:
                print(f"Error: Target date column '{standardized_date_col}' was not created after processing '{original_date_col}'.")

        else:
            print(f"Error: Original date column '{original_date_col}' not found. Creating '{standardized_date_col}' filled with NaT.")
            df[standardized_date_col] = pd.NaT 

        if lat_col_original in df.columns and lat_col_original != standardized_lat_col:
            df.rename(columns={lat_col_original: standardized_lat_col}, inplace=True)
        if lon_col_original in df.columns and lon_col_original != standardized_lon_col:
            df.rename(columns={lon_col_original: standardized_lon_col}, inplace=True)


        
        if standardized_date_col in df.columns and pd.api.types.is_datetime64_any_dtype(df[standardized_date_col]):
            df['Year'] = df[standardized_date_col].dt.year
            
        else:
            print(f"Warning: Standardized date column '{standardized_date_col}' not found or not in datetime format. Cannot extract Year/Month/Day.")
            df['Year'] = None
            

        
        
        numeric_cols_to_process = []
        if temp_col in df.columns: numeric_cols_to_process.append(temp_col)
        if temp_uncertainty_col in df.columns: numeric_cols_to_process.append(temp_uncertainty_col)
        if standardized_lat_col in df.columns: numeric_cols_to_process.append(standardized_lat_col)
        if standardized_lon_col in df.columns: numeric_cols_to_process.append(standardized_lon_col)

        for col in numeric_cols_to_process:
            if col in df.columns:
                
                
                df[col] = pd.to_numeric(df[col], errors='coerce')
                if df[col].isnull().any():
                    print(f"Warning: Some values in '{col}' were non-numeric and are now NaN.")
            else:
                print(f"Warning: Expected numeric column '{col}' not found for schema correction.")


        
        if temp_uncertainty_col in df.columns and pd.api.types.is_numeric_dtype(df[temp_uncertainty_col]):
            invalid_uncertainty_count = df[df[temp_uncertainty_col] < 0].shape[0]
            if invalid_uncertainty_count > 0:
                print(f"Found {invalid_uncertainty_count} records with negative '{temp_uncertainty_col}'. Setting to NaN.")
                df.loc[df[temp_uncertainty_col] < 0, temp_uncertainty_col] = np.nan

        
        if temp_col in df.columns and pd.api.types.is_numeric_dtype(df[temp_col]):
            abs_zero_celsius = -273.15
            invalid_temp_count = df[df[temp_col] < abs_zero_celsius].shape[0]
            if invalid_temp_count > 0:
                print(f"Found {invalid_temp_count} records with temperatures below absolute zero in '{temp_col}'. Setting to NaN.")
                df.loc[df[temp_col] < abs_zero_celsius, temp_col] = np.nan


        if standardized_lat_col in df.columns and pd.api.types.is_numeric_dtype(df[standardized_lat_col]):
            invalid_lat_count = df[~df[standardized_lat_col].between(-90, 90, inclusive='both')].shape[0]
            if invalid_lat_count > 0:
                print(f"Found {invalid_lat_count} records with invalid '{standardized_lat_col}' values. Setting to NaN.")
                df.loc[~df[standardized_lat_col].between(-90, 90, inclusive='both'), standardized_lat_col] = np.nan

        
        if standardized_lon_col in df.columns and pd.api.types.is_numeric_dtype(df[standardized_lon_col]):
            invalid_lon_count = df[~df[standardized_lon_col].between(-180, 180, inclusive='both')].shape[0]
            if invalid_lon_count > 0:
                print(f"Found {invalid_lon_count} records with invalid '{standardized_lon_col}' values. Setting to NaN.")
                df.loc[~df[standardized_lon_col].between(-180, 180, inclusive='both'), standardized_lon_col] = np.nan

        
        deduplication_keys = []
        if standardized_date_col in df.columns and pd.api.types.is_datetime64_any_dtype(df[standardized_date_col]):
            deduplication_keys.append(standardized_date_col)
        if city_col in df.columns:
            deduplication_keys.append(city_col)
        if iso_country_col in df.columns:
            deduplication_keys.append(iso_country_col)
        elif country_col in df.columns:
            deduplication_keys.append(country_col)

        if len(deduplication_keys) >= 2:
            print(f"\nPerforming deduplication based on: {deduplication_keys}...")
            initial_rows = len(df)
            df.drop_duplicates(subset=deduplication_keys, keep='first', inplace=True)
            deduplicated_rows = initial_rows - len(df)
            if deduplicated_rows > 0:
                print(f"Removed {deduplicated_rows} duplicate rows.")
            else:
                print("No duplicate rows found based on the specified keys.")
        else:
            print("\nSkipping deduplication: Not enough key columns identified or available.")
        df.rename(columns={"city": "City"}, inplace=True)
        df.drop(columns=['date'], inplace=True, errors='ignore')
        return df

    def global_environment_temperature_trends(self, df):
        bdmlib = BDMLibrary()
        year_col = 'Year'
        country_col_original = 'Country'
        avg_temp_col = 'Avg_Temperature_degC'
        co2_emissions_col = 'CO2_Emissions_tons_per_capita'
        sea_level_rise_col = 'Sea_Level_Rise_mm'
        rainfall_col = 'Rainfall_mm'
        population_col = 'Population'
        renewable_energy_col = 'Renewable_Energy_pct'
        extreme_weather_col = 'Extreme_Weather_Events'
        forest_area_col = 'Forest_Area_pct'



        iso_country_col = 'CountryISO'



        if country_col_original in df.columns:
            df[country_col_original] = df[country_col_original].astype(str).str.strip()
            
            df[country_col_original] = df[country_col_original].str.strip("'")
            
        else:
            print(f"Warning: Original country column '{country_col_original}' not found.")

        df[iso_country_col] = df[country_col_original].map(bdmlib.country_iso_dict)
        
  
        
        if year_col in df.columns:
            df[year_col] = pd.to_numeric(df[year_col], errors='coerce').astype('Int64')
        else:
            print(f"Warning: Year column '{year_col}' not found.")

        numeric_cols_list = [
            avg_temp_col, co2_emissions_col, sea_level_rise_col, rainfall_col,
            population_col, renewable_energy_col, extreme_weather_col, forest_area_col
        ]

        for col in numeric_cols_list:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                if df[col].isnull().any():
                    print(f"Warning: Some values in '{col}' were non-numeric and are now NaN.")
            else:
                print(f"Warning: Expected numeric column '{col}' not found.")

        
        if population_col in df.columns and pd.api.types.is_numeric_dtype(df[population_col]):
            df[population_col] = df[population_col].astype('Int64')
        if extreme_weather_col in df.columns and pd.api.types.is_numeric_dtype(df[extreme_weather_col]):
            df[extreme_weather_col] = df[extreme_weather_col].astype('Int64')
        
        if avg_temp_col in df.columns and pd.api.types.is_numeric_dtype(df[avg_temp_col]):
            abs_zero_celsius = -273.15
            df.loc[df[avg_temp_col] < abs_zero_celsius, avg_temp_col] = np.nan

        non_negative_cols = [
            co2_emissions_col, sea_level_rise_col, rainfall_col, population_col,
            extreme_weather_col
        ] 
        for col in non_negative_cols:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                df.loc[df[col] < 0, col] = np.nan

        percentage_cols = [renewable_energy_col, forest_area_col]
        for col in percentage_cols:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                df.loc[~df[col].between(0, 100, inclusive='both'), col] = np.nan

        deduplication_keys = []
        if year_col in df.columns:
            deduplication_keys.append(year_col)
        if iso_country_col in df.columns:
            deduplication_keys.append(iso_country_col)
        elif country_col_original in df.columns:
            deduplication_keys.append(country_col_original)

        if len(deduplication_keys) >= 2:
            print(f"\nPerforming deduplication based on: {deduplication_keys}...")
            initial_rows = len(df)
            df.drop_duplicates(subset=deduplication_keys, keep='first', inplace=True)
            deduplicated_rows = initial_rows - len(df)
            if deduplicated_rows > 0:
                print(f"Removed {deduplicated_rows} duplicate rows.")
            else:
                print("No duplicate rows found based on the specified keys.")
        else:
            print("\nSkipping deduplication: Not enough key columns identified.")

        if avg_temp_col in df.columns:
            df = df[(df[avg_temp_col].notna()) & (df[avg_temp_col] >= -100.0) & (df[avg_temp_col] <= 60.0)]

        if forest_area_col in df.columns:
            df = df[(df[forest_area_col].notna()) & (df[forest_area_col] >= 0) & (df[forest_area_col] <= 100)]
        
        if renewable_energy_col in df.columns:
            df = df[(df[renewable_energy_col].notna()) & (df[renewable_energy_col] >= 0) & (df[renewable_energy_col] <= 100)]

        if population_col in df.columns:
            df = df[(df[population_col].notna()) & (df[population_col] >= 0) & (df[population_col] <= 10**12)]
        
        if rainfall_col in df.columns:
            df = df[(df[rainfall_col].notna()) & (df[rainfall_col] >= 0) ]
       
        return df
    














    def global_land_temperature_by_major_city(self, df):
        bdmlib = BDMLibrary()

        df.drop_duplicates(inplace=True)



        key_columns = ['dt', 'City', 'Country']
        if df.duplicated(subset=key_columns).any():
            
            df.drop_duplicates(subset=key_columns, keep='first', inplace=True)
        else:
            print(f"No duplicates found based on key columns: {key_columns} after initial full deduplication.")
        



        df['DateTime'] = pd.to_datetime(df['dt'], errors='coerce') 


        df['Year'] = df['DateTime'].dt.year
        df['AverageTemperature'] = pd.to_numeric(df['AverageTemperature'], errors='coerce')
        df['AverageTemperatureUncertainty'] = pd.to_numeric(df['AverageTemperatureUncertainty'], errors='coerce')


        # Correctly call the library functions
        df = bdmlib.convert_latitude(df, 'Latitude')
        df = bdmlib.convert_longitude(df, 'Longitude')




        df.info(memory_usage='deep')





        rows_before_constraints = len(df)


        df.dropna(subset=['DateTime', 'Latitude', 'Longitude'], inplace=True) 
        print(f"Removed {rows_before_constraints - len(df)} rows with NaT dates or unparseable Lat/Lon after schema correction.")
        rows_before_constraints = len(df)

        temp_lower_bound, temp_upper_bound = -100.0, 60.0
        invalid_temp_mask = ~df['AverageTemperature'].between(temp_lower_bound, temp_upper_bound, inclusive='both')
        print(f"Found {invalid_temp_mask.sum()} temperatures outside [{temp_lower_bound}, {temp_upper_bound}]°C. Setting them to NaN.")
        df.loc[invalid_temp_mask, 'AverageTemperature'] = np.nan

        
        negative_uncertainty_mask = df['AverageTemperatureUncertainty'] < 0
        
        df.loc[negative_uncertainty_mask, 'AverageTemperatureUncertainty'] = np.nan

        # Latitude: -90 to 90
        invalid_lat_mask = ~df['Latitude'].between(-90, 90, inclusive='both')
        print(f"Found {invalid_lat_mask.sum()} latitudes outside [-90, 90]. Removing these rows.")
        df = df[~invalid_lat_mask]

        # Longitude: -180 to 180
        invalid_lon_mask = ~df['Longitude'].between(-180, 180, inclusive='both')
        print(f"Found {invalid_lon_mask.sum()} longitudes outside [-180, 180]. Removing these rows.")
        df = df[~invalid_lon_mask]
        print(f"Total rows removed due to invalid Lat/Lon constraints: {rows_before_constraints - len(df)}")
        print(f"Shape after constraint validation: {df.shape}")


        
        text_cols = ['City', 'Country']
        for col in text_cols:
            df[col] = df[col].astype(str).str.strip() # Remove leading/trailing whitespace
            df[col] = df[col].str.lower() # Convert to lowercase
        
        

        df['CountryISO'] = df['Country'].apply(bdmlib.get_country_iso_code)






        float_cols = df.select_dtypes(include=['float64']).columns
        for col in float_cols:
            df[col] = df[col].astype('float32')



        object_cols = df.select_dtypes(include=['object']).columns
        for col in object_cols:
            if df[col].nunique() / len(df) < 0.5:
                print(f"Converting column '{col}' to category type.")
                df[col] = df[col].astype('category')
            else:
                print(f"Column '{col}' has high cardinality, not converting to category.")





        if 'dt' in df.columns:
            df.drop(columns=['dt'], inplace=True)
            print("Dropped redundant 'dt' column after conversion to DateTime.")
        else:
            print("No 'dt' column found to drop.")
        

        if 'Latitude' in df.columns and 'Longitude' in df.columns:
            if df['Latitude'].between(-90, 90).all() and df['Longitude'].between(-180, 180).all():
                print("All Latitude and Longitude values are within valid ranges.")
            else:
                print("Warning: Some Latitude or Longitude values are outside valid ranges. This should have been handled in 'Constraint validation'.")
        
        df['City'] = df['City'].astype(str).str.title()  # Convert to title case for better readability
        df['Country'] = df['Country'].str.title()  # Convert to title case for better readability
        
        return df






    def global_land_temperature_by_major_city5555(self, df):
        bdmlib = BDMLibrary()

        df.drop_duplicates(inplace=True)



        key_columns = ['dt', 'City', 'Country']
        if df.duplicated(subset=key_columns).any():
            
            df.drop_duplicates(subset=key_columns, keep='first', inplace=True)
        else:
            print(f"No duplicates found based on key columns: {key_columns} after initial full deduplication.")
        

        

        df['DateTime'] = pd.to_datetime(df['dt'], errors='coerce') 


        df['Year'] = df['DateTime'].dt.year
        df['AverageTemperature'] = pd.to_numeric(df['AverageTemperature'], errors='coerce')
        df['AverageTemperatureUncertainty'] = pd.to_numeric(df['AverageTemperatureUncertainty'], errors='coerce')


        # Correctly call the library functions
        df = bdmlib.convert_latitude(df, 'Latitude')
        df = bdmlib.convert_longitude(df, 'Longitude')




        df.info(memory_usage='deep')





        rows_before_constraints = len(df)


        df.dropna(subset=['DateTime', 'Latitude', 'Longitude'], inplace=True) 
        print(f"Removed {rows_before_constraints - len(df)} rows with NaT dates or unparseable Lat/Lon after schema correction.")
        rows_before_constraints = len(df)

        temp_lower_bound, temp_upper_bound = -100.0, 60.0
        invalid_temp_mask = ~df['AverageTemperature'].between(temp_lower_bound, temp_upper_bound, inclusive='both')
        print(f"Found {invalid_temp_mask.sum()} temperatures outside [{temp_lower_bound}, {temp_upper_bound}]°C. Setting them to NaN.")
        df.loc[invalid_temp_mask, 'AverageTemperature'] = np.nan

        
        negative_uncertainty_mask = df['AverageTemperatureUncertainty'] < 0
        
        df.loc[negative_uncertainty_mask, 'AverageTemperatureUncertainty'] = np.nan

        # Latitude: -90 to 90
        invalid_lat_mask = ~df['Latitude'].between(-90, 90, inclusive='both')
        print(f"Found {invalid_lat_mask.sum()} latitudes outside [-90, 90]. Removing these rows.")
        df = df[~invalid_lat_mask]

        # Longitude: -180 to 180
        invalid_lon_mask = ~df['Longitude'].between(-180, 180, inclusive='both')
        print(f"Found {invalid_lon_mask.sum()} longitudes outside [-180, 180]. Removing these rows.")
        df = df[~invalid_lon_mask]
        print(f"Total rows removed due to invalid Lat/Lon constraints: {rows_before_constraints - len(df)}")
        print(f"Shape after constraint validation: {df.shape}")


        
        text_cols = ['City', 'Country']
        for col in text_cols:
            df[col] = df[col].astype(str).str.strip() # Remove leading/trailing whitespace
            df[col] = df[col].str.lower() # Convert to lowercase
        

        
        # Convert country names to ISO codes using the BDMLibrary
        df['CountryISO'] = df['Country'].map(bdmlib.country_iso_dict)
        


        df['Country'] = df['Country'].str.title()  # Convert to title case for better readability



        float_cols = df.select_dtypes(include=['float64']).columns
        for col in float_cols:
            df[col] = df[col].astype('float32')



        object_cols = df.select_dtypes(include=['object']).columns
        for col in object_cols:
            if df[col].nunique() / len(df) < 0.5:
                print(f"Converting column '{col}' to category type.")
                df[col] = df[col].astype('category')
            else:
                print(f"Column '{col}' has high cardinality, not converting to category.")





        if 'dt' in df.columns:
            df.drop(columns=['dt'], inplace=True)
            print("Dropped redundant 'dt' column after conversion to DateTime.")
        else:
            print("No 'dt' column found to drop.")
        

        if 'Latitude' in df.columns and 'Longitude' in df.columns:
            if df['Latitude'].between(-90, 90).all() and df['Longitude'].between(-180, 180).all():
                print("All Latitude and Longitude values are within valid ranges.")
            else:
                print("Warning: Some Latitude or Longitude values are outside valid ranges. This should have been handled in 'Constraint validation'.")
        df['City']= df['City'].astype(str).str.title()
        return df


    '''
    def global_environment_temperature_trends2(self, df): ##HE DUPLICADO ESTA FUNCION PORQUE NO ME HE DADO CUENTA. LA DE ARRIBA DEBERIA SER MAS ACTUAL. USA LA QUE PREFIERAS.
        bdmlib = BDMLibrary()
        df.info(memory_usage='deep')
        df.drop_duplicates(inplace=True)
        
        key_columns = ['dt', 'City', 'Country']
        if df.duplicated(subset=key_columns).any():
            
            df.drop_duplicates(subset=key_columns, keep='first', inplace=True)
        else:
            print(f"No duplicates found based on key columns: {key_columns} after initial full deduplication.")
        
        df['DateTime'] = pd.to_datetime(df['dt'], errors='coerce')


        df['Year'] = df['DateTime'].dt.year 

        df['AverageTemperature'] = pd.to_numeric(df['AverageTemperature'], errors='coerce')
        df['AverageTemperatureUncertainty'] = pd.to_numeric(df['AverageTemperatureUncertainty'], errors='coerce')


        df = bdmlib.convert_latitude(df, 'Latitude')
        df = bdmlib.convert_longitude(df, 'Longitude')

        df.dropna(subset=['DateTime', 'Latitude', 'Longitude'], inplace=True)


        temp_lower_bound, temp_upper_bound = -90.0, 60.0
        invalid_temp_mask = ~df['AverageTemperature'].between(temp_lower_bound, temp_upper_bound, inclusive='both')
        df.loc[invalid_temp_mask, 'AverageTemperature'] = np.nan

        negative_uncertainty_mask = df['AverageTemperatureUncertainty'] < 0
        
        df.loc[negative_uncertainty_mask, 'AverageTemperatureUncertainty'] = np.nan

        # Latitude: -90 to 90
        invalid_lat_mask = ~df['Latitude'].between(-90, 90, inclusive='both')
        df = df[~invalid_lat_mask]

        # Longitude: -180 to 180
        invalid_lon_mask = ~df['Longitude'].between(-180, 180, inclusive='both')
        df = df[~invalid_lon_mask]



        text_cols = ['City', 'Country']
        for col in text_cols:
            df[col] = df[col].astype(str).str.strip() # Remove leading/trailing whitespace
            df[col] = df[col].str.lower() # Convert to lowercase


        df['CountryISO'] = df['Country'].apply(bdmlib.get_country_iso_code)
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='float')
        for col in df.select_dtypes(include=['int64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='integer')

        return df
    '''


    def happiness_data(self, df):
        EXPECTED_DTYPES = {
            "Country Name": "string",
            "Country Code": "string",
            "Time": "int64", # Year
            "Time Code": "string",
            "Intentional homicides (per 100,000 people) [VC.IHR.PSRC.P5]": "float64",
            "General government final consumption expenditure (% of GDP) [NE.CON.GOVT.ZS]": "float64",
            "Community health workers (per 1,000 people) [SH.MED.CMHW.P3]": "float64",
            "School enrollment, tertiary (% gross) [SE.TER.ENRR]": "float64",
            "School enrollment, secondary (% gross) [SE.SEC.ENRR]": "float64",
            "Unemployment, total (% of total labor force) (national estimate) [SL.UEM.TOTL.NE.ZS]": "float64",
            "Unemployment, youth total (% of total labor force ages 15-24) (national estimate) [SL.UEM.1524.NE.ZS]": "float64",
            "Gross capital formation (% of GDP) [NE.GDI.TOTL.ZS]": "float64",
            "Gross domestic savings (% of GDP) [NY.GDS.TOTL.ZS]": "float64",
            "Government expenditure per student, tertiary (% of GDP per capita) [SE.XPD.TERT.PC.ZS]": "float64",
            "Government expenditure per student, secondary (% of GDP per capita) [SE.XPD.SECO.PC.ZS]": "float64",
            "Government expenditure on education, total (% of GDP) [SE.XPD.TOTL.GD.ZS]": "float64",
            "Life expectancy at birth, total (years) [SP.DYN.LE00.IN]": "float64",
            "Age dependency ratio, young (% of working-age population) [SP.POP.DPND.YG]": "float64",
            "GDP per capita, PPP (constant 2021 international $) [NY.GDP.PCAP.PP.KD]": "float64",
            "GDP per capita (constant 2015 US$) [NY.GDP.PCAP.KD]": "float64",
            "Foreign direct investment, net inflows (% of GDP) [BX.KLT.DINV.WD.GD.ZS]": "float64",
            "GDP per capita growth (annual %) [NY.GDP.PCAP.KD.ZG]": "float64",
            "Market capitalization of listed domestic companies (% of GDP) [CM.MKT.LCAP.GD.ZS]": "float64",
            "Trade (% of GDP) [NE.TRD.GNFS.ZS]": "float64",
            "Inflation, consumer prices (annual %) [FP.CPI.TOTL.ZG]": "float64",
            "Net migration [SM.POP.NETM]": "float64",
            "Strength of legal rights index (0=weak to 12=strong) [IC.LGL.CRED.XQ]": "float64",
            "Mortality rate, under-5 (per 1,000 live births) [SH.DYN.MORT]": "float64",
            "Gini index [SI.POV.GINI]": "float64",
            "Net primary income (Net income from abroad) (constant LCU) [NY.GSR.NFCY.KN]": "float64",
            "Population growth (annual %) [SP.POP.GROW]": "float64",
            "Secondary education, teachers [SE.SEC.TCHR]": "float64",
            "Lending interest rate (%) [FR.INR.LEND]": "float64"
        }


        
        df.drop_duplicates(subset=["Country Code", "Time"], keep='first', inplace=True)
        
        if 'CountryName' in df.columns:
            df = df[df['CountryName'].str.strip().str.lower() != 'kosovo']


        for col, dtype in EXPECTED_DTYPES.items():
            if col in df.columns:
                try:
                    if dtype == "datetime64[ns]":
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    elif dtype == "int64":
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        if col == "Time":
                            df[col] = df[col].astype(pd.Int64Dtype())
                    elif dtype == "float64":
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    else:
                        df[col] = df[col].astype(dtype, errors='ignore')
                except Exception as e:
                    print(f"Could not convert column {col} to {dtype}: {e}")



        df.rename(columns={
            "Country Name": "CountryName", 
            "Country Code": "CountryISO", 
            "Time": "Year"
        }, inplace=True)
        

        
        if 'CountryISO' in df.columns:
            df['CountryISO'] = df['CountryISO'].str.upper()

        
        if 'Year' in df.columns:
            
            
            if not df['Year'].isnull().all():
                df = df[(df['Year'] >= 1900) & (df['Year'] <= pd.Timestamp.now().year)]
                print(f"Rows after year constraint (1900-{pd.Timestamp.now().year}): {len(df)}")
            else:
                print("Warning: 'Year' column is all NaN, skipping year constraint.")
        
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='float')
        for col in df.select_dtypes(include=['int64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='integer')


       
        if 'CountryISO' in df.columns:
            print(f"Nulls in CountryISO: {df['CountryISO'].isnull().sum()}")
        if 'Year' in df.columns:
            print(f"Nulls in Year: {df['Year'].isnull().sum()}")

       
        return df

    def preprocess_natural_disaster_data(self, df):
        bdmlib = BDMLibrary()
        df = bdmlib.clean_column_names(df)

        df.drop_duplicates(inplace=True)
  
        country_col = 'country'
        disaster_type_col = 'disaster_type'
        disaster_subtype_col = 'disaster_subtype'
        region_col = 'region'
        location_col = 'location'
        event_name_col = 'event_name'

        start_year_col = 'start_year'
        start_month_col = 'start_month'
        start_day_col = 'start_day'
        end_year_col = 'end_year'
        end_month_col = 'end_month'
        end_day_col = 'end_day'

        total_deaths_col = 'total_deaths'
        no_injured_col = 'no_injured'
        no_affected_col = 'no_affected'
        no_homeless_col = 'no_homeless'
        total_affected_col = 'total_affected'
        reconstruction_costs_col = 'reconstruction_costs_usd'
        insured_damages_col = 'insured_damages_usd'
        total_damages_col = 'total_damages_usd'
        latitude_col = 'latitude'
        longitude_col = 'longitude'

        text_cols_to_normalize = [col for col in [disaster_type_col, disaster_subtype_col, country_col, region_col, location_col, event_name_col] if col in df.columns]
        for col in text_cols_to_normalize:
            df = bdmlib.normalize_text_column(df, col)

        df = bdmlib.create_date_from_3_columns(df, start_year_col, start_month_col, start_day_col, 'startdate')
     

        df = bdmlib.create_date_from_3_columns(df, end_year_col, end_month_col, end_day_col, 'enddate')
        

        numeric_cols_list = [col for col in [
            total_deaths_col, no_injured_col, no_affected_col, no_homeless_col, total_affected_col,
            reconstruction_costs_col, insured_damages_col, total_damages_col,
            latitude_col, longitude_col
        ] if col in df.columns]
        for col in numeric_cols_list:
        
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                print(f"Warning: Column '{col}' not found for numeric conversion.")
            



        cols_to_check_non_negative = [total_deaths_col, no_injured_col, no_affected_col, no_homeless_col, total_affected_col]
        for col in cols_to_check_non_negative:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                negative_mask = df[col] < 0
                if negative_mask.any():
                    print(f"Warning: Column '{col}' has {negative_mask.sum()} negative values. Setting them to NA.")
                    df.loc[negative_mask, col] = pd.NA

        if ('startdate' in df.columns and 'enddate' in df.columns and
            pd.api.types.is_datetime64_any_dtype(df['startdate']) and
            pd.api.types.is_datetime64_any_dtype(df['enddate'])):
            invalid_dates_mask = df['startdate'] > df['enddate']
            if invalid_dates_mask.any():
                print(f"Warning: {invalid_dates_mask.sum()} records where StartDate > EndDate. Setting EndDate to StartDate for these records.")
                df.loc[invalid_dates_mask, 'enddate'] = df.loc[invalid_dates_mask, 'startdate']
        else:
            print("Warning: 'startdate' or 'enddate' not suitable for StartDate <= EndDate check.")

        
        for col in df.columns:
            if pd.api.types.is_integer_dtype(df[col]):
                df[col] = pd.to_numeric(df[col], downcast='integer')
            elif pd.api.types.is_float_dtype(df[col]):
                df[col] = pd.to_numeric(df[col], downcast='float')


        # Move ISO column to the end
        if 'iso' in df.columns:
            df.rename(columns={'iso': 'CountryISO'}, inplace=True)
            cols = [col for col in df.columns if col != 'CountryISO'] + ['CountryISO']
            df = df[cols]

        # Rename 'latitude' to 'Latitude'
        if 'latitude' in df.columns:
            df.rename(columns={'latitude': 'Latitude'}, inplace=True)

        # Rename 'longitude' to 'Longitude'
        if 'longitude' in df.columns:
            df.rename(columns={'longitude': 'Longitude'}, inplace=True)

        #rename startdate and enddate to StartDate and EndDate
        if 'startdate' in df.columns:
            df.rename(columns={'startdate': 'StartDate'}, inplace=True)
        if 'enddate' in df.columns:
            df.rename(columns={'enddate': 'EndDate'}, inplace=True)

        return df

    def preprocess_air_quality5(self, df):
        original_date_col = 'Date'       
        country_col_original = 'Country' 
        city_col = 'City'                
        aqi_value_col = 'aqi_value'
        aqi_category_col = 'AQI Category'

        standardized_date_col = 'DateTime'
        standardized_country_col = 'CountryName'
        iso_country_col = 'CountryISO'


        if country_col_original in df.columns:
            df[country_col_original] = df[country_col_original].astype(str).str.strip()
        else:
            print(f"Warning: Original country column '{country_col_original}' not found.")

        if city_col in df.columns:
            df[city_col] = df[city_col].astype(str).str.strip()
        else:
            print(f"Warning: City column '{city_col}' not found. This might be okay if data is country-level.")

        if aqi_category_col in df.columns:
            df[aqi_category_col] = df[aqi_category_col].astype(str).str.strip()
        else:
            print(f"Warning: AQI Category column '{aqi_category_col}' not found.")


        if country_col_original in df.columns and country_col_original != standardized_country_col:
            df.rename(columns={country_col_original: standardized_country_col}, inplace=True)

        if standardized_country_col in df.columns:
            df = df[df[standardized_country_col] != 'Kosovo']

        if standardized_country_col in df.columns and hasattr(BDMLibrary, 'get_country_iso_code'):
            bdm = BDMLibrary()
            df[iso_country_col] = df[standardized_country_col].apply(bdm.get_country_iso_code)
            
            def validate_iso_code(iso_value):
                if isinstance(iso_value, str) and len(iso_value) == 3 and iso_value.isupper():
                    return iso_value
                return None if pd.isna(iso_value) or not (isinstance(iso_value, str) and len(iso_value) == 3 and iso_value.isupper()) else iso_value
            df[iso_country_col] = df[iso_country_col].apply(validate_iso_code)
            
        elif not hasattr(BDMLibrary, 'get_country_iso_code'):
            df[iso_country_col] = None
        else:
            df[iso_country_col] = None
        bdmlib = BDMLibrary()

        if original_date_col in df.columns:
            if hasattr(bdmlib, 'cast_date_column'):
                df = bdmlib.cast_date_column(df, column_name=original_date_col)
                if 'DateTime' in df.columns and 'DateTime' != standardized_date_col:
                    df.rename(columns={'DateTime': standardized_date_col}, inplace=True)
                elif standardized_date_col not in df.columns: 
                    df[standardized_date_col] = pd.to_datetime(df[original_date_col], errors='coerce')
            else:
                df[standardized_date_col] = pd.to_datetime(df[original_date_col], errors='coerce')
            
            if df[standardized_date_col].isnull().any():
                print(f"Warning: Some dates in '{original_date_col}' could not be parsed and are now NaT in '{standardized_date_col}'.")
        else:
            df[standardized_date_col] = pd.NaT # Create column with NaT if source is missing

        
        
        if standardized_date_col in df.columns and pd.api.types.is_datetime64_any_dtype(df[standardized_date_col]):
            df['Year'] = df[standardized_date_col].dt.year

        else:
            df['Year'] = None
            df['Month'] = None
            df['Day'] = None


        numeric_cols_to_process = []
        if aqi_value_col in df.columns: numeric_cols_to_process.append(aqi_value_col)


        for col in numeric_cols_to_process:
            if col in df.columns:
                
                df[col] = df[col].replace(['-', 'N/A', ''], np.nan) 
                df[col] = pd.to_numeric(df[col], errors='coerce')
                if df[col].isnull().any():
                    print(f"Warning: Some values in '{col}' were non-numeric and are now NaN.")
            else:
                print(f"Warning: Expected numeric column '{col}' not found for schema correction.")



        if aqi_value_col in df.columns and pd.api.types.is_numeric_dtype(df[aqi_value_col]):
            
            invalid_aqi_negative = df[df[aqi_value_col] < 0].shape[0]
            if invalid_aqi_negative > 0:
                df.loc[df[aqi_value_col] < 0, aqi_value_col] = np.nan
            
        deduplication_keys = []
        if standardized_date_col in df.columns and pd.api.types.is_datetime64_any_dtype(df[standardized_date_col]):
            deduplication_keys.append(standardized_date_col)
        if iso_country_col in df.columns:
            deduplication_keys.append(iso_country_col)
        elif standardized_country_col in df.columns:
            deduplication_keys.append(standardized_country_col)
        if city_col in df.columns:
            deduplication_keys.append(city_col)


        
        
        if standardized_date_col in df.columns and original_date_col in df.columns and original_date_col != standardized_date_col:
            df.drop(columns=[original_date_col], inplace=True)

        df.rename(columns={"AQI Value": "aqi"}, inplace=True)
        df = df[df['aqi'].notna() & (df['aqi'] >= 0)]
        
        return df




    def world_happiness_data(self, df):
        bdmlib = BDMLibrary()
        #df.columns = [bdmlib.clean_column_names(col) for col in df.columns]
        df = bdmlib.clean_column_names(df)

        if 'country_name' in df.columns: 
            df.rename(columns={'country_name': 'country'}, inplace=True)
        if 'country_name' in df.columns: 
            df.rename(columns={'country_name': 'country'}, inplace=True)

        initial_rows = len(df)
        df.drop_duplicates(inplace=True)
        print(f"Removed {initial_rows - len(df)} duplicate rows.")



        if 'country' in df.columns:
            print("Attempting to get ISO codes for countries...")
            df['CountryISO'] = df['country'].apply(lambda x: bdmlib.get_country_iso_code(x) if pd.notnull(x) else None)
            missing_iso = df[df['CountryISO'].isnull()]['country'].unique()
            if len(missing_iso) > 0:
                print(f"Warning: Could not find ISO codes for: {missing_iso.tolist()}")
            else:
                print("Successfully generated ISO codes for all found countries.")
        else:
            print("Warning: 'country' column not found for ISO code generation.")


        numeric_cols_candidates = [
            'ladder_score', 'standard_error_of_ladder_score',
            'standard_error', 
            'upperwhisker', 'lowerwhisker', 
            'upper_whisker', 'lower_whisker',
            'logged_gdp_per_capita', 'gdp_per_capita',
            'social_support',
            'healthy_life_expectancy_at_birth', 'healthy_life_expectancy',
            'freedom_to_make_life_choices', 'freedom',
            'generosity',
            'perceptions_of_corruption', 'trust_government_corruption',
            'dystopia_residual', 'dystopia_plus_residual'
        ]
        
        actual_numeric_cols = [col for col in numeric_cols_candidates if col in df.columns]

        for col in actual_numeric_cols:
            if not pd.api.types.is_numeric_dtype(df[col]):
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    print(f"Converted column '{col}' to numeric.")
                except Exception as e:
                    print(f"Could not convert column '{col}' to numeric: {e}")

        for col in df.select_dtypes(include=np.number).columns:
            if df[col].isnull().any():
                median_val = df[col].median()
                df[col].fillna(median_val, inplace=True)
                

        if 'ladder_score' in df.columns:
            invalid_scores = df[(df['ladder_score'] < 0) | (df['ladder_score'] > 10)]
            if not invalid_scores.empty:
                print(f"Warning: {len(invalid_scores)} records with ladder_score outside 0-10 range.")
                

        
        lower_whisker_col, upper_whisker_col = None, None
        if 'lowerwhisker' in df.columns and 'upperwhisker' in df.columns:
            lower_whisker_col, upper_whisker_col = 'lowerwhisker', 'upperwhisker'
        elif 'lower_whisker' in df.columns and 'upper_whisker' in df.columns:
            lower_whisker_col, upper_whisker_col = 'lower_whisker', 'upper_whisker'
            
        if lower_whisker_col and upper_whisker_col:
            if pd.api.types.is_numeric_dtype(df[lower_whisker_col]) and pd.api.types.is_numeric_dtype(df[upper_whisker_col]):
                invalid_whiskers = df[df[lower_whisker_col] > df[upper_whisker_col]]
                if not invalid_whiskers.empty:
                    print(f"Warning: {len(invalid_whiskers)} records where {lower_whisker_col} > {upper_whisker_col}.")
            else:
                print(f"Warning: Whisker columns ({lower_whisker_col}, {upper_whisker_col}) are not both numeric, skipping consistency check.")


        
        for col in df.select_dtypes(include=np.number).columns:
            original_type = df[col].dtype
            
            if pd.api.types.is_float_dtype(original_type):
                df[col] = pd.to_numeric(df[col], downcast='float')
            
            elif pd.api.types.is_integer_dtype(original_type) or pd.api.types.is_bool_dtype(original_type):
                
                if (df[col].fillna(0) % 1 == 0).all():
                    df[col] = pd.to_numeric(df[col], downcast='integer')



        cols_to_front = []
        if 'country' in df.columns: cols_to_front.append('country')
        if 'CountryISO' in df.columns: cols_to_front.append('CountryISO')
        existing_cols_to_front = [col for col in cols_to_front if col in df.columns]
        other_cols = [col for col in df.columns if col not in existing_cols_to_front]
        df = df[existing_cols_to_front + other_cols]
        if existing_cols_to_front:
            print(f"Reordered columns. New order starts with: {existing_cols_to_front}")


        if 'country' in df.columns:
            kosovo_count = df[df['country'].str.strip().str.lower() == 'kosovo'].shape[0]
            df = df[df['country'].str.strip().str.lower() != 'kosovo']
            if kosovo_count > 0:
                print(f"Removed {kosovo_count} rows where country is Kosovo.")

        
        
        if 'ladder_score' in df.columns and lower_whisker_col and upper_whisker_col:
            invalid_whiskers = df[(df[lower_whisker_col].notnull() & df[upper_whisker_col].notnull()) &
                                ((df[lower_whisker_col] > df['ladder_score']) | (df['ladder_score'] > df[upper_whisker_col]))]
            if not invalid_whiskers.empty:
                print(f"Warning: {len(invalid_whiskers)} records where {lower_whisker_col} > ladder_score or ladder_score > {upper_whisker_col}.")


        return df
    
    def preprocess_air_quality(self, df):
        bdmlib = BDMLibrary()
        df.columns = [col.strip() for col in df.columns]

        if 'Country' in df.columns:
            df = df[df['Country'].str.strip() != 'Kosovo']

        if 'AQI Value' in df.columns:
            df['AQI Value'] = pd.to_numeric(df['AQI Value'], errors='coerce')
            df = df[df['AQI Value'].notna() & (df['AQI Value'] >= 0)]

        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        
        df['Year'] = df['Date'].dt.year if 'Date' in df.columns else None

        df.rename(columns={'AQI_Value': 'aqi'}, inplace=True)
        df.rename(columns={'Date': 'DateTime'}, inplace=True)
        
        df['CountryISO'] = df['Country'].apply(lambda x: bdmlib.get_country_iso_code(x) if pd.notnull(x) else None)
        return df

    def perform_transformations(self):
        cleaned = []

        for df in self.dataframes:
            name = df.name.lower()
            print(f"Processing DataFrame: {repr(name)}")
            
            # Clean column names
            '''
            df.columns = (
                df.columns.str.strip()
                        .str.lower()
                        .str.replace(r'\s+', '_', regex=True)
                        .str.replace(r'__', '_', regex=True)
            )
            '''

            # Drop duplicates
            #df.drop_duplicates(inplace=True)

            #Apply transformations based on DataFrame name
            if name.lower().startswith("air_quality_index"):
                print("Performing air quality index preprocessing...")
                df = self.preprocess_air_quality(df)
                
            if name.lower().startswith("current_air_quality_data"):
                print("Performing air quality data preprocessing...")
                df = self.preprocess_air_quality_data(df)
            if name.lower().startswith("historical_weather"):
                print("Performing historical temperatures preprocessing...")
                df = self.preprocess_historical_temperatures(df)
            if name.lower().startswith("globalenvironmental"):
                print("Performing global environment temperature preprocessing...")
                df = self.global_environment_temperature_trends(df)
            if name.lower().startswith("globallandtemperaturesby"):
                print("Performing global land temperatures by major city...")
                #df = self.global_environment_temperature_trends(df)     #LLAMAS 2 VECES A LA MISMA FUNCION. ¿NO ES UN ERROR?
                df = self.global_land_temperature_by_major_city(df)
            
            
            #if name.startswith("world_hapiness_scores"):
            #    print("Performing happiness data preprocessing...")
            #    #df = self.happiness_data(df)
            
            
            if name.lower().startswith("natural_disaster"):
                print("Performing natural disaster data preprocessing...")
                df = self.preprocess_natural_disaster_data(df)

            
            if name.lower().startswith("world"):
                print("Performing world happiness data preprocessing...")
                df = self.world_happiness_data(df)
             

            df.name = name 
            cleaned.append(df)

        # Replace original dataframes with cleaned ones
        self.dataframes = cleaned