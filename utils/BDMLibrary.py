import pandas as pd
import numpy as np
import pycountry
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
import pandas as pd
import re




class BDMLibrary():


    country_iso_dict = {
        "UK": "GBR",
        "USA": "USA",
        "France": "FRA",
        "Argentina": "ARG",
        "Germany": "DEU",
        "China": "CHN",
        "Australia": "AUS",
        "Indonesia": "IDN",
        "Brazil": "BRA",
        "India": "IND",
        "Russia": "RUS",
        "Canada": "CAN",
        "Mexico": "MEX",
        "Japan": "JPN",
        "'South Africa'": "ZAF", 
        "South Africa": "ZAF",
    }
    country_name_corrections = {
        "Albania": "Albania",
        "Algeria": "Algeria",
        "Andorra": "Andorra",
        "Angola": "Angola",
        "Argentina": "Argentina",
        "Armenia": "Armenia",
        "Australia": "Australia",
        "Austria": "Austria",
        "Azerbaijan": "Azerbaijan",
        "Bahrain": "Bahrain",
        "Bangladesh": "Bangladesh",
        "Belarus": "Belarus",
        "Belgium": "Belgium",
        "Belize": "Belize",
        "Bermuda": "Bermuda",
        "Bolivia": "Bolivia, Plurinational State of",
        "Bosnia and Herzegovina": "Bosnia and Herzegovina",
        "Brazil": "Brazil",
        "Brunei": "Brunei Darussalam",
        "Bulgaria": "Bulgaria",
        "Burkina Faso": "Burkina Faso",
        "Cambodia": "Cambodia",
        "Canada": "Canada",
        "Cape Verde": "Cabo Verde",
        "Cayman Islands": "Cayman Islands",
        "Central African Republic": "Central African Republic",
        "Chad": "Chad",
        "Chile": "Chile",
        "China": "China",
        "Colombia": "Colombia",
        "Congo (Brazzaville)": "Congo",
        "Costa Rica": "Costa Rica",
        "Croatia": "Croatia",
        "Cyprus": "Cyprus",
        "Czech Republic": "Czechia",
        "Denmark": "Denmark",
        "Dominican Republic": "Dominican Republic",
        "Ecuador": "Ecuador",
        "Egypt": "Egypt",
        "El Salvador": "El Salvador",
        "Estonia": "Estonia",
        "Ethiopia": "Ethiopia",
        "Finland": "Finland",
        "France": "France",
        "French Guiana": "French Guiana",
        "Gabon": "Gabon",
        "Gambia": "Gambia",
        "Georgia": "Georgia",
        "Germany": "Germany",
        "Ghana": "Ghana",
        "Gibraltar": "Gibraltar",
        "Greece": "Greece",
        "Grenada": "Grenada",
        "Guadeloupe": "Guadeloupe",
        "Guam": "Guam",
        "Guatemala": "Guatemala",
        "Honduras": "Honduras",
        "Hong Kong": "Hong Kong",
        "Hungary": "Hungary",
        "Iceland": "Iceland",
        "India": "India",
        "Indonesia": "Indonesia",
        "Iran": "Iran, Islamic Republic of",
        "Iraq": "Iraq",
        "Ireland": "Ireland",
        "Israel": "Israel",
        "Italy": "Italy",
        "Ivory Coast": "Côte d'Ivoire",
        "Japan": "Japan",
        "Jersey": "Jersey",
        "Jordan": "Jordan",
        "Kazakhstan": "Kazakhstan",
        "Kenya": "Kenya",
        "Kosovo": None,
        "Kuwait": "Kuwait",
        "Kyrgyzstan": "Kyrgyzstan",
        "Laos": "Lao People's Democratic Republic",
        "Latvia": "Latvia",
        "Lebanon": "Lebanon",
        "Liberia": "Liberia",
        "Liechtenstein": "Liechtenstein",
        "Lithuania": "Lithuania",
        "Luxembourg": "Luxembourg",
        "Macao": "Macao",
        "Macedonia": "North Macedonia",
        "Madagascar": "Madagascar",
        "Malaysia": "Malaysia",
        "Malta": "Malta",
        "Martinique": "Martinique",
        "Mexico": "Mexico",
        "Moldova": "Moldova, Republic of",
        "Monaco": "Monaco",
        "Mongolia": "Mongolia",
        "Montenegro": "Montenegro",
        "Myanmar": "Myanmar",
        "Nepal": "Nepal",
        "Netherlands": "Netherlands",
        "New Caledonia": "New Caledonia",
        "New Zealand": "New Zealand",
        "Nigeria": "Nigeria",
        "Norway": "Norway",
        "Pakistan": "Pakistan",
        "Palestinian Territory": "Palestine, State of",
        "Peru": "Peru",
        "Philippines": "Philippines",
        "Poland": "Poland",
        "Portugal": "Portugal",
        "Puerto Rico": "Puerto Rico",
        "Qatar": "Qatar",
        "Reunion": "Réunion",
        "Romania": "Romania",
        "Russia": "Russian Federation",
        "San Marino": "San Marino",
        "Saudi Arabia": "Saudi Arabia",
        "Senegal": "Senegal",
        "Serbia": "Serbia",
        "Singapore": "Singapore",
        "Slovakia": "Slovakia",
        "Slovenia": "Slovenia",
        "South Africa": "South Africa",
        "South Korea": "Korea, Republic of",
        "Spain": "Spain",
        "Sri Lanka": "Sri Lanka",
        "Sudan": "Sudan",
        "Sweden": "Sweden",
        "Switzerland": "Switzerland",
        "Taiwan": "Taiwan, Province of China",
        "Taiwan": "Taiwan Province of China",
        "Taiwan Province Of China": "Taiwan, Province of China",
        "Taiwan Province of China": "Taiwan, Province of China",
        "Tajikistan": "Tajikistan",
        "Tanzania": "Tanzania, United Republic of",
        "Thailand": "Thailand",
        "Togo": "Togo",
        "Trinidad and Tobago": "Trinidad and Tobago",
        "Turkey": "Türkiye",
        "Turkmenistan": "Turkmenistan",
        "Uganda": "Uganda",
        "Ukraine": "Ukraine",
        "United Arab Emirates": "United Arab Emirates",
        "United Kingdom Of Great Britain And Northern Ireland": "United Kingdom",
        "United States Of America": "United States",
        "Uzbekistan": "Uzbekistan",
        "Vatican": "Holy See (Vatican City State)",
        "Venezuela": "Venezuela, Bolivarian Republic of",
        "Vietnam": "Viet Nam",
        "Zambia": "Zambia",
        "Turkiye": "Turkey",
        "Turkiye": "Türkiye",
        "Congo (Kinshasa)": "Congo, Democratic Republic of the",
        "Congo (Kinshasa)": "Congo, Democratic Republic Of The",
        "Congo (Kinshasa)": "Congo, The Democratic Republic of the",
        "Congo (Kinshasa)": "Congo, The Democratic Republic Of The",
        "Palestinian Territory": "Palestine, State of",
        "State of Palestine": "Palestine, State of",
        "Palestinian Territory": "Palestine, State of",
        "State Of Palestine": "Palestine, State of",
        "Hong Kong S.A.R. of China": "Hong Kong",
        "Hong Kong S.A.R. Of China": "Hong Kong",
        "UK": "United Kingdom",
        "Uk": "United Kingdom",
        "USA": "United States, The",
        "Usa": "United States",
    }

    
    city_to_iso = {
        "Paris": "FRA",
        "London": "GBR",
        "Berlin": "DEU",
        "Rome": "ITA",
        "Madrid": "ESP",
        "Vienna": "AUT",
        "Amsterdam": "NLD",
        "Brussels": "BEL",
        "Lisbon": "PRT",
        "Athens": "GRC",
        "Oslo": "NOR",
        "Stockholm": "SWE",
        "Copenhagen": "DNK",
        "Helsinki": "FIN",
        "Dublin": "IRL",
        "Warsaw": "POL",
        "Prague": "CZE",
        "Budapest": "HUN",
        "Zurich": "CHE",
        "Barcelona": "ESP",
        "Milan": "ITA",
        "Istanbul": "TUR",
        "Moscow": "RUS",
        "New York": "USA",
        "Washington D.C.": "USA",
        "Los Angeles": "USA",
        "San Francisco": "USA",
        "Chicago": "USA",
        "Toronto": "CAN",
        "Ottawa": "CAN",
        "Mexico City": "MEX",
        "Buenos Aires": "ARG",
        "Brasília": "BRA",
        "Rio de Janeiro": "BRA",
        "Lima": "PER",
        "Santiago": "CHL",
        "Bogotá": "COL",
        "Tokyo": "JPN",
        "Beijing": "CHN",
        "Shanghai": "CHN",
        "Seoul": "KOR",
        "Bangkok": "THA",
        "Singapore": "SGP",
        "Jakarta": "IDN",
        "Delhi": "IND",
        "Mumbai": "IND",
        "Dubai": "ARE",
        "Riyadh": "SAU",
        "Tehran": "IRN",
        "Cairo": "EGY",
        "Cape Town": "ZAF",
        "Johannesburg": "ZAF",
        "Nairobi": "KEN",
        "Sydney": "AUS",
        "Melbourne": "AUS",
        "Canberra": "AUS",
        "Wellington": "NZL"
    }


    def __init__(self):
        pass

    def get_country_iso_from_api(self,city_name: str, retries=3, delay=2, geolocator_user_agent="city_to_iso_resolver_v1"):

        if not city_name or not isinstance(city_name, str) or city_name.strip() == "":
            return None

        geolocator = Nominatim(user_agent=geolocator_user_agent)
        for attempt in range(retries):
            try:
                location = geolocator.geocode(city_name, language='en', addressdetails=True)
                if location and 'address' in location.raw and 'country_code' in location.raw['address']:
                    country_iso_alpha2 = location.raw['address']['country_code'].upper()
                    country = pycountry.countries.get(alpha_2=country_iso_alpha2)
                    return country.alpha_3 if country else None
                return None
            except GeocoderTimedOut:
                time.sleep(delay)
                continue
            except GeocoderUnavailable:
                return None
            except Exception:
                return None
        return None

    
    def get_country_iso_code(self,country_name):
        try:
            #Country name to upper camel case
            country_name = country_name.strip().title()  # Standardize the country name

            
            corrected_name = self.country_name_corrections.get(country_name, country_name)
            if corrected_name is None:
                return None
            country = pycountry.countries.get(name=corrected_name)
            if country:
                return country.alpha_3

            print(f"Warning: Country ISO code not found for '{country_name}' (corrected to '{corrected_name}').")
            return None
        except Exception as e:
            print(f"Error getting ISO code for '{country_name}': {e}")
            return None


    def cast_date_column_bk(self,df, column_name):

        print(df[column_name])
        df['DateTime'] = pd.to_datetime(df[column_name], format='%Y-%m-%d', errors='raise')
        return df




    def create_date_from_3_columns(self,df, year_col, month_col, day_col, new_column_name='DateTime', date_type='start', errors='coerce'):

        if year_col not in df.columns:
            print(f"Warning: Year column '{year_col}' not found. Returning original DataFrame without new date column.")

            return df

        if date_type not in ['start', 'end']:
            print(f"Warning: Invalid date_type '{date_type}'. Must be 'start' or 'end'. Defaulting to 'start' behavior.")
            date_type = 'start'

        dates_list = []

        for index, row_data in df.iterrows():
            year_val = row_data.get(year_col)
            year = pd.to_numeric(year_val, errors='coerce')

            if pd.isna(year):
                dates_list.append(pd.NaT)
                continue
            

            try:
                current_year = int(year)
            except ValueError:
                dates_list.append(pd.NaT)
                continue



            month_val = row_data.get(month_col)
            month_numeric = pd.to_numeric(month_val, errors='coerce')

            if pd.isna(month_numeric):
                current_month = 1 if date_type == 'start' else 12
            else:
                current_month = int(month_numeric)
                if not (1 <= current_month <= 12):
                    if errors == 'raise':
                        raise ValueError(f"Month value {current_month} for year {current_year} is out of bounds (1-12) for row index {index}.")
                    dates_list.append(pd.NaT)
                    continue
            

            day_val = row_data.get(day_col)
            day_numeric = pd.to_numeric(day_val, errors='coerce')

            if pd.isna(day_numeric):
                if date_type == 'start':
                    current_day = 1
                else:
                    try:
                        
                        if current_month == 12:
                            
                            next_month_first_day = pd.Timestamp(year=current_year + 1, month=1, day=1)
                        else:
                            
                            next_month_first_day = pd.Timestamp(year=current_year, month=current_month + 1, day=1)
                        current_day = (next_month_first_day - pd.Timedelta(days=1)).day
                    except ValueError as e_day_calc: 
                        if errors == 'raise':
                            raise ValueError(f"Error calculating last day for Y:{current_year}, M:{current_month} (row {index}): {e_day_calc}")
                        dates_list.append(pd.NaT)
                        continue
            else:
                current_day = int(day_numeric)

            
            try:
                dt_obj = pd.Timestamp(year=current_year, month=current_month, day=current_day)
                dates_list.append(dt_obj)
            except ValueError: 
                if errors == 'coerce':
                    dates_list.append(pd.NaT)
                elif errors == 'raise':
                    raise ValueError(f"Invalid date components for row index {index}: Year={current_year}, Month={current_month}, Day={current_day}")
                elif errors == 'ignore': 
                    dates_list.append(pd.NaT) 
            except Exception as e: 
                print(f"Unexpected error creating Timestamp for Y:{current_year}, M:{current_month}, D:{current_day} (row index {index}): {e}")
                if errors == 'coerce':
                    dates_list.append(pd.NaT)
                elif errors == 'raise':
                    raise
                else: 
                    dates_list.append(pd.NaT)

        df[new_column_name] = pd.Series(dates_list, index=df.index, dtype='datetime64[ns]')
        return df




    def cast_date_column(self,df, column_name, new_column_name='DateTime', errors='coerce'):

        df[new_column_name] = pd.to_datetime(df[column_name], errors=errors, infer_datetime_format=True)
        return df


    def convert_latitude(self,df, lat_col, errors='coerce'):

        def process_value(val):
            if isinstance(val, str):

                processed_val = val 
                val_upper = val.upper() 
                
                if val_upper.endswith('S'):

                    processed_val = '-' + val_upper.rstrip('S')
                elif val_upper.endswith('N'):

                    processed_val = val_upper.rstrip('N')
                return processed_val 
            return val 


        processed_series = df[lat_col].apply(process_value)
        

        df[lat_col] = pd.to_numeric(processed_series, errors=errors)
        
        return df


    def convert_longitude(self,df, lon_col, errors='coerce'):
        
        def process_value(val):
            if isinstance(val, str):
                processed_val = val 
                val_upper = val.upper() 
                
                if val_upper.endswith('W'):

                    processed_val = '-' + val_upper.rstrip('W')
                elif val_upper.endswith('E'):

                    processed_val = val_upper.rstrip('E')
                return processed_val
            return val 


        processed_series = df[lon_col].apply(process_value)
        
        df[lon_col] = pd.to_numeric(processed_series, errors=errors)
        
        return df

    def clean_column_names(self,df):
        '''Cleans DataFrame column names.'''
        new_columns = []
        for col in df.columns:
            new_col = str(col).lower() 
            new_col = re.sub(r'\s+', '_', new_col)
            new_col = re.sub(r'[^a-zA-Z0-9_]', '', new_col) 
            if not new_col:
                new_col = 'unnamed_col'
            new_columns.append(new_col)
        df.columns = new_columns


        
        cols = pd.Series(df.columns)
        if cols.duplicated().any():
            counts = cols.groupby(cols).cumcount()
            df.columns = np.where(cols.duplicated(keep=False), 
                                cols + '_' + counts.astype(str), 
                                cols)
        return df

    def normalize_text_column(self,df, column_name):
        '''Normalizes a text column: strips whitespace, converts to lowercase.'''
        if column_name in df.columns:
            df[column_name] = df[column_name].astype(str).str.strip().str.lower()
            df[column_name].replace(['nan', 'none', '', 'null'], pd.NA, inplace=True)
        return df