# 1. Import Key Libraries:
import pandas as pd 
import requests
import sqlite3
from datetime import datetime, timezone
import logging
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

# Constants:
DB_NAME = 'data/yhz_db.sqlite'
TABLE_NAME = 'hiaa_geomet_hourly'
URL = "https://api.weather.gc.ca/collections/climate-hourly/items"
CLIMATE_IDENTIFIER = "8202251"


# 2. Extract Data from API Endpoint:
def extract():
   """
    Extracts hourly climate data for the latest available date from the Geomet API.
    Uses a two-step check process to ensure 24 hours of data are captured.
   """
   try: 
      # Step 1: Check the latest available date                                                                                                    
      params = {                                                                                            
         "CLIMATE_IDENTIFIER": CLIMATE_IDENTIFIER,
         "sortby": "-LOCAL_DATE",
         "limit": 1
      }
      response = requests.get(URL, params=params, timeout=30)
      response.raise_for_status()                                                                            
      data = response.json()
      
      if not data.get('features'):
         logging.error("No data found in API response")
         return None
      
      latest_date = data['features'][0]['properties']['LOCAL_DATE'].split(' ')[0]

      # Step 2: Fetch all 24 hours of data for that specific dat   
      params = {                                                                                            
         "CLIMATE_IDENTIFIER": CLIMATE_IDENTIFIER,
         "datetime": f"{latest_date}/{latest_date}",
         "limit": 24,
      }
      response = requests.get(URL, params=params, timeout=30)
      response.raise_for_status()                                                                                                           
      
      logging.info("Data Extracted Successfully")
      return response.json()
   
   except requests.exceptions.RequestException as e:
      logging.error(f"Request Failed with status code: {e}")
      return None


# 3. Transform Data:
def transform(data):
   """
    Cleans and normalizes the raw API JSON response into a Pandas DataFrame.
    Aligns data types and handles missing values.
   """
   if not data:
      return None
    
   try:                                                                                                    
      df = pd.DataFrame([feature['properties'] for feature in data['features']])                                                                                                                    
       
      # Select and normalize columns
      df = df[['CLIMATE_IDENTIFIER','LOCAL_DATE','TEMP','DEW_POINT_TEMP', 
            'HUMIDEX','PRECIP_AMOUNT','RELATIVE_HUMIDITY',                                 
            'STATION_PRESSURE','VISIBILITY','WEATHER_ENG_DESC','WINDCHILL',
            'WIND_DIRECTION','WIND_SPEED']]
       
      # Convert column names to lowercase for consistency
      df.columns = df.columns.str.lower()

      # Convert numeric columns to appropriate data types and handle missing values                                                                        
      num_cols = ['temp', 'dew_point_temp', 'humidex', 'precip_amount',                                  
                'relative_humidity','station_pressure','visibility',
                'windchill','wind_direction','wind_speed']
      df[num_cols] = df[num_cols].apply(pd.to_numeric, errors='coerce')                                         
      df[num_cols] = df[num_cols].fillna(0)

      # Add insert_time column with current UTC timestamp                                                                   
      df['insert_time'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')      
                         
      logging.info("Data Transformed Successfully")
      logging.info(f"Sample data: \n{df.head()}")
      return df
   except Exception as e:
      logging.error(f"Data Transformation Failed with error:{e}")
      return None


# 4. Load Data into SQLite Database:
def load(df, DB_NAME, TABLE_NAME):
   """
    Loads data into SQLite with Idempotency logic to prevent duplicates.
    Ensures the table and unique constraints exist before insertion.
   """    
   if df is None or df.empty:
      logging.error("No data to load into the database")
      return  
   
   try:
      conn = sqlite3.connect(DB_NAME)
      cursor = conn.cursor()

       # Create table if it doesn't exist with a unique constraint on climate_identifier and local_date to prevent duplicates
      cursor.execute(f"""
         CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            climate_identifier TEXT,
            local_date TEXT,
            temp REAL,
            dew_point_temp REAL,
            humidex REAL,
            precip_amount REAL,
            relative_humidity REAL,
            station_pressure REAL,
            visibility REAL,
            weather_eng_desc TEXT,
            windchill REAL,
            wind_direction REAL,
            wind_speed REAL,
            insert_time TEXT,
            UNIQUE(climate_identifier, local_date)
         )
      """)

      for _, row in df.iterrows():
         cursor.execute(f"""
            INSERT OR IGNORE INTO {TABLE_NAME} (
               climate_identifier, local_date, temp, dew_point_temp, humidex, precip_amount,
               relative_humidity, station_pressure, visibility, weather_eng_desc, windchill,
               wind_direction, wind_speed, insert_time
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         """, tuple(row))
      
      conn.commit()
      conn.close()
      logging.info("Data Loaded Successfully!")
   except Exception as e:
     logging.error(f"Failed to load data into the database: {e}")


# 5. Main Function to Run the ETL Process:
def main():
   data = extract()
   df = transform(data)
   load(df, DB_NAME, TABLE_NAME)

if __name__ == "__main__":
   main()