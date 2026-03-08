# 1. Import Key Libraries:
import pandas as pd 
import requests
import sqlite3
from datetime import datetime, timezone
import logging
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

# Constants:
DB_NAME = 'yhz_db.sqlite'
TABLE_NAME = 'hiaa_geomet_hourly'
URL = "https://api.weather.gc.ca/collections/climate-hourly/items"
CLIMATE_IDENTIFIER = "8202251"


# 2. Extract Data from API Endpoint:
def extract():
   try:                                                                                                     # Extracts hourly climate data for the latest available full date from the Geomet API
      params = {                                                                                            # Step 1: Get the latest available date by sorting records in descending order and limiting to 1
         "CLIMATE_IDENTIFIER": CLIMATE_IDENTIFIER,
         "sortby": "-LOCAL_DATE",
         "limit": 1
      }
      response = requests.get(URL, params=params, timeout=30)
      response.raise_for_status()                                                                           # Check for HTTP errors    
      data = response.json()
      if not data.get('features'):
         logging.error("No data found in API response")
         return None
      
      latest_date = data['features'][0]['properties']['LOCAL_DATE']                                         # Extract just the DATE part from the LOCAL_DATE timestamp                          
      latest_date = latest_date.split(' ')[0]
      
      params = {                                                                                            # Step 2: Fetch all 24 hours of data for the latest available date
         "CLIMATE_IDENTIFIER": CLIMATE_IDENTIFIER,
         "datetime": f"{latest_date}/{latest_date}",
         "limit": 24,
      }
      response = requests.get(URL, params=params, timeout=30)
      response.raise_for_status()                                                                           # Check for HTTP errors                                       
      
      logging.info("Data Extracted Successfully")
      return response.json()
   
   except requests.exceptions.RequestException as e:
      logging.error(f"Request Failed with status code: {e}")
      return None


# 3. Transform Data:
def transform(data):
    if not data:
       return None
    
    try:                                                                                                    # Transform the raw API response to match the schema of the hiaa_geomet_hourly table
       df = pd.DataFrame([feature['properties'] for feature in data['features']])                           # Pull out the properties from each feature and convert to a pandas DataFrame
       #print(df.head())                                                                                    # Display the first few rows of the DataFrame            
       
       df = df[['CLIMATE_IDENTIFIER','LOCAL_DATE','TEMP','DEW_POINT_TEMP',                                  # Select only Columns that are in the SQLite Database
            'HUMIDEX','PRECIP_AMOUNT','RELATIVE_HUMIDITY',
            'STATION_PRESSURE','VISIBILITY','WEATHER_ENG_DESC','WINDCHILL',
            'WIND_DIRECTION','WIND_SPEED']]
       
       df.columns = df.columns.str.lower()                                                                  # Convert API Column Names to lower case to match what is in the databse           
       num_cols = ['temp', 'dew_point_temp', 'humidex', 'precip_amount',                                    # Select only Numeric Columns
                'relative_humidity','station_pressure','visibility',
                'windchill','wind_direction','wind_speed']
       #print(df.dtypes)                                                                                    # This will show that these numeric columns are stored as objects in the DataFrame 
       
       df[num_cols] = df[num_cols].apply(pd.to_numeric, errors='coerce')                                    # So, convert these columns to numeric type, coercing any non-numeric values to NaN     
       df[num_cols] = df[num_cols].fillna(0)                                                                # Convert numeric columns with values "None" to 0   
       df['insert_time'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')                         # Add a new column to log the timestamp when the data is inserted into the database  
       logging.info("Data Transformed Successfully")
       logging.info(f"Sample data: \n{df.head()}")
       return df
    except Exception as e:
        logging.error(f"Data Transformation Failed with error:{e}")
        return None


# 4. Load Data into SQLite Database:
def load(df, DB_NAME, TABLE_NAME):      
  try:                                                                                                      # Loads the transformed DataFrame into the existing SQLite table
     conn = sqlite3.connect(DB_NAME)
     df.to_sql(TABLE_NAME, conn, if_exists='append', index=False)                                           # Append to existing table without adding an index column
     conn.commit()
     conn.close()
     logging.info("Data Loaded Successfully!")
  except:
     logging.error("Failed to load data into the database")



# 5. Main Function to Run the ETL Process:
def main():
   data = extract()
   df = transform(data)
   load(df, DB_NAME, TABLE_NAME)

if __name__ == "__main__":
   main()