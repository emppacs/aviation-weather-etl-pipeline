import pytest
import pandas as pd
from src.pipeline import transform

def test_transform_valid_data():
    """Test that valid API data is correctly transformed into a DataFrame."""
    # mock data 
    mock_api_data = {
        'features': [
            {
                'properties': {
                    'CLIMATE_IDENTIFIER': '8202251',
                    'LOCAL_DATE': '2026-03-19 10:00:00',
                    'TEMP': '5.5',
                    'DEW_POINT_TEMP': '2.1',
                    'HUMIDEX': None,
                    'PRECIP_AMOUNT': '0.0',
                    'RELATIVE_HUMIDITY': '75',
                    'STATION_PRESSURE': '101.2',
                    'VISIBILITY': '15.0',
                    'WEATHER_ENG_DESC': 'Mainly Cloudy',
                    'WINDCHILL': None,
                    'WIND_DIRECTION': '280',
                    'WIND_SPEED': '15'
                }
            }
        ]
    }

    # 2. Run transform function
    df = transform(mock_api_data)

    # 3. Assertions
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df['temp'].iloc[0] == 5.5  # Check numeric conversion
    assert df['humidex'].iloc[0] == 0   # Check None -> 0 imputation
    assert 'insert_time' in df.columns # Check metadata was added

def test_transform_empty_data():
    """Test that the function handles empty input gracefully."""
    assert transform(None) is None
    assert transform({}) is None