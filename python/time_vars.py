"""Functions for working with data that does not contain a spatial component

This data will be the same for all observations in a given period, but will
vary over time, e.g. weather data, scheduled events, etc.
"""

import csv

from s3_utils import get_from_s3

def get_weather_data(path):
    """Helper function to get weather data from S3 or locally

    Args:
      path (str): path (either local or s3 (s3://...)) to CSV to load weather data from

    Returns:
      dict: period => list(weather data)
    """

    if path.startswith('s3'):
        csvpath = get_from_s3(path)
    else:
        csvpath = path

    weather_dict = {}
    with open(csvpath, 'rb') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            date_str = row.pop('datetime')
            date, time = date_str.split('T')
            year, month, day = date.split('-')
            hour, _, _ = time.split(':')
            period = year + month + day + hour
            weather_dict[period] = row.values()
    return weather_dict
