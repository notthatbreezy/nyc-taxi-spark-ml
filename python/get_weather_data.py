"""Downloads and parses Forecast IO data"""

import requests
import json
import csv

from pytz import timezone
from datetime import datetime, timedelta

# INSERT YOUR KEY HERE
API_KEY = ""
BASE_URL = "https://api.forecast.io/forecast/%(api_key)s/%(latitude)s,%(longitude)s,%(dt)s"

def get_config():
    """Loads config file with cities"""
    with open(CONFIG, 'rb') as fh:
        return json.load(fh)


class CityWeather(object):
    """Handles getting city weather data"""

    KEYS_TO_DELETE = ['summary', 'icon']
    CSV_KEYS = ['datetime',
                'apparentTemperatureMinTime',
                'cloudCover',
                'temperatureMin',
                'dewPoint',
                'apparentTemperatureMax',
                'temperatureMax',
                'sunlightHours',
                'temperatureMaxTime',
                'windBearing',
                'moonPhase',
                'visibility',
                'sunsetTime',
                'pressure',
                'precipProbability',
                'apparentTemperatureMin',
                'precipIntensityMax',
                'apparentTemperatureMaxTime',
                'humidity',
                'windSpeed',
                'time',
                'precipIntensity',
                'sunriseTime',
                'temperatureMinTime']

    def __init__(self, latitude, longitude, name, startyear, tz_string):
        self.name = name
        self.date_format = '%Y-%m-%dT%H:%M:%S%z'
        import ipdb; ipdb.set_trace()
        self.datetime_start = datetime(startyear, 1, 1, tzinfo=timezone(tz_string))
        self.DICT = dict(api_key=API_KEY,
                         latitude=latitude,
                         longitude=longitude)
        self.date_generator = self._date_iterator(startyear)


    def _date_iterator(self, startyear):
        dt = datetime(startyear, 1, 1)
        td = timedelta(hours=1)
        while dt < datetime.now():
            yield dt
            dt += td

    def make_single_request(self, dt):
        """Makes request, returns dict"""
        self.DICT.update(dict(dt=dt.strftime(self.date_format)))
        url = BASE_URL % self.DICT
        r = requests.get(BASE_URL % self.DICT)
        r.raise_for_status()
        return self.transform_response(r.json(), dt)

    def transform_response(self, response, dt):
        """Transforms response into dictionary to write to CSV"""
        data = response['daily']['data'][0]
        data['datetime'] = dt.strftime(self.date_format)
        for k in self.KEYS_TO_DELETE:
            del data[k]

        data['sunlightHours'] = (data['sunsetTime'] - data['sunriseTime'])/3600.0
        return data

    def write_csv(self):
        with open('%s.csv' % self.name, 'wb') as csvfile:
            csvwriter = csv.DictWriter(csvfile, self.CSV_KEYS, extrasaction='ignore')
            csvwriter.writeheader()
            counter = 0
            for dt in self.date_generator:
                counter += 1
                if counter % 50 == 0:
                    print counter
                csvwriter.writerow(self.make_single_request(dt))

if __name__ == "__main__":
    lat = 40.739710
    lng = -73.969098
    city = 'NYC'
    start_year = 2013
    time_zone = "US/Eastern"
    cw = CityWeather(lat, lng, city, start_year, time_zone)
    print "Getting %s" % city
    cw.write_csv()