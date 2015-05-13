"""Constants and other things that do not change"""

from datetime import datetime, timedelta
from collections import namedtuple
from rasterio.transform import from_bounds

raster_extent_fields = ['x_min', 'y_min', 'x_max', 'y_max',
                        'cell_height', 'cell_width', 'rows',
                        'cols']
RasterExtent = namedtuple('RasterExtent', raster_extent_fields)
raster_extent = RasterExtent(562786.017, 4482993.928, 609786.017, 4530093.928,
                             50.0, 50.0, 942, 949)

# rasterio kwargs
raster_kwargs = {'blockxsize': 949, 'blockysize': 942, 'count': 1,
                 'crs': {'init': u'epsg:26918'}, 'driver': u'GTiff',
                 'dtype': 'float32', 'nodata': None,
                 'height': 949, 'width': 942,
                 'tiled': False, 'transform': from_bounds(
                     raster_extent.x_min,
                     raster_extent.y_min,
                     raster_extent.x_max,
                     raster_extent.y_max,
                     raster_extent.cols,
                     raster_extent.rows
                 )}

def get_weekday_dict():
    """Return a dictionary that maps period => list of weekday obs

    Used to create indicator variables for day of week for each period
    """
    current_day = datetime(2013, 01, 01)

    days_of_week = {}

    while current_day < datetime(2014, 01, 01):
        l = [0, 0, 0, 0, 0, 0, 0]
        key = current_day.strftime('%Y%m%d')
        weekday = current_day.weekday()
        l[weekday] = 1
        days_of_week[key] = l
        current_day = current_day + timedelta(days=1)
    return days_of_week

def get_hours_dict():
    """Function to create a dictionary converting periods -> hours
    and hours -> periods

    hours is the number of hours since 2013/01/01 at Midnight

    This dictionary is necessary for properly incrementing periods during aggregation
    """
    current_day = datetime(2012, 1, 1)
    last_day = datetime(2015, 1, 2)
    hours_dict = {'hours': {},
                  'period': {}}

    num_hours = 0
    while current_day < last_day:
        period = current_day.strftime('%Y%m%d%H')
        hours_dict['hours'][num_hours] = period
        hours_dict['period'][period] = num_hours
        num_hours += 1
        current_day = current_day + timedelta(hours=1)

    return hours_dict

weekday_dict = get_weekday_dict()
hours_dict = get_hours_dict()
