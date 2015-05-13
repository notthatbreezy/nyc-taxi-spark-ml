"""Utility functions for working with RDDs

These functions either take RDDs as their arguments
or can be used in the function call for a .map, .flatMap,
etc."""

from pyspark.mllib.regression import LabeledPoint

from constants import raster_extent
from raster_utils import coords_to_utm


def parse_taxi_record(line):
    """Parse a CSV line from Taxi data CSV, used in
    .map call for initial RDD

    Args:
      line (str) - line from CSV to be parsed

    Returns
      ((int, int, str,), int) - represents row, column, period, and count
    """
    (medallion, hack_license, vendor_id, rate_code, store_and_fwd_flag,
    pickup_datetime, dropoff_datetime, passenger_count, trip_time_in_secs,
    trip_distance, pickup_longitude, pickup_latitude, dropoff_longitude,
    dropoff_latitude) = line.split(',')

    pickup_date, pickup_time = pickup_datetime.split()
    pickup_year, pickup_month, pickup_day = pickup_date.split('-')
    pickup_hour, pickup_minute, _ = pickup_time.split(':')

    try:
        pickup_lat, pickup_lng = coords_to_utm(pickup_latitude, pickup_longitude)
    except:
        # There's some bad data here -- default to 0.0 for now'
        pickup_lat, pickup_lng = 0.0, 0.0

    def get_row_col(x, y):
        """Get a cell row and column given a point"""
        col = int((x - raster_extent.x_min) / raster_extent.cell_width)
        row = -1 * int((y - raster_extent.y_max) / raster_extent.cell_height)
        return col, row

    pickup_col, pickup_row = get_row_col(pickup_lat, pickup_lng)

    period = "{pickup_year}{pickup_month:02d}{pickup_day:02d}{pickup_hour:02d}".format(
        pickup_year=int(pickup_year), pickup_month=int(pickup_month), pickup_day=int(pickup_day),
        pickup_hour=int(pickup_hour))

    return ((pickup_row, pickup_col, period), 1)


def combine_values(d, v):
    """Helper function used in call to .aggregateByKey

    Args:
      d (dict): python dictionary to be aggregated into
      v (tuple): tuple of key, value to be inserted into d

    Returns:
      dict: updated python dictionary from v
    """
    key, value = v
    d[key] = value
    return d


def merge_combiner(d1, d2):
    """Merges to dictionary used in call to .aggregateByKey

    Args:
      d1 (dict): python dictionary representing aggregate
      d2 (dict): python dictionary representing aggregate

    Returns:
      dict: Returns dictionary of merged combiners
    """
    d1.update(d2)
    return d1


def labeled_point_to_row_col_period(labeled_point):
    """Helper function to reconstruct period, row, and column of labeled point

    Used in .map call for predictions

    Args:
      labeled_point (LabeledPoint): cell with label and features

    Returns:
      tuple (int, int, str): row, col, period
    """
    features = labeled_point.features
    row, col = features[0], features[1]
    month, day, hour = features[2], features[3], features[4]
    period = '2013{:02d}{:02d}{:02d}'.format(int(month), int(day), int(hour))
    return row, col, period


def get_labeled_points(period, data_dict, data_ordering, mask, weekday, geovars, weather_data):
    """Produce a set of labeled points for a period

    Args:
      period (str): period the data represents
      data_dict (dict): dictionary type_of_array => array
      mask (list): list of tuples (row, col)
      weekday (broadcast dict): broadcast var where period => list of 1/0 for day of week
      geovars (broadcast list): list of static geovars used for modeling, each is an array
      weather_data (dict): period => list of weather observations

    Returns:
      list of LabeledPoints with labels and features
    """
    ## Verify this period has observed values and all the other
    ## Necessary Variables
    observed_arr = data_dict.pop('observed', None)
    if observed_arr is None:
        return []

    if set(data_dict.keys()) != set(data_ordering):
        return []

    month = int(period[4:6])
    day = int(period[6:8])
    hour = int(period[-2:])
    weekday_list = weekday.value[period[:-2]]
    weather = weather_data.value[period]

    def get_labeled_point(row, col):
        observed = observed_arr[(row, col)]
        measurements = [row, col, month, day, hour] + weekday_list + weather
        for k in data_ordering:
            value = data_dict[k]
            measurements.append(value[(row, col)])
            for geovar in geovars:
                measurements.append(geovar.value[(row, col)])

        return LabeledPoint(observed, measurements)

    return [get_labeled_point(row, col) for row, col in mask.value]


def get_aggregates_rdd(trips_rdd, hours, start, end, step_size):
    """Create an RDD of aggregate values for each period

    This function is used to generate 'aggregate' RDDs such that
    an RDD where each row represents the aggregate value for a given
    cell over a number of periods (e.g. you wish to calculate the sum
    of the previous two period for a given cell, excluding the current value).

    This is generalized because it is possible to pyramid these values as long
    as they are multiples of one another

    Args:
      trips_rdd (RDD): Key/Value RDD (row, col, period), number_of_trips
      hours_dict (Broadcast Var): dictionary with mapping of hours -> periods and
    periods -> hours, necessary to properly increment periods as they are here
      start: start period to aggregate from (0 if current period included, 1 if not)
      end: value to increment to
      step_size: size of steps (used if pyramiding)

    Returns:
      RDD (key/value): key/value rdd of (row, col, period), number

    """

    def increment_period(period, increment):
        hours_since = hours.value['period'][period]
        incremented_hours = hours_since + increment
        return hours.value['hours'][incremented_hours]

    def get_aggregates(args):
        (row, col, period), num = args
        return [((row, col, increment_period(period, x)), num) for x in xrange(start, end, step_size)]

    return trips_rdd.flatMap( get_aggregates ).reduceByKey(lambda a, b: a + b)
