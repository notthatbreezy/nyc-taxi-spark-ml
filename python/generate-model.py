from pyspark import StorageLevel, SparkContext

sc = SparkContext()

# Change to project path
root_dir = '/home/cbrown/Projects/nyc-taxi/python/'
sc.addPyFile(root_dir + 'raster_utils.py')
sc.addPyFile(root_dir + 'rdd_utils.py')
sc.addPyFile(root_dir + 'constants.py')
sc.addPyFile(root_dir + 's3_utils.py')


from geo_vars import (
    tiff_to_array,
    get_mask_indices
)

from time_vars import (
    get_weather_data
)

from rdd_utils import (
    parse_taxi_record,
    get_aggregates_rdd,
    combine_values,
    merge_combiner,
    labeled_point_to_row_col_period,
    get_labeled_points
)

from raster_utils import (
    trips_to_raster,
    kernel_density,
    neighbor_average
)

# Load Additional Variables
from constants import (
    weekday_dict,
    hours_dict
)

weekday_map = sc.broadcast(weekday_dict)
hours_dict = sc.broadcast(hours_dict)

data_dir = '/home/cbrown/Projects/nyc-taxi/data/'
weather_data = sc.broadcast(get_weather_data(data_dir + 'weather_data.csv'))

# Get Mask Raster Indices
mask = sc.broadcast(get_mask_indices(data_dir + '/nyc-utm-zone-50m.tiff'))

# Roads
roads = sc.broadcast(
    tiff_to_array(data_dir + 'nyc-osm-roads.tiff'))
roads_distance = sc.broadcast(
    tiff_to_array(data_dir + 'nyc-osm-roads-distance.tiff'))

# Airports
airports = sc.broadcast(
    tiff_to_array(data_dir + 'nyc-osm-aeroways.tiff'))
airports_distance = sc.broadcast(
    tiff_to_array(data_dir + 'nyc-osm-aeroways-distance.tiff'))

# Transport Points
transport = sc.broadcast(
    tiff_to_array(data_dir + 'nyc-osm-transport.tiff'))
transport_distance = sc.broadcast(
    tiff_to_array(data_dir + 'nyc-osm-transport-distance.tiff'))

raw_data_url = data_dir + 'trips-subset.csv'
# raw_data_url = "s3a://nyc-taxi-trip-data-test/*.gz"
raw_data = sc.textFile(raw_data_url)
trips = (raw_data.map( parse_taxi_record )            # row => ((row, col, period), 1)
                 .reduceByKey( lambda a, b: a + b )   # row => ((row, col, period), sum)
                 .repartition(128))                   # repartition to appropriate number (2x # cores)

trips.persist(StorageLevel.MEMORY_AND_DISK)           # persist so we can look at this later if we want
trips_rasters = trips_to_raster(trips, 'observed')    # row => (period, (raster_type, numpy array))

## Calculate Aggregates --
two_hour_agg = get_aggregates_rdd(trips, hours_dict, 1, 3, 1)
two_hour_raster = trips_to_raster(two_hour_agg, 'two_hour')
two_hour_kd = two_hour_raster.mapValues(
    lambda (label, raster): (kernel_density('two_hour_kd', raster)))
two_hour_neighbor_avg = two_hour_raster.mapValues(
    lambda (label, raster): (neighbor_average('two_hour_neighbor_avg', raster)))

four_hour_agg = get_aggregates_rdd(two_hour_agg, hours_dict, 0, 4, 2)
four_hour_raster = trips_to_raster(four_hour_agg, 'four_hour')
four_hour_kd = four_hour_raster.mapValues(
    lambda (label, raster): (kernel_density('four_hour_kd', raster)))
four_hour_neighbor_avg = four_hour_raster.mapValues(
    lambda (label, raster): (neighbor_average('four_hour_neighbor_avg', raster)))

eight_hour_agg = get_aggregates_rdd(four_hour_agg, hours_dict, 0, 8, 4)
eight_hour_raster = trips_to_raster(eight_hour_agg, 'eight_hour')
eight_hour_kd = eight_hour_raster.mapValues(
    lambda (label, raster): (kernel_density('eight_hour_kd', raster)))
eight_hour_neighbor_avg = eight_hour_raster.mapValues(
    lambda (label, raster): (neighbor_average('eight_hour_neighbor_avg', raster)))

previous_day = get_aggregates_rdd(trips, hours_dict, 24, 25, 1)
previous_day_raster = trips_to_raster(previous_day, 'previous_day')

previous_week = get_aggregates_rdd(trips, hours_dict, 168, 169, 1)
previous_week_raster = trips_to_raster(previous_week, 'previous_week')

# Rasterize everything
unioned_rdd = (trips_rasters.union(two_hour_raster)
                            .union(four_hour_raster)
                            .union(eight_hour_raster)
                            .union(previous_day_raster)
                            .union(previous_week_raster)
                            .union(two_hour_kd)
                            .union(two_hour_neighbor_avg)
                            .union(four_hour_kd)
                            .union(four_hour_neighbor_avg)
                            .union(eight_hour_kd)
                            .union(eight_hour_neighbor_avg))

combined = unioned_rdd.aggregateByKey({}, combine_values, merge_combiner)

(training_data, test_data) = combined.randomSplit([0.7, 0.3])

geovars = [roads, roads_distance, transport, transport_distance, airports, airports_distance]
data_ordering = ['two_hour', 'two_hour_kd', 'two_hour_neighbor_avg',
                 'four_hour',  'four_hour_kd', 'four_hour_neighbor_avg',
                 'eight_hour',  'eight_hour_kd', 'eight_hour_neighbor_avg',
                 'previous_day', 'previous_week']

training_data_points = training_data.flatMap( lambda (period, data_dict):
    get_labeled_points(period, data_dict, data_ordering, mask,
                       weekday_map, geovars, weather_data))
test_data_points = test_data.flatMap( lambda (period, data_dict):
    get_labeled_points(period, data_dict, data_ordering, mask,
                       weekday_map, geovars, weather_data))

from pyspark.mllib.tree import RandomForest

model = RandomForest.trainRegressor(training_data_points, categoricalFeaturesInfo={},
                                    numTrees=50, featureSubsetStrategy="auto",
                                    impurity='variance', maxDepth=3, maxBins=32)


predictions = model.predict(test_data_points.map(lambda x: x.features))
predicted_row_col = test_data_points.map(labeled_point_to_row_col_period).zip(predictions)

predicted_rasters = (trips_to_raster(predicted_row_col, 'predicted')
                     .map( lambda (period, (label, raster)): (period, raster)) )
observed_test_rasters = test_data.filter(lambda (x, d): 'observed' in d).mapValues(lambda d: d['observed'])
two_hour_avg = test_data.filter(lambda (x, d): 'two_hour' in d).mapValues(lambda d: d['two_hour']/2)

# Refactor this
# predicted_rasters.map(lambda (period, raster): write_raster(period, raster, 'predicted')).count()
# observed_test_rasters.map(lambda (period, raster): write_raster(period, raster, 'observed')).count()
# two_hour_avg.map(lambda (period, raster): write_raster(period, raster, 'two_hour')).count()