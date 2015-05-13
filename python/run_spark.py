from pyspark import SparkContext, StorageLevel, SparkConf
import boto

# conf = SparkConf().setAppName("nyc-taxi")
# sc = SparkContext(conf=conf)

root_dir = '/tmp/'
# root_dir = '/home/cbrown/Projects/nyc-taxi/python/'
# root_dir = '/home/cbrown/Projects/nyc-taxi-spark/python/'

sc.addPyFile(root_dir + 'data_utils.py')
sc.addPyFile(root_dir + 'constants.py')
sc.addPyFile(root_dir + 'spark_utils.py')

from data_utils import parse_taxi_record, get_mask_indices, get_hour, get_raster_s3

from spark_utils import (
    setup_spark_context_s3,
    get_trips_raster,
    get_labeled_values,
    kernel_density,
    combineValues,
    mergeCombiner,
    get_aggregates_rdd)
from constants import get_weekday_map, raster_kwargs, get_hours_dict

setup_spark_context_s3(sc)

# Load Additional Variables
# Dictionary of PERIOD -> WEEKDAY (list)
weekday_map = sc.broadcast(get_weekday_map())

# Dictionary tracking HOURS -> PERIODS
hours_dict = sc.broadcast(get_hours_dict())

# Get Mask Raster Indices
mask = sc.broadcast(get_mask_indices())

# Roads
roads = sc.broadcast(get_raster_s3('nyc-osm-roads.tiff'))
roads_distance = sc.broadcast(get_raster_s3('nyc-osm-roads-distance.tiff'))

# Airports
airports = sc.broadcast(get_raster_s3('nyc-osm-aeroways.tiff'))
airports_distance = sc.broadcast(get_raster_s3('nyc-osm-aeroways-distance.tiff'))

# Transport Points
transport = sc.broadcast(get_raster_s3('nyc-osm-transport.tiff'))
transport_distance = sc.broadcast(get_raster_s3('nyc-osm-transport-distance.tiff'))

# Load Data
# raw_data_url = "/media/cbrown/ba8aab6d-4c33-421c-b40c-2c4f97fe01f3/Data/nyc-taxi/combined/split/test/*.bz2"
# raw_data_url = "/home/cbrown/Projects/nyc-taxi/data/nyc-trips-split-aa.csv"
raw_data_url = "s3a://nyc-taxi-trip-data/*.gz"
raw_data = sc.textFile(raw_data_url)
trips = raw_data.map(parse_taxi_record).reduceByKey( lambda a, b: a + b )
trips.persist(StorageLevel.MEMORY_AND_DISK)
trips.count()

## Summary Statistics
trips_by_day = (trips.map(lambda ((r, c, hour), v): (hour[:-2], v) )
                .reduceByKey( lambda a, b: a + b ))
trips_by_day.persist(StorageLevel.MEMORY_AND_DISK)
trips_by_month = (trips_by_day.map( lambda (day, v): (day[:-2], v ))
                              .reduceByKey( lambda a, b: a + b ))
trips_by_month.count()

## Calculate Aggregates --
two_hour_agg = get_aggregates_rdd(trips, hours_dict, 1, 3, 1)
two_hour_agg.persist(StorageLevel.MEMORY_AND_DISK)
two_hour_raster = get_trips_raster(two_hour_agg, 'two_hour')

four_hour_agg = get_aggregates_rdd(two_hour_agg, hours_dict, 0, 4, 2)
four_hour_agg.persist(StorageLevel.MEMORY_AND_DISK)
four_hour_raster = get_trips_raster(four_hour_agg, 'four_hour')

eight_hour_agg = get_aggregates_rdd(four_hour_agg, hours_dict, 0, 8, 4)
eight_hour_agg.persist(StorageLevel.MEMORY_AND_DISK)
eight_hour_raster = get_trips_raster(eight_hour_agg, 'eight_hour')

previous_day = get_aggregates_rdd(trips, hours_dict, 24, 25, 1)
previous_day_raster = get_trips_raster(previous_day, 'previous_day')

previous_week = get_aggregates_rdd(trips, hours_dict, 168, 169, 1)
previous_week_raster = get_trips_raster(previous_week, 'previous_week')

# Rasterize everything
trips_rasters = get_trips_raster(trips, 'observed')
trips_rasters.persist(StorageLevel.MEMORY_AND_DISK)

unioned_rdd = (trips_rasters.union(two_hour_raster)
                            .union(four_hour_raster)
                            .union(eight_hour_raster)
                            .union(previous_day_raster)
                            .union(previous_week_raster))
unioned_rdd.persist(StorageLevel.MEMORY_AND_DISK)


combined_rdd = unioned_rdd.aggregateByKey(
        {}, combineValues, mergeCombiner)
combined_rdd.persist(StorageLevel.MEMORY_AND_DISK)
combined_rdd.count()

# held_out_for_test = combined_rdd.filter(lambda x: int(x) > 2013120000)
# held_out_for_train = combined_rdd.filter(lambda x: int(x) < 2013120000 and int(x) > 2013011000)

geovars = [roads, roads_distance, transport, transport_distance, airports, airports_distance]
data_ordering = ['two_hour', 'four_hour', 'eight_hour', 'previous_day', 'previous_week']
labeled_values = combined_rdd.flatMap(
    lambda (period, data_dict): get_labeled_values(period, data_dict, data_ordering,
                                                   mask, weekday_map, geovars))
labeled_values.persist(StorageLevel.MEMORY_AND_DISK)
labeled_values.count()

from pyspark.mllib.regression import LabeledPoint
labeled_points = labeled_values.map(lambda (x, y): LabeledPoint(x, y))
labeled_points.persist(StorageLevel.MEMORY_AND_DISK)
(trainingData, testData) = labeled_points.randomSplit([0.7, 0.3])

from pyspark.mllib.tree import RandomForest, RandomForestModel

model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo={},
                                    numTrees=300, featureSubsetStrategy="auto",
                                    impurity='variance', maxDepth=10, maxBins=64)
