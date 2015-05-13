"""Functions used for the creation/manipulation
of rasters/numpy arrays"""
import numpy as np
from constants import raster_extent
from scipy import ndimage

import rasterio
import utm


def raster_to_array(raster_path):
    """Helper function to get raster data (tiff) as numpy array

    Args:
      raster_name (str): name of raster to get from s3

    Returns:
      2d numpy array
    """
    raster = None
    with rasterio.open(raster_path, 'r') as src:
        raster = src.read(1)
    return raster


def coords_to_utm(lat, lng):
    """Helper function to transform lat and lng -> UTM coords

    Args:
      lat (str) - float wrapped as string (from CSV)
      lng (str) - float wrapped as string (from CSV)

    Returns:
      (float, float) x, y UTM coordinates
    """
    try:
        lng = float(lng)
    except:
        lng = 0.0
    try:
        lat = float(lat)
    except:
        lat = 0.0

    x, y, _, _ = utm.from_latlon(lat, lng)
    return x, y


def array_to_raster(trips):
    """List of trips (row, col, num) => numpy array

    Function takes a list of tuples (row, col, num) and puts them into a numpy array,
    thus, rasterizing them

    Args:
      trips (int, int, float): represents a list

    Returns:
      numpy 2d array
    """
    trips_array = np.zeros((raster_extent.cols, raster_extent.rows), dtype=np.float32)
    for counter, trip_tuple in enumerate(trips):
        row, col, num = trip_tuple
        if row >= raster_extent.cols or col >= raster_extent.rows or row < 0 or col < 0:
            continue
        trips_array[row, col] += num
    return trips_array


def kernel_density(label, arr):
    """Helper function to produce kernel density plot

    Args:
      label (str): label to use for tuple to return
      arr (array): array to calculate kernel density of

    Returns
      (str, array): label and new array produced
    """
    return (label, ndimage.gaussian_filter(arr, sigma=1))


def neighbor_average(label, arr):
    """Helper function to produce focal average for cells

    Args:
      label (str): label to use for tuple to return
      arr (array): array to calculate focal average of

    Returns
      (str, array): label and new array produced
    """
    return (label, ndimage.uniform_filter(arr, size=1))


def trips_to_raster(trips_rdd, label):
    """Takes an RDD of trips and returns a raster

    RDD should be of the form:
      ((row, col, period), number_of_trips)
    """

    trips_by_hour = trips_rdd.map(
        lambda ((row, col, period), num): (period, (row, col, num)) ).groupByKey()
    return (trips_by_hour.mapValues( array_to_raster )
                         .mapValues( lambda x: (label, x) ))