"""Functions that deal with geographic data that does not vary over time

This type of data varies based on geography, but does not differ across
different time periods (or at least not on a relevant scale). For instance,
a raster of euclidean distance to airports would be included in this category
"""

from s3_utils import get_from_s3
from raster_utils import raster_to_array
import numpy as np


def tiff_to_array(path):
    """Function that loads a geotiff into a numpy array

    Args:
      path (str): s3 or local path to load tiff from

    Returns:
      numpy array
    """

    if path.startswith('s3'):
        local_raster_path = get_from_s3(path)
    else:
        local_raster_path = path

    return raster_to_array(local_raster_path)


def get_mask_indices(path):
    """Helper function to get raster mask for NYC

    Returns:
      list: returns list of tuples (row, column) that represent area of interest
    """
    raster = tiff_to_array(path)
    indices = []
    it = np.nditer(raster, flags=['multi_index'])
    while not it.finished:
        if it[0] == 1:
            r, c = it.multi_index
            indices.append((r, c))
        it.iternext()
    return indices
