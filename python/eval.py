from collections import namedtuple
import os

def list_rasters(path):
    list_rasters = [x for x in os.listdir(path)]
    return list_rasters


predicted = list_rasters('predicted/')
observed = list_rasters('observed/')
two_hour = list_rasters('two_hour/')

all_three = []
for raster in predicted:
    in_observed = (raster in observed)
    in_two_hour = (raster in two_hour)
    if in_observed and in_two_hour:
        all_three.append(raster)

def read_raster(raster_path, just_read=True):
    np_raster = None
    with rasterio.open(raster_path, 'r') as src:
        np_raster = src.read(1)
    if just_read:
        return np_raster
    flattened = np_raster.flatten()
    flattened.sort()
    return flattened[::-1]



def get_mask_indices():
    """Helper function to get raster mask for NYC

    Returns:
      list: returns list of tuples (row, column) that represent area of interest
    """
    raster = get_raster_s3('nyc-utm-zone-50m.tiff')
    indices = []
    data = raster
    it = np.nditer(data, flags=['multi_index'])
    while not it.finished:
        if it[0] == 1:
            r, c = it.multi_index
            indices.append((r, c))
        it.iternext()
    return indices


area = 313024

Areas = namedtuple('Areas', ['one', 'five', 'ten', 'twenty'])

areas = Areas(int(area * .01), int(area * .05), int(area * .10), int(area * .20))

RasterStat = namedtuple('RasterStat',
                        ['one_percent', 'five_percent'
                         'ten_percent', 'twenty_percent'
                         'total'])
CombinedStats = namedtuple('CombinedStats',
                           ['obs', 'pred', 'two'])


def get_raster_stat(arr):
    one = arr[:areas.one].sum()
    five = arr[:areas.five].sum()
    ten = arr[:areas.ten].sum()
    twenty = arr[:areas.twenty].sum()
    total = arr.sum()
    return RasterStat(one, five, ten, twenty, total)



def get_raster_stats(raster):
    p = read_raster(os.path.join('predicted/', raster), just_read=False)
    o = read_raster(os.path.join('observed/', raster), just_read=False)
    t = read_raster(os.path.join('two_hour/', raster), just_read=False)

    return CombinedStats(
        get_raster_stat(o),
        get_raster_stat(p),
        get_raster_stat(t)
    )

stats = {}
for raster in all_three:
    results = get_raster_stats(raster)
    stats[raster] = results