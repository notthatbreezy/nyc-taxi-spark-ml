"""Functions to interact with S3"""
import os
import numpy as np

from constants import (
    raster_extent,
    raster_kwargs)

import rasterio
import utm
import csv

import boto
from boto.s3.key import Key
import tempfile

import urlparse

def get_from_s3(s3_url):
    """Helper function that parses and gets a file from S3

    Args:
      s3_url (str): S3 url (s3://...) to pull down to local file

    Returns:
      str : local path to s3 file
    """

    parsed_s3_url = urlparse.urlsplit(path)

    _, localpath = tempfile.mkstemp('.csv')

    conn = boto.connect_s3()
    bucket = conn.get_bucket(parsed_s3_url.netloc)
    k = Key(bucket)
    k.key = 'parsed_s3_url'
    k.get_contents_to_filename(localpath)


def setup_spark_context_s3(sc):
    """Helper function to set up spark context when running locally

    This is necessary because s3a:// paths for hadoop will automatically
    use instance profiles (when on ec2); however, when not on ec2 there
    isn't a great way to set up credentials. This assumes credentials are
    stored in the default location using the `aws` cli library

    Args:
      sc (SparkContext): spark context to updated
    """
    aws_dir = os.path.expanduser('~/.aws')
    boto_config_path = os.path.join(aws_dir, 'config')
    aws_creds_path = os.path.join(aws_dir, 'credentials')
    boto.config.read([boto_config_path, aws_creds_path])
    aws_access_key_id = boto.config.get('default', 'aws_access_key_id')
    aws_secret_access_key = boto.config.get('default', 'aws_secret_access_key')

    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key_id)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_access_key)


colormap = (1, {
    1: (0, 0, 0),
    2: (247,251,255),
    3: (222,235,247),
    4: (198,219,239),
    5: (158,202,225),
    6: (107,174,214),
    7: (66,146,198),
    8: (33,113,181),
    9: (8,81,156),
    10: (8,48,107)}
)


def write_raster(period, np_array, grouping='scratch'):
    """Helper method to write a raster to S3

    Args:
      period (str): period that the raster represents
      np_array (numpy array): represents raster values
      grouping (str) [optional]: logical grouping to be placed on s3
    (e.g. prediction, observed, etc.)
    """
    _, filepath = tempfile.mkstemp('.tiff', str(period))

    with rasterio.open(filepath, 'w', **raster_kwargs) as dst:
        dst.write_band(1, np_array)

    conn = boto.connect_s3()
    bucket = conn.get_bucket('nyc-taxi-uploaded-tiffs')
    k = Key(bucket)
    k.key = '{grouping}/{period}.tiff'.format(grouping=grouping, period=period)
    k.set_contents_from_filename(filepath)
    os.remove(filepath)
    return 1

def write_png(period, np_array, grouping='scratch'):
    """Helper method to write a raster as a PNG to s3

    Args:
      period (str): period that the raster represents
      np_array (numpy array): represents raster values
      grouping (str) [optional]: logical grouping to be placed on s3
    (e.g. prediction, observed, etc.)
    """
    _, filepath = tempfile.mkstemp('.png', str(period))

    try:
        with rasterio.open(filepath, 'w', **raster_kwargs) as dst:
            dst.write_band(1, np_array)

        conn = boto.connect_s3()
        bucket = conn.get_bucket('nyc-taxi-uploaded-pngs')
        k = Key(bucket)
        k.key = '{grouping}/{period}.png'.format(grouping=grouping, period=period)
        k.set_contents_from_file(filepath)
    except:
        print "Got an exception creating a raster"

    finally:
        os.remove(filepath)
