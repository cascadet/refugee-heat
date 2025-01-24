##################################################################################
#
#       Ten Year Average
#       By Cascade Tuholske, cascade (dot) tuholske1 (at) montana (dot) edu  
#
#       This script opens a set of count days per year above a threshold (himax, 
#       tmax, wbgtmax, etc) and averages them. 
#
#       This produces the count number of days that exceed a threshold. For example,
#       for 2016 and a wbgtmax threshold of 30°C, then count values for a given
#       grid-cell could be 16, meaning 16 days in thaat cell exceeded 30°C.
#
#       Update args for each run (e.g. wbgt 28, 30, and 32) in main.
#
#################################################################################

# Dependencies 
import numpy as np
import xarray 
import os
import glob
import rasterio
import time
import multiprocessing as mp 
from multiprocessing import Pool
import sys
import matplotlib.pyplot as plt
import rasterstats
import geopandas as gpd
import xarray as xr
from rasterstats import zonal_stats
import matplotlib
import pandas as pd

def raster_avg(fns, fn_out):
    
    """ Averages raster files over time and writes the output to a new file.

    This function takes a list of raster files (e.g., GeoTIFFs), computes the average 
    across all input rasters along the time dimension (represented as bands), and 
    saves the resulting averaged raster to a specified output file.

    Args:
        fns (list of str): List of file paths to the raster files to be averaged. 
            Each file should be in a format supported by rasterio and xarray.
        fn_out (str): File path for the output raster file.

    Note:
        - The output file inherits metadata from the first raster in the input list.
        - Ensure all input rasters have the same dimensions and coordinate reference system (CRS).
    """
     
    # Open the files into an Xarray Data Array
    ds = xr.concat([xr.open_dataset(f) for f in fns], dim='band')
    
    # Get the ten year average as an array
    avg = ds.band_data.mean(dim = 'band').data
    
    # write it to disk
    avg = np.nan_to_num(avg, nan = -9999) # fill nan
    meta = rasterio.open(fns[0]).meta # meta data

    with rasterio.open(fn_out, 'w', **meta) as out:
        out.write_band(1, avg)
        
    # clear memory
    del(ds)
    del(avg)
    print('done')

# Run it 
if __name__ == "__main__":

    # data out 
    ssp = '2050_SSP245' # '2050_SSP245' #'2050_SSP585' 'obs'
    data = 'wbgtmax' # data set
    thresh = '30' # threshold 

    # Select 2007 - 2016 counts and SSP counts 
    path = os.path.join('../../../grp-ct/data/processed/CHC-CMIP6/')
    fns = sorted(glob.glob(os.path.join(path + 'annual_counts/' + data + '/' + ssp + '*' + thresh + '*.tif')))[24:] # + ssp + thresh + '*.tif')))[24:]
    print(len(fns))

    # fn out
    fn_out = os.path.join(path + ssp + '.' +data + thresh + '.avg_count_07-16.tif')
    print(fn_out)

    # run it
    raster_avg(fns, fn_out)