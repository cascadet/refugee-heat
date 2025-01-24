##################################################################################
#
#       Count Days
#       By Cascade Tuholske, cascade (dot) tuholske1 (at) montana (dot) edu  
#
#       This script opens a set of daily temperature rasters (himax, tmax, wbgtmax, etc)
#       and applies a threshold to create a binary array for each day based on the
#       threshold. Then the bianary arrays are stacked into a single array.
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

# Functions

def annual_count_array(year):
    
    """
    Processes daily raster files for a given year, thresholds them to create binary arrays, 
    sums these binary arrays to produce a count array, and writes the result to a new raster file.

    This function reads a set of raster files for a specified year, applies a threshold to 
    each raster to create a binary mask, and sums these masks across all rasters for the year. 
    The output is a count array where each pixel value represents the number of days exceeding 
    the threshold during that year. The result is saved to a GeoTIFF file.

    Args:
        year (int): The year for which to process raster files and compute the count array.


    Output:
        - A GeoTIFF file with the annual count array saved in the output directory.
    
        - Ensure that the `path_in`, `path_out`, `data_in`, and `thresh` variables are 
          defined and accessible in the global scope.
        - Input rasters should have the same spatial dimensions and CRS.

    """
    
    # print process
    print(mp.current_process(), year)
    
    # Get the rasters and check them
    fn_list = sorted(glob.glob(path_in+str(year)+'/*.tif'))
    #print(fn_list[0])
    
    # Test
    # fn_list = fn_list[180:185]
    
    # out path and fn out
    fn_out = os.path.join(path_out, data_in+str(thresh) + '/'+ data_in+str(thresh)+'.count.'+str(year)+'.tif')
    print(fn_out)
    print(thresh)
    
    # Open rasters, mask them to binary, and add the binary arrays
    for i, fn in enumerate(fn_list):
        
        print(fn)

        if i == 0: # first year
            meta = rasterio.open(fn).meta   # get meta data to write raster
            arr = rasterio.open(fn).read(1) # read raster to array
            arr = np.nan_to_num(arr, copy=False, nan=-9999.0, posinf=-9999.0, neginf=-9999.0) # revalue nan if inf to -9999
            nan_mask = np.where(arr == -9999.0, -9999.0, 0) # make an nan mask to track ocean/nan locations
            arr_final = np.where(arr > thresh, 1, 0) # set mask to one, else 0 

        if i > 0: # add additional years
            arr = rasterio.open(fn).read(1) # read raster to array
            arr = np.nan_to_num(arr, copy=False, nan=-9999.0, posinf=-9999.0, neginf=-9999.0) # revalue nan if inf to -9999
            nan_mask = np.where(arr == -9999.0, -9999.0, 0) # make an nan mask to track ocean/nan locations
            arr_thresh = np.where(arr > thresh, 1, 0) # set mask to one, else 0 
            arr_final = arr_final + arr_thresh # add the binary arrays together
    
    # mask ocean
    arr_out = arr_final + nan_mask # sets any zero values that were nan at the start to -9999
    
    # update data types
    arr_out = arr_out.astype('int16') # set array int16 to keep it small type
    meta['dtype'] = 'int16'
    meta['nodata'] = -9999
    
    # write it
    with rasterio.open(fn_out, 'w', **meta) as out:
        out.write_band(1, arr_out)

def parallel_loop(function, start_list, cpu_num):
    """
    Executes a given function in parallel using multiple CPU cores.

    This function leverages Python's multiprocessing capabilities to run the 
    specified function concurrently across a given number of CPU cores. The 
    function is applied to each element of the provided `start_list`.

    Args:
        function (callable): The function to execute in parallel. It should be 
            able to accept elements from the `start_list` as arguments.
        start_list (list): A list of arguments to pass to the `function` in parallel.
        cpu_num (int): The number of CPU cores to utilize for parallel processing.

    Note:
        - Ensure that the `function` is defined at the top level of the module, 
          as `multiprocessing` requires functions to be pickleable.
        - The `start_list` should contain all the required inputs for the function 
          to operate correctly.
    """
    start = time.time()
    pool = Pool(processes = cpu_num)
    pool.map(function, start_list)
    pool.close()

    end = time.time()
    print(end-start)

# Run it
if __name__ == "__main__":
    
    # Set args
    path_in = os.path.join('') # path to himax or wbgtmax files 
    path_out = os.path.join('') # path to annualcounts
    data_in = 'wbgtmax'
    thresh = 30
    
    # set year list to feed annual_count_array 
    year_list = list(range(1983,2016+1))
    
    # test
    # year_list = year_list[:3]
    
    # run it
    parallel_loop(function = annual_count_array, start_list = year_list, cpu_num = 24) # number of CPUS available 
    
    print('done!')