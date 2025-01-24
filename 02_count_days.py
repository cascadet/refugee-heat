##################################################################################
#
#       Count Days
#       By Cascade Tuholske May 2022
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
#       Note (2022-07-06): Right now this runs on the UHE dataset using the
#       RHmin and Tmax combo used in Tuholske et al 2021 PNAS.
#
#       Update 2022-07-06: Working on ocean NAN problem. Right now they're 
#       set to zero going to change them back and see what happens. 
#
#       Update 2023-03-05: Running on new WBGTmax made with RHx observational data.
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
    
    """ Open a set of daily rasters for a given year and threhold them to make a binary array. Then add
    the binary arrays together to get a count array. Then write out the array.
    
    Args:
        year = year for a MP process to count the number of threshold days
        
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
    """Run a routine in parallel
    Args: 
        function = function to apply in parallel
        start_list = list of args for function to loop through in parallel
        cpu_num = numper of cpus to fire  
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
    path_in = os.path.join('/home/cascade/CHIRTS/wbgtmax-tmax-rhx/')
    path_out = os.path.join('/home/cascade/CHIRTS/wbgtmax-tmax-rhx/annualcounts/')
    data_in = 'wbgtmax'
    thresh = 30
    
    # set year list to feed annual_count_array 
    year_list = list(range(1983,2016+1))
    
    # test
    # year_list = year_list[:3]
    
    # run it
    parallel_loop(function = annual_count_array, start_list = year_list, cpu_num = 24)
    
    print('done!')