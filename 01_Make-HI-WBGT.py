##################################################################################
#
#       Make Heat Index and Wet Bulb Globe Temperature 
#       By Cascade Tuholske, cascade (dot) tuholske1 (at) montana (dot) edu  
#
#       ALWAYS CHECK FILE PATHS AND FILE NAMES BEFORE RUNNING
#
#       Make daily maximum heat index tifs with CHC-CMIP Tmax and 
#       RHx to estimate daily maximum heat index and indoor shaded wet bulb globe 
#       temperature (Bernard and Iheanacho 2015) 
#
#       NOAA Heat Index Equation - https://www.wpc.ncep.noaa.gov/html/heatindex_equation.shtml
#
#       Bernard, T. E., & Iheanacho, I. (2015). Heat index and adjusted temperature as surrogates 
#       for wet bulb globe temperature to screen for occupational heat stress. 
#       Journal of Occupational and Environmental Hygiene, 12(5), 323-333.
#
#       CHC-CMIP6 Data: https://www.chc.ucsb.edu/data/chc-cmip6
#
#################################################################################

#### Dependencies
import numpy as np
import pandas as pd
import xarray 
import os
import glob
import rasterio
import time
import multiprocessing as mp 
from multiprocessing import Pool
import multiprocessing
import ClimFuncs

def hi_loop(year):
    """
    Processes daily CHIRTS Tmax and RHx rasters to compute pixel-level HImax (Heat Index maximum) 
    and WBGTmax (Wet Bulb Globe Temperature maximum) for a given year.

    This function reads daily CHIRTS Tmax (maximum temperature) and RHx (maximum relative humidity) 
    raster files, calculates the HImax and WBGTmax indices for each day using custom functions 
    from the `ClimFuncs` module, and saves the resulting rasters in a specified output directory.

    Args:
    year (int): The year for which the function processes data. All input files 
                should be organized in directories by year.
    Notes:
    - Ensure that the `ClimFuncs` module is accessible and includes the required 
      functions: `heatindex`, `C_to_F`, and `hi_to_wbgt`.
    - The `SSP_dataset` variable can be modified to indicate specific scenarios or 
      observational data.
    - Input and output file paths need to be appropriately defined.


    """
    
    print(multiprocessing.current_process(), year)
    
    # data handle + dataset for SSPs
    rh_handle = 'RHx.' # or Tmax. 
    SSP_dataset = '2050_SSP245' # blank for observational data 
    #print(dataset)
    
    # Set up file paths for CMIP SSP 
    path = os.path.join('') #PATH/TO/DATA 
    rh_path = os.path.join(path, SSP_dataset + '/' + rh_handle.split('.')[0] + '/' + str(year))  
    tmax_path = os.path.join(path, SSP_dataset + '/Tmax/' + str(year))   
    hi_path = os.path.join(path, SSP_dataset + '/himax/' + str(year)) 
    wbgt_path = os.path.join(path, SSP_dataset + '/wbgtmax/' + str(year)) 
     
    # Set up file paths for obsevational
#     rh_path = os.path.join('', str(year))  
#     tmax_path = os.path.join('', str(year)) 
#     hi_path = os.path.join('himax-tmax-rhx/', str(year))
#     wbgt_path = os.path.join('wbgtmax-tmax-rhx/', str(year))
                            
    # make dir to write 
#     cmd = 'mkdir '+ hi_path 
#     os.system(cmd)
#     print(cmd)
    
#     cmd = 'mkdir '+ wbgt_path 
#     os.system(cmd)
#     print(cmd)
    
    # CHIRTS-daily Tmax + RH
    rh_fns = sorted(glob.glob(rh_path+'/*'+ rh_handle + '*.tif')) # RHx added 
    tmax_fns = sorted(glob.glob(tmax_path+'/*.tif'))
    zipped_list = list(zip(rh_fns,tmax_fns))
    
    # test
    zipped_list = zipped_list[340:]
    
    # start loop
    for fns in zipped_list:
        
        print(fns)
        
        # get date
        date =fns[0].split(rh_handle)[1].split('.tif')[0]
    
        # data type
        data_out = 'himax'
    
        # get meta data
        meta = rasterio.open(fns[0]).meta
        meta['dtype'] = 'float32'
        meta['nodata'] = -9999
    
        # make hi
        rh_fn = fns[0] 
        tmax_fn = fns[1]
        tmax = xarray.open_rasterio(tmax_fn)
        rh = xarray.open_rasterio(rh_fn)
 
        # Update No data value / RHx nan are literally str 'nan' -- CPT March 2023
        # rh.data = np.nan_to_num(rh.data, nan = -9999)
        
        # Update NaN value in meta data for CMIP -- CPT March 2023
        rh.attrs['nodatavals'] = -9999
        tmax.attrs['nodatavals'] = -9999
        
        # calculate heat index
        hi = ClimFuncs.heatindex(Tmax = tmax, RH = rh, unit_in = 'C', unit_out = 'C')

        # get array
        arr = hi.data[0]
        arr = arr.astype('float32') # to save space 
        print(type(arr))
        
        # CMIP NaN
        arr[arr < -1000] = -9999

        # FN out
        print(date)
#         fn_out = os.path.join(hi_path, data_out +'.'+date+'.tif') 
        fn_out = os.path.join(hi_path, SSP_dataset + '.' + data_out + '.' + date+'.tif') # CMIP 
        
        with rasterio.open(fn_out, 'w', **meta) as out:
            out.write_band(1, arr)
        print(fn_out, 'done')
            
        # make wbgt
        data_out = 'wbgtmax'
        fn_out = os.path.join(wbgt_path, data_out +'.'+date+'.tif')
#         fn_out = os.path.join(wbgt_path, SSP_dataset + '.' + data_out + '.' + date+'.tif') # CMIP 
        
        hi_arr_f = ClimFuncs.C_to_F(arr) # convert hi to F
        wbgt_arr = ClimFuncs.hi_to_wbgt(hi_arr_f) # write wbgt in c
        wbgt_arr = wbgt_arr.astype('float32')
        
        # CMIP NaN
        wbgt_arr[wbgt_arr < -1000] = -9999
             
        with rasterio.open(fn_out, 'w', **meta) as out:
            out.write_band(1, wbgt_arr)
        print(fn_out, 'done')
            
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

if __name__ == "__main__":
    
    print('start')
    
    # Make years 
    year_list = list(range(1983,2016+1))
    #year_list = list(range(2010, 2016+1,1))
    
    #year_list = year_list[:3]# test
    print(year_list)
    
    #Run it
    parallel_loop(function = hi_loop, start_list = year_list, cpu_num = 7) # set to available CPUS for speed
    
    print('done')