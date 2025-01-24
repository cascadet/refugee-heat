##################################################################################
#
#       Make Heat Index
#       By Cascade Tuholske Spring 2021
#
#       ALWAYS CHECK FILE PATHS AND FILE NAMES BEFORE RUNNING
#
#       Program makes daily maximum heat index tifs with CHIRTS-daily Tmax and 
#       relative humidity min estimated with CHIRTS-daily Tmax
#
#       NOAA Heat Index Equation - https://www.wpc.ncep.noaa.gov/html/heatindex_equation.shtml
#
#       Note - right now units are all in C. They can be updated to F or C as needed. 
#              See make_hi function to make changes CPT July 2021
#
#       Re-run on Tmax and RHx May 2022 for 1983 - CPT
#        RHx - /home/CHIRTS/daily_ERA5/w-ERA5_Td.eq2-2-spechum/
#        Tmax - /home/cascade/chc-data-out/products/CHIRTSdaily/v1.0/global_tifs_p05/Tmax
#        RHx added to Line 59 for this run 
#
#       Re-run on Tmax and RHx Sep 2022 for all data - CPT
#        RHx - /home/cascade/CHIRTS/daily_ERA5/ERA5-RH/
#        Tmax - /home/cascade/chc-data-out/products/CHIRTSdaily/v1.0/global_tifs_p05/Tmax/
#        HI(hi-tmax-rhx) - /home/cascade/CHIRTS/hi-tmax-rhx/  
#
#       NOTE - uncomment "make dir to write" for future runs (CPT Sep 2022)
#
#       Oct 2022 - CPT
#       Running on CHIRTS-CMIP6 projections
#       10/25/22 --- /home/cascade/CMIP6/2030_SSP245/Tmax + RHx started on Forge 
#
#       March 3 2023 - Observations ReRun CPT
#       
#       Re-run on new RHx observations. Had to replace nan values with -9999 for logic in HI equation 
#
#       Inputs:
#       RHx (RH at hour of Tmax) CHIRTS-daily: Path: /home/CHIRTS/daily_ERA5/ERA5.2023.via-vp/ (RHx.***)
#       Tmax CHIRTS-daily: /home/chc-data-out/products/CHIRTSdaily/v1.0/global_tifs_p05/Tmax/1983/ (Tmax.***)
#
#       Outputs:
#       himax: /home/CHIRTS/himax-tmax-rhx/
#       wbgtmax: /home/CHIRTS/wbgtmax-tmax-rhx/
#
#       #   UPDATED MARCH 2023 by Cascade Tuholkse
#       Move 02_HI-to_WBGT into 01_MakeHeatIndex.py, renamed 01_Make-HI-WBGT.py
#       to run the whole thing at once. Should speed things up as it cuts the I/O
#       down. 
#
#       # March 8 2023 - Running on all CHC-CMIP datasets
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
    
    """ Function takes the year from a list and makes the pixel level HImax & WBGTmax from CHIRTS Tmax and RHmin using
    ClimFuncs.py functions. 
    
    Args:
        year = all files should be in a dir by year
    """
    
    print(multiprocessing.current_process(), year)
    
    # data handle + dataset for SSPs
    rh_handle = 'RHx.'
    SSP_dataset = '2050_SSP245'
    #print(dataset)
    
    # Set up file paths for CMIP
    path = os.path.join('/home/cascade/CMIP6/')
    rh_path = os.path.join(path, SSP_dataset + '/' + rh_handle.split('.')[0] + '/' + str(year))  
    tmax_path = os.path.join(path, SSP_dataset + '/Tmax/' + str(year))   
    hi_path = os.path.join(path, SSP_dataset + '/himax/' + str(year)) 
    wbgt_path = os.path.join(path, SSP_dataset + '/wbgtmax/' + str(year)) 
     
    # Set up file paths for obsevational
#     rh_path = os.path.join('/home/CHIRTS/daily_ERA5/ERA5.2023.via-vp/', str(year))  
#     tmax_path = os.path.join('/home/chc-data-out/products/CHIRTSdaily/v1.0/global_tifs_p05/Tmax/', str(year)) 
#     hi_path = os.path.join('/home/CHIRTS/himax-tmax-rhx/', str(year))
#     wbgt_path = os.path.join('/home/CHIRTS/wbgtmax-tmax-rhx/', str(year))
                            
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

if __name__ == "__main__":
    
    print('start')
    
    # Make years 
    #year_list = list(range(1983,2016+1))
    year_list = list(range(2010, 2016+1,1))
    
    #year_list = year_list[:3]# test
    #year_list = ['2016']
    print(year_list)
    
    #Run it
    parallel_loop(function = hi_loop, start_list = year_list, cpu_num = 7)
    
    print('done')