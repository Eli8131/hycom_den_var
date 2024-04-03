# usage: ptyhon hycom_subsample.py step depth
# where step is one through 11 and depth is 0 (0m) or 1 (15m)

import xarray as xr
import numpy as np
import zarr
import dask
import sys
import time

from dask.distributed import Client
from dask_jobqueue import LSFCluster

dask.config.set({"distributed.scheduler.worker-saturation":1.0})

cluster = LSFCluster(cores=1,project="tidaldrift",walltime="72:00",queue="bigmem",memory="250GB")
time.sleep(5)
cluster.scale(10)
time.sleep(10)
client = Client(cluster)
time.sleep(5)

list11 = np.linspace(1,11,11)

for i in list11:
    # open and combine zarr archives
    file1 = '/projectnb/msldrift/hycom/zstore/hycom12-'+str(int(i))+'-rechunked-corr.zarr'
    ds1 = xr.open_zarr(file1,chunks="auto")

    file2 = '/projectnb/msldrift/hycom/zstore/hycom12-'+str(int(i+1))+'-rechunked-corr.zarr'
    if i != 11:
        ds2 = xr.open_zarr(file2,chunks="auto")
    else:
        # limit to 720 steps    
        ds2 = xr.open_zarr(file2,chunks="auto").isel(time=slice(0,720))

    # combine and subset
    ds12 = xr.combine_by_coords([ds1.isel(X=slice(None, None, 3), Y=slice(None, None, 3), Depth=0),ds2.isel(X=slice(None, None, 3), Y=slice(None, None, 3), Depth=0)],data_vars=['u','v'],combine_attrs='drop_conflicts').chunk({"time":1440})


    ds12.to_zarr('/scratch/tidaldrift/hycom_ss/hycom12_step_'+str(int(i))+'_0m_E3.zarr', mode='w')

    print('File written: '+ str(int(i)))
    
for i in list11:
    # open and combine zarr archives
    file1 = '/projectnb/msldrift/hycom/zstore/hycom12-'+str(int(i))+'-rechunked-corr.zarr'
    ds1 = xr.open_zarr(file1,chunks="auto")

    file2 = '/projectnb/msldrift/hycom/zstore/hycom12-'+str(int(i+1))+'-rechunked-corr.zarr'
    if i != 11:
        ds2 = xr.open_zarr(file2,chunks="auto")
    else:
        # limit to 720 steps    
        ds2 = xr.open_zarr(file2,chunks="auto").isel(time=slice(0,720))

    # combine and subset
    ds12 = xr.combine_by_coords([ds1.isel(X=slice(None, None, 3), Y=slice(None, None, 3), Depth=1),ds2.isel(X=slice(None, None, 3), Y=slice(None, None, 3), Depth=1)],data_vars=['u','v'],combine_attrs='drop_conflicts').chunk({"time":1440})


    ds12.to_zarr('/scratch/tidaldrift/hycom_ss/hycom12_step_'+str(int(i))+'_15m_E3.zarr', mode='w')

    print('File written: '+str(int(i)))

cluster.close()