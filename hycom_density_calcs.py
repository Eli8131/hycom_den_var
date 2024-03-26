# computational import
import scipy as sc
import numpy as np
import xarray as xr

# designate bins
lon = np.linspace(-180,180, 360*2)
lat = np.linspace(-90, 90, 180*2)

# open datasets and save in dictionaries for easy access
list11 = np.linspace(1,11,11)

# define dictionaries 
ds = {}
ds15 = {}
ds_g = {}
ds15_g = {}
for i in list11:
    ds[int(i)] = xr.open_zarr('/projectnb/msldrift/pra-drifters/aws/global_hycom_0m_step_'+str(int(i))+'.zarr')
    
    # create dataset for 'grounded' trajectories
    ds_g[int(i)] = ds[int(i)].where(ds[int(i)]['grounding'].compute() == True, drop=True)

    # mask the 'grounded' trajectories in original datasets
    ds[int(i)] = ds[int(i)].where(ds[int(i)]['grounding'].compute() == False, drop=True)

    
    # use scipy.binned_2dstat() to take density of obs coordinate in 1/2 degree bins
    count = sc.stats.binned_statistic_2d(ds[int(i)].lon.to_numpy().flatten(), 
                                ds[int(i)].lat.to_numpy().flatten(),
                                ds[int(i)].obs.to_numpy().flatten(),
                                statistic = 'count',
                                bins= [lon,lat])
    
    # use scipy.binned_2dstat() to take density of obs coordinate in 1/2 degree bins for grounded datasets
    count_g = sc.stats.binned_statistic_2d(ds_g[int(i)].lon.to_numpy().flatten(), 
                                ds_g[int(i)].lat.to_numpy().flatten(),
                                ds_g[int(i)].obs.to_numpy().flatten(),
                                statistic = 'count',
                                bins= [lon,lat])
  
    # organize results into an xarray dataset
    countds = xr.Dataset(data_vars=dict(den = (['lon','lat'], count.statistic.real),
                             den_grounded = (['lon','lat'], count_g.statistic)),
              coords = dict(lon = count.x_edge[0:-1], lat = count.y_edge[0:-1]),)
    
    # save the dataset
    countds.to_netcdf('/scratch/tidaldrift/aws_density/hycom_aws_density_'+str(int(i))+'.nc')

