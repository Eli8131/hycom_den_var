# computational import
import numpy as np
import xarray as xr
import zarr
import scipy as sc

# open eulerian dataset step 1 
ds = xr.open_zarr('/scratch/tidaldrift/hycom_ss/hycom12_step_1_15m_E3.zarr/')

# create complex velocity array
x = ds['u'] + 1j*(ds["v"])

# take variance of complex velocity array
y = np.nanvar(x, axis = 0)

# define 1/2 degree for stats_2d 
lon = np.linspace(-180,180, 360*2)
lat = np.linspace(-90, 90, 180*2)

# use 2d_stats to bin variance to lon/lat
var = sc.stats.binned_statistic_2d(ds.Longitude.to_numpy().flatten(), 
                               ds.Latitude.to_numpy().flatten(),
                               y.flatten(),
                               statistic = np.nanmean,
                               bins= [lon,lat])
# delete y for memory space
del y 

# open lagrangian dataset step 1
ds = xr.open_zarr('/projectnb/msldrift/pra-drifters/aws/global_hycom_15m_step_1.zarr')

# mask the 'grounded' trajectories
ds = ds.where(ds['grounding'].compute() == False, drop=True)

# create complex velocity array
x = ds.ve + 1j*ds.vn

# take variance 
var2 = sc.stats.binned_statistic_2d(ds.lon.to_numpy().flatten(), 
                                   ds.lat.to_numpy().flatten(),
                                   x.to_numpy().flatten(),
                                   statistic = np.nanvar,
                                   bins= [lon,lat])

# create a dataset to store calculations
ds = xr.Dataset(data_vars=dict(eul_var = (['lon','lat'], var.statistic.real),
                             lag_var = (['lon','lat'], var2.statistic.real),),
              coords = dict(lon = var.x_edge[0:-1], lat = var.y_edge[0:-1]),)

# save dataset
ds.to_netcdf('/scratch/tidaldrift/aws/hycom_variance_step_1_15m.nc')

# repeat process at 0m

# open eulerian dataset step 1 
ds = xr.open_zarr('/scratch/tidaldrift/hycom_ss/hycom12_step_1_0m_E3.zarr/')

# create complex velocity array
x = ds['u'] + 1j*(ds["v"])

# take variance of complex velocity array
y = np.nanvar(x, axis = 0)

# use 2d_stats to bin variance to lon/lat
var = sc.stats.binned_statistic_2d(ds.Longitude.to_numpy().flatten(), 
                               ds.Latitude.to_numpy().flatten(),
                               y.flatten(),
                               statistic = np.nanmean,
                               bins= [lon,lat])
# delete y for memory space
del y 

# open lagrangian dataset step 1
ds = xr.open_zarr('/projectnb/msldrift/pra-drifters/aws/global_hycom_0m_step_1.zarr')

# mask the 'grounded' trajectories
ds = ds.where(ds['grounding'].compute() == False, drop=True)

# create complex velocity array
x = ds.ve + 1j*ds.vn

# take variance 
var2 = sc.stats.binned_statistic_2d(ds.lon.to_numpy().flatten(), 
                                   ds.lat.to_numpy().flatten(),
                                   x.to_numpy().flatten(),
                                   statistic = np.nanvar,
                                   bins= [lon,lat])

# create a dataset to store calculations
ds = xr.Dataset(data_vars=dict(eul_var = (['lon','lat'], var.statistic.real),
                             lag_var = (['lon','lat'], var2.statistic.real),),
              coords = dict(lon = var.x_edge[0:-1], lat = var.y_edge[0:-1]),)

# save dataset
ds.to_netcdf('/scratch/tidaldrift/aws/hycom_variance_step_1_0m.nc')