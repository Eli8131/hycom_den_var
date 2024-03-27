# computational import
import scipy as sc
import numpy as np
import xarray as xr
import s3fs

# designate bins
lon = np.linspace(-180,180, 360*2)
lat = np.linspace(-90, 90, 180*2)

# open datasets and save in dictionaries for easy access
list11 = np.linspace(1,11,11)

# define dictionaries 

ds = xr.open_zarr('/projectnb/msldrift/pra-drifters/aws/global_hycom_0m_step_1.zarr')


# use scipy.binned_2dstat() to take density of obs coordinate in 1/2 degree bins
aws = sc.stats.binned_statistic_2d(ds.lon.to_numpy().flatten(), 
                            ds.lat.to_numpy().flatten(),
                            ds.obs.to_numpy().flatten(),
                            statistic = 'count',
                            bins= [lon,lat])

ds = xr.open_zarr('/projectnb/msldrift/pra-drifters/hycom/global_hycom_step_1_2d.zarr')
c = (ds.ve) + (1j*(ds.vn))

hycom = sc.stats.binned_statistic_2d(ds.lon.to_numpy().flatten(), 
                            ds.lat.to_numpy().flatten(),
                            ds.obs.to_numpy().flatten(),
                            statistic = 'count',
                            bins= [lon,lat])


# organize results into an xarray dataset
countds = xr.Dataset(data_vars=dict(aws_hpc = (['lon','lat'], aws.statistic),
                                 hycom = (['lon','lat'], hycom.statistic)),
          coords = dict(lon = aws.x_edge[0:-1], lat = aws.y_edge[0:-1]),)



# save the dataset
countds.to_netcdf('/scratch/tidaldrift/aws/hycom_aws_density_1_testing.nc')

