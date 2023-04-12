# Code for computing snow deposition from WRF data
# based on code from A. Koskkin 
# 

# time ranges 

cmip="cnrm-esm2-1_r1i1p1f2_historical_bc"
date_range = pd.date_range("1980-09-01", "2014-09-01", freq='D')
basepath = pl.Path("/glade/campaign/uwyo/wyom0112/gcm/")

import xarray as xr
import matplotlib.pyplot as plt
import glob
import numpy as np
import pathlib as pl
import pandas as pd

# goff gratch equation
# returns vapor pressure w.r.t ice in hpa
# t2 is in kelvin
# t2 can be any temperature (skin temp as well)
def gg(T):
    return 10**(-9.09718 * (273.16/T - 1) - 3.56654*np.log(273.16 / T) + 0.876793*(1-T/273.16) + np.log(6.1071))

def SatVap_2m(ds):
    return gg(ds.T2)

def SatVap_skin(ds):
    return gg(ds.TSK)

# return the vapor pressure at two meters 
# in units of hpa
def aVap_2m(ds):
    # Q2 is  mxiing ratio  kg kg-1
    # PSFC is in PA
    return ds.Q2*ds.PSFC/0.622/100

#output directory 
destination_path = pl.Path("/glade/scratch/rudisill/").joinpath(cmip)

# loop thru the times...
for d in date_range:
    year  = d.strftime("%Y")
    month = d.strftime("%m")
    day   = d.strftime("%d")
    yearp1 = int(year)+1 if month not in ["09","10","11","12"] else int(year)
    
    # get the wrf files 
    wrffile="auxhist_d01_%s-%s-%s_*"%(yearp1,month,day)
    the_files = sorted(basepath.joinpath(cmip, "hourly", year, "d02").glob(wrffile))
    
    # open them allup
    ds = xr.open_mfdataset(the_files, concat_dim='XTIME', combine='nested')

    # do some calcs...
    ae2m  = vap_p_2m(ds) # actual vapor pressure at two meters 
    esk   = esatSK(ds)   # saturated vapor pressure w.r.t ice at the skin temperature 
    diff = ae2m - esk
    snow_present = ds.SNOW.where(ds.SNOW <= 0, 1)
    deposition   = (diff.where((diff>0) & (snow_present==1)) * 1/deposition)   
    
    # make an output dataset 
    dsout = xr.Dataset(
        data_vars=dict(
            max_ediff    =(["time", "west_east", "south_north"],   diff.max(dim='XTIME').values),        
            min_ediff    =(["time", "west_east", "south_north"],   diff.min(dim='XTIME').values),        
            deposition   =(["time", "west_east", "south_north"],   deposition.sum(dim='XTIME').values),        
        ),
    attrs=dict(description="Weather related data."),
    )
        
    # save it 
    destination_path_b = destination_path.joinpath(year)
    destination_path_b.mkdir(exist_ok=True, parents=True)
    destination_filename = "deposition_%s-%s-%s.nc"%(yearp1, month,day)
    dsout.to_netcdf(destination_filename)
    
    
    del dsout 
    del ds
    
    
# wrffile="auxhist_d01_%s-%s-%s_%s:00:00"%(yearp1,month,day,hour)
