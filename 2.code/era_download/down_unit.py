# Script for Downloading ERA5-Land data 


import cdsapi
import sys
import yaml
import socket 
import os
from pathlib import Path
import calendar

## home directory where .cdsapi_[climate,atmosphere] file is stored
home_path = Path.home()

cdsapi_path=os.path.join(home_path,'.cdsapi_climate')

with open(cdsapi_path, 'r') as f:
            credentials = yaml.safe_load(f)
            c = cdsapi.Client(url=credentials['url'], key=credentials['key'])


## if the folder does not exist, then create it.
if not Path(output_path).is_dir():
    Path(output_path).mkdir(parents=True, exist_ok=True)


## get year and variable from I/O
[variable, year, output_dir]=sys.argv[1:]


def era_download(variable,year,output_dir='./'):
    ## short names for variables   
    vdict={'u10':'10m_u_component_of_wind',
           'v10':'10m_v_component_of_wind',
           'd2':'2m_dewpoint_temperature',
           't2':'2m_temperature'} 
    
    varn=vdict[variable]

    filename = output_dir+variable+'_'+year+".zip"

    # months= 1-12, days=1-31, time= 00:00-23:00 
    # CDS download available data for all days in each month
    months= [str(i) for i in range(1,13)]
    
    days=[str(i) for i in range(1,32)]
    
    times=[f'{i:02d}:00' for i in range(0,24)]
    
    # # number of days in the chosen year-month
    # n_days = calendar.monthrange(int(year),int(month))[1]

    # # array of day in char
    # days = [str(i) for i in range(1,n_days+1)]

    r= c.retrieve(
      'reanalysis-era5-land',
      {
        'time': times,
        'format': 'netcdf.zip',
        'year': year,
        'variable': varn,
        'month': months,
        'area':[51, 8.5, 47, 14],  # bounding box for Bayern
        'day': days
      })
    r.download(filename)


## downloading variable for year....
print('dowloading', variable,'for year: ',year,'\n')

era_download(variable,year,output_dir)