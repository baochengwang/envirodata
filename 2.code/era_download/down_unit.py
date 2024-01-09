import cdsapi
import sys
import yaml
import socket 
import os
from pathlib import Path
import calendar

# home directory where .cdsapi_[climate,atmosphere] file is stored
home_path = Path.home()

cdsapi_path=os.path.join(home_path,'.cdsapi_climate')

with open(cdsapi_path, 'r') as f:
            credentials = yaml.safe_load(f)
            c = cdsapi.Client(url=credentials['url'], key=credentials['key'])

# Folder to store download data
output_path = '../../0.raw/era5_land/'

# if the folder does not exist, then create it.
if not Path(output_path).is_dir():
    Path(output_path).mkdir(parents=True, exist_ok=True)


## get year and variable from I/O
[variable, year, month]=sys.argv[1:]


def era_download(variable,year,month):
    ## short names for variables   
    vdict={'u10':'10m_u_component_of_wind',
           'v10':'10m_v_component_of_wind',
           'd2':'2m_dewpoint_temperature',
           't2':'2m_temperature'
           } 
    
    varn=vdict[variable]

    filename = output_path+variable+'_'+year+'_'+month+".zip"

    # number of days in the chosen year-month
    n_days = calendar.monthrange(int(year),int(month))[1]

    # array of day in char
    days = [str(i) for i in range(1,n_days+1)]

    r= c.retrieve(
      'reanalysis-era5-land',
      {
        'time': [
            '00:00', '01:00', '02:00',
            '03:00', '04:00', '05:00',
            '06:00', '07:00', '08:00',
            '09:00', '10:00', '11:00',
            '12:00', '13:00', '14:00',
            '15:00', '16:00', '17:00',
            '18:00', '19:00', '20:00',
            '21:00', '22:00', '23:00',
        ],
        'format': 'netcdf.zip',
        'year': year,
        'variable': varn,
        'month': month,
        'area':[51, 8.5, 47, 14], 
        'day': days
      })
    r.download(filename)


## downloading variable for year....
print('dowloading ', variable,' for year-month: ',year, month,'\n')

era_download(variable,year, month)