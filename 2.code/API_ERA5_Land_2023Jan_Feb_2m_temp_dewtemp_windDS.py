import cdsapi

c = cdsapi.Client()

c.retrieve(
    'reanalysis-era5-land',
    {
          'variable': [
            #'10m_u_component_of_wind'#, '10m_v_component_of_wind', '2m_dewpoint_temperature',
            '2m_temperature',
        ],
        'year': '2023',  #set year#
        'month': ['01','02'],   #set month#
        'day': [
            '01', '02', '03',
            '04', '05', '06',
            '07', '08', '09',
            '10', '11', '12',
            '13', '14', '15',
            '16', '17', '18',
            '19', '20', '21',
            '22', '23', '24',
            '25', '26', '27',
            '28', '29', '30',
            '31',
        ],
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
        'area': [                   #to set a boundary box based on extreme coordinates of bayern#
            51, 8, 47,
            14,
        ],
        'format': 'netcdf.zip',   #recommended   
                  #  'grib',

    },
    'ERA5_Land_2023_Jan_Feb_2m_temp.netcdf.zip')     #rename the download file#
    #'ERA5_Land_2023_Jan_Feb_4_variables.grib') 



