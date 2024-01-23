def get_from_era5(url, var):
    data = 5.0
    return data


def get_from_dwd(url):
    data = ... download data ...
    return data



def var():
    return lon, lat, data


lon 2d np.array
lat 2d np.array
data = dict( key=datetime, value=2d np.array)


lon, lat, data = var()

lon = [[2, 3], [1,2]]
lat = [[2, 3], [1,2]]

data = {  datetime.datetime(2023, 1, 1): [[345,457], [234,454]], datetime.datetime(2023, 1, 2):  }

for time in data:
    write_to_postgres( lon, lat, time, data[time])