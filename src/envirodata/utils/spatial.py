from math import asin, cos, radians, sin, sqrt, floor
from pyproj import Transformer

# Earth radius in meters
R_EARTH = 6371000.0


def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    m = R_EARTH * c
    return m


EPSG_4326_to_3035_transformer = Transformer.from_crs("EPSG:4326", "EPSG:3035")


# per https://sg.geodatenzentrum.de/web_public/gdz/dokumentation/deu/geogitter.pdf
def calculate_inspire_grid_id(lon: float, lat: float, cell_size: int):
    x, y = EPSG_4326_to_3035_transformer.transform(
        lat, lon
    )  # dont ask why lat/lon is switched...

    # if size > 999m, we use km in name
    pretty_cell_size = str(cell_size) + "m"
    if cell_size > 999:
        pretty_cell_size = str(int(cell_size / 1000)) + "km"

    pretty_x = floor(x / cell_size)
    pretty_y = floor(y / cell_size)

    return f"CRS3035RES{pretty_cell_size}N{pretty_x:0<7d}E{pretty_y:0<7d}"
