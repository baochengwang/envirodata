import os
import logging
import datetime

import cdsapi
import netCDF4
import numpy as np

logger = logging.getLogger(__name__)


def _calc_time_since_analysis(date, nc):
    return [
        date.replace(hour=0, minute=0, second=0) + datetime.timedelta(hours=float(x))
        for x in nc.variables["time"][:]
    ]


def _calc_time_epoch(date, nc):
    return netCDF4.num2date(
        nc.variables["time"],
        nc.variables["time"].units,
        calendar=nc.variables["time"].calendar,
        only_use_cftime_datetimes=False,
    )


time_calculators = {
    "time_since_analysis": _calc_time_since_analysis,
    "hours_since_epoch": _calc_time_epoch,
}


def download(
    date,
    dataset,
    request,
    output_fpath_pattern,
    cdsurl=os.environ.get("CDSAPI_URL"),
    cdskey=os.environ.get("CDSAPI_KEY"),
):
    output_fname = date.strftime(output_fpath_pattern)

    output_dir = os.path.dirname(output_fname)
    os.makedirs(output_dir, exist_ok=True)

    if not os.path.exists(output_fname):
        try:
            c = cdsapi.Client(url=cdsurl, key=cdskey)

            for datevar in ["date", "year", "month", "day"]:
                if datevar in request:
                    request[datevar] = date.strftime(request[datevar])

            logger.info("Downloading from %s for %s", dataset, date.isoformat())

            c.retrieve(
                dataset,
                request,
                output_fname,
            )
        except:
            logger.critical(
                "Could not download new data for %s, continuing anyway",
                date.isoformat(),
            )
            pass

    try:
        nc = netCDF4.Dataset(output_fname)
        nc.close()
    except Exception as exc:
        raise IOError("No data found for {:s}!".format(date.isoformat())) from exc


def get(
    date,
    variable,
    longitude,
    latitude,
    cache_fpath_pattern,
    time_calculation,
    variable_translation_table,
):
    output_fname = date.strftime(cache_fpath_pattern)

    try:
        nc = netCDF4.Dataset(output_fname)
    except Exception as exc:
        raise IOError(
            "No data found for {:s}!".format(date.strftime("%Y-%m-%d"))
        ) from exc

    def _get_index(lons, lats, lon, lat):
        ddelta = (lons - lon) ** 2 + (lats - lat) ** 2
        idxes = np.where(ddelta == np.min(ddelta))
        return (idxes[0][0], idxes[1][0])

    lons, lats = np.meshgrid(nc.variables["longitude"], nc.variables["latitude"])

    times = time_calculators[time_calculation](date, nc)

    tdeltas = [np.abs((date - t).total_seconds()) for t in times]

    tidx = np.where(tdeltas == np.min(tdeltas))[0][0]  # no idea why sub-levels
    xidx, yidx = _get_index(lons, lats, longitude, latitude)

    fvarname = variable_translation_table[variable]

    logger.debug(
        "%s: %s, %s, %s is at %s: %s, %s, %s",
        variable,
        date.isoformat(),
        longitude,
        latitude,
        fvarname,
        tidx,
        xidx,
        yidx,
    )

    try:
        if len(nc.dimensions) == 3:
            return float(nc.variables[fvarname][tidx, yidx, xidx])
        elif len(nc.dimensions) == 4:
            return float(nc.variables[fvarname][tidx, 0, yidx, xidx])
        else:
            raise RuntimeError("Unknown number of dimensions in NetCDF file.")
    except Exception as exc:
        raise RuntimeError(
            f"Could not get data for {variable} (file variable: {fvarname})"
        ) from exc
