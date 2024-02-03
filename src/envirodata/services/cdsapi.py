import os
import logging
import datetime
import copy

import cdsapi
import netCDF4
import numpy as np

logger = logging.getLogger(__name__)


class Loader:
    def __init__(
        self,
        dataset,
        request,
        output_fpath_pattern,
        cdsurl=os.environ.get("CDSAPI_URL"),
        cdskey=os.environ.get("CDSAPI_KEY"),
    ):
        self.dataset = dataset
        self.request = request
        self.output_fpath_pattern = output_fpath_pattern
        self.cdsurl = cdsurl
        self.cdskey = cdskey

    def load(
        self,
        start_date,
        end_date,
    ):
        cur_date = start_date
        while cur_date <= end_date:
            self.download_date(cur_date)
            cur_date += datetime.timedelta(days=1)

    def download_date(
        self,
        date,
    ):
        output_fname = date.strftime(self.output_fpath_pattern)

        output_dir = os.path.dirname(output_fname)
        os.makedirs(output_dir, exist_ok=True)

        if not os.path.exists(output_fname):
            try:
                c = cdsapi.Client(url=self.cdsurl, key=self.cdskey)
                request = copy.copy(self.request)

                for datevar in ["date", "year", "month", "day"]:
                    if datevar in request:
                        request[datevar] = date.strftime(request[datevar])

                logger.info(
                    "Downloading from %s for %s", self.dataset, date.isoformat()
                )

                c.retrieve(
                    self.dataset,
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


class Getter:
    def __init__(
        self,
        cache_fpath_pattern,
        time_calculation,
        variable_translation_table,
    ):
        self.cache_fpath_pattern = cache_fpath_pattern
        self.variable_translation_table = variable_translation_table

        time_calculators = {
            "time_since_analysis": self._calc_time_since_analysis,
            "hours_since_epoch": self._calc_time_epoch,
        }
        self.calc_time = time_calculators[time_calculation]

    def _calc_time_since_analysis(self, date, nc):
        return [
            date.replace(hour=0, minute=0, second=0)
            + datetime.timedelta(hours=float(x))
            for x in nc.variables["time"][:]
        ]

    def _calc_time_epoch(self, date, nc):
        return netCDF4.num2date(
            nc.variables["time"],
            nc.variables["time"].units,
            calendar=nc.variables["time"].calendar,
            only_use_cftime_datetimes=False,
        )

    def get(
        self,
        date,
        variable,
        longitude,
        latitude,
    ):
        output_fname = date.strftime(self.cache_fpath_pattern)

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

        times = self.calc_time(date, nc)

        tdeltas = [np.abs((date - t).total_seconds()) for t in times]

        tidx = np.where(tdeltas == np.min(tdeltas))[0][0]  # no idea why sub-levels
        xidx, yidx = _get_index(lons, lats, longitude, latitude)

        fvarname = self.variable_translation_table[variable]

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
