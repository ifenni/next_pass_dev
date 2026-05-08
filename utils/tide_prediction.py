import requests
import os
import time
import json
import logging
from typing import Optional
from datetime import datetime, timedelta
from shapely.geometry import Point
from shapely.geometry.base import BaseGeometry

LOGGER = logging.getLogger("tide_prediction")

NOAA_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
STATIONS_URL = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json"
_STATIONS_CACHE = None


def get_stations(filepath="noaa_stations.json"):
    global _STATIONS_CACHE
    if _STATIONS_CACHE is None:
        _STATIONS_CACHE = load_stations(filepath)
    return _STATIONS_CACHE


def cache_stations(filepath="noaa_stations.json"):
    global _STATIONS_CACHE
    response = requests.get(STATIONS_URL, timeout=10)
    response.raise_for_status()

    with open(filepath, "w") as f:
        json.dump(response.json(), f)

    _STATIONS_CACHE = None


def load_stations(filepath="noaa_stations.json"):
    with open(filepath, "r") as f:
        data = json.load(f)
    return data.get("stations", [])


def ensure_station_cache(filepath="noaa_stations.json", max_age_days=30):
    if not os.path.exists(filepath):
        cache_stations(filepath)
        return

    age_days = (time.time() - os.path.getmtime(filepath)) / 86400
    if age_days > max_age_days:
        cache_stations(filepath)


def parse_datetime(dt_str: str) -> datetime:
    return datetime.fromisoformat(dt_str.replace("Z", ""))


def get_stations_in_aoi(polygon: BaseGeometry) -> list:
    """Return full station dicts (id, name, lat, lng) for stations inside the polygon."""
    ensure_station_cache()
    stations = get_stations()
    result = []
    for st in stations:
        lat = st.get("lat")
        lon = st.get("lng")
        if lat is None or lon is None:
            continue
        if polygon.contains(Point(float(lon), float(lat))):
            result.append({
                "id": st["id"],
                "name": st.get("name", st["id"]),
                "lat": float(lat),
                "lng": float(lon),
            })
    return result


def find_stations_in_polygon(polygon_geojson, stations):
    """
    polygon_geojson: GeoJSON polygon
    stations: preloaded station list

    Returns: list of station IDs
    """

    poly = polygon_geojson
    selected = []

    for st in stations:
        lat = st.get("lat")
        lon = st.get("lng")

        if lat is None or lon is None:
            continue

        pt = Point(float(lon), float(lat))

        if poly.contains(pt):
            selected.append(st["id"])

    return selected


def interpolate_tide(times, values, target_dt):
    for i in range(len(times) - 1):
        t1 = parse_datetime(times[i])
        t2 = parse_datetime(times[i + 1])

        if t1 <= target_dt <= t2:
            v1 = values[i]
            v2 = values[i + 1]

            fraction = (
                (target_dt - t1).total_seconds()
                / (t2 - t1).total_seconds()
            )
            return v1 + fraction * (v2 - v1)

    return None


def get_tide_info(
    polygon: BaseGeometry,
    target_iso: str,
    allow_interpolation: bool = True,
    session: Optional[requests.Session] = None,
) -> Optional[float]:

    if session is None:
        session = requests.Session()

    try:
        ensure_station_cache()
        stations_data = get_stations()
        stations = find_stations_in_polygon(polygon, stations_data)

        if not stations:
            LOGGER.warning("No stations in polygon")
            return None

        target_dt = parse_datetime(target_iso)

        begin_date = (target_dt - timedelta(days=1)).strftime("%Y%m%d")
        end_date = (target_dt + timedelta(days=1)).strftime("%Y%m%d")

        tide_values = []

        for station in stations:
            params = {
                "product": "predictions",
                "application": "tide_app",
                "begin_date": begin_date,
                "end_date": end_date,
                "datum": "MLLW",
                "station": station,
                "time_zone": "gmt",
                "units": "metric",
                "interval": "h",
                "format": "json",
            }

            try:
                response = session.get(NOAA_URL, params=params, timeout=10)
                response.raise_for_status()

                predictions = response.json().get("predictions", [])
                if not predictions:
                    continue

                times_iso = [p["t"].replace(" ", "T") for p in predictions]
                values = [float(p["v"]) for p in predictions]

                # Exact match
                if target_iso in times_iso:
                    tide_values.append(values[times_iso.index(target_iso)])
                    continue

                # Interpolation
                if allow_interpolation:
                    interp_value = interpolate_tide(times_iso,
                                                    values,
                                                    target_dt)
                    if interp_value is not None:
                        tide_values.append(interp_value)
                        continue

                # Fallback: nearest
                time_diffs = [
                    abs((parse_datetime(t) - target_dt).total_seconds())
                    for t in times_iso
                ]
                min_idx = time_diffs.index(min(time_diffs))
                tide_values.append(values[min_idx])

            except requests.RequestException:
                continue

        if not tide_values:
            return None

        # 🔥 Simple aggregation (mean)
        return sum(tide_values) / len(tide_values)

    except (requests.RequestException, KeyError, ValueError) as e:
        LOGGER.error("Error retrieving tide data from NOAA: %s", e)
        return None


def _find_nearest_hilo_label(hilo_predictions: list, target_dt: datetime) -> str:
    """Return the type (H, HH, L, LL) of the hilo event nearest to target_dt."""
    if not hilo_predictions:
        return ""
    nearest_label = ""
    min_diff = None
    for p in hilo_predictions:
        t = parse_datetime(p["t"].replace(" ", "T"))
        diff = abs((t - target_dt).total_seconds())
        if min_diff is None or diff < min_diff:
            min_diff = diff
            nearest_label = p.get("type", "")
    return nearest_label


def get_tide_info_batch(
    polygon: BaseGeometry,
    target_isos: list,
    allow_interpolation: bool = True,
    session: Optional[requests.Session] = None,
) -> list:
    """Return a dict per target time with 'nearest' (table) and 'per_station' (map) values."""

    if session is None:
        session = requests.Session()

    try:
        station_dicts = get_stations_in_aoi(polygon)

        if not station_dicts:
            LOGGER.warning("No stations in polygon")
            return [None] * len(target_isos)

        # Find nearest station to polygon centroid
        centroid = polygon.centroid
        nearest_id = min(
            station_dicts,
            key=lambda st: (st["lat"] - centroid.y) ** 2 + (st["lng"] - centroid.x) ** 2,
        )["id"]

        target_dts = [parse_datetime(t) for t in target_isos]
        begin_date = (min(target_dts) - timedelta(days=1)).strftime("%Y%m%d")
        end_date = (max(target_dts) + timedelta(days=1)).strftime("%Y%m%d")

        # per_station_results[i] = list of (station_id, formatted_string)
        per_station_results = {i: [] for i in range(len(target_isos))}
        nearest_results = {i: None for i in range(len(target_isos))}

        for st in station_dicts:
            station_id = st["id"]
            base_params = {
                "application": "tide_app",
                "begin_date": begin_date,
                "end_date": end_date,
                "datum": "MLLW",
                "station": station_id,
                "time_zone": "gmt",
                "units": "metric",
                "format": "json",
            }

            try:
                hourly_resp = session.get(
                    NOAA_URL, params={**base_params, "product": "predictions", "interval": "h"}, timeout=10
                )
                hourly_resp.raise_for_status()
                predictions = hourly_resp.json().get("predictions", [])

                hilo_resp = session.get(
                    NOAA_URL, params={**base_params, "product": "predictions", "interval": "hilo"}, timeout=10
                )
                hilo_resp.raise_for_status()
                hilo_predictions = hilo_resp.json().get("predictions", [])

                if not predictions:
                    continue

                times_iso = [p["t"].replace(" ", "T") for p in predictions]
                values = [float(p["v"]) for p in predictions]

                for i, (target_iso, target_dt) in enumerate(zip(target_isos, target_dts)):
                    if target_iso in times_iso:
                        value = values[times_iso.index(target_iso)]
                    elif allow_interpolation:
                        value = interpolate_tide(times_iso, values, target_dt)
                        if value is None:
                            diffs = [abs((parse_datetime(t) - target_dt).total_seconds()) for t in times_iso]
                            value = values[diffs.index(min(diffs))]
                    else:
                        diffs = [abs((parse_datetime(t) - target_dt).total_seconds()) for t in times_iso]
                        value = values[diffs.index(min(diffs))]

                    label = _find_nearest_hilo_label(hilo_predictions, target_dt)
                    label_str = f"({label})" if label else ""
                    formatted = f"{value:.2f}{label_str}"

                    per_station_results[i].append((station_id, formatted))
                    if station_id == nearest_id:
                        nearest_results[i] = formatted

            except requests.RequestException:
                continue

        results = []
        for i in range(len(target_isos)):
            per = per_station_results[i]
            if not per:
                results.append(None)
                continue
            nearest = nearest_results[i] or per[0][1]
            results.append({"nearest": nearest, "per_station": {sid: v for sid, v in per}})

        return results

    except (requests.RequestException, KeyError, ValueError) as e:
        LOGGER.error("Error retrieving tide data from NOAA: %s", e)
        return [None] * len(target_isos)


def make_get_tide_for_row(aoi_geometry):
    def get_tide_for_row(row):
        times = row["begin_date"]

        if not isinstance(times, list):
            times = [times]

        target_isos = []
        for t in times:
            if isinstance(t, datetime):
                t = t.strftime("%Y-%m-%dT%H:%M:%S")
            target_isos.append(t)

        intersection = row.geometry.intersection(aoi_geometry)
        polygon = aoi_geometry if intersection.is_empty else intersection

        return get_tide_info_batch(
            polygon=polygon,
            target_isos=target_isos,
            allow_interpolation=True,
        )

    return get_tide_for_row
