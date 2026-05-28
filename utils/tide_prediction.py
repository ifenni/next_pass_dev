import requests
import os
import time
import json
import logging
from pathlib import Path
from typing import Optional
from datetime import datetime, timedelta
from shapely.geometry import Point
from shapely.geometry.base import BaseGeometry

LOGGER = logging.getLogger("tide_prediction")

NOAA_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
STATIONS_URL = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json"
_STATIONS_CACHE = None
TIDE_DIRECTION_UNKNOWN = "slack"
SCRATCH_DIR = Path.cwd() / "scratch"


def resolve_station_cache_path(filepath: str | Path | None = None) -> Path:
    """Return the cache path for NOAA station metadata."""
    return Path(filepath) if filepath is not None else SCRATCH_DIR / "noaa_stations.json"


def get_stations(filepath: str | Path | None = None):
    global _STATIONS_CACHE
    if _STATIONS_CACHE is None:
        _STATIONS_CACHE = load_stations(filepath)
    return _STATIONS_CACHE


def cache_stations(filepath: str | Path | None = None):
    global _STATIONS_CACHE
    cache_path = resolve_station_cache_path(filepath)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(STATIONS_URL, timeout=10)
    response.raise_for_status()

    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(response.json(), f)

    _STATIONS_CACHE = None


def load_stations(filepath: str | Path | None = None):
    cache_path = resolve_station_cache_path(filepath)
    with open(cache_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("stations", [])


def ensure_station_cache(filepath: str | Path | None = None, max_age_days=30):
    cache_path = resolve_station_cache_path(filepath)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if not cache_path.exists():
        cache_stations(cache_path)
        return

    age_days = (time.time() - os.path.getmtime(cache_path)) / 86400
    if age_days > max_age_days:
        cache_stations(cache_path)


def parse_datetime(dt_str: str) -> datetime:
    return datetime.fromisoformat(dt_str.replace("Z", ""))


def get_stations_in_aoi(polygon: BaseGeometry, max_stations: int = 3) -> list:
    """Return full station dicts (id, name, lat, lng) for stations inside the polygon.

    For Point geometries, buffers by ~50km before searching. If no stations are found
    within the AOI (point, small polygon, or sparse coverage), falls back to returning
    the nearest max_stations to the AOI centroid.
    """
    ensure_station_cache()
    stations = get_stations()

    # Buffer point AOIs to ~50km radius (0.5 degrees latitude ≈ 55km)
    search_geom = polygon.buffer(0.5) if polygon.geom_type == "Point" else polygon

    result = []
    for st in stations:
        lat = st.get("lat")
        lon = st.get("lng")
        if lat is None or lon is None:
            continue
        if search_geom.contains(Point(float(lon), float(lat))):
            result.append({
                "id": st["id"],
                "name": st.get("name", st["id"]),
                "lat": float(lat),
                "lng": float(lon),
            })

    # Fallback: if no stations in AOI, find nearest to centroid
    if not result:
        centroid = polygon.centroid
        stations_with_dist = []
        for st in stations:
            lat = st.get("lat")
            lon = st.get("lng")
            if lat is None or lon is None:
                continue
            dist_sq = (float(lat) - centroid.y) ** 2 + (float(lon) - centroid.x) ** 2
            stations_with_dist.append((dist_sq, st))

        stations_with_dist.sort(key=lambda x: x[0])
        for _, st in stations_with_dist[:max_stations]:
            result.append({
                "id": st["id"],
                "name": st.get("name", st["id"]),
                "lat": float(st["lat"]),
                "lng": float(st["lng"]),
            })

    return result



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



def _find_tide_direction(times_iso: list, values: list, target_dt: datetime) -> str:
    """Return 'rising' or 'falling' based on hourly values bracketing target_dt."""
    before_val = before_t = None
    after_val = after_t = None

    for i, t_str in enumerate(times_iso):
        t = parse_datetime(t_str)
        if t <= target_dt:
            if before_t is None or t > before_t:
                before_t, before_val = t, values[i]
        else:
            if after_t is None or t < after_t:
                after_t, after_val = t, values[i]

    if before_val is not None and after_val is not None:
        if after_val > before_val:
            return "rising"
        if after_val < before_val:
            return "falling"
        return TIDE_DIRECTION_UNKNOWN
    return ""


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
                times_iso_short = [t[:16] for t in times_iso]

                for i, (target_iso, target_dt) in enumerate(zip(target_isos, target_dts)):
                    target_iso_short = target_iso[:16]
                    if target_iso_short in times_iso_short:
                        value = values[times_iso_short.index(target_iso_short)]
                    elif allow_interpolation:
                        value = interpolate_tide(times_iso, values, target_dt)
                        if value is None:
                            diffs = [abs((parse_datetime(t) - target_dt).total_seconds()) for t in times_iso]
                            value = values[diffs.index(min(diffs))]
                    else:
                        diffs = [abs((parse_datetime(t) - target_dt).total_seconds()) for t in times_iso]
                        value = values[diffs.index(min(diffs))]

                    label = _find_nearest_hilo_label(hilo_predictions, target_dt)
                    direction = _find_tide_direction(times_iso, values, target_dt)
                    tag = "-".join(filter(None, [label, direction]))
                    label_str = f"({tag})" if tag else ""
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

    except (requests.RequestException, KeyError, ValueError, OSError) as e:
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

        if row.geometry is not None:
            intersection = row.geometry.intersection(aoi_geometry)
            polygon = aoi_geometry if intersection.is_empty else intersection
        else:
            polygon = aoi_geometry

        return get_tide_info_batch(
            polygon=polygon,
            target_isos=target_isos,
            allow_interpolation=True,
        )

    return get_tide_for_row
