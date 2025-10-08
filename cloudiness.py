import logging
import requests
import rasterio
import numpy as np
import tempfile
import os
import random
from shapely.geometry import Polygon, Point, shape, mapping
from dateutil.parser import parse as parse_datetime
from datetime import datetime, timedelta
from typing import Union, Dict, Optional, List
from utils import get_spatial_extent_km

LOGGER = logging.getLogger('cloudiness_utils')

WEATHERAPI_API_KEY = os.getenv("WEATHERAPI_KEY")


def get_cloudiness(url):
    """Downloads a CLOUD*.tif file and calculates cloud pixel percentage."""
    cloud_values = {4, 5, 6, 7, 12, 13, 14, 15}
    exclude_values = {255}
    try:
        with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
            response = requests.get(url, stream=True)
            if response.status_code != 200:
                print(f"Failed to download: {url}")
                return None
            for chunk in response.iter_content(chunk_size=8192):
                tmp.write(chunk)
            tmp_path = tmp.name

        with rasterio.open(tmp_path) as src:
            band = src.read(1)

        os.remove(tmp_path)

        # Create mask of valid pixels (exclude 255, 8)
        valid_mask = ~np.isin(band, list(exclude_values))
        if not np.any(valid_mask):
            return None

        # Count cloud-affected pixels
        cloud_mask = np.isin(band, list(cloud_values))
        cloud_affected_pixels = np.count_nonzero(cloud_mask & valid_mask)
        total_valid_pixels = np.count_nonzero(valid_mask)

        area_km2 = 0.03*0.03*total_valid_pixels
        cloud_percent = (cloud_affected_pixels / total_valid_pixels) * 100
        return round(cloud_percent, 2), round(area_km2, 2)

    except Exception as e:
        print(f"Error processing {url}: {e}")
        return None


def generate_random_sample_points(
        polygon: Polygon, n: int = 10) -> List[Point]:
    """Generate random sample points within a polygon."""
    minx, miny, maxx, maxy = polygon.bounds
    points = []
    attempts = 0
    max_attempts = n * 10  # avoid infinite loop if polygon is small
    while len(points) < n and attempts < max_attempts:
        x = random.uniform(minx, maxx)
        y = random.uniform(miny, maxy)
        p = Point(x, y)
        if polygon.contains(p):
            points.append(p)
        attempts += 1
    return points


def generate_grid_sample_points(
        polygon: Polygon, num_points: int = 10) -> List[Point]:
    """ Generate approximately `num_points` evenly spaced points within a
    polygon using a grid. """
    minx, miny, maxx, maxy = polygon.bounds
    area = polygon.area

    # Estimate grid spacing based on area and number of points
    grid_spacing = (area / num_points) ** 0.5

    # Create grid points
    x_coords = np.arange(minx, maxx, grid_spacing)
    y_coords = np.arange(miny, maxy, grid_spacing)

    points = []
    for x in x_coords:
        for y in y_coords:
            p = Point(x, y)
            if polygon.contains(p):
                points.append(p)

    return points


def get_cloudiness_at_point(
        lat: float, lon: float, target_iso: str,
        allow_nearest: bool = False) -> Optional[float]:
    """
    Get cloudiness forecast at a single point and time.

    Parameters
    ----------
    lat, lon : float
        Coordinates of the point of interest
    target_iso : str
        Target datetime in ISO format (e.g. '2025-08-28T15:00') in UTC.
    allow_nearest : bool, optional
        If True, will return cloudiness for the nearest available time
        if exact time is not found.
    Returns
    -------
    cloudiness : float or None
        Cloud cover in %, or None if not available or on API error.
    """
    try:
        # Request forecast from Open-Meteo API
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "cloudcover",
            "timezone": "UTC"
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        # Validate response structure
        times = data.get("hourly", {}).get("time", [])
        clouds = data.get("hourly", {}).get("cloudcover", [])

        if not times or not clouds or len(times) != len(clouds):
            return None

        # Try exact match
        if target_iso in times:
            idx = times.index(target_iso)
            return clouds[idx]

        # If not found, find closest time (optional)
        if allow_nearest:
            target_dt_obj = parse_datetime(target_iso)
            time_diffs = [abs((parse_datetime(t) - target_dt_obj
                               ).total_seconds()) for t in times]
            min_idx = time_diffs.index(min(time_diffs))
            return clouds[min_idx]

        return None

    except (requests.RequestException, KeyError, ValueError) as e:
        print(f"Error calculating cloudiness using {url}: {e}")
        return None


def predict_cloudiness(
    polygon_geojson: Dict,
    target_datetime: Union[str, datetime],
    num_samples: int = 10,
    allow_nearest: bool = False,
    sampling_method: str = "random"  # "random" or "grid"
) -> Optional[float]:
    """
    Get forecasted average cloudiness over a polygon area.

    Parameters
    ----------
    polygon_geojson : dict
        GeoJSON polygon.
    target_datetime : str or datetime
        Datetime string or datetime object (UTC).
    num_samples : int
        Number of sample points inside the polygon.
    allow_nearest : bool
        Use nearest forecast time if exact not found.

    Returns
    -------
    float or None
        Average cloudiness over polygon at specified time.
    """
    try:
        # Normalize datetime
        if isinstance(target_datetime, str):
            target_dt = parse_datetime(target_datetime)
        else:
            target_dt = target_datetime
        target_iso = target_dt.strftime("%Y-%m-%dT%H:%M")

        # Prepare polygon and sample points
        poly = shape(polygon_geojson)
        if not poly.is_valid:
            poly = poly.buffer(0)
        if sampling_method == "grid":
            points = generate_grid_sample_points(poly, num_points=num_samples)
        else:
            points = generate_random_sample_points(poly, n=num_samples)

        if not points:
            return None

        # Query each point and collect cloudiness
        cloudiness_values = []
        for pt in points:
            val = get_cloudiness_at_point(pt.y, pt.x,
                                          target_iso,
                                          allow_nearest=allow_nearest)
            if val is not None:
                cloudiness_values.append(val)

        if not cloudiness_values:
            return None

        return sum(cloudiness_values) / len(cloudiness_values)

    except Exception as e:
        print(f"Error predict_cloudiness : {e}")
        return None


def make_get_cloudiness_for_row(aoi_polygon):
    def get_cloudiness_for_row(row):
        # Check if we have a list of timestamps
        timestamps = row.begin_date if isinstance(
            row.begin_date, list) else [row.begin_date]
        cloudiness_vals = []

        for timestamp in timestamps:
            now = datetime.now()
            four_days_later = now + timedelta(days=4)
            fourteen_days_later = now + timedelta(days=14)

            intersection_geom = row.geometry.intersection(aoi_polygon)

            if intersection_geom.is_empty:
                cloudiness_vals.append(None)
                continue

            geojson_geom = mapping(intersection_geom)

            n_samples = 200 if now <= timestamp <= four_days_later else 60

            if now <= timestamp <= fourteen_days_later:
                try:
                    cloudiness = predict_cloudiness(
                        polygon_geojson=geojson_geom,
                        target_datetime=timestamp,
                        num_samples=n_samples,
                        allow_nearest=True,
                        sampling_method="grid"
                    )
                    cloudiness_vals.append(cloudiness)
                except Exception as e:
                    LOGGER.warning(f"Cloudiness prediction failed for {
                        timestamp}: {e}")
                    cloudiness_vals.append(None)
            else:
                cloudiness_vals.append(None)

        return cloudiness_vals

    return get_cloudiness_for_row
