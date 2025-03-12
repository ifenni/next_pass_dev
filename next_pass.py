#! /usr/bin/env python3
import argparse
import logging
from datetime import datetime
from typing import Optional, Tuple, Union, Dict

import asf_search as asf
import geopandas as gpd
from shapely.geometry import Point, Polygon, shape, box
from tabulate import tabulate

import xml.etree.ElementTree as ET
from landsat_pass import next_landsat_pass
from s1_collection import create_s1_collection_plan
from s2_collection import create_s2_collection_plan

LOGGER = logging.getLogger("next_pass")

EXAMPLE = """Example usage:
    next_pass.py --latitude 34.615 --longitude -81.936 --satellite sentinel-1
"""


def format_collection_outputs(next_collect, next_collect_orbit):
    table_data = [
        [i + 1, dt.strftime("%Y-%m-%d %H:%M:%S"), orbit]
        for i, (dt, orbit) in enumerate(zip(next_collect, next_collect_orbit))
    ]
    return tabulate(
        table_data,
        headers=["#", "Collection Date & Time", "Relative Orbit"],
        tablefmt="grid",
    )
def valid_latitude(value: float) -> float:
    """Validate latitude range (-90 to 90)."""
    if value < -90 or value > 90:
        raise argparse.ArgumentTypeError(f"Latitude must be between -90 and 90 degrees, got {value}.")
    return value

def valid_longitude(value: float) -> float:
    """Validate longitude range (-180 to 180)."""
    if value < -180 or value > 180:
        raise argparse.ArgumentTypeError(f"Longitude must be between -180 and 180 degrees, got {value}.")
    return value


def create_parser() -> argparse.ArgumentParser:
    """Create parser for command line arguments."""
    parser = argparse.ArgumentParser(
        description="Find the next satellite overpass date for a given point",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=EXAMPLE,
    )

    # Coordinates of the location bounding box as a single list of floats
    parser.add_argument(
        "--bounding_box", "-bb", required=True, type=float, nargs=4,
        help="Coordinates as four floats: lat_south lat_north lon_west lon_east"
    )

    # path to KML file including location shape exported from Google Earth for example
    parser.add_argument(
        "--area_shape_path", "-fp", required=True, type=str,
        help="Full path to the KML location shape file"
    )
  
    parser.add_argument(
        "--satellite",
        "-sat",
        required=True,
        type=str,
        choices=["sentinel-1", "sentinel-2", "landsat"],
        help="Satellite mission: sentinel-1, sentinel-2, or landsat",
    )
    parser.add_argument(
        "--granule", "-g", type=str, help="Granule name for Sentinel-1 processing"
    )
    parser.add_argument("--log_level", "-l", default="info", type=str, help="Log level")
    return parser

def parse_kml(kml_file):
    
    tree = ET.parse(kml_file)
    root = tree.getroot()
    
    # KML namespace handling
    namespaces = {'kml': 'http://www.opengis.net/kml/2.2'}
    
    # Find all coordinates in the KML (assuming the first place is a polygon)
    coordinates = []
    for placemark in root.findall('.//kml:Placemark//kml:coordinates', namespaces):
        coords_text = placemark.text.strip()
        coords_list = coords_text.split()
        for coord in coords_list:
            lon, lat, _ = coord.split(',')
            coordinates.append((float(lon), float(lat)))
    
    return coordinates

# Function to create a polygon from coordinates
def create_polygon_from_kml(kml_file):
    coordinates = parse_kml(kml_file)
    
    # Create a Polygon object using the coordinates from the KML file
    if coordinates:
        polygon = Polygon(coordinates)
        return polygon
    else:
        return None


def get_granule_info(granule: str) -> Tuple[Polygon, str, int]:
    """Retrieve granule information from ASF API."""
    try:
        result = asf.granule_search(granule)[0]

        footprint = shape(result.geometry)
        mode = result.properties["beamModeType"]
        orbit_relative = result.properties["pathNumber"]
        return footprint, mode, orbit_relative
    except Exception as e:
        LOGGER.error(f"Error retrieving granule info: {e}")
        raise


def find_valid_insar_collects(
    collections: gpd.GeoDataFrame, mode: str, orbit_relative: int
) -> gpd.GeoDataFrame:
    """Filter collections by mode and orbit relative number."""
    return collections.loc[
        (collections["orbit_relative"] == orbit_relative)
        & (collections["mode"] == mode)
    ]


def find_valid_collect_point(
    gdf: gpd.GeoDataFrame, footprint: Union[Polygon, Point], mode=None
) -> Tuple[bool, Optional[datetime]]:
    """Find valid collects intersecting a footprint."""
    gdf = gdf.loc[gdf["geometry"].intersects(footprint)].copy()

    if not gdf.empty:
        gdf = gdf.sort_values("begin_date", ascending=True).reset_index(drop=True)
        #return True, gdf["begin_date"].iloc[0]
        return True, gdf["begin_date"].tolist(), gdf["orbit_relative"].tolist(), gdf["geometry"].tolist()
    return False, None

def find_valid_collect_polygon(
    gdf: gpd.GeoDataFrame, footprint: Union[Polygon, Polygon], mode=None
) -> Tuple[bool, Optional[datetime]]:
    """Find valid collects intersecting a footprint."""
    gdf = gdf.loc[gdf["geometry"].intersects(footprint)].copy()

    if not gdf.empty:
        gdf = gdf.sort_values("begin_date", ascending=True).reset_index(drop=True)
        #return True, gdf["begin_date"].iloc[0]
        return True, gdf["begin_date"].tolist(), gdf["orbit_relative"].tolist(), gdf["geometry"].tolist()
    return False, None


def get_next_collect(
    lat_min: float, lat_max: float,lon_min: float,lon_max: float,location_str: str, collection_dataset: gpd.GeoDataFrame, mode: Optional[str] = None
) -> Dict[str, Union[str, Polygon, Point]]:
    """Get the next collect for a given point and optional mode."""
    if mode:
        collection_dataset = collection_dataset.loc[collection_dataset["mode"] == mode].copy()
        mode_msg = f" {mode} "
    else:
        mode_msg = " "

    if location_str: 
        area_polygon = create_polygon_from_kml(location_str)
        collect_scheduled, next_collect, next_collect_orbit, next_collect_geometry  = find_valid_collect_polygon(collection_dataset, area_polygon)
    elif lat_min == lat_max and lon_min == lon_max:
        point = Point(lon_min, lat_min)
        collect_scheduled, next_collect, next_collect_orbit, next_collect_geometry  = find_valid_collect_point(collection_dataset, point)
    else:
        west_longitude = lon_min
        south_latitude = lat_min
        east_longitude = lon_max
        north_latitude = lat_max
        bounding_box = box(west_longitude, south_latitude, east_longitude, north_latitude)
        collect_scheduled, next_collect, next_collect_orbit, next_collect_geometry  = find_valid_collect_polygon(collection_dataset, bounding_box)

    if collect_scheduled:
        #return f"Next{mode_msg}collect is {next_collect[0].strftime('%Y-%m-%d %H:%M:%S')}"
        #return f"Next{mode_msg} collect is: " + ", ".join(dt.strftime('%Y-%m-%d %H:%M:%S') for dt in next_collect)
        return {
            "next_collect_info": format_collection_outputs(next_collect, next_collect_orbit),
            "next_collect_geometry": next_collect_geometry  # Returning the geometry
        }
    max_date = collection_dataset["end_date"].max().date()
    return f"No{mode_msg}collect is scheduled on or before {max_date}"


def find_next_sentinel1_overpass(
    granule: str, collection_dataset: gpd.GeoDataFrame
) -> str:
    """Find the next interferometric collect for Sentinel-1."""
    footprint, mode, orbit_relative = get_granule_info(granule)
    valid_insar_collects = find_valid_insar_collects(collection_dataset, mode, orbit_relative)
    return get_next_collect(footprint, valid_insar_collects)

def find_next_sentinel2_overpass(
    lat_min: float, lat_max: float,lon_min: float,lon_max: float,location_str: str, collection_dataset: gpd.GeoDataFrame
) -> Dict[str, Union[str, Polygon, Point]]:
    """Find the next overpass for Sentinel-2 using its acquisition plans."""
    if location_str:
        area_polygon = create_polygon_from_kml(location_str)
        collect_scheduled, next_collect, next_collect_orbit, next_collect_geometry  = find_valid_collect_polygon(collection_dataset, area_polygon)
    elif lat_min == lat_max and lon_min == lon_max:
        point = Point(lon_min, lat_min)
        collect_scheduled, next_collect, next_collect_orbit, next_collect_geometry  = find_valid_collect_point(collection_dataset, point)
    else:
        west_longitude = lon_min
        south_latitude = lat_min
        east_longitude = lon_max
        north_latitude = lat_max
        bounding_box = box(west_longitude, south_latitude, east_longitude, north_latitude)
        collect_scheduled, next_collect, next_collect_orbit, next_collect_geometry  = find_valid_collect_polygon(collection_dataset, bounding_box)

    if collect_scheduled:
        #return f"Next Sentinel-2 collect is {next_collect.strftime('%Y-%m-%d %H:%M:%S')}"
        #return f"Next collect is: " + ", ".join(dt.strftime('%Y-%m-%d %H:%M:%S') for dt in next_collect)
        return {
            "next_collect_info": format_collection_outputs(next_collect, next_collect_orbit),
            "next_collect_geometry": next_collect_geometry  # Returning the geometry
        }
    max_date = collection_dataset["end_date"].max().date()
    return f"No Sentinel-2 collect is scheduled on or before {max_date}"


def find_next_overpass(
    lat_min: float,lat_max: float, long_min: float, long_max: float,location_str: str, satellite: str, granule: Optional[str] = None
) -> Dict[str, Union[str, Polygon, Point]]:
    """Find the next overpass for the given satellite and location."""
    if satellite == "sentinel-1":
        LOGGER.info("Processing Sentinel-1 data...")
        collection_path = create_s1_collection_plan()
        gdf = gpd.read_file(collection_path)
        if granule:
            return find_next_sentinel1_overpass(granule, gdf)
        #point = Point(longitude_min, latitude_min)
        return get_next_collect(lat_min,lat_max,long_min,long_max,location_str, gdf)

    if satellite == "sentinel-2":
        LOGGER.info("Processing Sentinel-2 data...")
        collection_path = create_s2_collection_plan()
        gdf = gpd.read_file(collection_path)
        return find_next_sentinel2_overpass(lat_min,lat_max,long_min,long_max,location_str,gdf)

    if satellite == "landsat":
        LOGGER.info("Fetching Landsat overpass information...")
        return next_landsat_pass(lat_min, long_min)

    LOGGER.error("Unsupported satellite: %s", satellite)
    return "Unsupported satellite."


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()

    log_level = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
    }.get(args.log_level.lower(), logging.INFO)

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    lat_south, lat_north, lon_west, lon_east = args.bounding_box
    valid_latitude(lat_south)
    valid_latitude(lat_north)
    valid_longitude(lon_west)
    valid_longitude(lon_east)

    result = find_next_overpass(
        lat_south, lat_north, lon_west,lon_east, args.area_shape_path, args.satellite, args.granule
    )
    print(result.get("next_collect_info", "No collection info available"))
