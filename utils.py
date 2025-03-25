import logging
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Tuple, Optional

import geopandas as gpd
import requests
from bs4 import BeautifulSoup
from lxml import etree
from shapely import LinearRing, Polygon, Point
import argparse
import xml.etree.ElementTree as ET

LOGGER = logging.getLogger('acquisition_utils')

def scrape_esa_download_urls(url: str, class_: str) -> List[str]:
    """Scrape ESA website for KML download URLs."""
    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')
    div = soup.find('div', class_=class_)
    hrefs = [a['href'] for a in div.find_all('a', href=True)]
    return [f'https://sentinel.esa.int{href}' for href in hrefs]


def download_kml(url: str, out_path: str = 'collection.kml') -> Path:
    """Download a KML file from a URL."""
    response = requests.get(url)
    response.raise_for_status()
    path = Path(out_path)
    path.write_bytes(response.content)
    LOGGER.info(f'File downloaded successfully: {path}')
    return path


def parse_placemark(placemark: etree.Element) -> Optional[Tuple]:
    """Parse a single placemark from KML."""
    ns = './/{http://www.opengis.net/kml/2.2}'
    begin_date = datetime.fromisoformat(placemark.find(f'{ns}begin').text)
    end_date = datetime.fromisoformat(placemark.find(f'{ns}end').text)

    data = placemark.find(f'{ns}ExtendedData')
    mode = data.find(f"{ns}Data[@name='Mode']/{ns}value").text
    orbit_absolute = int(data.find(f"{ns}Data[@name='OrbitAbsolute']/{ns}value").text)
    orbit_relative = int(data.find(f"{ns}Data[@name='OrbitRelative']/{ns}value").text)

    coords_text = placemark.find(f'{ns}LinearRing/{ns}coordinates').text.strip()
    coords = [tuple(map(float, coord.split(',')[:2])) for coord in coords_text.split()]
    footprint = Polygon(LinearRing(coords))

    return begin_date, end_date, mode, orbit_absolute, orbit_relative, footprint


def parse_kml(kml_path: Path) -> gpd.GeoDataFrame:
    """Parse a KML file into a GeoDataFrame."""
    tree = etree.parse(kml_path)
    placemarks = [parse_placemark(elem) for elem in tree.findall('.//{http://www.opengis.net/kml/2.2}Placemark')]
    placemarks = [p for p in placemarks if p]

    columns = ['begin_date', 'end_date', 'mode', 'orbit_absolute', 'orbit_relative', 'geometry']
    return gpd.GeoDataFrame(placemarks, columns=columns, geometry='geometry', crs='EPSG:4326')


def parse_kml_polygon_coords(kml_file: Path) -> List[Tuple[float, float]]:
    """Extract coordinates from a KML polygon."""
    tree = ET.parse(kml_file)
    ns = {'kml': 'http://www.opengis.net/kml/2.2'}
    coords_text = tree.find('.//kml:Placemark//kml:coordinates', ns).text.strip()
    return [tuple(map(float, coord.split(',')[:2])) for coord in coords_text.split()]


def create_polygon_from_kml(kml_file: Path) -> Optional[Polygon]:
    """Create a Shapely polygon from a KML file."""
    coordinates = parse_kml_polygon_coords(kml_file)
    return Polygon(coordinates) if coordinates else None


def find_intersecting_collects(
    gdf: gpd.GeoDataFrame,
    geometry: Polygon | Point,
    mode: Optional[str] = None,
    orbit_relative: Optional[int] = None,
) -> gpd.GeoDataFrame:
    intersects = gdf[gdf.intersects(geometry)]
    if mode:
        intersects = intersects[intersects['mode'] == mode]
    if orbit_relative is not None:
        intersects = intersects[intersects['orbit_relative'] == orbit_relative]
    return intersects.sort_values('begin_date').reset_index()


def bbox_type(coords):
    if len(coords) != 4:
        raise argparse.ArgumentTypeError(
            "bbox requires exactly four arguments: lat_min lat_max lon_min lon_max"
        )
    lat_min, lat_max, lon_min, lon_max = map(float, coords)

    if not (-90 <= lat_min <= 90 and -90 <= lat_max <= 90):
        raise argparse.ArgumentTypeError(
            f"Latitudes must be between -90 and 90 degrees. Got: {lat_min}, {lat_max}"
        )
    if not (-180 <= lon_min <= 180 and -180 <= lon_max <= 180):
        raise argparse.ArgumentTypeError(
            f"Longitudes must be between -180 and 180 degrees. Got: {lon_min}, {lon_max}"
        )
    if lat_min > lat_max:
        raise argparse.ArgumentTypeError(
            f"Minimum latitude (lat_min) cannot be greater than maximum latitude (lat_max). "
            f"Got: {lat_min} > {lat_max}"
        )
    if lon_min > lon_max:
        raise argparse.ArgumentTypeError(
            f"Minimum longitude (lon_min) cannot be greater than maximum longitude (lon_max). "
            f"Got: {lon_min} > {lon_max}"
        )

    return lat_min, lat_max, lon_min, lon_max
