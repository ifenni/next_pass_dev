import logging
from datetime import datetime
from pathlib import Path
from typing import Iterable, List
from urllib.request import urlopen

import geopandas as gpd
from bs4 import BeautifulSoup
from lxml import etree
from shapely import LinearRing, Polygon
import requests

LOGGER = logging.getLogger('acquisition_utils')

def scrape_esa_website_for_download_urls(url: str, class_: str) -> List[str]:
    """Scrape the ESA website for KML download URLs."""
    try:
        page = urlopen(url)
        html = page.read().decode('utf-8')
        soup = BeautifulSoup(html, 'html.parser')
        div = soup.find_all('div', class_=class_)[0]
        ul = div.find('ul')
        hrefs = [a['href'] for a in ul.find_all('a')]
        download_urls = [f'https://sentinel.esa.int{href}' for href in hrefs]
        return download_urls
    except Exception as e:
        LOGGER.error(f"Error scraping ESA website: {e}")
        return []

def download_kml(url: str, out_path: str = 'collection.kml') -> Path:
    """Download a KML file from a given URL."""
    try:
        response = requests.get(url)
        if response.status_code == 200:
            with open(out_path, 'wb') as file:
                file.write(response.content)
            LOGGER.info(f'File downloaded successfully: {out_path}')
        else:
            LOGGER.warning(f'Failed to download the file: {url}')
        return Path(out_path)
    except Exception as e:
        LOGGER.error(f"Error downloading KML file: {e}")
        return Path()

def parse_placemark(placemark: etree.Element) -> Iterable:
    """Parse a single placemark element from the KML."""
    prefix = './/{http://www.opengis.net/kml/2.2}'
    try:
        begin_date = datetime.fromisoformat(placemark.find(f'{prefix}begin').text)
        end_date = datetime.fromisoformat(placemark.find(f'{prefix}end').text)

        data = placemark.find(f'{prefix}ExtendedData')
        mode = data.find(f"{prefix}Data[@name='Mode']").find(f'{prefix}value').text
        orbit_absolute = int(data.find(f"{prefix}Data[@name='OrbitAbsolute']").find(f'{prefix}value').text)
        orbit_relative = int(data.find(f"{prefix}Data[@name='OrbitRelative']").find(f'{prefix}value').text)

        footprint = placemark.find(f'{prefix}LinearRing').find(f'{prefix}coordinates').text
        x_coords = [float(point.split(',')[0]) for point in footprint.split(' ')]
        y_coords = [float(point.split(',')[1]) for point in footprint.split(' ')]
        footprint = Polygon(LinearRing(zip(x_coords, y_coords)))

        return (begin_date, end_date, mode, orbit_absolute, orbit_relative, footprint)
    except Exception as e:
        LOGGER.error(f"Error parsing placemark: {e}")
        return None

def parse_kml(kml_path: Path) -> gpd.GeoDataFrame:
    """Parse a KML file and return a GeoDataFrame."""
    try:
        placemark_pattern = './/{http://www.opengis.net/kml/2.2}Placemark'
        tree = etree.parse(kml_path).getroot()
        placemarks = [parse_placemark(elem) for elem in tree.findall(placemark_pattern)]
        placemarks = [p for p in placemarks if p is not None]  # Remove any None entries
        
        columns = ['begin_date', 'end_date', 'mode', 'orbit_absolute', 'orbit_relative', 'geometry']
        gdf = gpd.GeoDataFrame(data=placemarks, columns=columns, geometry='geometry', crs='EPSG:4326')
        return gdf
    except Exception as e:
        LOGGER.error(f"Error parsing KML file: {e}")
        return gpd.GeoDataFrame()
