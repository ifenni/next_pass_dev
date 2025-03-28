import argparse
import logging
from pathlib import Path
from datetime import datetime
import geopandas as gpd
import pandas as pd

from utils import scrape_esa_download_urls, download_kml, parse_kml

SENT2_URL = 'https://sentinel.esa.int/web/sentinel/copernicus/sentinel-2/acquisition-plans'
LOGGER = logging.getLogger('s2_collection')

EXAMPLE = """Example usage:
    s2_collection.py --log_level info
"""

def create_parser() -> argparse.ArgumentParser:
    """Create parser for command line arguments."""
    parser = argparse.ArgumentParser(
        description="Create a Sentinel-2 acquisition plan collection",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=EXAMPLE,
    )

    parser.add_argument("--log_level", "-l", default="info", type=str, help="Log level")
    return parser

def create_s2_collection_plan() -> Path:
    """Create a collection plan for Sentinel-2."""
    urls = scrape_esa_download_urls(SENT2_URL, 'sentinel-2a')
    urls.extend(scrape_esa_download_urls(SENT2_URL, 'sentinel-2b'))

    gdfs = []

    for url in urls:
        name = Path(url).name
        collection_gdf_path = Path(f'{name}.geojson')

        if collection_gdf_path.exists():
            LOGGER.info(f"Collection already prepared: {collection_gdf_path}")
            gdf = gpd.read_file(collection_gdf_path)
        else:
            LOGGER.info(f"Downloading and parsing: {url}")
            file_path = download_kml(url, f'{name[0:3]}_collection.kml')
            gdf = parse_kml(file_path)
            if not gdf.empty:
                gdf.to_file(collection_gdf_path)
            else:
                LOGGER.warning(f"No valid data found in file: {file_path}")
        gdfs.append(gdf)

    if not gdfs:
        LOGGER.error("No GeoDataFrames created. Exiting.")
        return Path()

    full_gdf = pd.concat(gdfs).drop_duplicates()
    full_gdf['begin_date'] = pd.to_datetime(full_gdf['begin_date'])
    full_gdf = full_gdf.loc[full_gdf['begin_date'] >= datetime.now()].copy()
    full_gdf = full_gdf.sort_values('begin_date', ascending=True).reset_index(drop=True)

    out_path = Path.cwd() / 'sentinel_2_collection.geojson'
    full_gdf.to_file(out_path)
    LOGGER.info(f"Sentinel-2 collection plan saved to: {out_path}")

    return out_path

if __name__ == '__main__':
    parser = create_parser()
    args = parser.parse_args()

    log_level = {
        'debug': logging.DEBUG, 
        'info': logging.INFO,
        'warning': logging.WARNING, 
        'error': logging.ERROR
    }.get(args.log_level.lower(), logging.INFO)
    
    logging.basicConfig(level=log_level, 
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    create_s2_collection_plan()
