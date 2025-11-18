import logging
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd
from tabulate import tabulate
from tqdm import tqdm

from cloudiness import make_get_cloudiness_for_row
from collection_builder import build_sentinel_collection
from utils import find_intersecting_collects, scrape_esa_download_urls

LOGGER = logging.getLogger("sentinel_pass")

SENT1_URL = "https://sentinels.copernicus.eu/web/sentinel/copernicus/sentinel-1/acquisition-plans"
SENT2_URL = "https://sentinels.copernicus.eu/web/sentinel/copernicus/sentinel-2/acquisition-plans"


def create_s1_collection_plan(n_day_past: float) -> Path:
    """Prepare Sentinel-1 acquisition plan collection."""
    urls = scrape_esa_download_urls(SENT1_URL, "sentinel-1a")
    urls += scrape_esa_download_urls(SENT1_URL, "sentinel-1c")
    return build_sentinel_collection(
        urls, n_day_past, "sentinel1", "sentinel_1_collection.geojson", LOGGER
    )


def create_s2_collection_plan(n_day_past: float) -> Path:
    """Prepare Sentinel-2 acquisition plan collection."""
    urls = scrape_esa_download_urls(SENT2_URL, "sentinel-2a")
    urls += scrape_esa_download_urls(SENT2_URL, "sentinel-2b")
    return build_sentinel_collection(
        urls, n_day_past, "sentinel2", "sentinel_2_collection.geojson", LOGGER
    )


def format_collects(gdf: gpd.GeoDataFrame) -> str:
    gdf_sorted = gdf.sort_values("intersection_pct", ascending=False)
    has_cloudiness = "cloudiness" in gdf.columns
    table = []
    for idx, row in gdf_sorted.iterrows():
        base_row = [
            idx + 1,
            row.orbit_relative,
            ", ".join(
                date.strftime("%Y-%m-%d %H:%M:%S")
                + (" (P)" if date < datetime.now(timezone.utc) else "")
                for date in row.begin_date
            ),
            f"{row.intersection_pct:.2f}",
        ]
        if has_cloudiness:
            cloud_vals = [
                f"{val:.2f}" if val is not None else "N/A" for val in row.cloudiness
            ]
            cloudiness_str = ", ".join(cloud_vals)
            base_row.append(cloudiness_str)
        table.append(base_row)

    headers = [
        "#",
        "Relative Orbit",
        "Collection Date & Time (P for past)",
        "AOI % Overlap",
    ]
    if has_cloudiness:
        headers.append("Cloudiness (%)")
    return tabulate(table, headers, tablefmt="grid")


def unique_geometry_per_orbit(collects):
    def first_unique_geoms(geoms):
        seen = set()
        unique = []
        for g in geoms:
            wkt = g.wkt
            if wkt not in seen:
                seen.add(wkt)
                unique.append(g)
        return unique

    has_cloudiness = "cloudiness" in collects.columns
    # Ensure dates are proper datetime
    collects["begin_date"] = pd.to_datetime(collects["begin_date"], errors="raise")

    # Group by orbit and keep dates as list of datetime
    if has_cloudiness:
        grouped = (
            collects.groupby("orbit_relative")
            .agg(
                {
                    "begin_date": lambda dates: sorted(dates),
                    "geometry": first_unique_geoms,
                    "intersection_pct": "first",
                    "cloudiness": "first",
                }
            )
            .reset_index()
        )
    else:
        grouped = (
            collects.groupby("orbit_relative")
            .agg(
                {
                    "begin_date": lambda dates: sorted(dates),
                    "geometry": first_unique_geoms,
                    "intersection_pct": "first",
                }
            )
            .reset_index()
        )

    # One geometry per orbit (first unique)
    grouped["geometry"] = grouped["geometry"].apply(
        lambda geoms: geoms[0] if geoms else None
    )

    # as done in format_collects, sort by intersection_pct descending
    grouped = grouped.sort_values("intersection_pct", ascending=False).reset_index(
        drop=True
    )
    return grouped


def next_sentinel_pass(sat, geometry, n_day_past, arg_cloudiness) -> dict:
    """
    Load Sentinel collection, find intersects, and format results.

    Args:
        create_plan_func: Function to create or fetch collection file.
        geometry: Shapely geometry (Point or Polygon) to check intersects.

    Returns:
        dict: Dictionary with formatted collect info, collect geometries,
        and percentage overlap of each collect with the input geometry (AOI).
    """
    try:
        if sat == "sentinel1":
            gdf = gpd.read_file(create_s1_collection_plan(n_day_past))
        elif sat == "sentinel2":
            gdf = gpd.read_file(create_s2_collection_plan(n_day_past))
    except (IOError, OSError) as e:
        LOGGER.error(f"Error reading Sentinel plan file: {e}")
        return {
            "next_collect_info": "Error reading plan file.",
            "next_collect_geometry": None,
            "intersection_pct": None,
        }

    # Enable progress bar for apply (optional)
    tqdm.pandas()

    collects = find_intersecting_collects(gdf, geometry)
    collects = collects.drop_duplicates(subset=["begin_date", "orbit_relative"])

    if not collects.empty:
        if arg_cloudiness:
            # Group collects by orbit, aggregate timestamps as list
            collects_grouped = (
                collects.groupby("orbit_relative", sort=False)
                .agg(
                    {
                        "begin_date": list,
                        "geometry": "first",  # Or use union if needed
                        "intersection_pct": "mean",  # Or max
                    }
                )
                .reset_index()
            )

            num_rows = len(collects_grouped)
            LOGGER.info(
                f"Calculating cloudiness for overpasses over {num_rows} relative orbits ..."
            )
            get_cloudiness_for_row = make_get_cloudiness_for_row(geometry)
            collects_grouped["cloudiness"] = collects_grouped.apply(
                get_cloudiness_for_row, axis=1
            )

            grouped = collects_grouped

            return {
                "next_collect_info": format_collects(grouped),
                "next_collect_geometry": grouped["geometry"].tolist(),
                "intersection_pct": grouped["intersection_pct"].tolist(),
                "cloudiness": grouped["cloudiness"].tolist(),
            }
        else:
            grouped = unique_geometry_per_orbit(collects)
            return {
                "next_collect_info": format_collects(grouped),
                "next_collect_geometry": grouped["geometry"].tolist(),
                "intersection_pct": grouped["intersection_pct"].tolist(),
            }
    else:
        return {
            "next_collect_info": f"No scheduled collects before {
                gdf['end_date'].max().date()}.",
            "intersection_pct": None,
            "cloudiness": None,
        }
