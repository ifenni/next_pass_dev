import logging
from pathlib import Path
import pandas as pd
import geopandas as gpd
from tabulate import tabulate
from datetime import datetime, timezone

from collection_builder import build_sentinel_collection
from utils import (find_intersecting_collects,
                   scrape_esa_download_urls)
from cloudiness import make_get_cloudiness_for_row
from tqdm import tqdm


LOGGER = logging.getLogger("sentinel_pass")

SENT1_URL = (
    "https://sentinels.copernicus.eu/web/sentinel/copernicus/sentinel-1/acquisition-plans"
)
SENT2_URL = (
    "https://sentinels.copernicus.eu/web/sentinel/copernicus/sentinel-2/acquisition-plans"
)


def create_s1_collection_plan(n_day_past: float) -> Path:
    urls = scrape_esa_download_urls(SENT1_URL, "sentinel-1a")
    urls += scrape_esa_download_urls(SENT1_URL, "sentinel-1c")
    platforms = ["S1A"] * len(
                        scrape_esa_download_urls(SENT1_URL, "sentinel-1a")) + \
                ["S1C"] * len(
                        scrape_esa_download_urls(SENT1_URL, "sentinel-1c"))
    return build_sentinel_collection(
        urls, n_day_past, "sentinel1", "sentinel_1_collection.geojson", 
        LOGGER, platforms
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

    has_cloudiness = "cloudiness" in gdf_sorted.columns

    # Only show platform column if it has at least one non-empty value
    has_platform = "platform" in gdf_sorted.columns \
        and gdf_sorted["platform"].notnull().any() \
        and (gdf_sorted["platform"].astype(str) != "").any()

    table = []

    for i, row in gdf_sorted.iterrows():
        base_row = [i + 1]  # Row number

        if has_platform:
            base_row.append(row.platform)

        # Relative orbit
        base_row.append(row.orbit_relative)

        # Dates
        dates_str = ", ".join(
            d.strftime("%Y-%m-%d %H:%M:%S"
                       ) + (
                           " (P)" if d < datetime.now(timezone.utc) else "")
            for d in row.begin_date
        )
        base_row.append(dates_str)

        # Intersection %
        base_row.append(f"{row.intersection_pct:.2f}")

        # Cloudiness if exists
        if has_cloudiness:
            if isinstance(row.cloudiness, list):
                cloud_str = ", ".join(
                    f"{v:.2f}" if v is not None else "N/A"
                    for v in row.cloudiness
                )
            else:
                cloud_str = f"{row.cloudiness:.2f}"
            base_row.append(cloud_str)

        table.append(base_row)

    # Headers
    headers = ["#"]
    if has_platform:
        headers.append("Platform")
    headers += [
        "Relative Orbit",
        "Collection Date & UTC Time (P = past)",
        "AOI % Overlap"
    ]
    if has_cloudiness:
        headers.append("Cloudiness (%)")

    return tabulate(table, headers=headers, tablefmt="grid")


def unique_geometry_per_orbit(collects):
    """
    Aggregate granules per orbit, keeping unique geometries and
    separating S1A and S1C even if they share the same orbit.
    """

    # Helper to keep only unique geometries
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

    # Ensure begin_date is datetime
    collects['begin_date'] = pd.to_datetime(
        collects['begin_date'], errors='raise'
        )

    # Aggregation dictionary
    agg_dict = {
        "begin_date": lambda dates: sorted(dates),
        "geometry": first_unique_geoms,
        "intersection_pct": "first",
    }

    if has_cloudiness:
        agg_dict["cloudiness"] = "first"

    # Group by both orbit_relative and platform for Sentinel-1
    groupby_cols = ["orbit_relative"]
    if "platform" in collects.columns and collects["platform"].notna().any():
        groupby_cols.append("platform")

    grouped = collects.groupby(groupby_cols).agg(agg_dict).reset_index()

    # Flatten geometry list to first geometry only
    grouped["geometry"] = grouped["geometry"].apply(
                        lambda geoms: geoms[0] if geoms else None)

    # Sort by intersection percentage
    grouped = grouped.sort_values("intersection_pct", ascending=False
                                  ).reset_index(drop=True)

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
    if "platform" not in gdf.columns:
        LOGGER.warning(
            "The collection plan does not contain a 'platform' column."
            )

    collects = find_intersecting_collects(gdf, geometry)
    collects = collects.drop_duplicates(
        subset=["begin_date", "orbit_relative"])

    if "platform" not in gdf.columns:
        LOGGER.warning(
            "The collection plan does not contain a 'platform' column."
            )

    if not collects.empty:
        if arg_cloudiness:
            # Group collects by orbit, aggregate timestamps as list
            collects_grouped = collects.groupby(
                "orbit_relative", sort=False
                ).agg({
                        "begin_date": list,
                        "geometry": "first",  # Or use union if needed
                        "intersection_pct": "mean"  # Or max
                    }).reset_index()

            num_rows = len(collects_grouped)
            LOGGER.info(f"Calculating cloudiness for overpasses over {
                num_rows} relative orbits ...")
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
            if "platform" not in gdf.columns:
                LOGGER.warning(
                    "The collection plan does not contain a 'platform' column."
                    )
            return {
                "next_collect_info": format_collects(grouped),
                "next_collect_geometry": grouped["geometry"].tolist(),
                "intersection_pct": grouped["intersection_pct"].tolist()
            }
    else:
        return {
            "next_collect_info": f"No scheduled collects before {
                gdf['end_date'].max().date()}.",
            "intersection_pct": None,
            "cloudiness": None,
        }

