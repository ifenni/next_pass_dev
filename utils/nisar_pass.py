import logging
import re
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from bs4 import BeautifulSoup
from shapely.geometry import Polygon
from tabulate import tabulate

from utils.utils import find_intersecting_collects

LOGGER = logging.getLogger(__name__)

NISAR_PLAN_URL = (
    "https://assets.science.nasa.gov/content/dam/science/missions/nisar/kmz/"
    "NISAR_ROP358_TFDB_ObservationPlan_CY2026-20260305.kmz?emrc=69bc6ef442719"
)
SCRATCH_DIR = Path.cwd() / "scratch"
KMZ_FILENAME = "nisar_observation_plan.kmz"
COLLECTION_FILENAME = "nisar_collection.geojson"
KML_NS = "{http://www.opengis.net/kml/2.2}"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TRACK_FRAME_RE = re.compile(r"^T(?P<track>\d+)_F(?P<frame>\d+)$", re.IGNORECASE)


def download_nisar_plan(url: str, output_path: Path) -> Path:
    """Download the NISAR observation-plan KMZ if needed."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        LOGGER.info("Using cached NISAR plan: %s", output_path)
        return output_path

    LOGGER.info("Downloading NISAR observation plan from NASA...")
    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with open(output_path, "wb") as file_obj:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file_obj.write(chunk)
    return output_path


def parse_nisar_description(
    description_html: str,
) -> tuple[list[tuple[datetime, str]], dict[str, str]]:
    """Extract acquisition dates, radar modes, and key attributes."""
    soup = BeautifulSoup(description_html or "", "html.parser")
    products: list[tuple[datetime, str]] = []
    attributes: dict[str, str] = {}

    for row in soup.find_all("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["th", "td"])]
        if len(cells) < 2:
            continue

        first, second = cells[0], cells[1]
        if DATE_RE.match(first):
            timestamp = datetime.combine(
                datetime.strptime(first, "%Y-%m-%d").date(),
                time.min,
                tzinfo=timezone.utc,
            )
            products.append((timestamp, second))
            continue

        if first in {"track", "frame", "passDirection"}:
            attributes[first] = second

    return products, attributes


def iter_nisar_placemarks(kmz_path: Path):
    """Yield parsed placemark records from the NISAR observation-plan KMZ."""
    with zipfile.ZipFile(kmz_path) as archive:
        with archive.open("doc.kml") as kml_stream:
            context = ET.iterparse(kml_stream, events=("end",))
            for _, elem in context:
                if elem.tag != f"{KML_NS}Placemark":
                    continue

                name = elem.findtext(f"{KML_NS}name", default="").strip()
                description = elem.findtext(f"{KML_NS}description", default="")
                coordinates_text = elem.findtext(
                    f".//{KML_NS}Polygon/{KML_NS}outerBoundaryIs/"
                    f"/{KML_NS}LinearRing/{KML_NS}coordinates",
                    default="",
                )
                elem.clear()

                if not coordinates_text:
                    continue

                coordinates = [
                    tuple(map(float, coord.split(",")[:2]))
                    for coord in coordinates_text.split()
                ]
                if len(coordinates) < 4:
                    continue

                products, attributes = parse_nisar_description(description)
                if not products:
                    continue

                match = TRACK_FRAME_RE.match(name)
                track = attributes.get("track")
                frame = attributes.get("frame")
                if match is not None:
                    track = track or match.group("track")
                    frame = frame or match.group("frame")

                geometry = Polygon(coordinates)
                if not geometry.is_valid:
                    geometry = geometry.buffer(0)
                if geometry.is_empty:
                    continue

                yield {
                    "name": name,
                    "track": int(track) if track else None,
                    "frame": int(frame) if frame else None,
                    "pass_direction": attributes.get("passDirection", "Unknown"),
                    "products": products,
                    "geometry": geometry,
                }


def create_nisar_collection_plan() -> Path:
    """Build or reuse a local GeoJSON collection for NISAR overpasses."""
    out_path = SCRATCH_DIR / COLLECTION_FILENAME
    if out_path.exists():
        LOGGER.info("Using cached NISAR collection: %s", out_path)
        return out_path

    kmz_path = download_nisar_plan(NISAR_PLAN_URL, SCRATCH_DIR / KMZ_FILENAME)

    rows: list[dict] = []
    for placemark in iter_nisar_placemarks(kmz_path):
        for begin_date, radar_mode in placemark["products"]:
            rows.append(
                {
                    "name": placemark["name"],
                    "track": placemark["track"],
                    "frame": placemark["frame"],
                    "pass_direction": placemark["pass_direction"],
                    "radar_mode": radar_mode,
                    "begin_date": begin_date,
                    "end_date": begin_date,
                    "geometry": placemark["geometry"],
                }
            )

    if not rows:
        LOGGER.error("No NISAR collects found in the requested date window.")
        return Path()

    gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")
    gdf = gdf.sort_values(["begin_date", "track", "frame"]).reset_index(drop=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(out_path)
    LOGGER.info("NISAR collection saved to: %s", out_path)
    return out_path


def format_collects(gdf: gpd.GeoDataFrame) -> str:
    """Format NISAR collects for CLI output."""
    gdf_sorted = gdf.sort_values("intersection_pct", ascending=False).reset_index(drop=True)
    table = []

    for index, row in gdf_sorted.iterrows():
        dates = row.begin_date if isinstance(row.begin_date, list) else [row.begin_date]
        dates_str = ", ".join(
            stamp.strftime("%Y-%m-%d")
            + (" (P)" if stamp < datetime.now(timezone.utc) else "")
            for stamp in dates
        )

        table.append(
            [
                index + 1,
                row.pass_direction,
                row.track,
                row.frame,
                dates_str,
                f"{row.intersection_pct:.2f}",
            ]
        )

    return tabulate(
        table,
        headers=[
            "#",
            "Direction",
            "Track",
            "Frame",
            "Acquisition Date (P = past)",
            "AOI % Overlap",
        ],
        tablefmt="grid",
    )


def next_nisar_pass(geometry, n_day_past: float) -> dict:
    """Return formatted NISAR overpasses intersecting the AOI."""
    try:
        collection_path = create_nisar_collection_plan()
        if not collection_path:
            raise OSError("NISAR collection could not be created.")
        gdf = gpd.read_file(collection_path)
    except (IOError, OSError, requests.RequestException, zipfile.BadZipFile) as error:
        LOGGER.error("Error reading NISAR plan file: %s", error)
        return {
            "next_collect_info": "Error reading NISAR plan file.",
            "next_collect_geometry": None,
            "intersection_pct": None,
        }

    gdf["begin_date"] = pd.to_datetime(gdf["begin_date"], utc=True, errors="coerce")
    gdf["end_date"] = pd.to_datetime(gdf["end_date"], utc=True, errors="coerce")
    gdf = gdf.dropna(subset=["begin_date", "geometry"]).reset_index(drop=True)
    n_days_earlier = datetime.now(timezone.utc) - timedelta(days=n_day_past)
    gdf = gdf.loc[gdf["begin_date"] >= n_days_earlier].reset_index(drop=True)

    collects = find_intersecting_collects(gdf, geometry)
    if collects.empty:
        last_date = gdf["end_date"].max()
        last_text = last_date.date().isoformat() if pd.notna(last_date) else "available plan"
        return {
            "next_collect_info": f"No scheduled collects before {last_text}.",
            "next_collect_geometry": None,
            "intersection_pct": None,
        }

    collects = collects.drop_duplicates(
        subset=["begin_date", "track", "frame", "pass_direction", "radar_mode"]
    )

    grouped = (
        collects.groupby(["pass_direction", "track", "frame"], dropna=False, sort=False)
        .agg(
            {
                "begin_date": lambda dates: sorted(dates),
                "radar_mode": lambda modes: list(dict.fromkeys(modes)),
                "geometry": "first",
                "intersection_pct": "max",
            }
        )
        .reset_index()
        .sort_values("intersection_pct", ascending=False)
        .reset_index(drop=True)
    )

    # NISAR neighboring frames can overlap slightly. For a given track and
    # pass direction, keep only the frame(s) with the strongest AOI overlap.
    best_overlap = grouped.groupby(
        ["pass_direction", "track"], dropna=False
    )["intersection_pct"].transform("max")
    grouped = grouped[grouped["intersection_pct"] == best_overlap].reset_index(
        drop=True
    )

    return {
        "next_collect_info": format_collects(grouped),
        "next_collect_geometry": grouped["geometry"].tolist(),
        "intersection_pct": grouped["intersection_pct"].tolist(),
    }
