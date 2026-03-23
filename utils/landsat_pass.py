import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

import requests
from shapely.geometry import Point, Polygon
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union
from tabulate import tabulate

from utils.utils import arcgis_to_polygon

logger = logging.getLogger(__name__)

# Base URLs
MAP_SERVICE_URL = (
    "https://nimbus.cr.usgs.gov/arcgis/rest/services/LLook_Outlines/MapServer/1/"
)
LEGACY_CYCLES_FULL_URL = (
    "https://landsat.usgs.gov/sites/default/files/landsat_acq/assets/json/"
    "cycles_full.json"
)
CYCLE_REFERENCE_URL = (
    "https://landsat.usgs.gov/sites/default/files/landsat_acq/assets/json/"
    "cycles.json"
)
CYCLE_PATH_ROW_URL = (
    "https://landsat.usgs.gov/sites/default/files/landsat_acq/assets/json/"
    "cycle_path_row.json"
)
LANDSAT_MISSIONS = ("landsat_8", "landsat_9")
DATE_FORMAT = "%m/%d/%Y"
MAX_SCHEDULE_SEARCH_DAYS = 365
UNIX_EPOCH = date(1970, 1, 1)


@dataclass
class LandsatScheduleSource:
    """Normalized Landsat schedule inputs from modern or legacy USGS sources."""

    source: str
    warnings: list[str] = field(default_factory=list)
    cycle_sequence: list[int] | None = None
    mission_cycle_paths: dict[str, dict[int, set[int]]] | None = None
    legacy_cycles: dict | None = None
    latest_legacy_date: date | None = None


def format_date_lines(date_strings: list[str], per_line: int = 5) -> str:
    """Wrap Landsat pass dates across multiple lines."""
    formatted_dates = [
        date_str
        + (" (P)" if datetime.strptime(date_str, DATE_FORMAT) < datetime.now() else "")
        for date_str in date_strings
    ]
    return "\n".join(
        ", ".join(formatted_dates[i:i + per_line])
        for i in range(0, len(formatted_dates), per_line)
    )


def shapely_to_esri_json(geometry: BaseGeometry) -> tuple[str, str]:
    """
    Convert a Shapely geometry to Esri JSON format and return geometryType.

    Args:
        geometry (BaseGeometry): A Shapely Point or Polygon.

    Returns:
        tuple: (Esri JSON geometry string, geometry type)
    """
    if isinstance(geometry, Point):
        coords = f"{geometry.x},{geometry.y}"
        return coords, "esriGeometryPoint"

    if isinstance(geometry, Polygon):
        coords = list(geometry.exterior.coords)
        rings = [[[x, y] for x, y in coords]]  # [ [lon, lat], ... ]
        esri_geom = {"rings": rings, "spatialReference": {"wkid": 4326}}
        return json.dumps(esri_geom), "esriGeometryPolygon"

    msg = "Unsupported geometry type. Only Point and Polygon are supported."
    raise ValueError(msg)


def _fetch_json(url: str, session: requests.Session) -> dict:
    """Fetch and decode a JSON document from USGS."""
    response = session.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


def _build_cycle_sequence(cycle_reference_data: dict) -> list[int]:
    """Map day offsets in the 16-day cycle to USGS cycle numbers."""
    mission_data = cycle_reference_data.get("landsat_8")
    if not isinstance(mission_data, dict):
        msg = "Missing landsat_8 cycle reference data."
        raise ValueError(msg)

    cycle_sequence = []
    for day_of_cycle in range(1, 17):
        cycle_key = f"1/{day_of_cycle}/1970"
        cycle_number = int(mission_data[cycle_key]["cycle"])
        cycle_sequence.append(cycle_number)

    if sorted(cycle_sequence) != list(range(1, 17)):
        msg = "Unexpected cycle reference values from USGS."
        raise ValueError(msg)

    return cycle_sequence


def _build_mission_cycle_paths(cycle_path_row_data: dict) -> dict[str, dict[int, set[int]]]:
    """Reduce cycle path/row data to mission -> cycle -> available paths."""
    mission_cycle_paths: dict[str, dict[int, set[int]]] = {}

    for mission in LANDSAT_MISSIONS:
        mission_data = cycle_path_row_data.get(mission)
        if not isinstance(mission_data, dict):
            msg = f"Missing {mission} cycle path/row data."
            raise ValueError(msg)

        mission_cycle_paths[mission] = {}
        for cycle_str, entries in mission_data.items():
            cycle_number = int(cycle_str)
            mission_cycle_paths[mission][cycle_number] = {
                int(entry["path"])
                for entry in entries
                if isinstance(entry, dict) and "path" in entry
            }

    return mission_cycle_paths


def _latest_legacy_date(legacy_cycles: dict) -> date | None:
    """Find the newest date exposed by the legacy cycles_full payload."""
    latest_dates = []
    for mission in LANDSAT_MISSIONS:
        mission_data = legacy_cycles.get(mission, {})
        if not isinstance(mission_data, dict):
            continue
        for date_str in mission_data:
            latest_dates.append(datetime.strptime(date_str, DATE_FORMAT).date())

    return max(latest_dates) if latest_dates else None


def load_landsat_schedule_source(session: requests.Session) -> LandsatScheduleSource:
    """Load Landsat schedule data, preferring current USGS path/row resources."""
    try:
        cycle_reference_data = _fetch_json(CYCLE_REFERENCE_URL, session)
        cycle_path_row_data = _fetch_json(CYCLE_PATH_ROW_URL, session)
        return LandsatScheduleSource(
            source="modern",
            cycle_sequence=_build_cycle_sequence(cycle_reference_data),
            mission_cycle_paths=_build_mission_cycle_paths(cycle_path_row_data),
        )
    except (requests.RequestException, KeyError, TypeError, ValueError) as error:
        logger.warning(
            "Unable to load modern Landsat schedule inputs; falling back to "
            "cycles_full.json: %s",
            error,
        )

    warnings = [
        (
            "Using legacy Landsat schedule fallback because the current USGS "
            "cycle mapping could not be loaded."
        ),
    ]

    try:
        legacy_cycles = _fetch_json(LEGACY_CYCLES_FULL_URL, session)
    except requests.RequestException as error:
        logger.error("Error fetching legacy Landsat cycles data: %s", error)
        warnings.append("Landsat schedule data is temporarily unavailable.")
        return LandsatScheduleSource(source="unavailable", warnings=warnings)

    return LandsatScheduleSource(
        source="legacy",
        warnings=warnings,
        legacy_cycles=legacy_cycles,
        latest_legacy_date=_latest_legacy_date(legacy_cycles),
    )


def _cycle_for_date(target_date: date, cycle_sequence: list[int]) -> int:
    """Resolve a date to the USGS 16-day cycle number."""
    days_since_epoch = (target_date - UNIX_EPOCH).days
    return cycle_sequence[days_since_epoch % len(cycle_sequence)]


def _find_passes_with_modern_schedule(
    path: int,
    start_date: date,
    num_passes: int,
    schedule_source: LandsatScheduleSource,
) -> dict[str, list[str]]:
    """Compute Landsat 8/9 pass dates from cycle-day path mappings."""
    next_passes = {mission: [] for mission in LANDSAT_MISSIONS}
    cycle_sequence = schedule_source.cycle_sequence or []
    mission_cycle_paths = schedule_source.mission_cycle_paths or {}

    for mission in LANDSAT_MISSIONS:
        mission_paths = mission_cycle_paths.get(mission, {})
        for offset in range(MAX_SCHEDULE_SEARCH_DAYS + 1):
            candidate_date = start_date + timedelta(days=offset)
            cycle_number = _cycle_for_date(candidate_date, cycle_sequence)
            if path in mission_paths.get(cycle_number, set()):
                next_passes[mission].append(candidate_date.strftime(DATE_FORMAT))
                if len(next_passes[mission]) >= num_passes:
                    break

    return next_passes


def _find_passes_with_legacy_schedule(
    path: int,
    start_date: date,
    num_passes: int,
    schedule_source: LandsatScheduleSource,
    today: date,
) -> tuple[dict[str, list[str]], list[str]]:
    """Read pass dates from the legacy full-date payload, with stale fallback."""
    next_passes = {mission: [] for mission in LANDSAT_MISSIONS}
    warnings = list(schedule_source.warnings)
    legacy_cycles = schedule_source.legacy_cycles or {}

    latest_legacy_date = schedule_source.latest_legacy_date
    if latest_legacy_date and latest_legacy_date < today:
        warnings.append(
            "USGS legacy Landsat schedule source is stale through "
            f"{latest_legacy_date.strftime(DATE_FORMAT)}; showing the last "
            "available matching dates."
        )

    for mission in LANDSAT_MISSIONS:
        mission_data = legacy_cycles.get(mission, {})
        if not isinstance(mission_data, dict):
            logger.warning("Mission %s not found in legacy JSON data.", mission)
            continue

        matching_dates = []
        in_window_dates = []
        sorted_dates = sorted(
            mission_data.items(),
            key=lambda item: datetime.strptime(item[0], DATE_FORMAT),
        )

        for date_str, details in sorted_dates:
            pass_date = datetime.strptime(date_str, DATE_FORMAT).date()
            paths = details.get("path", "").split(",")

            if str(path) not in paths:
                continue

            matching_dates.append(date_str)
            if pass_date >= start_date:
                in_window_dates.append(date_str)

        if in_window_dates:
            next_passes[mission] = in_window_dates[:num_passes]
        elif latest_legacy_date and latest_legacy_date < today:
            next_passes[mission] = matching_dates[-num_passes:]

    return next_passes, warnings


def ll2pr(geometry: BaseGeometry, session: requests.Session) -> dict:
    """
    Convert a Shapely geometry (Point or Polygon) to Path/Row and their geometries.

    Args:
        geometry (BaseGeometry): Shapely Point or Polygon.
        session (requests.Session): HTTP session object.

    Returns:
        dict: Dictionary with 'ascending' and 'descending' data.
    """
    results: dict[str, list | None] = {"ascending": None, "descending": None}
    directions = {"ascending": "A", "descending": "D"}

    geometry_json, geometry_type = shapely_to_esri_json(geometry)

    for direction, mode in directions.items():
        query_url = f"{MAP_SERVICE_URL}query"
        params = {
            "where": f"MODE='{mode}'",
            "geometryType": geometry_type,
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "PATH,ROW",
            "returnGeometry": "true",
            "f": "json",
        }

        try:
            response = session.post(
                query_url,
                params=params,
                data={"geometry": geometry_json},
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()

            if not data.get("features"):
                results[direction] = None
                continue

            features = []
            for feature in data["features"]:
                attributes = feature["attributes"]
                geom = feature.get("geometry")
                features.append(
                    {
                        "path": attributes["PATH"],
                        "row": attributes["ROW"],
                        "geometry": geom,
                    }
                )

            results[direction] = features

        except requests.RequestException as error:
            logger.error(
                "Error fetching data for %s direction: %s",
                direction.capitalize(),
                error,
            )
            results[direction] = None

    return results


def find_next_landsat_pass(
    path: int,
    n_day_past: float,
    schedule_source: LandsatScheduleSource,
    num_passes: int = 5,
    today: date | None = None,
) -> tuple[dict[str, list[str]], list[str]]:
    """
    Find the next Landsat-8 and Landsat-9 passes for a given path.

    Args:
        path (int): WRS-2 path number.
        schedule_source (LandsatScheduleSource): Loaded USGS schedule data.
        num_passes (int): Number of future passes to find (default is 5).
        today (date | None): Optional override for deterministic testing.

    Returns:
        tuple: Next pass dates for each mission plus any schedule warnings.
    """
    current_date = today or date.today()
    start_date = current_date - timedelta(days=n_day_past)

    if schedule_source.source == "modern":
        return (
            _find_passes_with_modern_schedule(
                path,
                start_date,
                num_passes,
                schedule_source,
            ),
            list(schedule_source.warnings),
        )

    if schedule_source.source == "legacy":
        return _find_passes_with_legacy_schedule(
            path,
            start_date,
            num_passes,
            schedule_source,
            current_date,
        )

    return ({mission: [] for mission in LANDSAT_MISSIONS}, list(schedule_source.warnings))


def next_landsat_pass(
    lat: float,
    lon: float,
    geometryAOI,
    n_day_past: float,
) -> dict | None:
    """
    Retrieve and format the next Landsat passes for a given location.

    Args:
        lat (float): Latitude.
        lon (float): Longitude.
        geometryAOI: Geometry of the area of interest used for computing
            intersection percentage.
        n_day_past (float): Number of days in the past to search cycles JSON.

    Returns:
        dict or None: Dictionary containing next Landsat passes information
        and geometries, or None on failure.
    """
    session = requests.Session()

    try:
        results = ll2pr(geometryAOI, session=session)
        schedule_source = load_landsat_schedule_source(session)
        aggregated_data = defaultdict(
            lambda: {"rows": set(), "overlap_pct": 0.0, "dates": None, "warnings": []}
        )
        geometry_groups = defaultdict(list)

        for direction, features in results.items():
            if features:
                for feature in features:
                    path = feature["path"]
                    row = feature["row"]
                    geom = feature.get("geometry")
                    polygon = arcgis_to_polygon(geom)

                    if geometryAOI.geom_type == "Point":
                        intersection_pct = 100
                    elif polygon and polygon.is_valid and geometryAOI.is_valid:
                        intersection = polygon.intersection(geometryAOI)
                        intersection_pct = 100 * (intersection.area / geometryAOI.area)
                    else:
                        intersection_pct = 0.0

                    next_pass_dates, schedule_warnings = find_next_landsat_pass(
                        path,
                        n_day_past,
                        schedule_source=schedule_source,
                        num_passes=5,
                    )
                    for mission, dates in next_pass_dates.items():
                        key = (direction.capitalize(), path, mission.capitalize())
                        aggregated_data[key]["rows"].add(row)
                        aggregated_data[key]["overlap_pct"] += intersection_pct
                        if aggregated_data[key]["dates"] is None:
                            aggregated_data[key]["dates"] = dates
                        if schedule_warnings:
                            aggregated_data[key]["warnings"] = schedule_warnings

                        if polygon:
                            geometry_groups[key].append(polygon)
            else:
                key = (direction.capitalize(), "N/A", "N/A")
                aggregated_data[key]["rows"].add("N/A")
                aggregated_data[key]["dates"] = []
                aggregated_data[key]["overlap_pct"] = 0.0

        row_data_with_keys = []
        for key, data in aggregated_data.items():
            direction, path, mission = key
            row_list = sorted(data["rows"])
            rows_str = ", ".join(str(r) for r in row_list)
            overlap = data["overlap_pct"]
            overlap_str = f"{overlap:.2f}%" if overlap > 0 else "N/A"

            if data["dates"]:
                dates_str = format_date_lines(data["dates"])
            else:
                dates_str = "No Landsat passes found."

            if data["warnings"]:
                warning_text = "\n".join(
                    f"Warning: {warning}" for warning in data["warnings"]
                )
                dates_str = f"{dates_str}\n{warning_text}"

            row_data = [direction, path, rows_str, mission, dates_str, overlap_str]
            summary = "\n".join(
                [
                    f"Direction: {direction}",
                    f"Path: {path}",
                    f"Row: {rows_str}",
                    f"Mission: {mission}",
                    f"Passes UTC dates (P for past):\n{dates_str}",
                    f"AOI % Overlap: {overlap_str}",
                ]
            )

            # Include key for geometry ordering
            row_data_with_keys.append((overlap, row_data, key, summary))

        sorted_row_data = sorted(row_data_with_keys, key=lambda x: x[0], reverse=True)
        table_data = [row for _, row, _, _ in sorted_row_data]
        geometry_keys = [key for _, _, key, _ in sorted_row_data]
        summaries = [summary for _, _, _, summary in sorted_row_data]

        geometry_data = []
        for key in geometry_keys:
            polygons = geometry_groups.get(key, [])
            if polygons:
                merged = unary_union(polygons)
                geometry_data.append(merged)

        return {
            "next_collect_info": tabulate(
                table_data,
                headers=[
                    "Direction",
                    "Path",
                    "Row",
                    "Mission",
                    "Passes UTC dates (P for past)",
                    "AOI % Overlap",
                ],
                tablefmt="grid",
            ),
            "next_collect_geometry": geometry_data,
            "next_collect_summary": summaries,
        }

    except Exception as error:  # noqa: BLE001
        logger.exception("An unexpected error occurred: %s", error)
        return None
    finally:
        session.close()
