import requests
from datetime import date, datetime

# Base URL for path/row queries
MAP_SERVICE_URL = "https://nimbus.cr.usgs.gov/arcgis/rest/services/LLook_Outlines/MapServer/1/"
JSON_URL = "https://landsat.usgs.gov/sites/default/files/landsat_acq/assets/json/cycles_full.json"

def ll2pr(lat: float, lon: float):
    """
    Convert Lat/Lon to Path/Row for both ascending (A) and descending (D) directions.
    Additionally, returns the geometries (shapes) in GeoJSON format for folium.

    Args:
        lat (float): Latitude coordinate.
        lon (float): Longitude coordinate.

    Returns:
        tuple: A dictionary of Path and Row lists, and a GeoJSON feature collection for geometries.
    """
    results = {"ascending": None, "descending": None}
    geojson_shapes = {"type": "FeatureCollection", "features": []}
    directions = {"ascending": "A", "descending": "D"}

    for direction, mode in directions.items():
        query_url = (
            f"{MAP_SERVICE_URL}query?where=MODE='{mode}'"
            f"&geometry={lon},{lat}"
            "&geometryType=esriGeometryPoint&spatialRel=esriSpatialRelIntersects"
            "&outFields=PATH,ROW"
            "&returnGeometry=true&f=json"
        )

        try:
            response = requests.get(query_url, timeout=10)
            response.raise_for_status()
            data = response.json()

            if not data.get("features"):
                results[direction] = None
                continue

            paths = [feature["attributes"]["PATH"] for feature in data["features"]]
            rows = [feature["attributes"]["ROW"] for feature in data["features"]]
            results[direction] = {"paths": paths, "rows": rows}

            # Convert geometries to GeoJSON format with validation
            for feature in data["features"]:
                geometry = feature.get("geometry", {})
                if geometry and geometry.get("type") in {"Polygon", "MultiPolygon"}:
                    geojson_shapes["features"].append({
                        "type": "Feature",
                        "geometry": geometry,
                        "properties": {
                            "path": feature["attributes"]["PATH"],
                            "row": feature["attributes"]["ROW"],
                            "direction": direction
                        }
                    })

        except requests.RequestException as error:
            print(f"Error fetching data for {direction.capitalize()} direction: {error}")
            results[direction] = None

    return results, geojson_shapes


def find_next_landsat_pass(path: int) -> dict:
    """
    Find the next Landsat pass for Landsat-8 and Landsat-9 for a given path.

    Args:
        path (int): The Landsat WRS-2 path number.

    Returns:
        dict: A dictionary containing the next pass dates for Landsat-8 and Landsat-9.
    """
    try:
        response = requests.get(JSON_URL, timeout=10)
        response.raise_for_status()
        cycles_data = response.json()
    except requests.RequestException as error:
        print(f"Error fetching cycles data: {error}")
        return {"landsat_8": None, "landsat_9": None}

    next_passes = {"landsat_8": None, "landsat_9": None}
    today = date.today()

    for mission in next_passes:
        if mission not in cycles_data:
            print(f"Mission {mission} not found in JSON data.")
            continue

        for date_str, details in sorted(cycles_data[mission].items(),
                                        key=lambda x: datetime.strptime(x[0], "%m/%d/%Y")):
            pass_date = datetime.strptime(date_str, "%m/%d/%Y").date()

            if pass_date >= today and str(path) in details["path"].split(","):
                next_passes[mission] = date_str
                break

    return next_passes


def next_landsat_pass(lat: float, lon: float):
    """Main function for running the script."""
    try:
        results, shapes = ll2pr(lat, lon)

        for direction, data in results.items():
            if data:
                print(f"{direction.capitalize()} Direction:")
                print(f"  Paths: {data['paths']}")
                print(f"  Rows: {data['rows']}")

                next_pass_dates = find_next_landsat_pass(data["paths"][0])
                print("  Next passes:")
                for mission, date in next_pass_dates.items():
                    print(f"    {mission.capitalize()}: {date}" if date else
                          f"    {mission.capitalize()}: No future pass found.")
            else:
                print(f"No data found for {direction.capitalize()} direction.")

    except Exception as error:
        print(f"An error occurred: {error}")
    return shapes
