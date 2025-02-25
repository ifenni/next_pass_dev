import requests
from datetime import date, datetime
from tabulate import tabulate

# Base URL for path/row queries
MAP_SERVICE_URL = "https://nimbus.cr.usgs.gov/arcgis/rest/services/LLook_Outlines/MapServer/1/"
JSON_URL = "https://landsat.usgs.gov/sites/default/files/landsat_acq/assets/json/cycles_full.json"

def ll2pr(lat: float, lon: float):
    """
    Convert Lat/Lon to Path/Row for both ascending (A) and descending (D) directions.
    """
    results = {"ascending": None, "descending": None}
    directions = {"ascending": "A", "descending": "D"}

    for direction, mode in directions.items():
        query_url = (
            f"{MAP_SERVICE_URL}query?where=MODE='{mode}'"
            f"&geometry={lon},{lat}"
            "&geometryType=esriGeometryPoint&spatialRel=esriSpatialRelIntersects"
            "&outFields=PATH,ROW"
            "&returnGeometry=false&f=json"
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

        except requests.RequestException as error:
            print(f"Error fetching data for {direction.capitalize()} direction: {error}")
            results[direction] = None

    return results

def find_next_landsat_pass(path: int, num_passes: int = 5) -> dict:
    """
    Find the next num_passes Landsat passes for Landsat-8 and Landsat-9 for a given path.
    """
    try:
        response = requests.get(JSON_URL, timeout=10)
        response.raise_for_status()
        cycles_data = response.json()
    except requests.RequestException as error:
        print(f"Error fetching cycles data: {error}")
        return {"landsat_8": [], "landsat_9": []}

    next_passes = {"landsat_8": [], "landsat_9": []}
    today = date.today()

    for mission in next_passes:
        if mission not in cycles_data:
            print(f"Mission {mission} not found in JSON data.")
            continue

        for date_str, details in sorted(cycles_data[mission].items(),
                                        key=lambda x: datetime.strptime(x[0], "%m/%d/%Y")):
            pass_date = datetime.strptime(date_str, "%m/%d/%Y").date()

            if pass_date >= today and str(path) in details["path"].split(","):
                next_passes[mission].append(date_str)
                if len(next_passes[mission]) >= num_passes:
                    break

    return next_passes

def next_landsat_pass(lat: float, lon: float):
    """Main function for running the script."""
    try:
        results = ll2pr(lat, lon)
        table_data = []
        
        for direction, data in results.items():
            if data:
                for path, row in zip(data['paths'], data['rows']):
                    next_pass_dates = find_next_landsat_pass(path, num_passes=5)
                    for mission, dates in next_pass_dates.items():
                        if dates:
                            table_data.append([direction.capitalize(), path, row, mission.capitalize(), ', '.join(dates)])
                        else:
                            table_data.append([direction.capitalize(), path, row, mission.capitalize(), "No future passes found."])
            else:
                table_data.append([direction.capitalize(), "N/A", "N/A", "N/A", "No data found."])
        
        print(tabulate(table_data, headers=["Direction", "Path", "Row", "Mission", "Next Passes"], tablefmt="grid"))
    
    except Exception as error:
        print(f"An error occurred: {error}")
