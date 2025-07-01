import csv
import logging
import time
from datetime import date, datetime

import leafmap
import pandas as pd
from dateutil.relativedelta import relativedelta
from utils import bbox_type, create_polygon_from_kml

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
LOGGER = logging.getLogger(__name__)


def find_print_available_opera_products(
                                        bbox, number_of_dates, date_str
                                        ) -> dict:
    opera_datasets = [
        "OPERA_L3_DSWX-HLS_V1",
        "OPERA_L3_DSWX-S1_V1",
        "OPERA_L3_DIST-ALERT-HLS_V1",
        "OPERA_L3_DIST-ANN-HLS_V1",
        "OPERA_L2_RTC-S1_V1",
        "OPERA_L2_CSLC-S1_V1",
        "OPERA_L3_DISP-S1_V1",
    ]

    # Parse the bbox argument
    bbox = bbox_type(bbox)

    if isinstance(bbox, str):
        # Create geometry from KML file
        geometry = create_polygon_from_kml(bbox)
        AOI = geometry.bounds
    else:
        # Extract bounding box
        lat_min, lat_max, lon_min, lon_max = bbox
        # (minx, miny, maxx, maxy)
        AOI = (lon_min, lat_min, lon_max, lat_max)

    if date_str == "today":
        today = date.today()
    else:
        today = datetime.strptime(date_str, "%Y-%m-%d").date()
    one_year_ago = today - relativedelta(months=12)
    StartDate_Recent = one_year_ago.strftime("%Y-%m-%d") + "T00:00:00"
    EndDate_Recent = today.strftime("%Y-%m-%d") + "T23:59:59"

    results_dict = {}
    LOGGER.info("\n** Available OPERA Products for Selected AOI **\n")
    for dataset in opera_datasets:
        LOGGER.info(f"* Searching {dataset} ...")

        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                results, gdf = leafmap.nasa_data_search(
                    short_name=dataset,
                    cloud_hosted=True,
                    bounding_box=AOI,
                    temporal=(StartDate_Recent, EndDate_Recent),
                    return_gdf=True,
                )

                if gdf is not None and not gdf.empty:
                    gdf = gdf.copy()
                    gdf["original_index"] = gdf.index
                    gdf["BeginningDateTime"] = pd.to_datetime(
                                                gdf["BeginningDateTime"])

                    # Extract unique acquisition dates
                    gdf["AcqDate"] = gdf["BeginningDateTime"].dt.date
                    unique_dates = gdf.sort_values(
                        "BeginningDateTime", ascending=False
                    )["AcqDate"].unique()
                    selected_dates = unique_dates[:number_of_dates]

                    # Keep all granules that match selected dates
                    gdf = gdf[gdf["AcqDate"].isin(selected_dates)]

                    # Final formatting
                    gdf["BeginningDateTime"] = gdf[
                                                "BeginningDateTime"
                                                ].dt.strftime(
                                                "%Y-%m-%dT%H:%M:%SZ"
                                                )
                    results = [results[k] for k in gdf["original_index"]]
                    gdf = gdf.drop(columns=["original_index", "AcqDate"])
                    LOGGER.info(
                        f"-> Success: {dataset} → {len(gdf)} granule(s) saved."
                    )
                    results_dict[dataset] = {
                        "results": results,
                        "gdf": gdf,
                    }
                    break
                else:
                    LOGGER.info(f"xxx Attempt {attempt}: "
                                f"No granules for {dataset}.")
            except Exception as e:
                LOGGER.info(
                    f"xxx Attempt {attempt}:"
                    f" Error fetching {dataset}: {str(e)}"
                )

            if attempt < max_attempts:
                time.sleep(2**attempt)
            else:
                LOGGER.info(
                    f"-> Failed to fetch {dataset}"
                    f" after {max_attempts} attempts."
                )

    return results_dict

def export_opera_products(results_dict, timestamp_dir):
    # export to csv file
    output_file = timestamp_dir / "opera_products_metadata.csv"
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            ["Dataset", "Granule ID", "Start Time", "End Time",
             "Download URL WTR", "Download URL BWTR", "Download URL VEG-ANOM-MAX",
             "Download URL VEG-DIST-STATUS", "Download URL VEG-DIST-DATE",
             "Download URL VEG-DIST-CONF", "Download URL S1A_30", "Download URL S1A_VV"]
        )

        for dataset, data in results_dict.items():
            for item in data["results"]:
                umm = item["umm"]
                granule_id = umm.get("GranuleUR", "N/A")
                temporal = umm.get("TemporalExtent", {})
                start_time = temporal.get("RangeDateTime", {}).get(
                    "BeginningDateTime", "N/A"
                )
                end_time = temporal.get("RangeDateTime", {}).get(
                    "EndingDateTime", "N/A"
                )

                urls = {
                    "water": "N/A",
                    "bwater": "N/A",
                    "veg_anom_max": "N/A",
                    "veg_dist_status": "N/A",
                    "veg_dist_date": "N/A",
                    "veg_dist_conf": "N/A",
                    "s1a_30": "N/A",
                    "s1a_vv": "N/A"
                }

                keyword_map = {
                    'B01_WTR': 'water',
                    'BWTR': 'bwater',
                    'VEG-ANOM-MAX': 'veg_anom_max',
                    'VEG-DIST-STATUS': 'veg_dist_status',
                    'VEG-DIST-DATE': 'veg_dist_date',
                    'VEG-DIST-CONF': 'veg_dist_conf',
                    'S1A_30': 's1a_30',
                    'S1A_VV': 's1a_vv'
                }
                
                related_urls = umm.get("RelatedUrls", [])
                for url_entry in related_urls:
                    url = url_entry.get("URL", "")
                    if not url.startswith("https://"):
                        continue
                    if not (url.endswith(".tif") or url.endswith(".h5")):
                        continue
                    for keyword, key in keyword_map.items():
                        if keyword in url:
                            urls[key] = url
                writer.writerow([
                    dataset, granule_id, start_time, end_time,
                    urls["water"], urls["bwater"], urls["veg_anom_max"],
                    urls["veg_dist_status"], urls["veg_dist_date"],
                    urls["veg_dist_conf"], urls["s1a_30"], urls["s1a_vv"]
                ])
    LOGGER.info(
        "-> OPERA products metadata successfully saved"
        "to opera_granule_metadata.csv"
    )