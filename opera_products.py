import csv
import logging
import time
from datetime import date

import folium
import leafmap
import matplotlib.pyplot as plt
import pandas as pd
from branca.element import MacroElement
from dateutil.relativedelta import relativedelta
from jinja2 import Template
from matplotlib.colors import to_hex

from utils import bbox_type, create_polygon_from_kml

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
LOGGER = logging.getLogger(__name__)


def find_print_available_opera_products(bbox, number_of_dates) -> dict:
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

    # Date ranges for search
    today = date.today()
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
                    gdf["BeginningDateTime"] = pd.to_datetime(gdf["BeginningDateTime"])

                    # Extract unique acquisition dates
                    gdf["AcqDate"] = gdf["BeginningDateTime"].dt.date
                    unique_dates = gdf.sort_values(
                        "BeginningDateTime", ascending=False
                    )["AcqDate"].unique()
                    selected_dates = unique_dates[:number_of_dates]

                    # Keep all granules that match selected dates
                    gdf = gdf[gdf["AcqDate"].isin(selected_dates)]

                    # Final formatting
                    gdf["BeginningDateTime"] = gdf["BeginningDateTime"].dt.strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    )
                    results = [results[k] for k in gdf["original_index"]]
                    gdf = gdf.drop(columns=["original_index", "AcqDate"])
                    LOGGER.info(f"-> Success: {dataset} → {len(gdf)} granule(s) saved.")
                    results_dict[dataset] = {
                        "results": results,
                        "gdf": gdf,
                    }
                    break
                else:
                    LOGGER.info(f"xxx Attempt {attempt}: No granules for {dataset}.")
            except Exception as e:
                LOGGER.info(
                    f"xxx Attempt {attempt}:" f" Error fetching {dataset}: {str(e)}"
                )

            if attempt < max_attempts:
                time.sleep(2**attempt)
            else:
                LOGGER.info(
                    f"-> Failed to fetch {dataset}" f" after {max_attempts} attempts."
                )

    return results_dict


def export_opera_products(results_dict):
    # export to csv file
    output_file = "opera_products_metadata.csv"
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            ["Dataset", "Granule ID", "Start Time", "End Time", "Download URL"]
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

                download_url = "N/A"
                for url_entry in umm.get("RelatedUrls", []):
                    if url_entry.get("Type") == "GET DATA":
                        download_url = url_entry.get("URL", "N/A")
                        break

                writer.writerow(
                    [dataset, granule_id, start_time, end_time, download_url]
                )
    LOGGER.info(
        "\n-> OPERA products metadata successfully saved"
        "to opera_granule_metadata.csv"
    )


def make_opera_granule_map(results_dict, bbox):
    """
    Create an interactive map displaying OPERA granules for all datasets
    with download links.
    """
    output_file = "opera_products_map.html"

    # Parse AOI center for initial map centering
    bbox = bbox_type(bbox)
    if isinstance(bbox, str):
        geometry = create_polygon_from_kml(bbox)
        AOI = geometry.bounds
    else:
        lat_min, lat_max, lon_min, lon_max = bbox
        AOI = (lon_min, lat_min, lon_max, lat_max)

    center_lat = (AOI[1] + AOI[3]) / 2
    center_lon = (AOI[0] + AOI[2]) / 2

    # Initialize base map
    map_object = folium.Map(location=[center_lat, center_lon], zoom_start=7)
    folium.TileLayer("Esri.WorldImagery").add_to(map_object)

    # Generate distinct colors for layers
    cmap = plt.get_cmap("tab20")
    dataset_names = list(results_dict.keys())
    colors = [to_hex(cmap(i % 20)) for i in range(len(dataset_names))]

    legend_entries = []

    for i, (dataset, data) in enumerate(results_dict.items()):
        gdf = data.get("gdf")
        if gdf is None or gdf.empty:
            LOGGER.info(f"Skipping {dataset}: empty or missing GeoDataFrame.")
            continue

        gdf = gdf.copy()
        if i < 4:
            pos_delta = 0.08 * (i - 1)
        else:
            pos_delta = 0.08 * (i - 5)
        # Add download URL and name for popup
        for idx, item in enumerate(data["results"]):
            try:
                umm = item["umm"]
                download_url = "N/A"
                for url_entry in umm.get("RelatedUrls", []):
                    if url_entry.get("Type") == "GET DATA":
                        download_url = url_entry.get("URL", "N/A")
                        break
                label = umm.get("GranuleUR", "OPERA Granule")

            except Exception as e:
                LOGGER.info(f"Unexpected error: {e}")
                download_url = "URL not available"
                label = "OPERA Granule"

            gdf.iloc[idx, gdf.columns.get_loc("URL")] = download_url
            gdf.iloc[idx, gdf.columns.get_loc("GranuleUR")] = label

        color = colors[i]
        style = {
            "color": color,
            "fillColor": color,
            "weight": 2,
            "fillOpacity": 0.5,
        }

        # Add geometries
        folium.GeoJson(
            gdf.__geo_interface__,
            name=dataset,
            style_function=lambda x, style=style: style,
        ).add_to(map_object)

        # Add popup markers
        for _, row in gdf.iterrows():
            centroid = row.geometry.centroid
            popup_html = f"""
                <b>{row['GranuleUR']}</b><br>
                <a href="{row['URL']}" target="_blank">
                    Download Granule
                </a>
            """
            # Use icon_color in folium.Icon for each marker
            folium.Marker(
                location=[centroid.y + pos_delta, centroid.x + pos_delta],
                popup=folium.Popup(popup_html, max_width=400),
                icon=folium.Icon(
                    color="lightgray", icon_color=color, icon="cloud-download"
                ),
            ).add_to(map_object)

        legend_entries.append((dataset, color))

    # Add layer control
    folium.LayerControl().add_to(map_object)

    # Add legend
    legend_html = """
    {% macro html(this, kwargs) %}
    <div style="position: fixed;
                bottom: 50px; left: 50px; width: 220px; height: auto;
                z-index:9999; font-size:14px;
                background-color: white;
                padding: 10px;
                border: 2px solid grey;
                border-radius: 5px;">
    <b>OPERA Products</b><br>
    {% for name, color in this.legend_items %}
        <div style="margin-bottom:4px">
            <span style="display:inline-block; width:12px; height:12px;
                        background-color:{{ color }}; margin-right:6px">
            </span>
            {{ name }}
        </div>
    {% endfor %}
    </div>
    {% endmacro %}
    """

    class Legend(MacroElement):
        def __init__(self, legend_items):
            super().__init__()
            self._template = Template(legend_html)
            self.legend_items = legend_items

    map_object.get_root().add_child(Legend(legend_entries))

    # Save map
    map_object.save(output_file)
    LOGGER.info(f"-> OPERA granules Map successfully saved to {output_file}")
