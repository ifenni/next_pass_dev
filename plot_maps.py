import colorsys
import json
import logging
import random
import re
from datetime import datetime, timezone

import folium
import geopandas as gpd
import matplotlib.pyplot as plt
from branca.element import MacroElement
from jinja2 import Template
from matplotlib.colors import to_hex
from shapely.geometry import Polygon, box

from utils import (
    bbox_type,
    check_opera_overpass_intersection,
    create_polygon_from_kml,
    style_function_factory,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
LOGGER = logging.getLogger(__name__)


def hsl_distinct_colors(n):
    colors = []
    for i in range(n):
        # Generate colors with different hues
        hue = i / float(n)
        color = colorsys.hsv_to_rgb(hue, 1.0, 1.0)
        # Convert from RGB (0-1) to hex (#RRGGBB)
        rgb = [int(c * 255) for c in color]
        hex_color = "#{:02x}{:02x}{:02x}".format(*rgb)
        colors.append(hex_color)
    return colors


def spread_rgb_colors(n):
    colors = []
    # Divide the color space into n parts
    step = 255 // n
    for i in range(n):
        # Spread out the color values across the RGB spectrum
        r = (i * step) % 256
        g = ((i + 1) * step) % 256
        b = ((i + 2) * step) % 256
        hex_color = "#{:02x}{:02x}{:02x}".format(r, g, b)
        colors.append(hex_color)
    return colors


def hsl_distinct_colors_improved(num_colors):
    colors = []
    for i in range(num_colors):
        hue = (i * 360 / num_colors) % 360
        saturation = random.randint(60, 80)
        lightness = random.randint(30, 50)
        r, g, b = colorsys.hls_to_rgb(hue / 360, lightness / 100, saturation / 100)
        hex_color = "#{:02x}{:02x}{:02x}".format(
            int(r * 255), int(g * 255), int(b * 255)
        )
        colors.append(hex_color)

    return colors


def make_opera_granule_map(results_dict, bbox, timestamp_dir):
    """
    Create an interactive map displaying OPERA granules for all datasets
    with download links.
    """
    output_file = timestamp_dir / "opera_products_map.html"

    # Parse AOI center for initial map centering
    bbox = bbox_type(bbox)
    if isinstance(bbox, str):
        geometry = create_polygon_from_kml(bbox)
        AOI = geometry.bounds
        AOI_polygon = geometry
    else:
        lat_min, lat_max, lon_min, lon_max = bbox
        AOI = (lon_min, lat_min, lon_max, lat_max)
        AOI_polygon = box(*AOI)

    center_lat = (AOI[1] + AOI[3]) / 2
    center_lon = (AOI[0] + AOI[2]) / 2

    # Initialize base map
    map_object = folium.Map(location=[center_lat, center_lon], zoom_start=7)
    # Get AOI bounding box
    aoi_geojson = gpd.GeoSeries([AOI_polygon]).__geo_interface__
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
        # set the color of the icon and gemetry
        color = colors[i]
        style = {
            "color": color,
            "fillColor": color,
            "weight": 2,
            "fillOpacity": 0.5,
        }
        # Create a FeatureGroup to hold both geometries and markers
        feature_group = folium.FeatureGroup(name=dataset)
        # Add geometries to FeatureGroup
        folium.GeoJson(
            gdf.__geo_interface__,
            style_function=lambda x, style=style: style,
        ).add_to(feature_group)

        # Add popup markers to FeatureGroup
        for _, row in gdf.iterrows():
            centroid = row.geometry.centroid
            popup_html = f"""
                <b>{row['GranuleUR']}</b><br>
                <a href="{row['URL']}" target="_blank">
                    Download Granule
                </a>
            """
            folium.Marker(
                location=[centroid.y + pos_delta, centroid.x + pos_delta],
                popup=folium.Popup(popup_html, max_width=400),
                icon=folium.Icon(
                    color="lightgray", icon_color=color, icon="cloud-download"
                ),
            ).add_to(feature_group)

        # Add the FeatureGroup to the map
        feature_group.add_to(map_object)
        legend_entries.append((dataset, color))
    # finish with AOI
    folium.GeoJson(
        aoi_geojson,
        name="AOI",
        style_function=lambda x: {"color": "black", "weight": 2, "fillOpacity": 0.0},
    ).add_to(map_object)
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

    # Save and retrun
    map_object.save(output_file)
    LOGGER.info(f"-> OPERA granules Map successfully saved to {output_file}")
    return map_object


def make_opera_granule_drcs_map(
    event_date, results_dict, result_s1, result_s2, result_l, bbox, timestamp_dir
):
    """
    Create an interactive map displaying OPERA granules for all datasets
    with download links.
    """
    output_file = timestamp_dir / "opera_products_drcs_map.html"
    # Parse AOI center for initial map centering
    bbox = bbox_type(bbox)
    if isinstance(bbox, str):
        geometry = create_polygon_from_kml(bbox)
        AOI = geometry.bounds
        AOI_polygon = geometry
    else:
        lat_min, lat_max, lon_min, lon_max = bbox
        AOI = (lon_min, lat_min, lon_max, lat_max)
        AOI_polygon = box(*AOI)

    center_lat = (AOI[1] + AOI[3]) / 2
    center_lon = (AOI[0] + AOI[2]) / 2

    # Initialize base map
    map_object = folium.Map(location=[center_lat, center_lon], zoom_start=7)
    # Get AOI bounding box
    aoi_geojson = gpd.GeoSeries([AOI_polygon]).__geo_interface__

    folium.TileLayer("Esri.WorldImagery").add_to(map_object)
    # Generate distinct colors for layers
    cmap = plt.get_cmap("tab20")
    dataset_names = list(results_dict.keys())
    colors = [to_hex(cmap(i % 20)) for i in range(len(dataset_names))]
    legend_entries = []
    # here we are looping over OPERA products
    for i, (dataset, data) in enumerate(results_dict.items()):
        # for now, let us not consider OPERA_L3_DIST-ANN-HLS_V1
        if dataset == "OPERA_L3_DIST-ANN-HLS_V1":
            continue
        gdf = data.get("gdf")
        if gdf is None or gdf.empty:
            LOGGER.info(f"Skipping {dataset}: empty or missing GeoDataFrame.")
            continue
        gdf = gdf.copy()
        if i < 4:
            pos_delta = 0.08 * (i - 1)
        else:
            pos_delta = 0.08 * (i - 5)

        feature_group = folium.FeatureGroup(name=dataset)
        gdf = gdf.reset_index(drop=True)
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

                parts = label.split("_")
                if parts[2] == "DISP-S1":
                    aqu_date = datetime.strptime(parts[7], "%Y%m%dT%H%M%SZ")
                else:
                    aqu_date = datetime.strptime(parts[4], "%Y%m%dT%H%M%SZ")
                aqu_date_utc = aqu_date.replace(tzinfo=timezone.utc)

            except Exception as e:
                LOGGER.info(f"Unexpected error: {e}")
                download_url = "URL not available"
                label = "OPERA Granule"

            # get geometry
            geom = gdf.iloc[idx].geometry
            centroid = geom.centroid
            # check if granule is post event
            condition_ok = aqu_date_utc > event_date

            if condition_ok:
                color = colors[i]
                popup_html = f"""
                    <b>{label}</b><br>
                    <a href="{download_url}" target="_blank">
                        Download Granule
                    </a>
                """
                url_value = download_url
                label_value = label
            else:
                product_geom = geom.intersection(AOI_polygon)
                report = check_opera_overpass_intersection(
                    label, product_geom, result_s1, result_s2, result_l,
                    event_date
                )
                color = "lightgray"
                sentences_html = (
                    report.replace("\n", "<br>")
                    if report
                    else "No overpass info available."
                )
                popup_html = f"""
                <b>Not available yet:</b><br>
                {sentences_html}
                """
                url_value = "N/A"
                label_value = f"{label} (old granule)"

            # Update GeoDataFrame
            gdf.at[idx, "URL"] = url_value
            gdf.at[idx, "GranuleUR"] = label_value
            gdf.at[idx, "condition_ok"] = condition_ok

            # Add marker with conditional color
            folium.Marker(
                location=[centroid.y + pos_delta, centroid.x + pos_delta],
                popup=folium.Popup(popup_html, max_width=800),
                icon=folium.Icon(
                    color=color if condition_ok else "lightgray",
                    icon_color="white",
                    icon="cloud-download" if condition_ok else "info-sign",
                ),
            ).add_to(feature_group)

        style_func = style_function_factory(colors[i])
        folium.GeoJson(
            data=json.loads(gdf.to_json()),
            style_function=style_func,
            name=f"{dataset}_geojson",
        ).add_to(feature_group)

        # Add the FeatureGroup to the map
        feature_group.add_to(map_object)
        legend_entries.append((dataset, colors[i]))

    # Add AOI layer last
    folium.GeoJson(
        aoi_geojson,
        name="AOI",
        style_function=lambda x: {"color": "black", "weight": 2, "fillOpacity": 0.0},
    ).add_to(map_object)

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

    # Save and retrun
    map_object.save(output_file)
    LOGGER.info(f"-> DRCS OPERA granules Map successfully saved to {output_file}")
    return map_object


def make_overpasses_map(result_s1, result_s2, result_l, bbox, timestamp_dir):
    """
    Create an interactive map displaying Sentinel
    and Landsat overpasses
    """
    output_file = timestamp_dir / "satellite_overpasses_map.html"

    satellite_results = {
        "Sentinel-1": result_s1,
        "Sentinel-2": result_s2,
        "landsat": result_l,
    }
    satellites = {}
    for name, result in satellite_results.items():
        if result:
            next_collect_info = result.get(
                "next_collect_info", "No collection info available"
            )
            next_collect_geometry = result.get("next_collect_geometry", None)
            satellites[name] = (next_collect_info, next_collect_geometry)

    # Parse AOI center for initial map centering
    bbox = bbox_type(bbox)
    if isinstance(bbox, str):
        geometry = create_polygon_from_kml(bbox)
        AOI = geometry.bounds
        AOI_polygon = geometry
    else:
        lat_min, lat_max, lon_min, lon_max = bbox
        AOI = (lon_min, lat_min, lon_max, lat_max)
        AOI_polygon = box(*AOI)

    center_lat = (AOI[1] + AOI[3]) / 2
    center_lon = (AOI[0] + AOI[2]) / 2
    # Base map
    map_object = folium.Map(location=[center_lat, center_lon], zoom_start=5)
    # Get AOI bounding box
    aoi_geojson = gpd.GeoSeries([AOI_polygon]).__geo_interface__
    for sat_name, (info_text, geometry_list) in satellites.items():
        # Clean and split info
        lines = info_text.split("\n")
        cleaned_info = [line for line in lines if re.search(r"[1-9]", line)]
        info_list = cleaned_info
        num_polygons = len(geometry_list)

        # Generate distinct colors
        colors = hsl_distinct_colors_improved(num_polygons)

        # Handle Landsat 8/9 ascending/descending with different groups
        if "landsat" in sat_name:
            fg_8_asc = folium.FeatureGroup(name=f"{sat_name} 8 Ascending")
            fg_8_desc = folium.FeatureGroup(name=f"{sat_name} 8 Descending")
            fg_9_asc = folium.FeatureGroup(name=f"{sat_name} 9 Ascending")
            fg_9_desc = folium.FeatureGroup(name=f"{sat_name} 9 Descending")

            for i, (polygon, info) in enumerate(zip(geometry_list, info_list)):
                # Determine satellite number (8 or 9) by index or info
                sat_num = 8 if i % 2 == 0 else 9

                # Determine ascending or descending from info text
                if re.search(r"ascending", info, re.IGNORECASE):
                    asc_desc = "Ascending"
                elif re.search(r"descending", info, re.IGNORECASE):
                    asc_desc = "Descending"
                else:
                    asc_desc = "Unknown"

                # Select the correct feature group and color
                if sat_num == 8 and asc_desc == "Ascending":
                    group = fg_8_asc
                    color = "red"
                elif sat_num == 8 and asc_desc == "Descending":
                    group = fg_8_desc
                    color = "darkred"
                elif sat_num == 9 and asc_desc == "Ascending":
                    group = fg_9_asc
                    color = "blue"
                elif sat_num == 9 and asc_desc == "Descending":
                    group = fg_9_desc
                    color = "darkblue"
                else:
                    group = fg_8_asc
                    color = "gray"

                geojson_data = gpd.GeoSeries([polygon]).__geo_interface__
                folium.GeoJson(
                    geojson_data,
                    name=f"{sat_name} Path/Row",
                    style_function=lambda x, color=color: {
                        "color": color,
                        "weight": 2,
                        "fillOpacity": 0.3,
                    },
                    popup=folium.Popup(f"{sat_name}: {info}", max_width=300),
                ).add_to(group)

            # Add all feature groups to map
            fg_8_asc.add_to(map_object)
            fg_8_desc.add_to(map_object)
            fg_9_asc.add_to(map_object)
            fg_9_desc.add_to(map_object)
        else:
            # Other satellites (Sentinel etc.)
            fg = folium.FeatureGroup(name=sat_name)
            for i, (polygon, info) in enumerate(zip(geometry_list, info_list), start=1):
                if isinstance(polygon, Polygon):
                    color = colors[i - 1]
                    geojson_data = gpd.GeoSeries([polygon]).__geo_interface__
                    folium.GeoJson(
                        geojson_data,
                        name=f"{sat_name} Area {i}",
                        style_function=lambda x, color=color: {
                            "color": color,
                            "weight": 2,
                            "fillOpacity": 0.3,
                        },
                        popup=folium.Popup(f"{sat_name}: {info}", max_width=300),
                    ).add_to(fg)
            fg.add_to(map_object)
    # add AOI layercontrol
    folium.GeoJson(
        aoi_geojson,
        name="AOI",
        style_function=lambda x: {"color": "black", "weight": 2, "fillOpacity": 0.0},
    ).add_to(map_object)
    # Add LayerControl to toggle on/off
    folium.LayerControl(collapsed=False).add_to(map_object)
    # Save and retrun
    map_object.save(output_file)
    return map_object
