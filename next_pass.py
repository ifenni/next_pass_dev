#!/usr/bin/env python3

import argparse
import logging
import sys
import datetime
import os

from shapely.geometry import Point, box
from pathlib import Path
from datetime import datetime
from utils import Tee

from landsat_pass import next_landsat_pass
from sentinel_pass import (
    next_sentinel_pass,
    create_s1_collection_plan,
    create_s2_collection_plan,
)
from utils import bbox_type, create_polygon_from_kml
from opera_products import (
    find_print_available_opera_products,
    export_opera_products
)
from plot_maps import (
    make_opera_granule_map,
    make_overpasses_map
)


LOGGER = logging.getLogger("next_pass")

EXAMPLE = """
EXAMPLE USAGE:
Point (lat/lon pair):
  python next_pass.py -b 34.20 -118.17

Bounding Box (SNWE):
  python next_pass.py -b 34.15 34.25 -118.20 -118.15

KML File:
  python next_pass.py -b /path/to/file.kml
"""


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser for CLI inputs."""
    desc = "Find next satellite overpass date."
    parser = argparse.ArgumentParser(
        description=desc,
        epilog=EXAMPLE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-b",
        "--bbox",
        required=True,
        nargs="+",
        type=str,
        help=("Bounding box: Either 2 or 4 floats (point or bbox) "
              "or a path to a .kml location file"),
    )
    parser.add_argument(
        "-s",
        "--sat",
        default="all",
        choices=["sentinel-1", "sentinel-2", "landsat", "all"],
        help="Satellite mission. Default is all.",
    )
    parser.add_argument(
        "-n",
        "--number-of-dates",
        default=5,
        type=int,
        help="Number of most recent dates to consider for OPERA products",
    )
    parser.add_argument(
        "-d",
        "--event-date",
        default="today",
        type=str,
        help="Date of the event to consider for OPERA products",
    )
    parser.add_argument(
        "-f",
        "--functionality",
        default="both",
        type=str,
        help="functionality to run : overpasses or opera_search or both",
    )
    parser.add_argument(
        "-p",
        "--products",
        default=[],
        nargs="*",
        help="A list containing a subset of OPERA products to be searched",
    )
    parser.add_argument(
        "-l",
        "--log-level",
        default="info",
        choices=["debug", "info", "warning", "error"],
        help="Set logging level (default: info).",
    )
    parser.add_argument(
        "--email",
        action="store_true",
        help="Send an email with the results.",
    )
    return parser


def find_next_overpass(args) -> dict:
    """Main logic for finding the next satellite overpasses."""
    bbox = bbox_type(args.bbox)

    if isinstance(bbox, str):
        # create geometry for Sentinel-1 and 2 and point (centroid) for Landsat
        geometry = create_polygon_from_kml(bbox)
        centroid = geometry.centroid
        lat_min = centroid.y
        lon_min = centroid.x
    else:
        lat_min, lat_max, lon_min, lon_max = bbox
        if lat_min == lat_max and lon_min == lon_max:
            geometry = Point(lon_min, lat_min)
        else:
            geometry = box(lon_min, lat_min, lon_max, lat_max)

    if args.sat == "all":
        LOGGER.info("Fetching Sentinel-1 data...")
        sentinel1 = next_sentinel_pass(create_s1_collection_plan, geometry)

        LOGGER.info("Fetching Sentinel-2 data...")
        sentinel2 = next_sentinel_pass(create_s2_collection_plan, geometry)

        LOGGER.info("Fetching Landsat data...")
        landsat = next_landsat_pass(lat_min, lon_min, geometry)

    if args.sat == "sentinel-1":
        LOGGER.info("Fetching Sentinel-1 data...")
        sentinel1 = next_sentinel_pass(create_s1_collection_plan, geometry)
        sentinel2 = []
        landsat = []

    if args.sat == "sentinel-2":
        LOGGER.info("Fetching Sentinel-2 data...")
        sentinel2 = next_sentinel_pass(create_s2_collection_plan, geometry)
        sentinel1 = []
        landsat = []

    if args.sat == "landsat":
        LOGGER.info("Fetching Landsat data...")
        landsat = next_landsat_pass(lat_min, lon_min, geometry)
        sentinel1 = []
        sentinel2 = []

    return {
            "sentinel-1": sentinel1,
            "sentinel-2": sentinel2,
            "landsat": landsat,
        }

    raise ValueError(
        "Satellite not recognized. "
        "Supported values: sentinel-1, sentinel-2, landsat, all."
    )


def format_arg(bbox_arg):
    if (
        isinstance(bbox_arg, list)
        and all(isinstance(x, str) for x in bbox_arg)
        and len(bbox_arg) in [1, 2, 4]
    ):
        return " ".join(bbox_arg)
    else:
        raise ValueError("Argument must be a list of 1, 2, or 4 strings.")

def send_email(subject, body, attachment=None):
    """
    Send an email with the next_pass information.
    :param subject: Subject of the email.
    :param body: Body of the email.
    :param attachment: Optional attachment file path.
    """
    import yagmail

    GMAIL_USER = 'aria.hazards.jpl@gmail.com'
    GMAIL_PSWD = os.environ['GMAIL_APP_PSWD']
    yag = yagmail.SMTP(GMAIL_USER,GMAIL_PSWD)

    receivers = ['cole.speed@jpl.nasa.gov', 'ines.fenni@jpl.nasa.gov', 'emre.havazli@jpl.nasa.gov']
    
    yag.send(
             bcc=receivers,
             subject=subject,
             contents=[body],
             attachments=[attachment]
             )
    return

def run_next_pass(
    bbox,
    number_of_dates=5,
    date=None,
    short_name=None,
    layer_name=None,
    mode="flood",
    output_dir=Path("output")
    ):
    """
    Programmatic entry point for next_pass. Wraps main() and builds CLI-style args.

    Args:
        bbox (list[float]): [south, north, west, east]
        satellites (str): Satellite string, default "all"
        number_of_dates (int): Number of recent dates to consider
        date (str or None): Optional date string (YYYY-MM-DD)
        short_name (str or None): Optional short name to filter OPERA products
        layer_name (str or None): Optional layer name (e.g. "WTR")
        mode (str): Operation mode, e.g., "flood"
        output_dir (Path): Directory to write output to
    """
    cli_args = [
        "-b", *map(str, bbox),
        "-n", str(number_of_dates),
        "-m", mode,
        "-o", str(output_dir)
    ]

    if date:
        cli_args += ["-d", date]
    if short_name:
        cli_args += ["-sn", short_name]
    if layer_name:
        cli_args += ["-l", layer_name]

    return main(cli_args)

def main(cli_args=None):
    """Main entry point."""
    if isinstance(cli_args, argparse.Namespace):
        args = cli_args
    elif cli_args is None:
        args = create_parser().parse_args()
    else:
        args = create_parser().parse_args(cli_args)
        
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Create a timestamp string
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Create the output directory
    timestamp_dir = Path(f"nextpass_outputs_{timestamp}")
    timestamp_dir.mkdir(parents=True, exist_ok=True)
    log_file = timestamp_dir / "run_output.txt"
    log = open(log_file, "w")
    sys.stdout = sys.stderr = Tee(sys.__stdout__, log)
    print(f"Log file created: {log_file}")
    print(f"BBox = {format_arg(args.bbox)}\n")

    if args.functionality in ("both", "overpasses"):
        result = find_next_overpass(args)
        result_s1 = result["sentinel-1"]
        result_s2 = result["sentinel-2"]
        result_l = result["landsat"]
        make_overpasses_map(result_s1, result_s2, result_l,
                            args.bbox, timestamp_dir)
        # loop over results and display only missions that were requested
        for mission, mission_result in result.items():
            if mission_result:
                print(f"\n=== {mission.upper()} ===")
                print(mission_result.get("next_collect_info",
                                         "No collection info available."))

    if args.functionality in ("both", "opera_search"):
        # search for & print OPERA results
        results_opera = find_print_available_opera_products(
                        args.bbox,
                        args.number_of_dates,
                        args.event_date,
                        args.products)
        export_opera_products(results_opera, timestamp_dir)
        make_opera_granule_map(results_opera, args.bbox, timestamp_dir)
        return timestamp_dir

    if args.email:
        overpasses_map = timestamp_dir / "satellite_overpasses_map.html"
        with open(log_file, "r") as f:
            lines = f.readlines()
            email_body = ''.join(lines[4:])
        send_email(
            f"Next Satellite Overpasses for {args.sat.upper()} as of {timestamp} UTC for AOI:{format_arg(args.bbox)}",
            email_body,
            overpasses_map
        )
        print('=========================================')
        print('Alert emailed to recipients.')
        print('=========================================')

if __name__ == "__main__":
    main()
