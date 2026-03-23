from __future__ import annotations

import argparse
import datetime as dt
import json

import pytest

import utils.utils as utils_mod

from tests.helpers import FakeGeometry


def test_bbox_type_accepts_point_and_bbox():
    assert utils_mod.bbox_type(["34.2", "-118.17"]) == (34.2, 34.2, -118.17, -118.17)
    assert utils_mod.bbox_type(["34.1", "34.3", "-118.2", "-118.0"]) == (34.1, 34.3, -118.2, -118.0)


def test_bbox_type_swaps_reversed_bounds():
    assert utils_mod.bbox_type(["34.3", "34.1", "-118.0", "-118.2"]) == (34.1, 34.3, -118.2, -118.0)


def test_bbox_type_accepts_wkt_url_and_local_file(tmp_path):
    geojson_path = tmp_path / "aoi.geojson"
    geojson_path.write_text("{}", encoding="utf-8")

    assert utils_mod.bbox_type("POINT (-118 34)") == "POINT (-118 34)"
    assert utils_mod.bbox_type("https://example.com/aoi.geojson") == "https://example.com/aoi.geojson"
    assert utils_mod.bbox_type([str(geojson_path)]) == str(geojson_path)


def test_bbox_type_rejects_invalid_inputs():
    with pytest.raises(argparse.ArgumentTypeError):
        utils_mod.bbox_type(["91", "0"])

    with pytest.raises(argparse.ArgumentTypeError):
        utils_mod.bbox_type(["34", "35", "-118"])


def test_bbox_to_geometry_builds_point_and_polygon(tmp_path):
    point, _, centroid = utils_mod.bbox_to_geometry((34.2, 34.2, -118.17, -118.17), tmp_path)
    polygon, _, polygon_centroid = utils_mod.bbox_to_geometry((34.1, 34.3, -118.2, -118.0), tmp_path)

    assert point.geom_type == "Point"
    assert centroid.x == -118.17
    assert polygon.geom_type == "Polygon"
    assert round(polygon_centroid.y, 2) in {34.18, 34.20}


def test_bbox_to_geometry_loads_wkt_and_downloaded_url(monkeypatch, tmp_path):
    geojson_path = tmp_path / "AOI_from_url.geojson"
    geojson_path.write_text(json.dumps({"type": "Point", "coordinates": [1, 2]}), encoding="utf-8")

    monkeypatch.setattr(utils_mod, "download_url_to_file", lambda url, out: geojson_path)

    wkt_geom, _, _ = utils_mod.bbox_to_geometry("POINT (1 2)", tmp_path)
    url_geom, _, _ = utils_mod.bbox_to_geometry("https://example.com/aoi.geojson", tmp_path)

    assert wkt_geom.geom_type == "Point"
    assert url_geom.geom_type == "Point"
    assert url_geom.x == 1


def test_download_url_to_file_ensures_geojson_suffix_and_validates_json(monkeypatch, tmp_path):
    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"type": "Point", "coordinates": [1, 2]}

    monkeypatch.setattr(utils_mod.requests, "get", lambda url, timeout=30: FakeResponse())

    output = utils_mod.download_url_to_file("https://example.com/aoi", tmp_path / "aoi")

    assert output.suffix == ".geojson"
    assert json.loads(output.read_text(encoding="utf-8"))["type"] == "Point"

    class BadResponse(FakeResponse):
        def json(self):
            raise ValueError("bad json")

    monkeypatch.setattr(utils_mod.requests, "get", lambda url, timeout=30: BadResponse())

    with pytest.raises(ValueError):
        utils_mod.download_url_to_file("https://example.com/aoi", tmp_path / "bad.geojson")


def test_geometry_from_file_reads_geojson_feature_and_feature_collection(tmp_path):
    feature_path = tmp_path / "feature.geojson"
    feature_path.write_text(
        json.dumps({"type": "Feature", "geometry": {"type": "Point", "coordinates": [3, 4]}}),
        encoding="utf-8",
    )

    fc_path = tmp_path / "collection.geojson"
    fc_path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [{"geometry": {"type": "Point", "coordinates": [5, 6]}}],
            }
        ),
        encoding="utf-8",
    )

    feature_geom = utils_mod.geometry_from_file(feature_path)
    collection_geom = utils_mod.geometry_from_file(fc_path)

    assert feature_geom.x == 3
    assert collection_geom.x == 5


def test_is_date_in_text_handles_millis_and_plain_seconds():
    assert utils_mod.is_date_in_text("2025-10-21T22:39:01.066Z", "event on 2025-10-21")
    assert utils_mod.is_date_in_text("2025-10-10T04:41:14Z", "window 2025-10-10 and later")
    assert not utils_mod.is_date_in_text("2025-10-10T04:41:14Z", "window 2025-10-11")


def test_style_function_factory_and_valid_drcs_datetime():
    style = utils_mod.style_function_factory("red", inactive_color="gray")
    assert style({"properties": {"condition_ok": True}})["color"] == "red"
    assert style({"properties": {"condition_ok": False}})["color"] == "gray"

    parsed = utils_mod.valid_drcs_datetime("2026-03-23T10:00")
    assert parsed.year == 2026
    assert parsed.tzinfo is not None

    with pytest.raises(argparse.ArgumentTypeError):
        utils_mod.valid_drcs_datetime("2026/03/23 10:00")


def test_scrape_esa_download_urls_normalizes_malformed_links(monkeypatch):
    class FakeResponse:
        text = """
        <html>
          <div class='sentinel-1a'>
            <a href='https://sentinel/path-1.kml'>one</a>
            <a href='/path-2.kml'>two</a>
          </div>
        </html>
        """

        def raise_for_status(self):
            return None

    monkeypatch.setattr(utils_mod.requests, "get", lambda url: FakeResponse())

    urls = utils_mod.scrape_esa_download_urls("https://example.com", "sentinel-1a")

    assert urls == [
        "https://sentinels.copernicus.eu/path-1.kml",
        "https://sentinels.copernicus.eu/path-2.kml",
    ]


def test_get_spatial_extent_km_uses_projected_bounds(monkeypatch):
    class FakeArea:
        def sum(self):
            return 5_000_000

    class FakeProjectedFrame:
        total_bounds = [0, 0, 2000, 3000]

        class _Geom:
            area = FakeArea()

        geometry = _Geom()

    class FakeFrame:
        def __init__(self, geometry=None, crs=None):
            self.geometry = geometry

        def to_crs(self, epsg=None):
            return FakeProjectedFrame()

    monkeypatch.setattr(utils_mod.gpd, "GeoDataFrame", FakeFrame)
    monkeypatch.setattr(utils_mod, "shape", lambda geojson: FakeGeometry(area=10.0))

    result = utils_mod.get_spatial_extent_km({"type": "Polygon", "coordinates": []})

    assert result == {"width_km": 2.0, "height_km": 3.0, "area_km2": 5.0}
