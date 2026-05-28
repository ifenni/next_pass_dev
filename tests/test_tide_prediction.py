from __future__ import annotations

import json

import utils.tide_prediction as tide_prediction


def test_ensure_station_cache_defaults_to_scratch(monkeypatch, tmp_path):
    payload = {"stations": [{"id": "9432780", "name": "LA", "lat": "34.0", "lng": "-118.0"}]}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return payload

    monkeypatch.setattr(tide_prediction, "SCRATCH_DIR", tmp_path)
    monkeypatch.setattr(tide_prediction.requests, "get", lambda *args, **kwargs: FakeResponse())
    monkeypatch.setattr(tide_prediction, "_STATIONS_CACHE", None)

    tide_prediction.ensure_station_cache()

    cache_path = tmp_path / "noaa_stations.json"
    assert cache_path.exists()
    assert json.loads(cache_path.read_text(encoding="utf-8")) == payload
