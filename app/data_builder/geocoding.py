from __future__ import annotations

import logging
from typing import Any

import requests

LOGGER = logging.getLogger(__name__)


def fetch_geocode_candidates(
    address_freeform: str,
    api_key: str,
    region: str = "es",
    language: str = "es",
    timeout_sec: int = 12,
) -> list[dict[str, Any]]:
    if not address_freeform or not api_key:
        return []

    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": address_freeform,
        "key": api_key,
        "region": region,
        "language": language,
        "components": "country:ES",
    }
    try:
        response = requests.get(url, params=params, timeout=timeout_sec)
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") not in {"OK", "ZERO_RESULTS"}:
            LOGGER.warning("Google Geocoding status=%s error=%s", payload.get("status"), payload.get("error_message"))
            return []
        return payload.get("results", [])
    except Exception:
        LOGGER.exception("Failed requesting Google Geocoding API.")
        return []
