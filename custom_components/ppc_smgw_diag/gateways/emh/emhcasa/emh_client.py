from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx
from obis_parser import OBIS

from custom_components.ppc_smgw_diag.gateways.reading import Information, Reading

from ..const import DEFAULT_MODEL, DEFAULT_NAME, MANUFACTURER


class EMHCasaClient:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        httpx_client: httpx.AsyncClient,
        logger,
        meter_id: str | None = None,
    ):
        if not base_url.startswith(("http://", "https://")):
            base_url = f"https://{base_url}"
        if base_url.endswith("/") and not base_url.endswith("://"):
            base_url = base_url[:-1]
        self.base_url = base_url
        self.username = username
        self.password = password
        self.meter_id: str | None = meter_id or None

        self.httpx_client = httpx_client
        self.logger = logger
        self._metadata_probe_done = False

        self.httpx_client.headers.setdefault("Content-Type", "application/json")
        self.httpx_client.follow_redirects = True

    def _get_auth(self) -> httpx.DigestAuth:
        return httpx.DigestAuth(self.username, self.password)

    async def get_data(self) -> Information:
        firmware_version = await self._probe_metadata_endpoints()
        information = Information(
            name=DEFAULT_NAME,
            model=DEFAULT_MODEL,
            manufacturer=MANUFACTURER,
            firmware_version=firmware_version or "Unknown",
            last_update=datetime.now(UTC),
            readings=await self._get_readings(),
        )

        self.logger.debug(f"Returning information: {information}")

        return information

    async def _probe_metadata_endpoints(self) -> str | None:
        """Probe likely EMH metadata routes once per client instance."""
        if self._metadata_probe_done:
            return None

        self._metadata_probe_done = True
        candidates = (
            "/",
            "/json/",
            "/json/info/",
            "/json/device/",
            "/json/system/",
            "/json/firmware/",
            "/json/version/",
        )

        for path in candidates:
            url = f"{self.base_url}{path}"
            try:
                response = await self.httpx_client.get(
                    url,
                    auth=self._get_auth(),
                    timeout=10,
                )
                self.logger.debug(
                    "SMGW probe response: method=GET url=%s status=%s content_type=%s body=%r",
                    url,
                    response.status_code,
                    response.headers.get("content-type"),
                    response.text,
                )
                if response.status_code != 200:
                    continue

                try:
                    payload = response.json()
                except ValueError:
                    continue

                firmware = self._find_firmware_value(payload)
                if firmware:
                    self.logger.info(
                        "Discovered EMH firmware value from %s: %s", url, firmware
                    )
                    return firmware
            except Exception as err:
                self.logger.debug("SMGW probe failed: url=%s error=%s", url, err)

        return None

    @classmethod
    def _find_firmware_value(cls, payload: Any) -> str | None:
        """Find common firmware/version keys in an unknown JSON shape."""
        if isinstance(payload, dict):
            for key, value in payload.items():
                normalized_key = str(key).lower().replace("-", "_")
                if normalized_key in {
                    "firmware",
                    "firmware_version",
                    "software_version",
                    "version",
                } and isinstance(value, (str, int, float)):
                    return str(value)

                found = cls._find_firmware_value(value)
                if found:
                    return found
        elif isinstance(payload, list):
            for item in payload:
                found = cls._find_firmware_value(item)
                if found:
                    return found

        return None

    async def discover_all_meter_ids(self) -> list[str]:
        """Return all meter IDs available on this gateway via /json/metering/origin/."""
        url = f"{self.base_url}/json/metering/origin/"
        self.logger.debug("Discovering all meter IDs from %s", url)

        try:
            response = await self.httpx_client.get(
                url,
                auth=self._get_auth(),
                timeout=10,
            )
            self.logger.debug(
                "SMGW response: method=GET url=%s status=%s content_type=%s body=%r",
                url,
                response.status_code,
                response.headers.get("content-type"),
                response.text,
            )
            meter_ids: list[str] = response.json()
        except Exception as e:
            self.logger.error(f"Failed to fetch meter list: {e}")
            return []

        self.logger.debug(f"Discovered meter IDs: {meter_ids}")
        return meter_ids

    async def _discover_meter_id(self) -> str | None:
        meter_ids = await self.discover_all_meter_ids()
        if meter_ids:
            return meter_ids[0]
        self.logger.error("No meter ID found")
        return None

    async def _get_readings(self) -> dict[OBIS, Reading]:
        self.logger.debug("Getting readings from %s", self.base_url)

        if self.meter_id is None:
            self.meter_id = await self._discover_meter_id()
            if self.meter_id is None:
                self.logger.error("Could not discover meter ID")
                return {}

        try:
            url = f"{self.base_url}/json/metering/origin/{self.meter_id}/extended"
            response = await self.httpx_client.get(
                url,
                auth=self._get_auth(),
                timeout=10,
            )
            self.logger.debug(
                "SMGW response: method=GET url=%s status=%s content_type=%s body=%r",
                url,
                response.status_code,
                response.headers.get("content-type"),
                response.text,
            )
            meter_reading = response.json()
        except Exception as e:
            self.logger.error(f"Failed to fetch meter readings: {e}")
            return {}

        readings: dict[OBIS, Reading] = {}
        now = datetime.now(UTC)

        for meter_value in meter_reading.get("values", []):
            obis_obj = OBIS.parse(meter_value.get("logical_name", ""))
            if obis_obj is None:
                continue

            # Scale value and convert Wh (unit 30) to kWh
            scaler = meter_value.get("scaler", 0)
            unit = meter_value.get("unit", 0)
            value = float(meter_value["value"]) * (10**scaler)
            if unit == 30:
                value /= 1000

            readings[obis_obj] = Reading(
                value=value,
                timestamp=now,
                obis=obis_obj,
            )

        self.logger.debug(f"Parsed {len(readings)} readings: {list(readings.keys())}")
        return readings
