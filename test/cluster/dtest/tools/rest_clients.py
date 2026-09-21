import logging

import requests
from ccmlib.node import Node
from requests import Response
from requests.exceptions import ConnectionError

logger = logging.getLogger(__name__)


class StorageServiceClient:
    def __init__(self, node: Node):
        self._node = node
        self._endpoint_url = f"http://{self._node.address()}:10000/storage_service/"

    def scrub_ks_cf(self, keyspace: str, cf: str | None, scrub_mode: str | None = None, **kwargs) -> Response:
        params = {"cf": cf} if cf else {}
        path = f"keyspace_scrub/{keyspace}"

        if scrub_mode:
            params.update({"scrub_mode": scrub_mode})

        response = requests.get(url=self._full_url(path), params=params)
        logger.debug(f"Request url: {response.request.url}")

        return response

    def cleanup_ks_cf(self, keyspace: str, cf: str | None) -> Response:
        params = {"cf": cf} if cf else {}
        path = f"keyspace_cleanup/{keyspace}"

        response = requests.post(url=self._full_url(path), params=params)
        logger.debug(f"Request url: {response.request.url}")

        return response

    def upgrade_sstables(self, keyspace: str = "ks", cf: str = "cf") -> Response:
        params = {"cf": cf} if cf else {}
        path = f"keyspace_upgrade_sstables/{keyspace}"
        response = requests.get(url=self._full_url(path), params=params)

        return response

    def compact_ks_cf(self, keyspace: str, cf: str) -> Response:
        params = {"cf": cf} if cf else {}
        path = f"keyspace_compaction/{keyspace}"

        logger.debug("Making REST API request to: %s with params: %s", self._full_url(path), params)
        response = requests.post(url=self._full_url(path), params=params)
        return response

    def _full_url(self, path: str):
        return f"{self._endpoint_url}{path}"


class SystemServiceClient:
    def __init__(self, node: Node):
        self._node = node
        self._endpoint_url = f"http://{self._node.address()}:{self._node.api_port}/system"

    def get_highest_supported_sstable_version(self, timeout=30):
        try:
            url_path = f"{self._endpoint_url}/highest_supported_sstable_version"
            response = requests.get(url=url_path, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except ConnectionError as e:
            raise ConnectionError(f"Failed to connect to the endpoint {e}")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to get highest_supported_sstable_version using {url_path}: {e}")
        return None

    def get_chosen_sstable_version(self, timeout=30):
        try:
            url_path = f"{self._endpoint_url}/chosen_sstable_version"
            response = requests.get(url=url_path, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except ConnectionError as e:
            raise ConnectionError(f"Failed to connect to the endpoint {e}")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to get chosen_sstable_version using {url_path}: {e}")
        return None
