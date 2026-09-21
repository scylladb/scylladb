import json
import logging
import urllib.request

from ccmlib.common import check_socket_listening
from docker.errors import DockerException

import docker
from tools.docker_utils import container_reload, container_remove, dump_container_logs, get_docker_client, get_ip_address_of_container, running_in_docker
from tools.retrying import retrying

LOGGER = logging.getLogger(__name__)


class FakeGCSDocker:
    def __init__(self, name, image):  # e.g: image="fsouza/fake-gcs-server:1.54.0"
        self.name = name
        self.container = None
        self.port = None
        self.address = None
        self.image = image
        self.access_key = None
        self.secret_key = None

    def __enter__(self):
        self.create_fake_gcs_container()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.remove_container()

    def create_fake_gcs_container(self):
        if self.container:
            raise Exception("fake gcs docker already exists for this instance")
        docker_client = get_docker_client()

        ports = None if running_in_docker() else {"4443/tcp": ("0.0.0.0", None)}

        @retrying(num_attempts=10, sleep_time=1, allowed_exceptions=DockerException, message="start fake gcs container")
        def start_fake_gcs():
            existing_images = docker_client.images.list(filters={"reference": self.image})
            if not existing_images:
                docker_client.images.pull(self.image)

            try:
                self.container = docker_client.containers.run(
                    ports=ports,
                    name=self.name,
                    image=self.image,
                    detach=True,
                    command="-scheme http",
                    labels=["dtest"],
                )
            except DockerException as e:
                LOGGER.error(f"Failed to start fake gcs container: {e}")
                docker_client.containers.get(self.name).remove(force=True)
                raise

        start_fake_gcs()
        container_reload(self.container)

        if running_in_docker():
            self.port = "4443"
            self.address = get_ip_address_of_container(self.container)
        else:
            self.port = self.container.ports["4443/tcp"][0]["HostPort"]
            self.address = "localhost"

        if self.container:
            try:
                check_socket_listening((self.address, int(self.port)), timeout=20)
            except Exception:
                dump_container_logs(self.container)
                raise
            self._update_external_url()

    def _update_external_url(self):
        """Set the external URL via the /_internal/config API so the server
        returns correct URLs in responses."""
        url = f"http://{self.address}:{self.port}/_internal/config"
        data = json.dumps({"externalUrl": self.endpoint_url}).encode()
        req = urllib.request.Request(url, data=data, method="PUT", headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as resp:
            LOGGER.debug("Updated fake-gcs-server external URL to %s (status %s)", self.endpoint_url, resp.status)

    @retrying(num_attempts=10, sleep_time=1, allowed_exceptions=DockerException, message="remove fake gcs container")
    def remove_container(self):
        if self.container:
            container_reload(self.container)
            container_remove(self.container, force=True)
            self.container = None

    @property
    def endpoint_url(self):
        return f"http://{self.address}:{self.port}"
