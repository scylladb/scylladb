import logging
import os

from ccmlib.common import check_socket_listening
from docker.errors import DockerException

import docker
from tools.docker_utils import container_reload, container_remove, dump_container_logs, get_docker_client, get_ip_address_of_container, running_in_docker
from tools.retrying import retrying

LOGGER = logging.getLogger(__name__)


class MinioDocker:
    def __init__(self, name, image):  # e.g: image="docker.io/pgsty/silo:RELEASE.2026-09-16T00-00-00Z"
        self.name = name
        self.container = None
        self.port = None
        self.address = None
        self.image = image
        self.access_key = os.getenv("AWS_ACCESS_KEY_ID", "test1")
        self.secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "12345678")

    def __enter__(self):
        self.create_minio_container()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.remove_container()

    def create_minio_container(self):
        if self.container:
            raise Exception("minio docker already exists for this instance")
        docker_client = get_docker_client()

        ports = None if running_in_docker() else {"9000/tcp": ("0.0.0.0", None)}

        @retrying(num_attempts=10, sleep_time=1, allowed_exceptions=DockerException, message="start minio container")
        def start_minio():
            existing_images = docker_client.images.list(filters={"reference": self.image})
            if not existing_images:
                docker_client.images.pull(self.image)

            try:
                self.container = docker_client.containers.run(
                    ports=ports,
                    name=self.name,
                    environment=[f"MINIO_ACCESS_KEY={self.access_key}", f"MINIO_SECRET_KEY={self.secret_key}"],
                    image=self.image,
                    detach=True,
                    command="server /data",
                    labels=["dtest"],
                )
            except DockerException as e:
                LOGGER.error(f"Failed to start minio container: {e}")
                docker_client.containers.get(self.name).remove(force=True)
                raise

        start_minio()
        container_reload(self.container)

        if running_in_docker():
            self.port = "9000"
            self.address = get_ip_address_of_container(self.container)
        else:
            self.port = self.container.ports["9000/tcp"][0]["HostPort"]
            self.address = "localhost"

        if self.container:
            try:
                check_socket_listening((self.address, int(self.port)), timeout=20)
            except Exception:
                dump_container_logs(self.container)
                raise

    @retrying(num_attempts=10, sleep_time=1, allowed_exceptions=DockerException, message="remove minio container")
    def remove_container(self):
        if self.container:
            container_reload(self.container)
            container_remove(self.container, force=True)
            self.container = None

    @property
    def endpoint_url(self):
        return f"http://{self.address}:{self.port}"


if __name__ == "__main__":
    import uuid

    import boto3

    with MinioDocker(name=f"test{str(uuid.uuid4())[:8]}") as minio:
        client = boto3.client(service_name="s3", aws_access_key_id=minio.access_key, aws_secret_access_key=minio.secret_key, endpoint_url=minio.endpoint_url)

        client.create_bucket(Bucket="test1")
