import json
import logging
import os

import boto3
from botocore.exceptions import ClientError
from cloud_detect import provider
from mypy_boto3_s3 import S3ServiceResource

logger = logging.getLogger(__name__)


class KeyStore:
    KEYSTORE_S3_BUCKET = "scylla-qa-keystore"

    def __init__(self):
        self.s3: S3ServiceResource = boto3.resource("s3")

    def get_file_contents(self, file_name):
        obj = self.s3.Object(self.KEYSTORE_S3_BUCKET, file_name)
        return obj.get()["Body"].read()

    def get_json(self, json_file):
        return json.loads(self.get_file_contents(json_file))

    def download_file(self, filename, dest_filename):
        logger.debug(f"Downloading file '{filename}'")
        obj = self.s3.Object(self.KEYSTORE_S3_BUCKET, filename)
        with open(str(dest_filename), "w") as file_obj:
            file_obj.write(obj.get()["Body"].read().decode())

    def get_elasticsearch_credentials(self):
        return self.get_json("es.json")

    def get_elasticsearch_token(self):
        return self.get_json("es_token.json")

    def get_email_credentials(self):
        return self.get_json("email_config.json")

    def get_docker_hub_credentials(self):
        return self.get_json("docker.json")

    def get_argus_rest_credentials_per_provider(self, cloud_provider: str | None = None):
        """
        Retrieve Argus REST credentials for the specified cloud provider.

        Args:
            cloud_provider (str | None): The name of the cloud provider. If None, the provider is auto-detected
                using the `provider(timeout=0.5)` function.

        Behavior:
            - If running in Jenkins (detected by the presence of the "JOB_NAME" environment variable), attempts to
              fetch credentials from a provider-specific JSON file named "argus_rest_credentials_{cloud_provider}.json".
            - If the provider-specific file does not exist (raises a ClientError with "NoSuchKey"), falls back to
              the generic "argus_rest_credentials.json" file.
            - If not running in Jenkins, always fetches credentials from the generic file.

        Exceptions:
            - May raise exceptions from `get_json` if the credentials file cannot be read or parsed.
            - Re-raises any ClientError except for "NoSuchKey" when attempting to fetch the provider-specific file.

        Returns:
            dict: The credentials loaded from the appropriate JSON file.
        """
        cloud_provider = cloud_provider or provider(timeout=0.5)

        if os.environ.get("JOB_NAME"):  # we are in Jenkins
            try:
                configuration_name = f"argus_rest_credentials_{cloud_provider}.json"
                logging.warning(f"Trying to get Argus REST credentials for cloud provider '{cloud_provider}' from '{configuration_name}'")
                return self.get_json(configuration_name)
            except ClientError as e:
                if not e.response["Error"]["Code"] == "NoSuchKey":
                    raise

        return self.get_json("argus_rest_credentials.json")

    def get_jira_credentials(self):
        return self.get_json("scylladb_jira.json")
