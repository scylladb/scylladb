import csv
import os
import re

from sphinx.application import Sphinx
from sphinx.util import logging
from scylladb_common_images import FileDownloader, BaseVersionsTemplateDirective

LOGGER = logging.getLogger(__name__)

class CloudFormationProcessor:
    FILENAME_REGEX = r"^.+_(\d+\.\d+\.\d+)_(\w+)(\.yaml)?$"

    def _extract_version_architecture(self, filename):
        match = re.match(self.FILENAME_REGEX, filename)
        if match:
            return match.groups()[:-1]
        return None, None

    @staticmethod
    def _append_to_csv(csv_path, link, architecture):
        with open(csv_path, mode="r") as file:
            data = list(csv.reader(file))

        header = data[0]
        col_name = f"Cluster_{architecture}"
        if col_name not in header:
            header.append(col_name)

        for row in data[1:]:
            row.append(link)

        with open(csv_path, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerows(data)

    def process_files(self, download_directory, links):
        for link in reversed(links):
            version, architecture = self._extract_version_architecture(link)
            if version and architecture:
                matching_csv = next(
                    (f for f in os.listdir(download_directory) if version in f), None
                )

                if matching_csv:
                    csv_path = os.path.join(download_directory, matching_csv)
                    self._append_to_csv(csv_path, link, architecture)
        LOGGER.info("Appended cloudformation information to AWS images CSVs.")


class AMIInformationDownloader:

    def run(self, app, exception=None):  
        config = app.config
        base_url = config.scylladb_aws_images_base_url
        bucket_directory = config.scylladb_aws_images_ami_bucket_directory
        download_directory = os.path.join(app.builder.srcdir, config.scylladb_aws_images_ami_download_directory)
        cloudformation_bucket_directory = config.scylladb_aws_images_cloudformation_bucket_directory

        if os.path.exists(download_directory) and os.listdir(download_directory):
            LOGGER.info(f"Files already exist in {download_directory}. Skipping download.")

        else:
            downloader = FileDownloader(base_url)
            downloader.download_files(bucket_directory, download_directory)
            processor = CloudFormationProcessor()
            links = downloader.get_links(cloudformation_bucket_directory, "yaml")
            processor.process_files(download_directory, links)


class AMIVersionsTemplateDirective(BaseVersionsTemplateDirective):
    FILENAME_REGEX = re.compile(r"ami_ids_(\d+(?:\.\d+)?(?:\.\d+)?)(?:.*?)\.csv")
    TEMPLATE = 'aws_image.tmpl'

    def get_download_directory(self, app):
        return os.path.join(app.builder.srcdir, app.config.scylladb_aws_images_ami_download_directory)

def setup(app: Sphinx):
    app.add_config_value(
        "scylladb_aws_images_base_url",
        default="https://s3.amazonaws.com/downloads.scylladb.com",
        rebuild="html",
    )
    app.add_config_value(
        "scylladb_aws_images_ami_bucket_directory",
        default="downloads/scylla/aws/ami/",
        rebuild="html",
    )
    app.add_config_value(
        "scylladb_aws_images_ami_download_directory",
        default="_data/opensource/aws/ami",
        rebuild="html",
    )
    app.add_config_value(
        "scylladb_aws_images_cloudformation_bucket_directory",
        default="downloads/scylla/aws/cloudformation/",
        rebuild="html",
    )
    app.connect("builder-inited",  AMIInformationDownloader().run)

    app.add_directive("scylladb_aws_images_template", AMIVersionsTemplateDirective)
   
    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }