import os
import re

from sphinx.application import Sphinx
from sphinx.util import logging
from scylladb_common_images import FileDownloader, BaseVersionsTemplateDirective

LOGGER = logging.getLogger(__name__)


class GCPImagesInformationDownloader:

    def run(self, app, exception=None):  
        config = app.config
        base_url = config.scylladb_gcp_images_base_url
        bucket_directory = config.scylladb_gcp_images_bucket_directory
        download_directory = os.path.join(app.builder.srcdir, config.scylladb_gcp_images_download_directory)

        if os.path.exists(download_directory) and os.listdir(download_directory):
            LOGGER.info(f"Files already exist in {download_directory}. Skipping download.")
        else:
            downloader = FileDownloader(base_url)
            downloader.download_files(bucket_directory, download_directory)


class GCPImagesVersionsTemplateDirective(BaseVersionsTemplateDirective):
    FILENAME_REGEX = re.compile(r"gce_image_ids_(\d+(?:\.\d+)?(?:\.\d+)?)(?:.*?)\.csv")
    TEMPLATE = "gcp_image.tmpl"

    def get_download_directory(self, app):
        return os.path.join(app.builder.srcdir, app.config.scylladb_gcp_images_download_directory)

def setup(app: Sphinx):
    app.add_config_value(
        "scylladb_gcp_images_base_url",
        default="https://s3.amazonaws.com/downloads.scylladb.com",
        rebuild="html",
    )
    app.add_config_value(
        "scylladb_gcp_images_bucket_directory",
        default="downloads/scylla/gcp/",
        rebuild="html",
    )
    app.add_config_value(
        "scylladb_gcp_images_download_directory",
        default="_data/opensource/gcp",
        rebuild="html",
    )
    app.connect("builder-inited",  GCPImagesInformationDownloader().run)

    app.add_directive("scylladb_gcp_images_template", GCPImagesVersionsTemplateDirective)
   
    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }