import os
import re

from sphinx.application import Sphinx
from sphinx.util import logging
from scylladb_common_images import FileDownloader, BaseVersionsTemplateDirective

LOGGER = logging.getLogger(__name__)

class AzureImagesInformationDownloader:

    def run(self, app, exception=None):  
        config = app.config
        base_url = config.scylladb_azure_images_base_url
        bucket_directory = config.scylladb_azure_images_ami_bucket_directory
        download_directory = os.path.join(app.builder.srcdir, config.scylladb_azure_images_download_directory)
        if os.path.exists(download_directory) and os.listdir(download_directory):
            LOGGER.info(f"Files already exist in {download_directory}. Skipping download.")
        else:
            downloader = FileDownloader(base_url)
            downloader.download_files(bucket_directory, download_directory)

class AzureImagesVersionsTemplateDirective(BaseVersionsTemplateDirective):
    FILENAME_REGEX = re.compile(r"azure_image_ids_(\d+(?:\.\d+)?(?:\.\d+)?)(?:.*?)\.csv")
    TEMPLATE = "azure_image.tmpl"

    def get_download_directory(self, app):
        return os.path.join(app.builder.srcdir, app.config.scylladb_azure_images_download_directory)

def setup(app: Sphinx):
    app.add_config_value(
        "scylladb_azure_images_base_url",
        default="https://s3.amazonaws.com/downloads.scylladb.com",
        rebuild="html",
    )
    app.add_config_value(
        "scylladb_azure_images_ami_bucket_directory",
        default="downloads/scylla/azure/",
        rebuild="html",
    )
    app.add_config_value(
        "scylladb_azure_images_download_directory",
        default="_data/opensource/azure",
        rebuild="html",
    )
    app.connect("builder-inited",  AzureImagesInformationDownloader().run)

    app.add_directive("scylladb_azure_images_template", AzureImagesVersionsTemplateDirective)
   
    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }