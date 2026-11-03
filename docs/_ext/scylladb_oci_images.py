# Copyright (C) 2026 ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import os
import re

from sphinx.application import Sphinx
from sphinx.util import logging

from scylladb_common_images import BaseVersionsTemplateDirective, FileDownloader

LOGGER = logging.getLogger(__name__)


class OCIImagesInformationDownloader:
    def run(self, app, exception=None):
        if app.tags.has("opensource"):
            return

        config = app.config
        download_directory = os.path.join(app.builder.srcdir, config.scylladb_oci_images_download_directory)

        if os.path.exists(download_directory) and os.listdir(download_directory):
            LOGGER.info(f"Files already exist in {download_directory}. Skipping download.")
        else:
            downloader = FileDownloader(config.scylladb_oci_images_base_url)
            downloader.download_files(config.scylladb_oci_images_bucket_directory, download_directory)


class OCIImagesVersionsTemplateDirective(BaseVersionsTemplateDirective):
    FILENAME_REGEX = re.compile(r"oci_image_ids_(\d+(?:\.\d+)?(?:\.\d+)?)(?:.*?)\.csv")
    TEMPLATE = "oci_image.tmpl"

    def get_download_directory(self, app):
        return os.path.join(app.builder.srcdir, app.config.scylladb_oci_images_download_directory)

    def run(self):
        # Sphinx parses directives inside ``only`` blocks before filtering by tag.
        if self.state.document.settings.env.app.tags.has("opensource"):
            return []
        return super().run()


def setup(app: Sphinx):
    app.add_config_value(
        "scylladb_oci_images_base_url",
        default="https://s3.amazonaws.com/downloads.scylladb.com",
        rebuild="html",
    )
    app.add_config_value(
        "scylladb_oci_images_bucket_directory",
        default="downloads/scylla/oci/",
        rebuild="html",
    )
    app.add_config_value(
        "scylladb_oci_images_download_directory",
        default="_data/opensource/oci",
        rebuild="html",
    )
    app.connect("builder-inited", OCIImagesInformationDownloader().run)
    app.add_directive("scylladb_oci_images_template", OCIImagesVersionsTemplateDirective)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
