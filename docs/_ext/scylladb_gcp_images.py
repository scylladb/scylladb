import os
import re

from sphinx.application import Sphinx
from sphinx.util import logging
from scylladb_common_images import FileDownloader, BaseVersionsTemplateDirective

LOGGER = logging.getLogger(__name__)


def merge_arm64_images(download_directory):
    # Release promotion publishes the arm64 image in its own gce_arm64_image_ids_<version>.csv.
    # Append its rows to gce_image_ids_<version>.csv, so each release has one table
    # with both architectures, like the Azure images page.
    # The llms.txt markdown build runs this in a second sphinx process at the same time,
    # so a second merge must change nothing, and readers must never see a half-written file.
    for name in os.listdir(download_directory):
        if not name.startswith("gce_arm64_image_ids_"):
            continue
        arm64_path = os.path.join(download_directory, name)
        x86_path = os.path.join(download_directory, name.replace("gce_arm64_image_ids_", "gce_image_ids_"))
        if not os.path.exists(x86_path):
            LOGGER.warning(f"No x86_64 image file for {name}, not listing it.")
            continue
        with open(x86_path) as x86, open(arm64_path) as arm64:
            rows = x86.read().splitlines()
            new_rows = [row for row in arm64.read().splitlines()[1:] if row not in rows]
        if new_rows:
            # Not named like an image file, so the directive never lists it.
            tmp_path = os.path.join(download_directory, f"merge-{os.getpid()}.tmp")
            with open(tmp_path, "w") as tmp:
                tmp.write("\n".join(rows + new_rows) + "\n")
            os.replace(tmp_path, x86_path)


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
        merge_arm64_images(download_directory)


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
