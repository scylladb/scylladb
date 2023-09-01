import csv
import os
import re
import requests

from sphinx.application import Sphinx
from docutils.parsers.rst import Directive, directives
from sphinxcontrib.datatemplates.directive import DataTemplateCSV
from sphinx.util import logging

LOGGER = logging.getLogger(__name__)

class AWSFileDownloader:
    def __init__(self, base_url, session=None):
        self.base_url = base_url
        self.session = session or requests.Session()

    def get_links(self, bucket_directory, extension):
        url = f"{self.base_url}/?delimiter=/&prefix={bucket_directory}"
        response = self.session.get(url)
        response.raise_for_status()
        return re.findall(rf"<Key>([^<]*\.{extension})</Key>", response.text)

    def download_files(self, bucket_directory, download_directory, extension="csv"):
        os.makedirs(download_directory, exist_ok=True)

        links = self.get_links(bucket_directory, extension)
        for link in links:
            file_url = f"{self.base_url}/{link}"
            print(f"Downloading {file_url}")
            file_response = self.session.get(file_url)
            file_response.raise_for_status()

            with open(os.path.join(download_directory, link.split("/")[-1]), "wb") as file:
                file.write(file_response.content)

        print(f"Download complete. The {extension.upper()} files are in {download_directory}")


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
        print("Appended cloudformation information to AWS images CSVs.")


class AMIInformationDownloader:

    def run(self, app, exception=None):  
        config = app.config
        base_url = config.scylladb_aws_images_base_url
        ami_bucket_directory = config.scylladb_aws_images_ami_bucket_directory
        ami_download_directory = os.path.join(app.builder.srcdir, config.scylladb_aws_images_ami_download_directory)
        cloudformation_bucket_directory = config.scylladb_aws_images_cloudformation_bucket_directory

        if os.path.exists(ami_download_directory) and os.listdir(ami_download_directory):
            print(f"Files already exist in {ami_download_directory}. Skipping download.")
            return

        downloader = AWSFileDownloader(base_url)
        downloader.download_files(ami_bucket_directory, ami_download_directory)

        processor = CloudFormationProcessor()
        links = downloader.get_links(cloudformation_bucket_directory, "yaml")
        processor.process_files(ami_download_directory, links)


class AMITemplateDirective(DataTemplateCSV):
    option_spec = DataTemplateCSV.option_spec.copy()
    option_spec["version"] = lambda x: x

    def _make_context(self, data, config, env):
        context = super()._make_context(data, config, env)
        context["version"] = self.options.get("version")
        return context

    def run(self):
        return super().run()


class AMIVersionsTemplateDirective(Directive):
    FILENAME_REGEX = re.compile(r"ami_ids_(\d+(?:\.\d+)?(?:\.\d+)?)(?:.*?)\.csv")

    has_content = True
    option_spec = {
        "version": directives.unchanged,
        "exclude": directives.unchanged,
    }

    def _extract_version_from_filename(self, filename):
        match = self.FILENAME_REGEX.search(filename)
        return match.group(1) if match else None

    def _matches_version(self, filename, version):
        if not version:
            return True

        file_version = self._extract_version_from_filename(filename)
        if not file_version:
            return False

        if "." in version:
            return file_version.startswith(version)

        return file_version.split(".")[0] == version

    def _excluded(self, filename, patterns):
        return any(pattern in filename for pattern in patterns if pattern)

    def _version_key(self, filename):
        version = self._extract_version_from_filename(filename)
        return tuple(map(int, version.split("."))) if version else (0,)


    def run(self):
        app = self.state.document.settings.env.app
        version_pattern = self.options.get("version", "")
        exclude_patterns = self.options.get("exclude", "").split(",")

        download_directory = os.path.join(
            app.builder.srcdir, app.config.scylladb_aws_images_ami_download_directory
        )
        docname = self.state.document.settings.env.docname
        current_rst_path = os.path.join(app.builder.srcdir, docname + ".rst")
        current_rst_dir = os.path.dirname(current_rst_path)
        relative_path_from_current_rst = os.path.relpath(
            download_directory, current_rst_dir
        )

        files = sorted([
            file for file in os.listdir(download_directory) if file.endswith('.csv') and 
            self._matches_version(file, version_pattern) and not self._excluded(file, exclude_patterns)
        ], key=self._version_key, reverse=True)

        if len(files) == 0:
            LOGGER.warning(
                f"No files match in directory '{download_directory}' with version pattern '{version_pattern}'."
            )

        output = []
        for file in files:
            data_directive = AMITemplateDirective(
                name=self.name,
                arguments=[os.path.join(relative_path_from_current_rst, file)],
                options=self.options,
                content=self.content,
                lineno=self.lineno,
                content_offset=self.content_offset,
                block_text=self.block_text,
                state=self.state,
                state_machine=self.state_machine,
            )
            data_directive.options["template"] = "aws_image.tmpl"
            data_directive.options["version"] = self._extract_version_from_filename(
                file
            )
            output.extend(data_directive.run())
        return output


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
