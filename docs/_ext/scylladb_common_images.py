from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import re
import requests

from docutils.parsers.rst import Directive, directives
from sphinxcontrib.datatemplates.directive import DataTemplateCSV
from sphinx.util import logging

LOGGER = logging.getLogger(__name__)

class FileDownloader:
    def __init__(self, base_url, session=None):
        self.base_url = base_url
        self.session = session or requests.Session()

    def get_links(self, bucket_directory, extension):
        url = f"{self.base_url}/?delimiter=/&prefix={bucket_directory}"
        response = self.session.get(url)
        response.raise_for_status()
        return re.findall(rf"<Key>([^<]*\.{extension})</Key>", response.text)

    def download_file(self, link, download_directory):
        file_url = f"{self.base_url}/{link}"
        LOGGER.info(f"Downloading {file_url}")
        file_response = self.session.get(file_url)
        file_response.raise_for_status()

        with open(os.path.join(download_directory, link.split("/")[-1]), "wb") as file:
            file.write(file_response.content)


    def download_files(self, bucket_directory, download_directory, extension="csv"):
        os.makedirs(download_directory, exist_ok=True)

        links = self.get_links(bucket_directory, extension)
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(self.download_file, link, download_directory) for link in links]
            for future in as_completed(futures):
                future.result()  # Handling exceptions can be added here

        LOGGER.info(f"Download complete. The {extension.upper()} files are in {download_directory}")

class BaseTemplateDirective(DataTemplateCSV):
    option_spec = DataTemplateCSV.option_spec.copy()
    option_spec["version"] = lambda x: x
    option_spec["latest"] = lambda x: x

    def _make_context(self, data, config, env):
        context = super()._make_context(data, config, env)
        context["version"] = self.options.get("version")
        context["latest"] = self.options.get("latest")
        return context

    def run(self):
        return super().run()

class BaseVersionsTemplateDirective(Directive):
    # Directives should implement the following variables
    FILENAME_REGEX = re.compile(r".*")
    TEMPLATE = 'template.tmpl'

    has_content = True
    option_spec = {
        "version": directives.unchanged,
        "exclude": directives.unchanged,
        "only_latest": directives.flag,
    }

    def _get_version_pattern(self, app):
        current_version = os.environ.get('SPHINX_MULTIVERSION_NAME', '')
        stable_version = app.config.smv_latest_version
        version_pattern = self._get_current_version(current_version, stable_version)
        return self.options.get("version", "") or version_pattern

    def _get_relative_path(self, download_directory, app, docname):
        current_rst_path = os.path.join(app.builder.srcdir, docname + ".rst")
        return os.path.relpath(download_directory, os.path.dirname(current_rst_path))

    def _filter_files(self, download_directory, version_pattern, exclude_patterns):
        return sorted(
            [file for file in os.listdir(download_directory) if file.endswith('.csv') and 
             self._matches_version(file, version_pattern) and not self._excluded(file, exclude_patterns)],
            key=self._version_key,
            reverse=True
        )

    def _process_file(self, file, relative_path_from_current_rst, is_latest=False):
        data_directive = BaseTemplateDirective(
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
        data_directive.options["template"] = self.TEMPLATE
        data_directive.options["version"] = self._extract_version_from_filename(file)
        data_directive.options["latest"] = is_latest
        return data_directive.run()

    def _get_exclude_patterns(self):
        return self.options.get("exclude", "").split(",")

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

    def _extract_version_from_filename(self, filename):
        match = self.FILENAME_REGEX.search(filename)
        return match.group(1) if match else None

    def _get_current_version(self, current_version, stable_version):
        prefix = 'branch-'
        version = current_version

        if current_version.startswith(prefix):
            version = current_version
        elif not stable_version.startswith(prefix):
            LOGGER.error("Invalid stable_version format '%s' in conf.py. It should start with 'branch-'",
                         stable_version)
        else:
            version = stable_version

        return version.replace(prefix, '')

    def get_download_directory(self, app):
        # Directives should implement the following function
        raise NotImplementedError

    def run(self):
        app = self.state.document.settings.env.app
        docname = self.state.document.settings.env.docname
        version_pattern = self._get_version_pattern(app)
        download_directory = self.get_download_directory(app)
        relative_path_from_current_rst = self._get_relative_path(download_directory, app, docname)

        files = self._filter_files(download_directory, version_pattern, self._get_exclude_patterns())

        if not files:
            LOGGER.warning(f"No files match in directory '{download_directory}' with version pattern '{version_pattern}'.")
            return []

        if "only_latest" in self.options:
            files = [files[0]]

        output = []
        for i, file in enumerate(files):
            is_latest = i == 0
            output.extend(self._process_file(file, relative_path_from_current_rst, is_latest))
        return output

