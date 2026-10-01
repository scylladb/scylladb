#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import boto3
import logging
import urllib.error
import urllib.request
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from packaging.version import Version
from pathlib import Path
import time
import re
import subprocess
import fcntl
import shutil
import tarfile
import os
from contextlib import contextmanager

from typing import Optional, List, Iterator

logger = logging.getLogger(__name__)


def _release_entry_pattern(pack: str = "") -> re.Pattern:
    """
    Return a regex matching Scylla/ScyllaDB release directory or archive names
    of the requested package type (`pack`).
    """
    pack_part = rf"{re.escape(pack)}-" if pack else ""
    return re.compile(rf"^scylla(?:db)?-{pack_part}\d{{4}}\.\d+(?:\.\d+)?(?:/|$|[~.-])")


def _list_scylla_release_entries(bucket: str, prefix: str, pack: str = "") -> list[str]:
    """
    List Scylla release entries directly under an S3 prefix.
    The S3 listing is paginated and limited to one prefix level. Returned entries
    have the requested prefix and trailing slash removed, and are filtered to
    Scylla/ScyllaDB release names matching the requested package type (`pack`).
    """
    if not prefix.endswith("/"):
        prefix = f"{prefix}/"

    pattern = _release_entry_pattern(pack)

    # Connecting to S3 normally takes well under a second. Note that each
    # attempt tries all of the endpoint's addresses (usually 8) in turn, each
    # with connect_timeout. Keeping the worst case well below the test's
    # timeout lets fetch_and_install_scylla_version() fall back to a cached
    # version when S3 is unreachable, instead of the test timing out.
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(
        signature_version=UNSIGNED,
        connect_timeout=10,
        read_timeout=60,
        retries={"max_attempts": 10, "mode": "adaptive"},
    ))

    files = []
    folders = []
    continuation_token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix, "Delimiter": "/"}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
        resp = s3.list_objects_v2(**kwargs)
        files += resp.get("Contents", [])
        folders += resp.get("CommonPrefixes", [])
        if not resp.get("IsTruncated"):
            break
        continuation_token = resp.get("NextContinuationToken")
        if not continuation_token:
            break

    objs = [f["Key"].removeprefix(prefix) for f in files] if files else [f["Prefix"].removeprefix(prefix) for f in folders]
    return [o.rstrip("/") for o in objs if pattern.match(o)]


def _version_key(v: str, pack: str = "") -> str:
    """
    Extract the sortable version component from a Scylla release entry or archive name.
    The `pack` argument identifies the archive-name structure, so the function knows
    where to find the version component: after `scylla-` for default archives, or
    after `scylla-<pack>-` for package variants such as `dev` and `debug`.
    For example:
    `scylla-2026.1.0-...` with `pack=""` returns `2026.1.0-...`.
    `scylla-dev-2026.1.0-...` with `pack="dev"` returns `2026.1.0-...`.
    `scylla-debug-2026.1.0-...` with `pack="debug"` returns `2026.1.0-...`.
    The returned version is normalized for `packaging.version.Version` sorting by
    converting release-candidate versions from `~rc` to `rc`.
    """
    if not pack:
        v = v.rstrip("/").split("-", 2)[1]  # name-version-postfix or name-version
    else:
        v = v.rstrip("/").split("-", 3)[2]  # name-pack-postfix    
    v = v.replace("~rc", "rc")
    return v


def _version_matches(v: str, match_pattern: str) -> bool:
    return v == match_pattern or v.startswith(f"{match_pattern}.") or v.startswith(f"{match_pattern}rc")


def _sorted_vers(objs: List[str], match_pattern: Optional[str] = None, pack: str = None) -> List[str]:
    sorted_vers = [_version_key(o, pack) for o in objs]
    if match_pattern:
        sorted_vers = [v for v in sorted_vers if _version_matches(v, match_pattern)]
    sorted_vers.sort(key=Version)
    return sorted_vers


def _get_latest_ver(objs: List[str], match_pattern: Optional[str] = None, pack: str = None) -> Optional[str]:
    sorted_vers = _sorted_vers(objs, match_pattern, pack)
    return sorted_vers[-1] if sorted_vers else None


def _select_archive(objs: List[str], ver: Optional[str], patch: Optional[int], rc: Optional[int],
                    arch: str, pack: str) -> Optional[str]:
    """
    Select from the archive names `objs` the one for `arch` of the latest version
    matching the version prefix `ver` (e.g., "2025.1") and `patch` and `rc`.
    Archives of other architectures are dropped *before* picking the latest
    version: while a new release is being uploaded, its archive for one
    architecture may already be visible while the one for `arch` isn't yet,
    and we want to fall back to the previous release rather than fail.
    Returns `None` if no archive matches.
    """
    objs = [o for o in objs if f".{arch}." in o]
    if patch is None:
        ver = _get_latest_ver(objs, ver, pack)
    elif rc is None:
        ver = _get_latest_ver(objs, f"{ver}.{patch}", pack)
    else:
        ver = f"{ver}.{patch}rc{rc}"

    if ver is None:
        return None

    filtered = [o for o in objs if _version_key(o, pack) == ver]
    return filtered[0] if filtered else None


def _get_version_url(major: Optional[int] = None, minor: Optional[int] = None,
                     patch: Optional[int] = None, rc: Optional[int] = None, 
                     arch: str = "x86_64", pack: str = "") -> Optional[str]:
    """
    Resolve the download URL for a Scylla relocatable package.
    The lookup walks the Scylla downloads S3 bucket in two stages: first selecting
    the matching release directory from `major` and `minor`, then selecting the
    matching package artifact from `patch`, `rc`, `arch`, and `pack`. Missing version
    components are resolved to the latest available match, considering only release
    directories with any `arch` archive. Returns the full HTTPS URL for the selected
    artifact, or `None` if no matching package is found.
    """

    bucket_url = "downloads.scylladb.com"
    prefix_url = "downloads/scylla/relocatable"

    if major is None or minor is None:
        entries = _list_scylla_release_entries(bucket_url, prefix_url)
        branches = reversed(_sorted_vers(entries, f"{major}" if major is not None else None))
    else:
        branches = [f"{major}.{minor}"]

    # Pick the latest release branch that has any archive for `arch`. A new
    # branch may not have one yet - e.g., while its first release is being
    # uploaded - and then we want the previous branch rather than fail.
    for ver in branches:
        branch_prefix_url = f"{prefix_url}/scylladb-{ver}/"
        objs = [o for o in _list_scylla_release_entries(bucket_url, branch_prefix_url, pack)
                if f".{arch}." in o]
        if objs:
            archive = _select_archive(objs, ver, patch, rc, arch, pack)
            return f"https://{bucket_url}/{branch_prefix_url}{archive}" if archive else None
    return None


def get_file_name_and_url_from_url(major: Optional[int] = None, minor: Optional[int] = None,
                                   patch: Optional[int] = None, rc: Optional[int] = None, 
                                   arch: str = "x86_64", pack: str = "", url: str = None):
    """
    Resolve the download URL and file name for a Scylla package.
    If `url` is provided, it is used directly. Otherwise, the URL is resolved from
    the requested `major`, `minor`, `patch`, `rc`, `arch`, and `pack` values, using
    the latest available value for any missing version component. If `rc` is
    provided, `patch` is treated as 0. Returns `(url, file_name)` on success, or
    `(None, None)` if no matching URL can be resolved.
    Args:
        major: ScyllaDB major version to fetch.
        minor: ScyllaDB minor version to fetch.
        patch: ScyllaDB patch version to fetch.
        rc: Release-candidate number, if fetching an RC build.
        arch: Target architecture of the archive.
        pack: Package/build variant to resolve. Use an empty string for the default
              archive name (`scylla-<version>-...`), or a variant such as `"dev"` or
              `"debug"` for archive names like `scylla-dev-<version>-...` and
              `scylla-debug-<version>-...`.
        url: Direct archive URL to fetch instead of resolving one from version parts.
             If `url` is provided, it is downloaded directly. Otherwise, the version URL is
             resolved from the requested `major`, `minor`, `patch`, `rc`, `arch`, and `pack`
             components, using the latest available value for any missing component.
    """

    if rc is not None:
        patch = 0

    if not url:
        url = _get_version_url(major, minor, patch, rc, arch, pack)
    
    return (url, url.rsplit("/", 1)[-1]) if url else (None, None)


def download_scylla_version(major: Optional[int] = None, minor: Optional[int] = None,
                            patch: Optional[int] = None, rc: Optional[int] = None,
                            arch: str = "x86_64", pack: str = "", 
                            output_dir: str | Path = ".", url: str = None, 
                            retry: int = 1) -> Optional[Path]:
    """
    Download a Scylla package matching the requested version components.
    If `url` is provided, it is downloaded directly. Otherwise, the version URL is
    resolved from the requested `major`, `minor`, `patch`, and `rc` components,
    using the latest available value for any missing component. If `rc` is provided,
    `patch` is treated as 0. Returns the path to the downloaded file on success, or
    `None` if no matching version is found or the download fails after all retries.
    Args:
        major: ScyllaDB major version to fetch.
        minor: ScyllaDB minor version to fetch.
        patch: ScyllaDB patch version to fetch.
        rc: Release-candidate number, if fetching an RC build.
        arch: Target architecture of the archive.
        pack: Package/build variant to resolve. Use an empty string for the default
              archive name (`scylla-<version>-...`), or a variant such as `"dev"` or
              `"debug"` for archive names like `scylla-dev-<version>-...` and
              `scylla-debug-<version>-...`.
        url: Direct archive URL to fetch instead of resolving one from version parts.
             If `url` is provided, it is downloaded directly. Otherwise, the version URL is
             resolved from the requested `major`, `minor`, `patch`, `rc`, `arch`, and `pack`
             components, using the latest available value for any missing component.
    """

    KB = 1024 * 1024

    if rc is not None:
        patch = 0

    if not url:
        url = _get_version_url(major, minor, patch, rc, arch, pack)
        if url is None:
            return None

    output_path = Path(output_dir) / url.rsplit("/", 1)[-1]

    for i in range(retry):
        try:
            with urllib.request.urlopen(url, timeout=60) as response:
                if response.status == 200:
                    with output_path.open("wb") as f:
                        while chunk := response.read(KB):
                            f.write(chunk)
                    return output_path
                if i == retry - 1:
                    return None
                time.sleep(1)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError):
            if i == retry - 1:
                return None
            time.sleep(1)
    return None


@contextmanager
def with_file_lock(lock_path: Path) -> Iterator[None]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def extract_tar_no_same_owner(archive_path: Path, unpack_dir: Path) -> None:
    with tarfile.open(archive_path, "r:*") as archive:
        archive.extractall(path=unpack_dir, filter="data")            


def fetch_and_install_scylla_version(major: Optional[int] = None, minor: Optional[int] = None,
                                     patch: Optional[int] = None, rc: Optional[int] = None,
                                     arch: str = "x86_64", pack: str = "", 
                                     url: str = None) -> Path:
    """Fetch, cache, unpack, and install a ScyllaDB release archive.
    The archive is resolved from either the supplied version components or a direct
    URL, then cached under XDG_CACHE_HOME, or ~/.cache if XDG_CACHE_HOME is unset.
    Download, unpack, and install steps are guarded by marker files so repeated
    calls reuse the existing installation. A file lock prevents concurrent callers
    from modifying the same cached archive directory at the same time.
    Args:
        major: ScyllaDB major version to fetch.
        minor: ScyllaDB minor version to fetch.
        patch: ScyllaDB patch version to fetch.
        rc: Release-candidate number, if fetching an RC build.
        arch: Target architecture of the archive.
        pack: Package/build variant to resolve. Use an empty string for the default
              archive name (`scylla-<version>-...`), or a variant such as `"dev"` or
              `"debug"` for archive names like `scylla-dev-<version>-...` and
              `scylla-debug-<version>-...`.
        url: Direct archive URL to fetch instead of resolving one from version parts.
             If `url` is provided, it is downloaded directly. Otherwise, the version URL is
             resolved from the requested `major`, `minor`, `patch`, `rc`, `arch`, and `pack`
             components, using the latest available value for any missing component.
    If the archive cannot be resolved or downloaded, e.g., because of a network problem,
    and the version was given by its components (not `url`), falls back to the latest
    matching version already installed in the cache by an earlier call.
    Returns:
        Path to the installed ScyllaDB binary.
    Raises:
        RuntimeError: If the archive name cannot be resolved or the archive cannot be downloaded,
                      and no matching version is already installed in the cache.
        subprocess.CalledProcessError: If the ScyllaDB install script fails.
    """
    xdg_cache_dir = Path(os.getenv("XDG_CACHE_HOME", str(Path.home() / ".cache")))
    cache_root = xdg_cache_dir / "scylladb" / "test.py"
    # Keep official releases resolved from version components apart from
    # archives fetched from an arbitrary `url` (e.g., test.py --exe-url),
    # which may be unofficial builds with release-like names. Only the
    # former may be used as a fallback by _find_cached_scylla_install().
    if not url:
        cache_root = cache_root / "releases"
    lookup_error = None
    try:
        archive_url, archive_name = get_file_name_and_url_from_url(major, minor, patch, rc, arch, pack, url)
    except (BotoCoreError, ClientError) as e:
        lookup_error = e
        archive_url, archive_name = None, None
    if archive_name:
        installed = _install_scylla_archive(cache_root / archive_name, archive_url, archive_name)
        if installed:
            return installed
        error = f"Couldnt download Scylla archive: {archive_url}"
    else:
        error = (f"couldnt get archive name for major: {major}, minor: {minor},"
                 f" patch: {patch}, rc: {rc}, arch: {arch}, pack: {pack}, url: {url}")
        if lookup_error:
            error += f": {lookup_error}"

    # Don't fail the test because of a transient network problem if a
    # suitable version was already installed by an earlier run.
    if not url:
        cached = _find_cached_scylla_install(cache_root, major, minor, patch, rc, arch, pack)
        if cached:
            logger.warning("%s; using previously installed %s instead", error, cached)
            return cached
    raise RuntimeError(error) from lookup_error


def _find_cached_scylla_install(cache_root: Path, major: Optional[int], minor: Optional[int],
                                patch: Optional[int], rc: Optional[int], arch: str, pack: str) -> Optional[Path]:
    """
    Find the latest official release installed under `cache_root` by
    fetch_and_install_scylla_version() matching the requested version components,
    resolving missing components the same way _get_version_url() does. Returns the
    path to its ScyllaDB binary, or `None` if none match.
    """
    if not cache_root.is_dir():
        return None
    if rc is not None:
        patch = 0
    pattern = _release_entry_pattern(pack)
    names = [d.name for d in cache_root.iterdir() if pattern.match(d.name) and (d / "installed.success").exists()]
    if major is None or minor is None:
        latest = _get_latest_ver([n for n in names if f".{arch}." in n],
                                 f"{major}" if major is not None else None, pack)
        if latest is None:
            return None
        ver = ".".join(latest.split(".")[:2])
    else:
        ver = f"{major}.{minor}"
    archive = _select_archive(names, ver, patch, rc, arch, pack)
    return cache_root / archive / "installed" / "bin" / "scylla" if archive else None


def _install_scylla_archive(cache_dir: Path, archive_url: str, archive_name: str) -> Optional[Path]:
    """
    Download, unpack, and install the archive `archive_name` from `archive_url` into
    `cache_dir`, skipping steps already completed by an earlier call. Returns the path
    to the installed ScyllaDB binary, or `None` if the archive cannot be downloaded.
    """
    cache_dir.mkdir(exist_ok=True, parents=True)

    archive_path = cache_dir/archive_name
    unpack_dir = cache_dir/"unpacked"
    install_dir = cache_dir/"installed"

    lock_file = cache_dir / "lock"
    downloaded_marker = cache_dir / "downloaded.success"
    unpacked_marker = cache_dir / "unpacked.success"
    installed_marker = cache_dir / "installed.success"

    with with_file_lock(lock_file):
        if not installed_marker.exists():
            if not unpacked_marker.exists():
                if not downloaded_marker.exists():
                    archive_path.unlink(missing_ok=True)
                    archive_path = download_scylla_version(url=archive_url, output_dir=cache_dir, retry=40)
                    if archive_path is None:
                        return None
                    downloaded_marker.touch()
                shutil.rmtree(unpack_dir, ignore_errors=True)
                unpack_dir.mkdir(exist_ok=True, parents=True)
                extract_tar_no_same_owner(archive_path, unpack_dir)
                unpacked_marker.touch()
            shutil.rmtree(install_dir, ignore_errors=True)
            install_dir.mkdir(exist_ok=True, parents=True)
            subprocess.run(["bash", "./install.sh", "--without-systemd", "--nonroot", "--prefix", str(install_dir)], cwd=unpack_dir/"scylla", check=True)
            installed_marker.touch()

    return install_dir/"bin"/"scylla"