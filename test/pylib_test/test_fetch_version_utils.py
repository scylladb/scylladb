#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest
import test.pylib.version_fetch_utils as vfu

def test_list_scylla_release_entries_prefers_files_when_files_exist(monkeypatch):
    class FakeS3Client:
        def __init__(self):
            self.calls = []

        def list_objects_v2(self, **kwargs):
            self.calls.append(kwargs)
            prefix = kwargs["Prefix"]
            return {
                "Contents": [
                    {"Key": f"{prefix}scylla-2026.1.0-0.20260125.f94296e0ae43.x86_64.tar.gz"},
                    {"Key": f"{prefix}not-a-scylla-object.txt"}],
                "CommonPrefixes": [
                    {"Prefix": f"{prefix}scylladb-2026.1/"},
                    {"Prefix": f"{prefix}other/"}]
            }

    fake_s3 = FakeS3Client()

    def fake_client(service_name, region_name, config):
        assert service_name == "s3"
        assert region_name == "us-east-1"
        return fake_s3

    monkeypatch.setattr(vfu.boto3, "client", fake_client)

    assert vfu._list_scylla_release_entries("downloads.scylladb.com", "downloads/scylla/relocatable") == [
        "scylla-2026.1.0-0.20260125.f94296e0ae43.x86_64.tar.gz"]
    assert fake_s3.calls == [{
            "Bucket": "downloads.scylladb.com",
            "Prefix": "downloads/scylla/relocatable/",
            "Delimiter": "/",}]
    

def test_list_scylla_release_entries_returns_folders_when_no_files_exist(monkeypatch):
    class FakeS3Client:
        def list_objects_v2(self, **kwargs):
            prefix = kwargs["Prefix"]
            return {
                "CommonPrefixes": [
                    {"Prefix": f"{prefix}scylladb-2025.1/"},
                    {"Prefix": f"{prefix}scylladb-2026.1/"},
                    {"Prefix": f"{prefix}other/"}]}

    monkeypatch.setattr(vfu.boto3, "client", lambda *args, **kwargs: FakeS3Client())

    assert vfu._list_scylla_release_entries("downloads.scylladb.com", "downloads/scylla/relocatable") == [
        "scylladb-2025.1", "scylladb-2026.1"]


def test_list_scylla_release_entries_reads_all_pages(monkeypatch):
    class FakeS3Client:
        def __init__(self):
            self.calls = []

        def list_objects_v2(self, **kwargs):
            self.calls.append(kwargs)
            prefix = kwargs["Prefix"]
            if "ContinuationToken" not in kwargs:
                return {
                    "IsTruncated": True,
                    "NextContinuationToken": "next-page",
                    "Contents": [{
                        "Key": f"{prefix}scylla-2026.1.0-0.20260125.f94296e0ae43.x86_64.tar.gz"}]}
            return {
                "IsTruncated": False,
                "Contents": [
                    {"Key": f"{prefix}scylla-2026.1.1-0.20260301.f94296e0ae43.x86_64.tar.gz"}]}

    fake_s3 = FakeS3Client()

    monkeypatch.setattr(vfu.boto3, "client", lambda *args, **kwargs: fake_s3)

    assert vfu._list_scylla_release_entries("downloads.scylladb.com", "downloads/scylla/relocatable/scylladb-2026.1/") == [
        "scylla-2026.1.0-0.20260125.f94296e0ae43.x86_64.tar.gz", "scylla-2026.1.1-0.20260301.f94296e0ae43.x86_64.tar.gz"]
    assert fake_s3.calls[1]["ContinuationToken"] == "next-page"


def test_get_latest_ver_prefers_latest_release_over_rc():
    objs = ["scylla-2026.1.0~rc0-0.20260125.f94296e0ae43.x86_64.tar.gz",
            "scylla-2026.1.0-0.20260201.f94296e0ae43.x86_64.tar.gz",
            "scylla-2026.1.1-0.20260301.f94296e0ae43.x86_64.tar.gz",
            "scylla-2026.2.0-0.20260401.f94296e0ae43.x86_64.tar.gz"]

    assert vfu._get_latest_ver(objs, "2026.1") == "2026.1.1"


def test_get_latest_ver_does_not_match_partial_minor_version():
    objs = ["scylla-2026.1.1-0.20260301.f94296e0ae43.x86_64.tar.gz",
            "scylla-2026.10.0-0.20260401.f94296e0ae43.x86_64.tar.gz"]

    assert vfu._get_latest_ver(objs, "2026.1") == "2026.1.1"


def test_get_latest_ver_handles_version_directories():
    assert (vfu._get_latest_ver(["scylladb-2025.1", "scylladb-2026.1", "scylladb-2024.2"]) == "2026.1")


def test_get_version_url_selects_latest_matching_arch(monkeypatch):
    def fake_list_scylla_release_entries(bucket, prefix, pack=""):
        assert bucket == "downloads.scylladb.com"
        assert prefix == "downloads/scylla/relocatable/scylladb-2026.1/"
        assert pack == ""
        return ["scylla-2026.1.0~rc0-0.20260125.f94296e0ae43.x86_64.tar.gz",
                "scylla-2026.1.0-0.20260201.f94296e0ae43.aarch64.tar.gz",
                "scylla-2026.1.1-0.20260301.f94296e0ae43.x86_64.tar.gz"]

    monkeypatch.setattr(vfu, "_list_scylla_release_entries", fake_list_scylla_release_entries)

    assert vfu._get_version_url(major=2026, minor=1, arch="x86_64") == (
        "https://downloads.scylladb.com/downloads/scylla/relocatable/scylladb-2026.1/"
        "scylla-2026.1.1-0.20260301.f94296e0ae43.x86_64.tar.gz")


def test_get_version_url_skips_release_missing_arch(monkeypatch):
    # While a new release (here 2025.1.16) is being uploaded, its archive for
    # one architecture may already be visible while the one we want isn't yet.
    # We should pick the previous release instead of failing (SCYLLADB-4881).
    def fake_list_scylla_release_entries(bucket, prefix, pack=""):
        assert prefix == "downloads/scylla/relocatable/scylladb-2025.1/"
        return ["scylla-2025.1.15-0.20260901.aaaaaaaaaaaa.aarch64.tar.gz",
                "scylla-2025.1.15-0.20260901.aaaaaaaaaaaa.x86_64.tar.gz",
                "scylla-2025.1.16-0.20260924.4f24ebf84be6.aarch64.tar.gz"]

    monkeypatch.setattr(vfu, "_list_scylla_release_entries", fake_list_scylla_release_entries)

    assert vfu._get_version_url(major=2025, minor=1, arch="x86_64") == (
        "https://downloads.scylladb.com/downloads/scylla/relocatable/scylladb-2025.1/"
        "scylla-2025.1.15-0.20260901.aaaaaaaaaaaa.x86_64.tar.gz")
    assert vfu._get_version_url(major=2025, minor=1, arch="aarch64") == (
        "https://downloads.scylladb.com/downloads/scylla/relocatable/scylladb-2025.1/"
        "scylla-2025.1.16-0.20260924.4f24ebf84be6.aarch64.tar.gz")


def test_get_version_url_skips_branch_missing_arch(monkeypatch):
    # Similarly, when the minor version isn't given, a new release branch
    # (here 2026.2) whose first archive for the architecture we want isn't
    # visible yet should be skipped in favor of the previous branch.
    listings = {
        "downloads/scylla/relocatable": ["scylladb-2025.1", "scylladb-2026.1", "scylladb-2026.2"],
        "downloads/scylla/relocatable/scylladb-2026.1/": [
            "scylla-2026.1.3-0.20260510.cccccccccccc.aarch64.tar.gz",
            "scylla-2026.1.3-0.20260510.cccccccccccc.x86_64.tar.gz"],
        "downloads/scylla/relocatable/scylladb-2026.2/": [
            "scylla-2026.2.0~rc0-0.20260930.dddddddddddd.aarch64.tar.gz"],
    }

    def fake_list_scylla_release_entries(bucket, prefix, pack=""):
        return listings.get(prefix, [])

    monkeypatch.setattr(vfu, "_list_scylla_release_entries", fake_list_scylla_release_entries)

    for major in (2026, None):
        assert vfu._get_version_url(major=major, arch="x86_64") == (
            "https://downloads.scylladb.com/downloads/scylla/relocatable/scylladb-2026.1/"
            "scylla-2026.1.3-0.20260510.cccccccccccc.x86_64.tar.gz")
        assert vfu._get_version_url(major=major, arch="aarch64") == (
            "https://downloads.scylladb.com/downloads/scylla/relocatable/scylladb-2026.2/"
            "scylla-2026.2.0~rc0-0.20260930.dddddddddddd.aarch64.tar.gz")
    assert vfu._get_version_url(major=2025, arch="x86_64") is None


def test_get_version_url_supports_rc_zero(monkeypatch):
    def fake_list_scylla_release_entries(bucket, prefix, pack=""):
        assert bucket == "downloads.scylladb.com"
        assert prefix == "downloads/scylla/relocatable/scylladb-2026.1/"
        assert pack == ""
        return ["scylla-2026.1.0~rc0-0.20260125.f94296e0ae43.aarch64.tar.gz",
                "scylla-2026.1.0-0.20260201.f94296e0ae43.aarch64.tar.gz"]

    monkeypatch.setattr(vfu, "_list_scylla_release_entries", fake_list_scylla_release_entries)

    assert vfu._get_version_url(major=2026, minor=1, patch=0, rc=0, arch="aarch64") == (
        "https://downloads.scylladb.com/downloads/scylla/relocatable/scylladb-2026.1/"
        "scylla-2026.1.0~rc0-0.20260125.f94296e0ae43.aarch64.tar.gz")


def test_get_version_url_exact_release_does_not_select_rc(monkeypatch):
    def fake_list_scylla_release_entries(bucket, prefix, pack=""):
        assert bucket == "downloads.scylladb.com"
        assert prefix == "downloads/scylla/relocatable/scylladb-2026.1/"
        assert pack == ""
        return ["scylla-2026.1.0~rc0-0.20260125.f94296e0ae43.x86_64.tar.gz",
                "scylla-2026.1.0-0.20260201.f94296e0ae43.x86_64.tar.gz"]

    monkeypatch.setattr(
        vfu, "_list_scylla_release_entries", fake_list_scylla_release_entries
    )

    assert vfu._get_version_url(major=2026, minor=1, patch=0, arch="x86_64") == (
        "https://downloads.scylladb.com/downloads/scylla/relocatable/scylladb-2026.1/"
        "scylla-2026.1.0-0.20260201.f94296e0ae43.x86_64.tar.gz")


def test_download_scylla_version_streams_to_output_dir(monkeypatch, tmp_path):
    url = "https://downloads.scylladb.com/downloads/scylla/relocatable/scylladb-2026.1/scylla-2026.1.0-0.20260201.f94296e0ae43.x86_64.tar.gz"
    calls = []

    class FakeResponse:
        def __init__(self):
            self.status = 200
            self.chunks = [b"abc", b"def", b""]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def read(self, chunk_size):
            assert chunk_size == 1024 * 1024
            return self.chunks.pop(0)

    def fake_get_version_url(*args, **kwargs):
        return url

    def fake_urlopen(request_url, timeout):
        calls.append(request_url)
        assert timeout == 60
        return FakeResponse()

    monkeypatch.setattr(vfu, "_get_version_url", fake_get_version_url)
    monkeypatch.setattr(vfu.urllib.request, "urlopen", fake_urlopen)

    path = vfu.download_scylla_version(major=2026, minor=1, output_dir=tmp_path)

    assert path == tmp_path / "scylla-2026.1.0-0.20260201.f94296e0ae43.x86_64.tar.gz"
    assert path.read_bytes() == b"abcdef"
    assert calls == [url]


def test_download_scylla_version_returns_none_when_no_version(monkeypatch, tmp_path):
    
    def fake_get_version_url(*args, **kwargs):
        return None

    monkeypatch.setattr(vfu, "_get_version_url", fake_get_version_url)
    monkeypatch.setattr(vfu.urllib.request, "urlopen",
                        lambda *args, **kwargs: pytest.fail("unexpected download"))

    assert vfu.download_scylla_version(output_dir=tmp_path) is None


def test_get_file_name_and_url_from_url_uses_direct_url():
    url = "https://example.com/path/scylla.tar.gz"
    assert vfu.get_file_name_and_url_from_url(url=url) == (url, "scylla.tar.gz")


def test_get_file_name_and_url_from_url_supports_rc_zero(monkeypatch):
    calls = []

    def fake_get_version_url(major, minor, patch, rc, arch, pack):
        calls.append((major, minor, patch, rc, arch, pack))
        return "https://example.com/scylla-2026.1.0~rc0.x86_64.tar.gz"

    monkeypatch.setattr(vfu, "_get_version_url", fake_get_version_url)

    assert vfu.get_file_name_and_url_from_url(2026, 1, rc=0) == (
        "https://example.com/scylla-2026.1.0~rc0.x86_64.tar.gz",
        "scylla-2026.1.0~rc0.x86_64.tar.gz")
    assert calls == [(2026, 1, 0, 0, "x86_64", "")]


def test_download_scylla_version_retries_after_url_error(monkeypatch, tmp_path):
    url = "https://example.com/scylla.tar.gz"
    calls = []
    class FakeResponse:
        status = 200
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, traceback):
            return False
        def read(self, chunk_size):
            return b"data" if not hasattr(self, "done") else b""
        
    def fake_urlopen(request_url, timeout):
        calls.append(request_url)
        if len(calls) == 1:
            raise vfu.urllib.error.URLError("temporary failure")
        response = FakeResponse()
        response.done = False

        def read(chunk_size):
            if response.done:
                return b""
            response.done = True
            return b"data"
        response.read = read
        return response
    
    monkeypatch.setattr(vfu.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(vfu.urllib.request, "urlopen", fake_urlopen)

    path = vfu.download_scylla_version(url=url, output_dir=tmp_path, retry=2)

    assert path == tmp_path / "scylla.tar.gz"
    assert path.read_bytes() == b"data"
    assert calls == [url, url]


def test_download_scylla_version_stops_retrying_after_deadline(monkeypatch, tmp_path):
    # When the server is unreachable, each attempt can take minutes. Even with
    # many retries allowed, we should give up after about 10 minutes, so the
    # caller can still fall back to a cached version before the test times out.
    url = "https://example.com/scylla.tar.gz"
    now = 0
    calls = []

    def fake_urlopen(request_url, timeout):
        nonlocal now
        calls.append(request_url)
        now += 120
        raise vfu.urllib.error.URLError("timed out")

    monkeypatch.setattr(vfu.time, "monotonic", lambda: now)
    monkeypatch.setattr(vfu.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(vfu.urllib.request, "urlopen", fake_urlopen)

    assert vfu.download_scylla_version(url=url, output_dir=tmp_path, retry=40) is None
    assert len(calls) == 6


def test_with_file_lock_creates_parent_and_runs_body(tmp_path):
    lock_path = tmp_path / "locks" / "scylla.lock"
    with vfu.with_file_lock(lock_path):
        assert lock_path.exists()
    assert lock_path.exists()


def make_cache(tmp_path, monkeypatch, installed, not_installed=()):
    """Point XDG_CACHE_HOME to tmp_path, with a cache of the given release archives."""
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    cache_root = tmp_path / "scylladb" / "test.py" / "releases"
    for name in [*installed, *not_installed]:
        (cache_root / name).mkdir(parents=True)
    for name in installed:
        (cache_root / name / "installed.success").touch()
    return cache_root


# A cache with several versions installed, the latest 2025.1 for x86_64 being
# 2025.1.15. The newer 2025.1.16 is only partly there: not installed for
# x86_64, and installed only for aarch64 or as a different package.
CACHED = ["scylla-2025.1.14-0.20260801.aaaaaaaaaaaa.x86_64.tar.gz",
          "scylla-2025.1.15-0.20260901.bbbbbbbbbbbb.x86_64.tar.gz",
          "scylla-2025.1.16-0.20260924.4f24ebf84be6.aarch64.tar.gz",
          "scylla-dev-2025.1.16-0.20260924.4f24ebf84be6.x86_64.tar.gz",
          "scylla-2026.1.3-0.20260510.cccccccccccc.x86_64.tar.gz"]
NOT_INSTALLED = ["scylla-2025.1.16-0.20260924.4f24ebf84be6.x86_64.tar.gz"]


def test_fetch_and_install_falls_back_to_cache_on_s3_error(monkeypatch, tmp_path):
    cache_root = make_cache(tmp_path, monkeypatch, CACHED, NOT_INSTALLED)

    def fake_get_file_name_and_url_from_url(*args, **kwargs):
        raise vfu.BotoCoreError()

    monkeypatch.setattr(vfu, "get_file_name_and_url_from_url", fake_get_file_name_and_url_from_url)

    assert vfu.fetch_and_install_scylla_version(2025, 1) == (
        cache_root / "scylla-2025.1.15-0.20260901.bbbbbbbbbbbb.x86_64.tar.gz" / "installed" / "bin" / "scylla")
    assert vfu.fetch_and_install_scylla_version(2025, 1, 14) == (
        cache_root / "scylla-2025.1.14-0.20260801.aaaaaaaaaaaa.x86_64.tar.gz" / "installed" / "bin" / "scylla")
    assert vfu.fetch_and_install_scylla_version(2025, 1, arch="aarch64") == (
        cache_root / "scylla-2025.1.16-0.20260924.4f24ebf84be6.aarch64.tar.gz" / "installed" / "bin" / "scylla")
    assert vfu.fetch_and_install_scylla_version(2025, 1, pack="dev") == (
        cache_root / "scylla-dev-2025.1.16-0.20260924.4f24ebf84be6.x86_64.tar.gz" / "installed" / "bin" / "scylla")
    assert vfu.fetch_and_install_scylla_version(2025) == (
        cache_root / "scylla-2025.1.15-0.20260901.bbbbbbbbbbbb.x86_64.tar.gz" / "installed" / "bin" / "scylla")
    assert vfu.fetch_and_install_scylla_version() == (
        cache_root / "scylla-2026.1.3-0.20260510.cccccccccccc.x86_64.tar.gz" / "installed" / "bin" / "scylla")
    with pytest.raises(RuntimeError, match="couldnt get archive name"):
        vfu.fetch_and_install_scylla_version(2025, 2)


def test_fetch_and_install_falls_back_to_cached_rc(monkeypatch, tmp_path):
    # As in S3 lookup, a requested rc implies patch 0, so the fallback
    # should pick the cached rc, not the newer cached stable release.
    rc = "scylla-2026.1.0~rc0-0.20260125.f94296e0ae43.x86_64.tar.gz"
    cache_root = make_cache(tmp_path, monkeypatch, [*CACHED, rc])

    def fake_get_file_name_and_url_from_url(*args, **kwargs):
        raise vfu.BotoCoreError()

    monkeypatch.setattr(vfu, "get_file_name_and_url_from_url", fake_get_file_name_and_url_from_url)

    assert vfu.fetch_and_install_scylla_version(2026, 1, rc=0) == cache_root / rc / "installed" / "bin" / "scylla"
    with pytest.raises(RuntimeError, match="couldnt get archive name"):
        vfu.fetch_and_install_scylla_version(2026, 1, rc=1)


def fake_install(monkeypatch):
    """Make installing an archive succeed without downloading or running anything."""
    def fake_download_scylla_version(url, output_dir, retry):
        path = output_dir / url.rsplit("/", 1)[-1]
        path.write_bytes(b"")
        return path

    def fake_extract_tar_no_same_owner(archive_path, unpack_dir):
        (unpack_dir / "scylla").mkdir()

    monkeypatch.setattr(vfu, "download_scylla_version", fake_download_scylla_version)
    monkeypatch.setattr(vfu, "extract_tar_no_same_owner", fake_extract_tar_no_same_owner)
    monkeypatch.setattr(vfu.subprocess, "run", lambda *args, **kwargs: None)


def test_fetch_and_install_falls_back_to_cache_on_download_failure(monkeypatch, tmp_path):
    cache_root = make_cache(tmp_path, monkeypatch, CACHED, NOT_INSTALLED)
    url = ("https://downloads.scylladb.com/downloads/scylla/relocatable/scylladb-2025.1/"
           "scylla-2025.1.16-0.20260924.4f24ebf84be6.x86_64.tar.gz")

    monkeypatch.setattr(vfu, "get_file_name_and_url_from_url",
                        lambda *args, **kwargs: (url, url.rsplit("/", 1)[-1]))
    monkeypatch.setattr(vfu, "download_scylla_version", lambda *args, **kwargs: None)

    assert vfu.fetch_and_install_scylla_version(2025, 1) == (
        cache_root / "scylla-2025.1.15-0.20260901.bbbbbbbbbbbb.x86_64.tar.gz" / "installed" / "bin" / "scylla")


def test_fetch_and_install_uses_cache_only_on_failure(monkeypatch, tmp_path):
    # If S3 has a newer release than the cache, it is the one we should install.
    cache_root = make_cache(tmp_path, monkeypatch, CACHED)
    url = ("https://downloads.scylladb.com/downloads/scylla/relocatable/scylladb-2025.1/"
           "scylla-2025.1.16-0.20260924.4f24ebf84be6.x86_64.tar.gz")
    name = url.rsplit("/", 1)[-1]
    fake_install(monkeypatch)
    monkeypatch.setattr(vfu, "get_file_name_and_url_from_url", lambda *args, **kwargs: (url, name))

    assert vfu.fetch_and_install_scylla_version(2025, 1) == cache_root / name / "installed" / "bin" / "scylla"
    assert (cache_root / name / "installed.success").exists()


def test_fetch_and_install_raises_without_cache(monkeypatch, tmp_path):
    make_cache(tmp_path, monkeypatch, [])

    def fake_get_file_name_and_url_from_url(*args, **kwargs):
        raise vfu.BotoCoreError()

    monkeypatch.setattr(vfu, "get_file_name_and_url_from_url", fake_get_file_name_and_url_from_url)

    with pytest.raises(RuntimeError, match="couldnt get archive name") as e:
        vfu.fetch_and_install_scylla_version(2025, 1)
    assert isinstance(e.value.__cause__, vfu.BotoCoreError)


def test_fetch_and_install_direct_url_does_not_fall_back(monkeypatch, tmp_path):
    # A direct URL asks for one specific archive, so another cached version won't do.
    make_cache(tmp_path, monkeypatch, CACHED)
    url = "https://example.com/scylla-2025.1.16-0.20260924.4f24ebf84be6.x86_64.tar.gz"
    monkeypatch.setattr(vfu, "download_scylla_version", lambda *args, **kwargs: None)

    with pytest.raises(RuntimeError, match="Couldnt download Scylla archive"):
        vfu.fetch_and_install_scylla_version(url=url)


def test_fetch_and_install_does_not_fall_back_to_direct_url(monkeypatch, tmp_path):
    # An archive fetched from a direct URL (e.g., test.py --exe-url) may be an
    # unofficial build with a release-like name, so it must not be mistaken
    # for that release, neither as a fallback nor when installing the release.
    cache_root = make_cache(tmp_path, monkeypatch, CACHED)
    name = "scylla-2025.1.16-0.20260924.4f24ebf84be6.x86_64.tar.gz"
    fake_install(monkeypatch)
    url_install = vfu.fetch_and_install_scylla_version(url=f"https://example.com/{name}")
    assert not url_install.is_relative_to(cache_root)

    def fake_get_file_name_and_url_from_url(*args, **kwargs):
        raise vfu.BotoCoreError()

    monkeypatch.setattr(vfu, "get_file_name_and_url_from_url", fake_get_file_name_and_url_from_url)
    assert vfu.fetch_and_install_scylla_version(2025, 1) == (
        cache_root / "scylla-2025.1.15-0.20260901.bbbbbbbbbbbb.x86_64.tar.gz" / "installed" / "bin" / "scylla")
