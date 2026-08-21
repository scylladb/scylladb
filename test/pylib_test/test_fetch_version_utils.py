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

def test_with_file_lock_creates_parent_and_runs_body(tmp_path):
    lock_path = tmp_path / "locks" / "scylla.lock"
    with vfu.with_file_lock(lock_path):
        assert lock_path.exists()
    assert lock_path.exists()