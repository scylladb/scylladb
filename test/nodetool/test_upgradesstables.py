# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#


import pytest
from test.nodetool.rest_api_mock import expected_request


def test_upgradesstables_all_one_keyspace(nodetool):
    nodetool("upgradesstables", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1"], multiple=expected_request.MULTIPLE),
        expected_request(
            "GET",
            "/storage_service/keyspace_upgrade_sstables/ks1",
            params={"exclude_current_version": "true"},
            response=0),
        ])


def test_upgradesstables_all_two_keyspace(nodetool):
    nodetool("upgradesstables", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"],
                         multiple=expected_request.MULTIPLE),
        expected_request(
            "GET",
            "/storage_service/keyspace_upgrade_sstables/ks1",
            params={"exclude_current_version": "true"},
            response=0),
        expected_request(
            "GET",
            "/storage_service/keyspace_upgrade_sstables/ks2",
            params={"exclude_current_version": "true"},
            response=0),
        ])


def test_upgradesstables_keyspace(nodetool):
    nodetool("upgradesstables", "ks1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"],
                         multiple=expected_request.MULTIPLE),
        expected_request(
            "GET",
            "/storage_service/keyspace_upgrade_sstables/ks1",
            params={"exclude_current_version": "true"},
            response=0),
        ])


def test_upgradesstables_one_table(nodetool):
    nodetool("upgradesstables", "ks1", "tbl1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"],
                         multiple=expected_request.MULTIPLE),
        expected_request(
            "GET",
            "/storage_service/keyspace_upgrade_sstables/ks1",
            params={"exclude_current_version": "true", "cf": "tbl1"},
            response=0),
        ])


def test_upgradesstables_two_tables(nodetool):
    nodetool("upgradesstables", "ks1", "tbl1", "tbl2", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"],
                         multiple=expected_request.MULTIPLE),
        expected_request(
            "GET",
            "/storage_service/keyspace_upgrade_sstables/ks1",
            params={"exclude_current_version": "true", "cf": "tbl1,tbl2"},
            response=0),
        ])


@pytest.mark.parametrize("jobs", ["-j", "--jobs"])
def test_upgradesstables_jobs(nodetool, jobs):
    nodetool("upgradesstables", "ks1", jobs, "2", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"],
                         multiple=expected_request.MULTIPLE),
        expected_request(
            "GET",
            "/storage_service/keyspace_upgrade_sstables/ks1",
            params={"exclude_current_version": "true"},
            response=0),
        ])


@pytest.mark.parametrize("include_all", ["-a", "--include-all-sstables"])
def test_upgradesstables_include_all(nodetool, include_all):
    nodetool("upgradesstables", "ks1", include_all, expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"],
                         multiple=expected_request.MULTIPLE),
        expected_request(
            "GET",
            "/storage_service/keyspace_upgrade_sstables/ks1",
            params={},
            response=0),
        ])
