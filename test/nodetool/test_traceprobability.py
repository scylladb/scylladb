#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from rest_api_mock import expected_request
import utils


def test_gettraceprobability(nodetool):
    out = nodetool("gettraceprobability", expected_requests=[
        expected_request("GET", "/storage_service/trace_probability", response=0.2)])

    assert out == "Current trace probability: 0.2\n"


def test_settraceprobability(nodetool):
    nodetool("settraceprobability", "0.2", expected_requests=[
        expected_request("POST", "/storage_service/trace_probability", params={"probability": "0.2"})])


def test_settraceprobability_missing_param(nodetool):
    utils.check_nodetool_fails_with(
            nodetool,
            ("settraceprobability",),
            {},
            ["nodetool: Required parameters are missing: trace_probability",
             "error processing arguments: required parameters are missing: trace_probability"])


def test_settraceprobability_invalid_type(nodetool):
    utils.check_nodetool_fails_with(
            nodetool,
            ("settraceprobability", "adadad"),
            {},
            ["nodetool: trace_probability: can not convert \"adadad\" to a Double",
             "error: the argument ('adadad') for option '--trace_probability' is invalid"])


def test_settraceprobability_out_of_bounds(nodetool):
    for value in ("-0.1", "1.1", "9000"):
        utils.check_nodetool_fails_with(
                nodetool,
                ("settraceprobability", "--", value),
                {},
                ["nodetool: Trace probability must be between 0 and 1",
                 "error processing arguments: trace probability must be between 0 and 1"])
