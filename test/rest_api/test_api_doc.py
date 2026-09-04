# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import json


def get_json(rest_api, path):
    resp = rest_api.send("GET", path)
    resp.raise_for_status()
    # Reading the body is the part that matters here. The api-doc records are
    # served by a file_handler that streams the file only after the 200 status
    # line and the headers have been sent, so a missing file does not turn into
    # an error status - the connection is just closed mid-body. requests
    # reports that as a ChunkedEncodingError while reading .content.
    return json.loads(resp.content)


def test_api_doc_index(rest_api):
    """The /api-doc index itself must be valid JSON in the swagger 1.2 shape."""
    doc = get_json(rest_api, "api-doc")
    assert doc["swaggerVersion"] == "1.2"
    assert doc["apis"], "the /api-doc index lists no APIs"
    for api in doc["apis"]:
        assert api["path"].startswith("/"), f"malformed api-doc record {api}"
        assert api["description"], f"api-doc record without description {api}"


def test_api_doc_records_resolve(rest_api):
    """Every record advertised by /api-doc must resolve to a valid swagger file.

    Registering an API module (api_registry::reg(), reached via
    api_registry_builder::register_function()) does two things at once: it adds
    a {path, description} record to the /api-doc index and it routes
    /api-doc/<name> to <api_doc_dir>/<name>.json. Nothing checks that the file
    is actually there, and when it isn't the server answers 200 and truncates
    the body, without even a log message. Walk the index the way a swagger
    client would, so that a module registered without its .json file is caught.
    """
    index = get_json(rest_api, "api-doc")
    broken = {}
    for api in index["apis"]:
        path = api["path"]
        try:
            doc = get_json(rest_api, "api-doc" + path)
            assert doc["swaggerVersion"] == "1.2", f"unexpected swaggerVersion {doc.get('swaggerVersion')}"
            assert "apis" in doc, "no 'apis' section"
        except Exception as e:
            broken[path] = f"{type(e).__name__}: {e}"
    assert not broken, f"unusable /api-doc records: {broken}"


def test_api_doc_v2(rest_api):
    """The v2 doc is concatenated from several files, so it is easy to break."""
    doc = get_json(rest_api, "v2")
    assert doc["swagger"] == "2.0"
    assert doc["paths"], "the v2 swagger doc lists no paths"
    assert doc["definitions"], "the v2 swagger doc lists no definitions"
