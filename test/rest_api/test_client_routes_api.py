# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
import uuid

def make_entry(key_seed, data_seed):
    connection_id = str(uuid.UUID(int=key_seed + 1))
    host_id = str(uuid.UUID(int=(key_seed + 100)))
    port = 8000 + (data_seed % 100)
    tls_port = port + 1
    alternator_port = port + 2
    alternator_https_port = port + 3
    address = f"addr{data_seed}.test"
    return {
        "connection_id": connection_id,
        "host_id": host_id,
        "address": address,
        "port": port,
        "tls_port": tls_port,
        "alternator_port": alternator_port,
        "alternator_https_port": alternator_https_port,
    }

def test_client_routes(cql, this_dc, rest_api):
    """
    Test basic operations on `v2/client-routes` endpoint
    """
    def to_tuple(e):
        return (
            e["connection_id"], e["host_id"], e["address"], e["port"], e["tls_port"],
            e["alternator_port"], e["alternator_https_port"]
        )

    def json_to_set(entries):
        s = set()
        for e in entries:
            s.add(to_tuple(e))
        return s

    json_entries = [make_entry(i, i+1) for i in range(4)]
    resp = rest_api.send("POST", "v2/client-routes", json_body=json_entries)
    resp.raise_for_status()

    def get_response_set():
        response = cql.execute("SELECT * FROM system.client_routes;")
        return set([(str(row.connection_id), str(row.host_id), row.address, row.port, row.tls_port, row.alternator_port, row.alternator_https_port) for row in response])

    assert get_response_set() == json_to_set(json_entries)

    # Test GET API
    resp = rest_api.send("GET", "v2/client-routes")
    resp.raise_for_status()
    assert json_to_set(resp.json()) == json_to_set(json_entries)

    # Upsert do nothing (send same first entry again via JSON body)
    first_entry = json_entries[0]
    resp = rest_api.send("POST", "v2/client-routes", json_body=[first_entry])
    resp.raise_for_status()
    assert get_response_set() == json_to_set(json_entries)

    # Updating address and port fields
    updated_first_entry = make_entry(0, 999)
    json_entries[0] = updated_first_entry
    resp = rest_api.send("POST", "v2/client-routes", json_body=[updated_first_entry])
    resp.raise_for_status()
    assert get_response_set() == json_to_set(json_entries)

    # Delete all
    for json_entry in json_entries:
        resp = rest_api.send("DELETE", "v2/client-routes", json_body=[json_entry])
        resp.raise_for_status()
    assert get_response_set() == set()

def test_client_routes_optional_ports(cql, this_dc, rest_api):
    """
    Verify that omitting some port fields (e.g., alternator_https_port) succeeds and those fields are absent in GET.
    """
    entry = {
        "connection_id": "00000000-0000-0000-0000-000000000001",
        "host_id": "00000000-0000-0000-0000-000000000002",
        "address": "opt.test",
        "port": 7001,
    }
    # Intentionally omit tls_port / alternator_port / alternator_https_port
    resp = rest_api.send("POST", "v2/client-routes", json_body=[entry])
    resp.raise_for_status()

    # Fetch raw rows via CQL
    rs = cql.execute(f"SELECT * FROM system.client_routes WHERE connection_id='{entry['connection_id']}' AND host_id={entry['host_id']};")
    row = rs.one()
    assert row is not None
    assert row.port == entry["port"]
    assert row.tls_port is None
    assert row.alternator_port is None
    assert row.alternator_https_port is None

    # GET endpoint should not serialize missing ports
    resp = rest_api.send("GET", "v2/client-routes")
    resp.raise_for_status()
    found = False
    for obj in resp.json():
        if obj["connection_id"] == entry["connection_id"] and obj["host_id"] == entry["host_id"]:
            found = True
            assert obj.get("port") == entry["port"]
            assert "tls_port" not in obj
            assert "alternator_port" not in obj
            assert "alternator_https_port" not in obj
            break
    assert found, "Inserted entry not returned by GET"

    resp = rest_api.send("DELETE", "v2/client-routes", json_body=[{"connection_id": entry["connection_id"], "host_id": entry["host_id"]}])
    resp.raise_for_status()

def test_client_routes_port_ranges(cql, this_dc, rest_api):
    """
    Test that ports within the 0-65535 range are accepted and others are rejected.
    """
    def entry_for_port(port):
        return {
            "connection_id": "00000000-0000-0000-0000-000000000001",
            "host_id": "00000000-0000-0000-0000-000000000002",
            "address": "test",
            "port": port,
            "tls_port": port,
            "alternator_port": port,
            "alternator_https_port": port,
        }

    for port in [1, 100, 65535]:
        resp = rest_api.send("POST", "v2/client-routes", json_body=[entry_for_port(port)])
        resp.raise_for_status()

    for port in [-1, 0, 65536, 1000000]:
        resp = rest_api.send("POST", "v2/client-routes", json_body=[entry_for_port(port)])
        assert resp.status_code == 400
        assert "outside the allowed port range" in resp.content.decode('utf-8')

    # Cleanup after the test
    resp = rest_api.send("DELETE", "v2/client-routes", json_body=[entry_for_port(0)])

def test_long_client_routes(cql, this_dc, rest_api):
    """
    Verify that `v2/client-routes` can handle very large inputs in a single request
    """
    number_of_entries = 10001
    json_entries = [make_entry(i, i+1) for i in range(number_of_entries)]

    # Test POST
    resp = rest_api.send("POST", "v2/client-routes", json_body=json_entries)
    resp.raise_for_status()
    rs = list(cql.execute(f"SELECT * FROM system.client_routes"))
    assert len(rs) == number_of_entries

    # Test GET
    resp = rest_api.send("GET", "v2/client-routes")
    resp.raise_for_status()
    assert len(resp.json()) == number_of_entries

    # Test DELETE
    resp = rest_api.send("DELETE", "v2/client-routes", json_body=json_entries)
    resp.raise_for_status()
    rs = list(cql.execute(f"SELECT * FROM system.client_routes"))
    assert len(rs) == 0

def test_client_routes_null_terminators(cql, this_dc, rest_api):
    """
    Handling embedded null terminators in C++ is tricky. Constructing a valid string
    with a null byte in the middle requires using both GetString() and GetStringLength()
    in RapidJSON to avoid truncation. This test verifies that v2/client-routes handles
    such values correctly.
    """
    string_with_null = "first\x00second"  # contains a literal NUL between 'first' and 'second'
    entry = {
        "connection_id": string_with_null,
        "host_id": "00000000-0000-0000-0000-000000000002",
        "address": string_with_null,
        "port": 7001,
    }
    rest_api.send("POST", "v2/client-routes", json_body=[entry])
    resp_json = rest_api.send("GET", "v2/client-routes").json()
    rs = list(cql.execute("SELECT * FROM system.client_routes"))
    assert rs[0].connection_id == resp_json[0]["connection_id"] == string_with_null
    assert rs[0].address == resp_json[0]["address"] == string_with_null

    resp = rest_api.send("DELETE", "v2/client-routes", json_body=[entry])
    resp.raise_for_status()
    assert len(list(cql.execute("SELECT * FROM system.client_routes"))) == 0
