import requests
import urllib.parse
import base64

def test_sync_point_checksum(rest_api):
    resp = rest_api.send('POST', "hinted_handoff/sync_point")
    sync_point = resp.json()
    # Decode the sync_point to bytes to ensure that every modification changes the data
    # (multiple base64 encoded strings may represent a single binary value)
    sync_point_b = base64.b64decode(sync_point.encode('ascii'))

    resp = rest_api.send('GET', "hinted_handoff/sync_point", { "id": urllib.parse.quote(sync_point) })
    assert resp.ok

    # Modify each sync_point's byte (except the first one) and send an incorrect request
    # The first byte representing version is omitted, because changing it causes a different error
    for i in range(1, len(sync_point_b)):
        bad_sync_point_b = sync_point_b[:i] + bytes([(sync_point_b[i] + 1) % 255]) + sync_point_b[i + 1:]
        bad_sync_point = base64.b64encode(bad_sync_point_b).decode('ascii')

        # Expect that checksum is different
        resp = rest_api.send('GET', "hinted_handoff/sync_point", { "id": urllib.parse.quote(bad_sync_point) })
        assert resp.status_code == requests.codes.bad_request
        assert "wrong checksum" in resp.json()['message']
