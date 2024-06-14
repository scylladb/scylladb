#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.nodetool.rest_api_mock import expected_request
from enum import Enum

import pytest


class operating_mode(Enum):
    NONE = "STARTING"
    STARTING = "STARTING"
    NORMAL = "NORMAL"
    JOINING = "JOINING"
    BOOTSTRAP = "BOOTSTRAP"
    LEAVING = "LEAVING"
    DECOMMISSIONED = "DECOMMISSIONED"
    MOVING = "MOVING"
    DRAINING = "DRAINING"
    DRAINED = "DRAINED"
    MAINTENANCE = "MAINTENANCE"


class stream_state(Enum):
    INITIALIZED = "INITIALIZED"
    PREPARING = "PREPARING"
    STREAMING = "STREAMING"
    WAIT_COMPLETE = "WAIT_COMPLETE"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


class direction(Enum):
    IN = "IN"
    OUT = "OUT"


class test_flag(Enum):
    normal = 0
    starting_mode = 1
    private_ip = 2
    one_stream = 3
    no_streams = 4
    human_readable_short = 5
    human_readable_long = 6


class SessionSummary():

    def __init__(self, summaries, files):
        self.total_count = 0
        self.total_size = 0
        self.done_count = 0
        self.done_size = 0

        for cf in summaries:
            self.total_count += cf["files"]
            self.total_size += cf["total_size"]

        for file in files:
            if file["current_bytes"] == file["total_bytes"]:
                self.done_count += 1
            self.done_size += file["current_bytes"]


def _is_starting(mode):
    return mode == operating_mode.NONE or mode == operating_mode.STARTING


def _format_bytes(v, human_readable):
    if v < 1024 or not human_readable:
        return f"{v} bytes"

    # Not general, but good enough for this test
    return "{:.2f} KiB".format(v / 1024)


def _check_output(
        res,
        human_readable,
        mode,
        streams,
        read_repair_attempted,
        read_repair_repaired_blocking,
        read_repair_repaired_background,
        messages_pending,
        messages_sent,
        messages_dropped,
        messages_respond_pending,
        messages_respond_completed):
    lines = res.split("\n")

    i = 0
    assert lines[i] == f"Mode: {mode.value}"

    if streams:
        streams_by_id = {s["plan_id"]: s for s in streams}
        # Streams are printed out-of-order
        while streams_by_id:
            i += 1
            description, plan_id = lines[i].split()
            assert plan_id in streams_by_id
            stream = streams_by_id[plan_id]
            assert description == stream["description"]

            for session in stream["sessions"]:
                i += 1
                if session["peer"] == session["connecting"]:
                    assert lines[i] == f'    /{session["peer"]}'
                else:
                    assert lines[i] == f'    /{session["peer"]} (using /{session["connecting"]})'

                for summaries, files, action_continuous, action_perfect, target in (
                        (session["receiving_summaries"], session["receiving_files"], "Receiving", "received", "from"),
                        (session["sending_summaries"], session["sending_files"], "Sending", "sent", "to")):
                    if not summaries:
                        continue
                    i += 1
                    files = [f["value"] for f in files]
                    summary = SessionSummary(summaries, files)
                    assert lines[i] == "        {} {} files, {} total. Already {} {} files, {} total".format(
                            action_continuous,
                            summary.total_count,
                            _format_bytes(summary.total_size, human_readable),
                            action_perfect,
                            summary.done_count,
                            _format_bytes(summary.done_size, human_readable))
                    for file in files:
                        i += 1
                        assert lines[i] == '            {} {}/{} bytes({}%) {} {} idx:{}/{}'.format(
                                file["file_name"],
                                file["current_bytes"],
                                file["total_bytes"],
                                int(file["current_bytes"]/file["total_bytes"] * 100),
                                action_perfect,
                                target,
                                file["session_index"],
                                file["peer"])

            del streams_by_id[plan_id]
    else:
        i += 1
        assert lines[i] == "Not sending any streams."

    i += 1

    if _is_starting(mode):
        assert lines[i] == ""
        assert i == len(lines) - 1
        return
    else:
        assert lines[i] == "Read Repair Statistics:"

    i += 1
    assert lines[i] == f"Attempted: {read_repair_attempted}"
    i += 1
    assert lines[i] == f"Mismatch (Blocking): {read_repair_repaired_blocking}"
    i += 1
    assert lines[i] == f"Mismatch (Background): {read_repair_repaired_background}"

    line_fmt = "{:<25}{:>10}{:>10}{:>15}{:>10}"

    i += 1
    assert lines[i] == line_fmt.format("Pool Name", "Active", "Pending", "Completed", "Dropped")

    def sum_nodes(nodes):
        return sum([n["value"] for n in nodes])

    i += 1
    assert lines[i] == line_fmt.format("Large messages", "n/a", sum_nodes(messages_pending),
                                       sum_nodes(messages_sent), 0)
    i += 1
    assert lines[i] == line_fmt.format("Small messages", "n/a", sum_nodes(messages_respond_pending),
                                       sum_nodes(messages_respond_completed), 0)
    i += 1
    assert lines[i] == line_fmt.format("Gossip messages", "n/a", 0, 0, 0)

    i += 1
    assert lines[i] == ""
    assert i == len(lines) - 1


@pytest.mark.parametrize("flag", (
    test_flag.normal,
    test_flag.starting_mode,
    test_flag.private_ip,
    test_flag.one_stream,
    test_flag.no_streams,
    test_flag.human_readable_short,
    test_flag.human_readable_long))
def test_netstats(nodetool, flag):
    mode = operating_mode.STARTING if flag == test_flag.starting_mode else operating_mode.NORMAL
    streams = [
        {
            "plan_id": "1e77eb26-a372-4eb4-aeaa-72f224cf6b4c",
            "description": "description1",
            "sessions": [
                {
                    "peer": "127.0.0.1",
                    "session_index": 1,
                    "connecting": "127.1.0.1",
                    "receiving_summaries": [
                        {
                            "cf_id": "eee7eb26-a372-4eb4-aeaa-72f224cf6b4c",
                            "files": 3,
                            "total_size": 13832,
                        },
                        {
                            "cf_id": "fff7eb26-a372-4eb4-aeaa-72f224cf6b4c",
                            "files": 2,
                            "total_size": 3832,
                        },
                    ],
                    "sending_summaries": [
                        {
                            "cf_id": "eee7eb26-a372-4eb4-aeaa-72f224cf6b4c",
                            "files": 1,
                            "total_size": 3832,
                        },
                        {
                            "cf_id": "fff7eb26-a372-4eb4-aeaa-72f224cf6b4c",
                            "files": 1,
                            "total_size": 3832,
                        },
                    ],
                    "state": stream_state.STREAMING.value,
                    "receiving_files": [
                        {
                            "key": "file1",
                            "value": {
                                "peer": "127.0.0.1",
                                "session_index": 1,
                                "file_name": "me-3ge2_0ly2_3e65c2erzwxcn7tws3-big-Data.db",
                                "direction": direction.IN.value,
                                "current_bytes": 134,
                                "total_bytes": 9603,
                            },
                        },
                        {
                            "key": "file2",
                            "value": {
                                "peer": "127.0.0.1",
                                "session_index": 1,
                                "file_name": "me-3ge2_0ly2_3e65c2erzwxcn7tws3-big-Index.db",
                                "direction": direction.IN.value,
                                "current_bytes": 963,
                                "total_bytes": 963,
                            },
                        },
                    ],
                    "sending_files": [
                        {
                            "key": "file3",
                            "value": {
                                "peer": "127.0.0.1",
                                "session_index": 1,
                                "file_name": "me-3ge2_0ly2_3we0g2vlf404p2fbjn-big-Index.db",
                                "direction": direction.OUT.value,
                                "current_bytes": 34,
                                "total_bytes": 603,
                            },
                        },
                        {
                            "key": "file4",
                            "value": {
                                "peer": "127.0.0.1",
                                "session_index": 1,
                                "file_name": "me-3ge2_0ly2_3we0g2vlf404p2fbjn-big-Filter.db",
                                "direction": direction.OUT.value,
                                "current_bytes": 63,
                                "total_bytes": 63,
                            },
                        },
                    ],
                },
            ],
        },
        {
            "plan_id": "2e87eb26-a372-4af4-aeaa-72f224cf6b4c",
            "description": "description2",
            "sessions": [
                {
                    "peer": "127.0.0.3",
                    "session_index": 2,
                    "connecting": "127.1.0.3",
                    "receiving_summaries": [],
                    "sending_summaries": [],
                    "state": stream_state.STREAMING.value,
                    "receiving_files": [],
                    "sending_files": [],
                },
                {
                    "peer": "127.0.0.3",
                    "session_index": 3,
                    "connecting": "127.1.0.3",
                    "receiving_summaries": [],
                    "sending_summaries": [],
                    "state": stream_state.STREAMING.value,
                    "receiving_files": [],
                    "sending_files": [],
                },
            ],
        }
    ]

    if flag == test_flag.private_ip:
        streams[0]["sessions"][0]["connecting"] = "52.65.9.1"
    elif flag == test_flag.one_stream:
        del streams[1]
    elif flag == test_flag.no_streams or flag == test_flag.starting_mode:
        streams.clear()

    read_repair_attempted = 78680
    read_repair_repaired_blocking = 78678
    read_repair_repaired_background = 2
    messages_pending = [
        {
            "key": "127.0.0.1",
            "value": 4,
        },
        {
            "key": "127.0.0.3",
            "value": 2,
        },
    ]
    messages_sent = [
        {
            "key": "127.0.0.1",
            "value": 48796,
        },
        {
            "key": "127.0.0.3",
            "value": 987,
        },
    ]
    messages_dropped = [
        {
            "key": "127.0.0.1",
            "value": 0,
        },
        {
            "key": "127.0.0.3",
            "value": 0,
        },
    ]
    messages_respond_pending = [
        {
            "key": "127.0.0.1",
            "value": 3,
        },
        {
            "key": "127.0.0.3",
            "value": 1,
        },
    ]
    messages_respond_completed = [
        {
            "key": "127.0.0.1",
            "value": 97870,
        },
        {
            "key": "127.0.0.3",
            "value": 980,
        },
    ]

    expected_requests = [
        expected_request("GET", "/storage_service/operation_mode", response=mode.value),
        expected_request("GET", "/stream_manager/", response=streams),
        expected_request("GET", "/storage_service/is_starting", response=_is_starting(mode)),
    ]

    if not _is_starting(mode):
        expected_requests += [
            expected_request("GET", "/storage_proxy/read_repair_attempted", response=read_repair_attempted),
            expected_request("GET", "/storage_proxy/read_repair_repaired_blocking",
                             response=read_repair_repaired_blocking),
            expected_request("GET", "/storage_proxy/read_repair_repaired_background",
                             response=read_repair_repaired_background),
            expected_request("GET", "/messaging_service/messages/pending", response=messages_pending),
            expected_request("GET", "/messaging_service/messages/sent", response=messages_sent),
            expected_request("GET", "/messaging_service/messages/dropped", response=messages_dropped,
                             multiple=expected_request.ANY),
            expected_request("GET", "/messaging_service/messages/respond_pending", response=messages_respond_pending),
            expected_request("GET", "/messaging_service/messages/respond_completed",
                             response=messages_respond_completed),
        ]

    args = ["netstats"]
    if flag == test_flag.human_readable_short:
        args.append("-H")
        human_readable = True
    elif flag == test_flag.human_readable_long:
        args.append("--human-readable")
        human_readable = True
    else:
        human_readable = False

    res = nodetool(*args, expected_requests=expected_requests)

    _check_output(
            res.stdout,
            human_readable,
            mode,
            streams,
            read_repair_attempted,
            read_repair_repaired_blocking,
            read_repair_repaired_background,
            messages_pending,
            messages_sent,
            messages_dropped,
            messages_respond_pending,
            messages_respond_completed,
    )
