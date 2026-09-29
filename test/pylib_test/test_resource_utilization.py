#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from datetime import datetime, timedelta

import pytest

from test import HOST_ID
# Test is renamed on import: a Test* class in a test module is one pytest tries to collect.
from test.pylib.db.model import HostInfo, Metric, SystemResourceMetric, Test as RecordedTest
from test.pylib.db.writer import (
    DEFAULT_DB_NAME,
    HOST_INFO_TABLE,
    METRICS_TABLE,
    SYSTEM_RESOURCE_METRICS_TABLE,
    TESTS_TABLE,
    SQLiteWriter,
)
from test.pylib.resource_gather import summarize_resource_utilization

RAM_BYTES = 100
RUN_START = datetime(2026, 9, 22, 10, 0, 0)


def write_samples(tmp_path, cpu_percents: list[float], test_window: tuple[int, int] | None = (0, 1000)) -> None:
    """Fill a metrics database with one sample per second, from RUN_START.

    Memory usage mirrors the CPU one, so both summaries can be asserted at once.
    test_window is the offset in seconds of the first test's start and the last test's
    end, or None for a database with no test in it.
    """
    sqlite_writer = SQLiteWriter(tmp_path / DEFAULT_DB_NAME)
    sqlite_writer.write_row(
        HostInfo(host_id=HOST_ID, cpu_model='test', cpu_cores=1, ram_bytes=RAM_BYTES), HOST_INFO_TABLE)

    if test_window is not None:
        test_id = sqlite_writer.write_row(
            RecordedTest(host_id=HOST_ID, architecture='x86_64', path='test/pylib_test',
                         file='test_x.py', mode='dev', run_id=1, test_name='test_x'),
            TESTS_TABLE)
        start, end = test_window
        sqlite_writer.write_row(
            Metric(test_id=test_id,
                   time_start=RUN_START + timedelta(seconds=start),
                   time_end=RUN_START + timedelta(seconds=end)),
            METRICS_TABLE)

    for second, cpu in enumerate(cpu_percents):
        sqlite_writer.write_row(
            SystemResourceMetric(
                host_id=HOST_ID,
                cpu=cpu,
                memory_free=0,
                memory_available=int(RAM_BYTES - cpu),
                memory_used=int(cpu),
                memory_active=0,
                memory_inactive=0,
                memory_buffers=0,
                timestamp=RUN_START + timedelta(seconds=second),
            ),
            SYSTEM_RESOURCE_METRICS_TABLE)
    sqlite_writer.close()


def test_summarize_resource_utilization(tmp_path):
    write_samples(tmp_path, [float(percent) for percent in range(0, 100, 10)])

    record = summarize_resource_utilization(tmp_path)

    assert record is not None
    assert record.samples == 10
    # Over 0, 10, ... 90: the median sits between 40 and 50, the percentiles are
    # interpolated at 0.95 * 9 and 0.99 * 9, and 80 and 90 are the two samples that
    # scored, being the ones inside the band.
    assert record.cpu_avg == pytest.approx(45)
    assert record.cpu_median == pytest.approx(45)
    assert record.cpu_p95 == pytest.approx(85.5)
    assert record.cpu_p99 == pytest.approx(89.1)
    assert record.cpu_score == pytest.approx(20)
    # memory_available is RAM_BYTES - cpu, so the used percentage repeats the CPU one.
    assert record.memory_avg == pytest.approx(45)
    assert record.memory_median == pytest.approx(45)
    assert record.memory_p95 == pytest.approx(85.5)
    assert record.memory_p99 == pytest.approx(89.1)
    assert record.memory_score == pytest.approx(20)


def test_samples_outside_the_test_window_are_ignored(tmp_path):
    # An idle machine before the first test and after the last one: seconds 0-2 and 7-9
    # are sampled outside the window, and only the 85% in between should be summarized.
    write_samples(tmp_path, [0.0, 0.0, 0.0] + [85.0] * 4 + [0.0, 0.0, 0.0], test_window=(3, 6))

    record = summarize_resource_utilization(tmp_path)

    assert record is not None
    assert record.samples == 4
    assert record.cpu_avg == pytest.approx(85)
    assert record.cpu_median == pytest.approx(85)
    assert record.cpu_score == pytest.approx(100)


def test_summary_falls_back_when_nothing_was_sampled_while_testing(tmp_path):
    # Every sample lands after the only test ended, which is what a run too short to be
    # sampled twice looks like. The record still describes what was measured.
    write_samples(tmp_path, [30.0, 40.0], test_window=(10, 20))

    record = summarize_resource_utilization(tmp_path)

    assert record is not None
    assert record.samples == 2
    assert record.cpu_avg == pytest.approx(35)


def test_a_session_that_ran_no_test_gets_no_record(tmp_path):
    # A selection that matched nothing, or a run that died during collection: the
    # samples describe an idle machine rather than a test run, and the record would
    # carry no mode to compare it against.
    write_samples(tmp_path, [30.0, 40.0], test_window=None)

    assert summarize_resource_utilization(tmp_path) is None


def test_summarize_single_sample(tmp_path):
    write_samples(tmp_path, [42.0])

    record = summarize_resource_utilization(tmp_path)

    assert record is not None
    assert (record.cpu_avg, record.cpu_median, record.cpu_p95, record.cpu_p99) == (42.0,) * 4
    assert record.cpu_score == pytest.approx(0)


def test_summarize_without_samples(tmp_path):
    assert summarize_resource_utilization(tmp_path) is None

    write_samples(tmp_path, [])
    assert summarize_resource_utilization(tmp_path) is None
