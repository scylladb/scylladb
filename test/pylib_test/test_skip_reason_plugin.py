#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests for the skip_reason_plugin.

Uses pytester to run sub-pytest processes that exercise the typed skip
markers, bare-skip warnings, and report enrichment.
"""

import textwrap

import pytest

pytest_plugins = ["pytester"]


# Base conftest that every sub-pytest process needs: define the enum,
# configure the plugin, and register it.
_BASE_CONFTEST = textwrap.dedent("""\
    import enum
    import pytest
    from test.pylib.skip_reason_plugin import SkipReasonPlugin
    from test.pylib.skip_types import SkipType as ProjectSkipType

    class SkipType(enum.StrEnum):
        SKIP_BUG = "bug"
        SKIP_SLOW = "slow"
        SKIP_ENV = "env"

        get_reason_default = staticmethod(ProjectSkipType.get_reason_default)
        get_reason_skip_bug = staticmethod(ProjectSkipType.get_reason_skip_bug)

        @property
        def marker_name(self):
            return self.name.lower()

    def pytest_configure(config):
        config.pluginmanager.register(SkipReasonPlugin(SkipType))
""")


@pytest.fixture
def skippytest(pytester: pytest.Pytester) -> pytest.Pytester:
    """Pytester with skip_reason_plugin loaded and sugar/xdist disabled."""
    pytester.makeconftest(_BASE_CONFTEST)
    pytester.makeini(
        "[pytest]\n"
        "addopts = -p no:sugar -p no:xdist\n"
        "asyncio_default_fixture_loop_scope = session\n"
    )
    return pytester


# -- Typed markers ----------------------------------------------------------

def test_skip_bug_marker_skips_with_prefix(skippytest):
    skippytest.makepyfile(f"""
        import pytest
        @pytest.mark.skip_bug(
            link="https://github.com/scylladb/scylladb/issues/99999",
            reason="Known tablet scheduler crash",
        )
        def test_target():
            assert False
    """)
    result = skippytest.runpytest("-rs")
    result.assert_outcomes(skipped=1)
    out = result.stdout.str()
    assert "[bug]" in out
    assert "Known tablet scheduler crash" in out
    assert "https://github.com/scylladb/scylladb/issues/99999" in out


@pytest.mark.parametrize("marker, skip_type, reason", [
    ("skip_env", "env", "need --special-flag"),
])
def test_non_bug_typed_marker_skips_with_prefix(skippytest, marker, skip_type, reason):
    skippytest.makepyfile(f"""
        import pytest
        @pytest.mark.{marker}(reason=\"{reason}\")
        def test_target():
            assert False
    """)
    result = skippytest.runpytest("-rs")
    result.assert_outcomes(skipped=1)
    out = result.stdout.str()
    assert f"[{skip_type}]" in out
    assert reason in out


def test_skip_bug_positional_reason_rejected(skippytest):
    """Old positional form for skip_bug must be rejected."""
    skippytest.makepyfile("""
        import pytest
        @pytest.mark.skip_bug("scylladb/scylladb#55555")
        def test_positional():
            assert False
    """)
    result = skippytest.runpytest()
    result.stderr.fnmatch_lines(["*no longer accepts positional arguments*"])
    assert result.ret != 0


@pytest.mark.parametrize("marker, skip_type", [
    ("skip_env", "env"),
])
def test_typed_marker_positional_reason_accepted(skippytest, marker, skip_type):
    """Non-bug typed markers still accept a positional reason for backwards compatibility."""
    skippytest.makepyfile(f"""
        import pytest
        @pytest.mark.{marker}("positional reason")
        def test_positional():
            pass
    """)
    result = skippytest.runpytest("-rs")
    result.assert_outcomes(skipped=1)
    out = result.stdout.str()
    assert f"[{skip_type}]" in out
    assert "positional reason" in out


# -- Missing reason ---------------------------------------------------------

@pytest.mark.parametrize("marker", ["skip_env"])
def test_missing_reason_is_rejected(skippytest, marker):
    skippytest.makepyfile(f"""
        import pytest
        @pytest.mark.{marker}()
        def test_no_reason():
            pass
    """)
    result = skippytest.runpytest()
    result.stderr.fnmatch_lines(["*requires a 'reason' argument*"])
    assert result.ret != 0


def test_skip_bug_requires_link_and_reason(skippytest):
    skippytest.makepyfile("""
        import pytest
        @pytest.mark.skip_bug(reason="Some explanation")
        def test_missing_link():
            pass
    """)
    result = skippytest.runpytest()
    result.stderr.fnmatch_lines(["*requires both 'link' and 'reason'*"])
    assert result.ret != 0


def test_skip_bug_invalid_link_rejected(skippytest):
    skippytest.makepyfile("""
        import pytest
        @pytest.mark.skip_bug(link="https://example.com/123", reason="Broken behavior")
        def test_invalid_link():
            pass
    """)
    result = skippytest.runpytest()
    result.stderr.fnmatch_lines(["*invalid 'link' value*"])
    assert result.ret != 0


# -- Bare skip rejection -----------------------------------------------------

def test_bare_skip_rejected_and_lists_alternatives(skippytest):
    """Bare skip must be rejected with UsageError listing all typed alternatives."""
    skippytest.makepyfile("""
        import pytest
        @pytest.mark.skip(reason="some bare reason")
        def test_bare():
            pass
    """)
    result = skippytest.runpytest()
    result.stderr.fnmatch_lines(["*Untyped skip*some bare reason*"])
    assert result.ret != 0
    out = result.stderr.str()
    for m in ("skip_bug", "skip_env"):
        assert m in out, f"expected '{m}' in error output"


def test_bare_skip_in_pytest_param_rejected(skippytest):
    skippytest.makepyfile("""
        import pytest
        @pytest.mark.parametrize("x", [
            pytest.param(1, id="ok"),
            pytest.param(2, id="skipped",
                         marks=[pytest.mark.skip(reason="bare in param")]),
        ])
        def test_p(x):
            pass
    """)
    result = skippytest.runpytest()
    result.stderr.fnmatch_lines(["*Untyped skip*bare in param*"])
    assert result.ret != 0


def test_typed_skip_does_not_reject(skippytest):
    skippytest.makepyfile("""
        import pytest
        @pytest.mark.skip_bug(
            link="https://github.com/scylladb/scylladb/issues/11111",
            reason="Known issue in validation pipeline",
        )
        def test_typed():
            pass
    """)
    result = skippytest.runpytest()
    result.assert_outcomes(skipped=1)
    assert "Untyped skip" not in result.stderr.str()


# -- Runtime skip helper ----------------------------------------------------

def test_runtime_skip_helper(skippytest):
    skippytest.makepyfile("""
        from test.pylib.skip_reason_plugin import skip
        from conftest import SkipType
        def test_runtime():
            skip("missing dependency", skip_type=SkipType.SKIP_ENV)
    """)
    result = skippytest.runpytest("-rs")
    result.assert_outcomes(skipped=1)
    out = result.stdout.str()
    assert "[env]" in out
    assert "missing dependency" in out


def test_runtime_skip_populates_junit(skippytest, tmp_path):
    """Runtime skip() must produce skip_type/skip_reason in JUnit XML."""
    skippytest.makepyfile("""
        from test.pylib.skip_reason_plugin import skip
        from conftest import SkipType
        def test_rt():
            skip("no HTTPS", skip_type=SkipType.SKIP_ENV)
    """)
    xml_path = tmp_path / "report.xml"
    result = skippytest.runpytest(f"--junitxml={xml_path}")
    result.assert_outcomes(skipped=1)

    xml = xml_path.read_text()
    assert 'name="skip_type"' in xml
    assert 'value="env"' in xml
    assert "no HTTPS" in xml


# -- JUnit XML enrichment ---------------------------------------------------

def test_junit_xml_contains_skip_type(skippytest, tmp_path):
    skippytest.makepyfile("""
        import pytest
        @pytest.mark.skip_bug(
            link="https://github.com/scylladb/scylladb/issues/77777",
            reason="Known issue in compaction path",
        )
        def test_bug():
            pass
    """)
    xml_path = tmp_path / "report.xml"
    result = skippytest.runpytest(f"--junitxml={xml_path}")
    result.assert_outcomes(skipped=1)

    xml = xml_path.read_text()
    assert 'name="skip_type"' in xml
    assert 'value="bug"' in xml
    assert 'name="skip_reason"' in xml
    assert "Known issue in compaction path" in xml
    assert "https://github.com/scylladb/scylladb/issues/77777" in xml


def test_report_callback_is_invoked(pytester: pytest.Pytester, tmp_path):
    """The report_callback passed to SkipReasonPlugin must be called for skipped tests."""
    cb_path = tmp_path / "cb.txt"
    pytester.makeconftest(textwrap.dedent(f"""\
        import enum
        import pytest
        from pathlib import Path
        from test.pylib.skip_reason_plugin import SkipReasonPlugin
        from test.pylib.skip_types import SkipType as ProjectSkipType

        class SkipType(enum.StrEnum):
            SKIP_BUG = "bug"

            get_reason_default = staticmethod(ProjectSkipType.get_reason_default)
            get_reason_skip_bug = staticmethod(ProjectSkipType.get_reason_skip_bug)

            @property
            def marker_name(self):
                return self.name.lower()

        def _callback(skip_type, reason):
            Path("{cb_path}").write_text(f"{{skip_type}}:{{reason}}")

        def pytest_configure(config):
            config.pluginmanager.register(SkipReasonPlugin(SkipType, report_callback=_callback))
    """))
    pytester.makeini(
        "[pytest]\n"
        "addopts = -p no:sugar -p no:xdist\n"
        "asyncio_default_fixture_loop_scope = session\n"
    )
    pytester.makepyfile("""
        import pytest
        @pytest.mark.skip_bug(
            link="https://github.com/scylladb/scylladb/issues/44444",
            reason="Known issue in callback flow",
        )
        def test_cb():
            pass
    """)
    result = pytester.runpytest()
    result.assert_outcomes(skipped=1)
    assert cb_path.read_text() == "bug:Known issue in callback flow (https://github.com/scylladb/scylladb/issues/44444)"


# -- Typed marker + skip_mode interaction -----------------------------------

# Simulates runner.py's skip_mode: injects a skip marker and sets stash
# keys via a conftest hook that runs before the plugin (no trylast).
_SKIP_MODE_CONFTEST = _BASE_CONFTEST + textwrap.dedent("""\
    from test.pylib.skip_reason_plugin import skip_marker
    def pytest_collection_modifyitems(items):
        for item in items:
            if any(item.iter_markers("skip_mode")):
                skip_marker(item, "not supported in release", skip_type="mode")
""")


def test_typed_marker_with_skip_mode_populates_junit(skippytest, tmp_path):
    """When both a typed marker and skip_mode exist on the same test,
    JUnit XML must contain the typed skip metadata regardless of which
    skip marker pytest uses for -rs output.
    """
    skippytest.makeconftest(_SKIP_MODE_CONFTEST)
    skippytest.makepyfile("""
        import pytest
        @pytest.mark.skip_bug(
            link="https://github.com/scylladb/scylladb/issues/26844",
            reason="Tablet repair scheduler crashes",
        )
        @pytest.mark.skip_mode(mode="release", reason="no error injections")
        def test_both():
            assert False
    """)
    xml_path = tmp_path / "report.xml"
    result = skippytest.runpytest(f"--junitxml={xml_path}")
    result.assert_outcomes(skipped=1)

    # JUnit XML must have the typed skip metadata.
    xml = xml_path.read_text()
    assert 'value="bug"' in xml
    assert "Tablet repair scheduler crashes" in xml
    assert "https://github.com/scylladb/scylladb/issues/26844" in xml


def test_skip_mode_prefix_populates_junit(skippytest, tmp_path):
    """When runner.py's skip_mode sets stash keys directly,
    the report hook must populate JUnit XML with skip_type=mode.
    """
    skippytest.makeconftest(_SKIP_MODE_CONFTEST)
    skippytest.makepyfile("""
        import pytest
        @pytest.mark.skip_mode(mode="release", reason="not supported in release")
        def test_skip_mode_only():
            assert False
    """)
    xml_path = tmp_path / "report.xml"
    result = skippytest.runpytest(f"--junitxml={xml_path}")
    result.assert_outcomes(skipped=1)

    xml = xml_path.read_text()
    assert 'value="mode"' in xml
    assert "not supported in release" in xml


def test_bare_skip_with_skip_mode_no_rejection(skippytest):
    """When skip_mode uses skip_marker(), bare-skip rejection is suppressed
    for the item even if it also has a bare @pytest.mark.skip. The
    skip_marker() call signals the item already has a typed skip.
    """
    skippytest.makeconftest(_SKIP_MODE_CONFTEST)
    skippytest.makepyfile("""
        import pytest
        @pytest.mark.skip(reason="some bare reason")
        @pytest.mark.skip_mode(mode="release", reason="not supported in release")
        def test_both_bare_and_mode():
            assert False
    """)
    result = skippytest.runpytest()
    result.assert_outcomes(skipped=1)
    assert "Untyped skip" not in result.stderr.str()


# -- Conditional markers (skip_storage) --------------------------------------

# The project's own SkipType, so these exercise the shipped skip_storage
# definition rather than a stand-in, plus the storage fixture it selects on.
_STORAGE_CONFTEST = textwrap.dedent("""\
    import pytest
    from test.pylib.skip_reason_plugin import SkipReasonPlugin
    from test.pylib.skip_types import SkipType

    def pytest_configure(config):
        config.pluginmanager.register(SkipReasonPlugin(SkipType))

    @pytest.fixture(params=[None, "s3", "gs"], ids=["local", "s3", "gs"])
    def storage(request):
        return request.param
""")


@pytest.fixture
def storagepytest(pytester: pytest.Pytester) -> pytest.Pytester:
    pytester.makeconftest(_STORAGE_CONFTEST)
    pytester.makeini(
        "[pytest]\n"
        "addopts = -p no:sugar -p no:xdist\n"
        "asyncio_default_fixture_loop_scope = session\n"
    )
    return pytester


def test_skip_storage_skips_only_the_named_flavor(storagepytest):
    storagepytest.makepyfile("""
        import pytest
        @pytest.mark.skip_storage('gs', reason='GCS gets stuck')
        def test_t(storage):
            pass
    """)
    result = storagepytest.runpytest()
    result.assert_outcomes(passed=2, skipped=1)


def test_skip_storage_skips_every_named_flavor(storagepytest):
    """Local is not a named flavor, so it keeps running."""
    storagepytest.makepyfile("""
        import pytest
        @pytest.mark.skip_storage('s3', 'gs', reason='both backends abort')
        def test_t(storage):
            pass
    """)
    result = storagepytest.runpytest()
    result.assert_outcomes(passed=1, skipped=2)


def test_skip_storage_reports_the_storage_type(storagepytest):
    storagepytest.makepyfile("""
        import pytest
        @pytest.mark.skip_storage('gs', reason='GCS gets stuck')
        def test_t(storage):
            pass
    """)
    result = storagepytest.runpytest("-rs")
    result.stdout.fnmatch_lines(["*[[]storage[]] GCS gets stuck*"])


def test_skip_storage_validated_on_flavors_it_does_not_name(storagepytest):
    """Validation precedes the predicate, so a malformed marker cannot hide
    behind the parametrizations it would not have skipped.
    """
    storagepytest.makepyfile("""
        import pytest
        @pytest.mark.skip_storage('gs')
        def test_t(storage):
            pass
    """)
    result = storagepytest.runpytest()
    result.stderr.fnmatch_lines(["*requires a 'reason' keyword argument*"])
    assert result.ret != 0


def test_skip_storage_requires_positional_flavors(storagepytest):
    storagepytest.makepyfile("""
        import pytest
        @pytest.mark.skip_storage(reason='no flavors named')
        def test_t(storage):
            pass
    """)
    result = storagepytest.runpytest()
    result.stderr.fnmatch_lines(["*takes the storage flavors positionally*"])
    assert result.ret != 0


def test_skip_storage_without_the_storage_fixture_skips_nothing(storagepytest):
    """A test with no storage parametrization has no flavor to match."""
    storagepytest.makepyfile("""
        import pytest
        @pytest.mark.skip_storage('gs', reason='GCS gets stuck')
        def test_t():
            pass
    """)
    result = storagepytest.runpytest()
    result.assert_outcomes(passed=1)
