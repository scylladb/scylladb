#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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

    class SkipType(enum.StrEnum):
        SKIP_BUG = "bug"
        SKIP_NOT_IMPLEMENTED = "not_implemented"
        SKIP_SLOW = "slow"
        SKIP_ENV = "env"
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

@pytest.mark.parametrize("marker, skip_type, reason", [
    ("skip_bug",             "bug",            "scylladb/scylladb#99999"),
    ("skip_not_implemented", "not_implemented", "feature X not built yet"),
    ("skip_slow",            "slow",            "takes 10 minutes"),
    ("skip_env",             "env",             "need --special-flag"),
], ids=["bug", "not_implemented", "slow", "env"])
def test_typed_marker_skips_with_prefix(skippytest, marker, skip_type, reason):
    skippytest.makepyfile(f"""
        import pytest
        @pytest.mark.{marker}(reason="{reason}")
        def test_target():
            assert False
    """)
    result = skippytest.runpytest("-rs")
    result.assert_outcomes(skipped=1)
    out = result.stdout.str()
    assert f"[{skip_type}]" in out
    assert reason in out


def test_typed_marker_positional_reason(skippytest):
    """Reason passed as a positional arg (not keyword) must also work."""
    skippytest.makepyfile("""
        import pytest
        @pytest.mark.skip_bug("scylladb/scylladb#55555")
        def test_positional():
            assert False
    """)
    result = skippytest.runpytest("-rs")
    result.assert_outcomes(skipped=1)
    out = result.stdout.str()
    assert "[bug]" in out
    assert "scylladb/scylladb#55555" in out


# -- Missing reason ---------------------------------------------------------

@pytest.mark.parametrize("marker", ["skip_bug", "skip_not_implemented"])
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


# -- Bare skip warning ------------------------------------------------------

def test_bare_skip_warns_and_lists_alternatives(skippytest):
    """Bare skip must warn and list all typed alternatives."""
    skippytest.makepyfile("""
        import pytest
        @pytest.mark.skip(reason="some bare reason")
        def test_bare():
            pass
    """)
    result = skippytest.runpytest("-W", "all")
    result.assert_outcomes(skipped=1)
    out = result.stdout.str()
    assert "Untyped skip" in out
    assert "some bare reason" in out
    for m in ("skip_bug", "skip_not_implemented", "skip_slow",
              "skip_env"):
        assert m in out, f"expected '{m}' in warning output"


def test_bare_skip_in_pytest_param_warns(skippytest):
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
    result = skippytest.runpytest("-W", "all")
    result.assert_outcomes(passed=1, skipped=1)
    assert "Untyped skip" in result.stdout.str()


def test_typed_skip_does_not_warn(skippytest):
    skippytest.makepyfile("""
        import pytest
        @pytest.mark.skip_bug(reason="scylladb/scylladb#11111")
        def test_typed():
            pass
    """)
    result = skippytest.runpytest("-W", "error::UserWarning")
    result.assert_outcomes(skipped=1)
    assert "Untyped skip" not in result.stdout.str()


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
        @pytest.mark.skip_bug(reason="scylladb/scylladb#77777")
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
    assert "scylladb/scylladb#77777" in xml


def test_report_callback_is_invoked(pytester: pytest.Pytester, tmp_path):
    """The report_callback passed to SkipReasonPlugin must be called for skipped tests."""
    cb_path = tmp_path / "cb.txt"
    pytester.makeconftest(textwrap.dedent(f"""\
        import enum
        import pytest
        from pathlib import Path
        from test.pylib.skip_reason_plugin import SkipReasonPlugin

        class SkipType(enum.StrEnum):
            SKIP_BUG = "bug"
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
        @pytest.mark.skip_bug(reason="scylladb/scylladb#44444")
        def test_cb():
            pass
    """)
    result = pytester.runpytest()
    result.assert_outcomes(skipped=1)
    assert cb_path.read_text() == "bug:scylladb/scylladb#44444"


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
        @pytest.mark.skip_bug(reason="scylladb/scylladb#26844")
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
    assert "scylladb/scylladb#26844" in xml


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


def test_bare_skip_with_skip_mode_no_warn(skippytest):
    """When skip_mode uses skip_marker(), bare-skip warning is suppressed
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
    result = skippytest.runpytest("-W", "all")
    result.assert_outcomes(skipped=1)
    out = result.stdout.str()
    assert "Untyped skip" not in out
