#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import importlib.metadata
import logging
import operator
import re
import unittest.mock
from collections.abc import Collection, Iterable
from functools import lru_cache
from typing import Literal, NamedTuple

import pytest
from cassandra.connection import DRIVER_NAME, DRIVER_VERSION
from packaging.requirements import Requirement
from packaging.version import Version

from dtest_config import DTestConfig
from tools.env import DTEST_REQUIRE

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.tools_unittest

ISSUE_PATTERN = re.compile(
    r"""
        \s*
        (
            (
                ((?P<user_id>[\w-]+)/)?
                (?P<repo_id>[\w-]+)
            )?
            \#
        |
            http(s?)://github.com/
            (?P<url_user_id>[\w-]+)/
            (?P<url_repo_id>[\w-]+)/
            (issues|pull)/
        )?
        (?P<id>\d+)
        \s*
        """,
    re.IGNORECASE | re.VERBOSE,
)

JIRA_PATTERN = re.compile(
    r"""
        \s*
        (
            jira:
            |
            https?://scylladb.atlassian.net/browse/
        )
        (?P<id>[\w-]+)
        \s*
    """,
    re.IGNORECASE | re.VERBOSE,
)


DEFAULT_GH_USER = "scylladb"
DEFAULT_GH_REPO = "scylladb"


def require(issue):
    return pytest.mark.require(issue=issue)


def requireif(condition: bool, issue):
    if condition:
        return require(issue=issue)
    else:
        return pytest.mark.noop


def scylla_mode(cassandra_dir, scylla_version):
    dtest_config = DTestConfig()
    dtest_config.cassandra_dir = cassandra_dir
    dtest_config.scylla_version = scylla_version
    return dtest_config.get_scylla_mode()


def get_version(cassandra_dir, scylla_version):
    dtest_config = DTestConfig()
    dtest_config.cassandra_dir = cassandra_dir
    dtest_config.scylla_version = scylla_version
    return Version(dtest_config.get_version_from_build())


def required_driver(*driver_requirements):
    """
    marker for setting required version of the driver for specific test

    @required_driver('scylla-driver>=3.24.5')
    def test_01():
        pass

    # pass a list, when test depends on different version on different drivers
    # keep in mind this list is used with OR only
    @required_driver('scylla-driver>=3.25.0', 'cassandra-driver==3.25.0')
    def test_01():
        pass

    """
    for req in driver_requirements:
        assert "scylla-driver" in req or "cassandra-driver" in req, "required_driver() is for cql drivers only"

    def check_requirement(requirement):
        # since both versions can be installed at the same time, we
        # first cross-check in the actual code the name of the driver
        is_scylla_driver = "scylla" in DRIVER_NAME.lower()
        if "scylla" in requirement and not is_scylla_driver:
            return False
        if "scylla" not in requirement and is_scylla_driver:
            return False
        req = Requirement(requirement)
        pkg_version = importlib.metadata.version(req.name)
        return pkg_version in req.specifier

    outcome = check_requirement(driver_requirements[0])
    for req in driver_requirements[1:]:
        outcome |= check_requirement(req)

    return pytest.mark.skipif(not outcome, reason=f"test expected: {driver_requirements}\ninstalled: {DRIVER_VERSION} - {DRIVER_NAME}")


class UnMarker:
    def __getattr__(self, item):
        # Return an marker remover
        if item[0] == "_":
            raise AttributeError("Marker name must NOT start with underscore")
        return unmark_if(item, condition=AlwaysTruePredicate())


unmark = UnMarker()


class MarkedLocals:
    """
    Should work with the eval() as the set of 'locals'. Will return
    true for any item keyword that begins with unmark. This should only work for
    marks set by 'unmarker', because you can't do `@pytest.mark.namewith:colon`.
    """

    def __init__(self, keys):
        self.keys = keys

    def __getitem__(self, item):
        return item in self.keys


def enable_with_features(features, enabled_features):
    for feature in features:
        if not feature:
            continue
        if negated := feature[0] == "!":
            feature = feature[1:]  # noqa: PLW2901
        if negated:
            disabled = feature in enabled_features
        else:
            disabled = feature not in enabled_features
        if disabled:
            return False
    return True


def cached_force_closed(*issues: str) -> frozenset[str]:
    return frozenset(parse_issue(s).normalized for s in issues)


class JiraIssue(NamedTuple):
    key: str

    @property
    def normalized(self) -> str:
        return f"jira:{self.key}"


class GitHubIssue(NamedTuple):
    user: str
    repo: str
    number: int

    @property
    def normalized(self) -> str:
        return f"{self.user}/{self.repo}#{self.number}"


def parse_issue(s: str = "") -> GitHubIssue | JiraIssue:
    s = s.strip()
    if not s:
        raise ValueError("empty issue reference")

    if m := JIRA_PATTERN.match(s):
        key = m.groupdict()["id"]
        return JiraIssue(key=key)

    if m := ISSUE_PATTERN.match(s):
        g = m.groupdict()
        user = g.get("user_id") or g.get("url_user_id") or DEFAULT_GH_USER
        repo = g.get("repo_id") or g.get("url_repo_id") or DEFAULT_GH_REPO
        raw_id = g["id"]
        if not raw_id.isdigit():
            raise ValueError(f"invalid GitHub id: {raw_id!r}")
        return GitHubIssue(user=user, repo=repo, number=int(raw_id))

    raise ValueError(f"invalid issue reference: {s!r}")


# NOTE: the real ccm/dtest `check_issue_closed` queried GitHub and Jira (via
# tools.github_issues.GithubRepo / tools.jira_issues.JiraProject) to decide
# whether an issue referenced by @pytest.mark.require(...) is closed yet, and
# would `set_issue_label(...)` on it under --label-required-issues. Neither
# the `github` nor the `jira` client package is installed in this in-tree
# checkout, and collection here must not touch the network anyway. So this is
# an offline stand-in: it keeps validating the issue reference (still raises
# on a malformed one, same as before) but never looks anything up, and always
# reports the issue as closed -- i.e. @pytest.mark.require never skips a test
# based on issue state. `force_closed_issues` (from --consider-as-closed) is
# still honored since it is a purely local override.
def check_issue_closed(pattern, scylla_version: Version, force_closed_issues: Collection[str] | None = None, label_required_issues: bool = False):
    if pattern is None:
        logger.debug("check_issue_closed: no pattern")
        return False

    try:
        ref = parse_issue(pattern)
    except ValueError as e:
        logger.warning("invalid reference %r: %s; treating as closed", pattern, e)
        return True

    normalized = ref.normalized
    # Fast-path: do not call external services if user forced a close
    if force_closed_issues and normalized in force_closed_issues:
        logger.debug(f"check_issue_closed: {normalized} force-closed")

    return True


class RequirePredicate:
    """the base class to be used by 'pytest.mark.require(condition=...)'

    like:
    @pytest.mark.require(condition=(IssueClosed("scylladb/scylla-dtest#3951") &
                                    ~EnableWithFeature('tablets')))
    """

    def __bool__(self):
        raise NotImplementedError()

    def apply(self, **kwargs):
        raise NotImplementedError()


class AlwaysTruePredicate(RequirePredicate):
    def __bool__(self):
        return True

    def apply(self, **kwargs):
        pass


class BasePredicate(RequirePredicate):
    def apply(self, **kwargs):
        pass

    def __bool__(self):
        raise NotImplementedError()

    def __and__(self, other):
        return CompositePredicate(operator.__and__, self, other)

    def __or__(self, other):
        return CompositePredicate(operator.__or__, self, other)

    def __invert__(self):
        # `not predicate` evaluates `predicate` with `__bool__()` and then
        # negates the its result. but we want to postpone the evaluation
        # until `apply()` is called, as some predicates cannot be evaluated
        # until they are updated with more information provided by apply().
        # even if some predicates can be evaluated without apply(), it's
        # simpler if we always create a composite predicate object and always
        # # evaluate it when selecting tests instead when evaluating the
        # pytest.mark decorator, instead of differentiating the "early"
        # evaluation from "late" evaluation.
        return CompositePredicate(operator.__not__, self, None)


class CompositePredicate(BasePredicate):
    def __init__(self, op, lhs, rhs):
        self.op = op
        self.lhs = lhs
        self.rhs = rhs

    def apply(self, **kwargs):
        constants = (None, True, False)
        if self.lhs not in constants:
            self.lhs.apply(**kwargs)
        if self.rhs not in constants:
            self.rhs.apply(**kwargs)

    def __bool__(self):
        if self.rhs is None:
            assert self.op is operator.__not__
            return not bool(self.lhs)
        return self.op(bool(self.lhs), bool(self.rhs))


class IssueClosed(BasePredicate):
    def __init__(self, issue):
        self.issue = issue
        self.verbose = False
        self.nodeid = None
        self.scylla_version: Version | None = None
        self.config = None
        self.label_required_issues = False

    def apply(
        self,
        *,
        verbose: bool = False,
        nodeid: str | None = None,
        scylla_version: Version | None = None,
        config: object | None = None,
        label_required_issues: bool = False,
        **kwargs,
    ) -> None:
        self.verbose = verbose
        self.nodeid = nodeid
        self.scylla_version = scylla_version
        self.config = config
        self.label_required_issues = label_required_issues

    def __bool__(self):
        # DTEST_REQUIRE - auto : default value, check issue state in @pytest.mark.require marker and run(state=closed) or skip(state=open) test
        #               - enabled : skip tests marked with @pytest.mark.require
        #               - disabled : disable @pytest.mark.require decorator and run test (mostly for manual tests)
        if DTEST_REQUIRE == "disabled":
            # disable mark
            logger.info("DTEST_REQUIRE is disabled. Test will be run")
            return True

        if DTEST_REQUIRE == "enabled":
            # "enable" always skips the test
            logger.info("DTEST_REQUIRE is enabled. Test will be skipped")
            return False

        force_closed = cached_force_closed(*self.config.getoption("--consider-as-closed"))
        closed = check_issue_closed(self.issue, self.scylla_version, force_closed, self.label_required_issues)

        if self.verbose:
            message = "marked with closed issue " if closed else ""
            logger.info(f"* {self.nodeid} - {message}{self.issue}")

        if closed:
            logger.info("Issues %s closed. Test will be run", self.issue)
        return closed


class EnableWithFeature(BasePredicate):
    def __init__(self, *features):
        self.features = features
        self.enabled_features = []

    def apply(self, **kwargs):
        self.enabled_features = kwargs.get("enabled_features", [])

    def __bool__(self):
        return enable_with_features(self.features, self.enabled_features)


def skip_if(condition: RequirePredicate):
    return pytest.mark.skip_if(condition)


def unmark_if(*markers: str, condition: RequirePredicate):
    return pytest.mark.unmark_if(*markers, condition=condition)


# lowercase aliases so that one can use
# @pytest.mark.require(condition=(issue_closed("#3951") & ~with_feature('tablets')))
def issue_closed(*args, **kwargs):
    return IssueClosed(*args, **kwargs)


def issue_open(*args, **kwargs):
    return ~IssueClosed(*args, **kwargs)


def with_feature(*args, **kwargs):
    return EnableWithFeature(*args, **kwargs)


# tests for
#   pytest.mark.require(condition=issue_closed("#3951") & ~with_feature("tablets"))
#
# please use following commands to run them:
#   cd tools
#   pytest marks.py
def test_require_with_feature_positive():
    cond = with_feature("tablets")
    cond.apply(enabled_features="tablets,wavelets")
    assert cond


def test_require_with_feature_neg_op():
    cond = ~with_feature("tablets")
    cond.apply(enabled_features="tablets,wavelets")
    assert not cond


def test_require_with_feature_neg_literal():
    cond = with_feature("!tablets")
    cond.apply(enabled_features="tablets,wavelets")
    assert not cond


cfg = unittest.mock.MagicMock()
cfg.getoption.return_value = ()


@pytest.mark.parametrize("closed", [True, False])
def test_require_issue_closed(closed):
    with unittest.mock.patch(f"{__name__}.check_issue_closed", return_value=closed):
        cond = issue_closed("#24601")
        cond.apply(verbose=False, config=cfg)
        assert bool(cond) == closed


@pytest.mark.parametrize("closed", [True, False])
@pytest.mark.parametrize("negated", [True, False])
@pytest.mark.parametrize("enabled_features", ["tablets,wavelets", "wavelets", ""])
def test_require_composite_basic(closed, negated, enabled_features):
    with unittest.mock.patch(f"{__name__}.check_issue_closed", return_value=closed):
        feature = "tablets"
        feature_expr = feature
        if negated:
            feature_expr = f"!{feature}"
        cond = issue_closed("#24601") & with_feature(feature_expr)
        cond.apply(enabled_features=enabled_features, config=cfg)
        if negated:
            expected_result = closed and feature not in enabled_features.split(",")
        else:
            expected_result = closed and feature in enabled_features.split(",")
        assert bool(cond) == expected_result


@pytest.mark.parametrize("closed", [True, False])
def test_require_composite_lhs_negated(closed):
    with unittest.mock.patch(f"{__name__}.check_issue_closed", return_value=closed):
        feature = "tablets"
        cond = ~issue_closed("#24601") & with_feature(feature)
        enabled_features = "tablets,wavelets"
        cond.apply(enabled_features=enabled_features, config=cfg)
        expected_result = not closed and feature in enabled_features.split(",")
        assert bool(cond) == expected_result


@pytest.mark.parametrize("enabled_features", ["tablets,wavelets", "wavelets", ""])
def test_require_composite_rhs_negated(enabled_features):
    closed = True
    with unittest.mock.patch(f"{__name__}.check_issue_closed", return_value=closed):
        feature = "tablets"
        cond = issue_closed("#24601") & ~with_feature(feature)
        cond.apply(enabled_features=enabled_features, config=cfg)
        feature_is_enabled = feature in enabled_features.split(",")
        expected_result = closed and not feature_is_enabled
        assert bool(cond) == expected_result


# NOTE: the original test_branch_skip_labels/test_jira_issues integration tests
# asserted on the real, live state of specific GitHub/Jira issues. They were
# dropped here along with the network lookups they exercised (see
# check_issue_closed above) since there is nothing left to query.


@pytest.mark.parametrize(
    "raw, expected_type, expected_norm, exp",
    [
        # GitHub
        ("888", GitHubIssue, f"{DEFAULT_GH_USER}/{DEFAULT_GH_REPO}#888", {"user": DEFAULT_GH_USER, "repo": DEFAULT_GH_REPO, "number": 888}),
        ("#888", GitHubIssue, f"{DEFAULT_GH_USER}/{DEFAULT_GH_REPO}#888", {"user": DEFAULT_GH_USER, "repo": DEFAULT_GH_REPO, "number": 888}),
        ("my-repo#888", GitHubIssue, f"{DEFAULT_GH_USER}/my-repo#888", {"user": DEFAULT_GH_USER, "repo": "my-repo", "number": 888}),
        ("my_user/my_repo#888", GitHubIssue, "my_user/my_repo#888", {"user": "my_user", "repo": "my_repo", "number": 888}),
        ("user/repo#000123", GitHubIssue, "user/repo#123", {"user": "user", "repo": "repo", "number": 123}),
        ("http://github.com/u/r/issues/42", GitHubIssue, "u/r#42", {"user": "u", "repo": "r", "number": 42}),
        ("https://github.com/u/r/issues/42", GitHubIssue, "u/r#42", {"user": "u", "repo": "r", "number": 42}),
        ("https://github.com/u/r/pull/42", GitHubIssue, "u/r#42", {"user": "u", "repo": "r", "number": 42}),
        # JIRA
        ("jira:STAG-399", JiraIssue, "jira:STAG-399", {"key": "STAG-399"}),
        ("JIRA:STAG-399", JiraIssue, "jira:STAG-399", {"key": "STAG-399"}),
        ("https://scylladb.atlassian.net/browse/STAG-399", JiraIssue, "jira:STAG-399", {"key": "STAG-399"}),
        ("http://scylladb.atlassian.net/browse/STAG-399", JiraIssue, "jira:STAG-399", {"key": "STAG-399"}),
    ],
    ids=[
        # GH
        "gh-bare-number",
        "gh-hash-number",
        "gh-repo-hash-number",
        "gh-user-repo-hash-number",
        "gh-leading-zeros",
        "gh-url-http-issue",
        "gh-url-https-issue",
        "gh-url-pr",
        # JIRA
        "jira-prefix",
        "jira-prefix-uppercase",
        "jira-url-https",
        "jira-url-http",
    ],
)
def test_parse_issue_success(raw, expected_type, expected_norm, exp):
    ref = parse_issue(raw)

    assert isinstance(ref, expected_type)
    assert ref.normalized == expected_norm

    if expected_type is GitHubIssue:
        assert (ref.user, ref.repo, ref.number) == (exp["user"], exp["repo"], exp["number"])
    else:  # JiraIssue
        assert ref.key == exp["key"]


INVALID_CASES = [
    # empty / whitespace
    pytest.param((), r"^empty issue reference$", id="empty-default"),
    pytest.param(("   ",), r"^empty issue reference$", id="empty-whitespace"),
    # malformed JIRA / GH / no match
    pytest.param(("jira:",), r"^invalid issue reference: 'jira:'$", id="jira-prefix-no-key"),
    pytest.param(("user/repo#",), r"^invalid issue reference: 'user/repo#'$", id="gh-missing-number"),
    pytest.param(("user/repo#abc",), r"^invalid issue reference: 'user/repo#abc'$", id="gh-nonnumeric-id"),
    pytest.param(("not-an-issue",), r"^invalid issue reference: 'not-an-issue'$", id="no-match"),
]


@pytest.mark.parametrize("args, expected_pattern", INVALID_CASES)
def test_parse_issue_invalid_raises(args, expected_pattern):
    with pytest.raises(ValueError, match=expected_pattern):
        parse_issue(*args)
