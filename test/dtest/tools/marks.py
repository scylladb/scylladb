#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import importlib.metadata
import logging
import operator
import re
import unittest.mock

import pytest
from cassandra.connection import DRIVER_NAME, DRIVER_VERSION
from packaging.requirements import Requirement
from packaging.version import Version

from test.dtest.dtest_config import DTestConfig
from test.dtest.tools.env import DTEST_REQUIRE
# from test.dtest.tools.github_issues import GithubRepo

logger = logging.getLogger(__name__)


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


def is_enterprise(cassandra_dir, scylla_version):
    dtest_config = DTestConfig()
    dtest_config.cassandra_dir = cassandra_dir
    dtest_config.scylla_version = scylla_version
    return dtest_config.is_enterprise


def enterprise_only_param(*param):
    return pytest.param(*param, marks=pytest.mark.skipif("get_version(config.getvalue('--cassandra-dir'), " "config.getvalue('--scylla-version')) < Version('2018.1')", reason=f"'{param}' is supported only in enterprise version"))


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

    return pytest.mark.skipif(not outcome, reason=f"test expected: {driver_requirements}\n" f"installed: {DRIVER_VERSION} - {DRIVER_NAME}")


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


def check_issue_closed(pattern):
    """check if issue is closed

    Parse pattern and find whether it matched
    issue or repo/issue format. If matched
    check on github whether issue is closed

    regexp match next comman patter: user/repo#issue
    where:
        user - github user, which used to get all repos
               if not found, default is scylladb
        repo - repo of user, where issue will be searching
               if not found, default is scylla
        issue - issue id for checking its state
               if not found, return False.

    Support next formats matched by regexp
        "888"
        "#888"
        "my-repo#888"
        "my_repo#888"
        "my-user/my-repo#888"
        "my_user/my_repo#888"
        "http://github.com/my-user/my-repo/issues/888"
        "https://github.com/my_user/my_repo/issues/888"

    Arguments:
        pattern {str} -- pattern passed to @require()

    Returns:
        bool -- True if closed, false otherwise
    """

    if pattern is None:
        logger.debug(f"check_issue_closed: no pattern")
        return False

    match = ISSUE_PATTERN.match(pattern.strip())
    if not match:
        raise ValueError(f"pattern [{pattern}] isn't valid for pytest.mark.require")

    obj = match.groupdict()
    user_id = obj.get("user_id") or obj.get("url_user_id") or "scylladb"
    repo_id = obj.get("repo_id") or obj.get("url_repo_id") or "scylladb"
    issue_id = int(obj["id"])  # this group is required and contains digits only

    msg = f"check_issue_closed: {user_id}/{repo_id}#{issue_id}"

    try:
        found_issue = GithubRepo().get_issue(user_id, repo_id, issue_id)
        logger.debug(f"{msg}: state={found_issue.state}")
        return found_issue.state == "closed"
    except Exception as ex:  # noqa: BLE001
        logger.debug(f"{msg} failed: {ex}")
        return False


class RequirePredicate:
    """the base class to be used by 'pytest.mark.require(condition=...)'

    like:
    @pytest.mark.require(condition=(IssueClosed("scylladb/scylla-dtest#3951") &
                                    ~EnableWithFeature('tablets')))
    """

    def __bool__(self):
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

    def apply(self, **kwargs):
        self.verbose = kwargs.get("verbose", self.verbose)
        self.nodeid = kwargs.get("nodeid", self.nodeid)

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

        closed = check_issue_closed(self.issue)
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


class ProductIs(BasePredicate):
    def __init__(self, name=None):
        self.expected_name = name
        self.name = "oss"

    def apply(self, **kwargs):
        is_ent = kwargs.get("is_enterprise", False)
        self.name = "enterprise" if is_ent else "oss"

    def __bool__(self):
        if self.expected_name is not None:
            return self.name == self.expected_name
        return True


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


def product_is(*args, **kwargs):
    return ProductIs(*args, **kwargs)


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


@pytest.mark.parametrize("closed", [True, False])
def test_require_issue_closed(closed):
    with unittest.mock.patch(f"{__name__}.check_issue_closed", return_value=closed):
        cond = issue_closed("#24601")
        cond.apply(verbose=False)
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
        cond.apply(enabled_features=enabled_features)
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
        cond.apply(enabled_features=enabled_features)
        expected_result = not closed and feature in enabled_features.split(",")
        assert bool(cond) == expected_result


@pytest.mark.parametrize("enabled_features", ["tablets,wavelets", "wavelets", ""])
def test_require_composite_rhs_negated(enabled_features):
    closed = True
    with unittest.mock.patch(f"{__name__}.check_issue_closed", return_value=closed):
        feature = "tablets"
        cond = issue_closed("#24601") & ~with_feature(feature)
        cond.apply(enabled_features=enabled_features)
        feature_is_enabled = feature in enabled_features.split(",")
        expected_result = closed and not feature_is_enabled
        assert bool(cond) == expected_result
