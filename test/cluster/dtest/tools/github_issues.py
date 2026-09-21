import csv
import logging
import warnings
from dataclasses import dataclass
from functools import cache, cached_property, lru_cache

import github
import github.Issue

from tools.env import GITHUB_TOKEN
from tools.keystore import KeyStore

logger = logging.getLogger(__name__)


@dataclass
class Issue:
    number: str | int
    state: str
    labels: list[str]
    title: str | None = None  # Title is optional, for github issue we don't fetch from cache


class CachedGitHubIssues:
    """
    This class provides a cache for issue data retrieved from an S3 bucket while adhering to GitHub API rate limits.

    The cache is automatically populated by the `scylladb/scylla-cluster-tests/blob/master/.github/workflows/cache-issues.yaml`
    workflow every 6 hours.
    """

    def __init__(self):
        self.storage = KeyStore()

    @lru_cache
    def get_repo(self, owner: str, repo: str) -> dict[int, Issue]:
        issues_csv = self.storage.get_file_contents(f"issues/{owner}_{repo}.csv")
        pull_requests_csv = self.storage.get_file_contents(f"issues/pull-requests/{owner}_{repo}.csv")
        scsv = issues_csv.strip() + pull_requests_csv.strip()
        issues = {}
        for issue in csv.DictReader(scsv.decode().splitlines(), fieldnames=("number", "state", "labels", "title")):
            issue_id = int(issue["number"])
            labels = [label for label in issue["labels"].strip().split("|") if label]
            issues[issue_id] = Issue(number=issue_id, state=issue["state"].lower(), labels=labels, title=issue.get("title", None))
        return issues

    def get_issue(self, owner: str, repo_id: str, issue_id: int) -> Issue:
        repo_issues_mapping = self.get_repo(owner, repo_id)
        return repo_issues_mapping.get(issue_id)


@cache
class GithubRepo:
    def __init__(self):
        self.s3_cache = CachedGitHubIssues()

    @cached_property
    def git(self):
        if GITHUB_TOKEN:
            try:
                auth = github.Auth.Token(token=GITHUB_TOKEN)
                return github.Github(auth=auth, retry=None)
            except Exception as ex:  # noqa: BLE001
                logger.warning(f"failed to create github client: {ex}")

    @lru_cache
    def get_issue(self, user_id: str, repo_id: str, issue_id: int) -> Issue | None:
        try:
            repo = self.s3_cache.get_repo(user_id, repo_id)
            return repo[issue_id]
        except Exception as ex:  # noqa: BLE001
            warnings.warn(f"failed to get issue from cache: {ex}", DeprecationWarning)

        try:
            if not self.git:
                raise ValueError("GitHub client is not initialized. Check your GITHUB_TOKEN.")
            repo = self.git.get_repo(f"{user_id}/{repo_id}", lazy=True)
            issue = repo.get_issue(issue_id)
            return Issue(
                number=issue.number,
                state=issue.state,
                labels=[label.name for label in issue.labels],
                title=issue.title,
            )
        except Exception as ex:  # noqa: BLE001
            warnings.warn(f"failed to get issue: {ex}", RuntimeWarning)
