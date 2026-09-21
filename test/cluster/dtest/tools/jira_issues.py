import csv
import logging
import warnings
from functools import cache, cached_property, lru_cache

from jira import JIRA

from tools.github_issues import Issue
from tools.keystore import KeyStore

logger = logging.getLogger(__name__)


class CachedJiraIssues:
    """
    This class provides a cache for issue data retrieved from an S3 bucket

    The cache is automatically populated by the `.github/workflows/cache-jira-issues.yaml` workflow every 6 hours.
    """

    def __init__(self):
        self.storage = KeyStore()

    @lru_cache
    def get_project(self, project: str) -> dict[str, Issue]:
        issues_csv = self.storage.get_file_contents(f"issues/jira_scylladb_{project}.csv")
        scsv = issues_csv.strip()
        issues = {}
        for issue in csv.DictReader(scsv.decode().splitlines()[1:], fieldnames=("number", "state", "labels", "title")):
            issue_id = issue["number"]
            labels = [label for label in issue["labels"].strip().split("|") if label]
            issues[issue_id] = Issue(number=issue_id, state=issue["state"].lower(), labels=labels, title=issue["title"])
        return issues

    def get_issue(self, issue_id: str) -> Issue:
        project = issue_id.split("-")[0]
        return self.get_project(project=project)[issue_id]


@cache
class JiraProject:
    def __init__(self):
        self.s3_cache = CachedJiraIssues()

    @cached_property
    def jira(self) -> JIRA | None:
        try:
            credentials = KeyStore().get_jira_credentials()

            return JIRA(server=credentials["jira_server"], basic_auth=(credentials["jira_email"], credentials["jira_api_token"]))
        except Exception as ex:  # noqa: BLE001
            logger.warning(f"failed to create jira client: {ex}")

    @lru_cache
    def get_issue(self, issue_id: str) -> Issue | None:
        try:
            return self.s3_cache.get_issue(issue_id)
        except Exception as ex:  # noqa: BLE001
            warnings.warn(f"failed to get issue from cache: {ex}", DeprecationWarning)

        try:
            if not self.jira:
                raise ValueError("Jira client is not initialized. Check your credentials.")
            jira_issue = self.jira.issue(issue_id, expand="labels,summary,status")
            return Issue(title=jira_issue.fields.summary, number=jira_issue.key, state=jira_issue.fields.status.name.lower(), labels=jira_issue.fields.labels)
        except Exception as ex:  # noqa: BLE001
            warnings.warn(f"failed to get issue: {ex}", RuntimeWarning)
