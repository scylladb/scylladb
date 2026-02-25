#!/usr/bin/env python3
import argparse
import os
import sys
from github import Github
import re

try:
    github_token = os.environ["GITHUB_TOKEN"]
except KeyError:
    print("Please set the 'GITHUB_TOKEN' environment variable")
    sys.exit(1)


def parser():
    parse = argparse.ArgumentParser()
    parse.add_argument('--repo', type=str, required=True, help='Github repository name (e.g., scylladb/scylladb)')
    parse.add_argument('--number', type=int, required=True, help='Pull request or issue number to sync labels from')
    parse.add_argument('--label', type=str, default=None, help='Label to add/remove from an issue or PR')
    parse.add_argument('--is_issue', action='store_true', help='Determined if label change is in Issue or not')
    parse.add_argument('--action', type=str, choices=['opened', 'labeled', 'unlabeled'], required=True, help='Sync labels action')
    return parse.parse_args()


def copy_labels_from_linked_issues(repo, pr_number):
    pr = repo.get_pull(pr_number)
    if pr.body:
        linked_issue_numbers = set(re.findall(r'Fixes:? (?:#|https.*?/issues/)(\d+)', pr.body))
        for issue_number in linked_issue_numbers:
            try:
                issue = repo.get_issue(int(issue_number))
                for label in issue.labels:
                    # Copy ALL labels from issues to PR when PR is opened
                    pr.add_to_labels(label.name)
                    print(f"Copied label '{label.name}' from issue #{issue_number} to PR #{pr_number}")
                    if label.name in ['P0', 'P1']:
                        pr.add_to_labels('force_on_cloud')
                        print(f"Added force_on_cloud label to PR #{pr_number} due to {label.name} label")
                print(f"All labels from issue #{issue_number} copied to PR #{pr_number}")
            except Exception as e:
                print(f"Error processing issue #{issue_number}: {e}")


def get_linked_pr_from_issue_number(repo, number):
    linked_prs = []
    for pr in repo.get_pulls(state='all', base='master'):
        if pr.body and f'{number}' in pr.body:
            linked_prs.append(pr.number)
            break
        else:
            continue
    return linked_prs


def get_linked_issues_based_on_pr_body(repo, number):
    pr = repo.get_pull(number)
    repo_name = repo.full_name
    pattern = rf"(?:fix(?:|es|ed)|resolve(?:|d|s))\s*:?\s*(?:(?:(?:{repo_name})?#)|https://github\.com/{repo_name}/issues/)(\d+)"
    issue_number_from_pr_body = []
    if pr.body is None:
        return issue_number_from_pr_body
    matches = re.findall(pattern, pr.body, re.IGNORECASE)
    if matches:
        for match in matches:
            issue_number_from_pr_body.append(match)
            print(f"Found issue number: {match}")
    return issue_number_from_pr_body


def sync_labels(repo, number, label, action, is_issue=False):
    if is_issue:
        linked_prs_or_issues = get_linked_pr_from_issue_number(repo, number)
    else:
        linked_prs_or_issues = get_linked_issues_based_on_pr_body(repo, number)
    for pr_or_issue_number in linked_prs_or_issues:
        if is_issue:
            target = repo.get_issue(pr_or_issue_number)
        else:
            target = repo.get_issue(int(pr_or_issue_number))
        if action == 'labeled':
            target.add_to_labels(label)
            if label in ['P0', 'P1'] and is_issue:
                # Only add force_on_cloud to PRs when P0/P1 is added to an issue
                target.add_to_labels('force_on_cloud')
                print(f"Added 'force_on_cloud' label to PR #{pr_or_issue_number} due to {label} label")
            print(f"Label '{label}' successfully added.")
        elif action == 'unlabeled':
            target.remove_from_labels(label)
            if label in ['P0', 'P1'] and is_issue:
                # Check if any other P0/P1 labels remain before removing force_on_cloud
                remaining_priority_labels = [l.name for l in target.labels if l.name in ['P0', 'P1']]
                if not remaining_priority_labels:
                    try:
                        target.remove_from_labels('force_on_cloud')
                        print(f"Removed 'force_on_cloud' label from PR #{pr_or_issue_number} as no P0/P1 labels remain")
                    except Exception as e:
                        print(f"Warning: Could not remove force_on_cloud label: {e}")
            print(f"Label '{label}' successfully removed.")
        elif action == 'opened':
            copy_labels_from_linked_issues(repo, number)
        else:
            print("Invalid action. Use 'labeled', 'unlabeled' or 'opened'.")


def main():
    args = parser()
    github = Github(github_token)
    repo = github.get_repo(args.repo)
    sync_labels(repo, args.number, args.label, args.action, args.is_issue)


if __name__ == "__main__":
    main()
