#!/usr/bin/env python3

import argparse
import os
import re
import sys
import tempfile
import logging

from github import Github, GithubException
from git import Repo, GitCommandError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
try:
    github_token = os.environ["GITHUB_TOKEN"]
except KeyError:
    print("Please set the 'GITHUB_TOKEN' environment variable")
    sys.exit(1)


def is_pull_request():
    return '--pull-request' in sys.argv[1:]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', type=str, required=True, help='Github repository name')
    parser.add_argument('--base-branch', type=str, default='refs/heads/master', help='Base branch')
    parser.add_argument('--commits', default=None, type=str, help='Range of promoted commits.')
    parser.add_argument('--pull-request', type=int, help='Pull request number to be backported')
    parser.add_argument('--head-commit', type=str, required=is_pull_request(), help='The HEAD of target branch after the pull request specified by --pull-request is merged')
    return parser.parse_args()


def create_pull_request(repo, new_branch_name, base_branch_name, pr, backport_pr_title, commits, is_draft=False):
    pr_body = f'{pr.body}\n\n'
    for commit in commits:
        pr_body += f'- (cherry picked from commit {commit})\n\n'
    pr_body += f'Parent PR: #{pr.number}'
    try:
        backport_pr = repo.create_pull(
            title=backport_pr_title,
            body=pr_body,
            head=f'scylladbbot:{new_branch_name}',
            base=base_branch_name,
            draft=is_draft
        )
        logging.info(f"Pull request created: {backport_pr.html_url}")
        backport_pr.add_to_assignees(pr.user)
        if is_draft:
            backport_pr.add_to_labels("conflicts")
            pr_comment = f"@{pr.user.login} - This PR was marked as draft because it has conflicts\n"
            pr_comment += "Please resolve them and mark this PR as ready for review"
            backport_pr.create_issue_comment(pr_comment)
        logging.info(f"Assigned PR to original author: {pr.user}")
        return backport_pr
    except GithubException as e:
        if 'A pull request already exists' in str(e):
            logging.warning(f'A pull request already exists for {pr.user}:{new_branch_name}')
        else:
            logging.error(f'Failed to create PR: {e}')


def get_pr_commits(repo, pr, stable_branch, start_commit=None):
    commits = []
    if pr.merged:
        merge_commit = repo.get_commit(pr.merge_commit_sha)
        if len(merge_commit.parents) > 1:  # Check if this merge commit includes multiple commits
            for commit in pr.get_commits():
                commits.append(commit.sha)
        else:
            if start_commit:
                promoted_commits = repo.compare(start_commit, stable_branch).commits
            else:
                promoted_commits = repo.get_commits(sha=stable_branch)
            for commit in pr.get_commits():
                for promoted_commit in promoted_commits:
                    commit_title = commit.commit.message.splitlines()[0]
                    # In Scylla-pkg and scylla-dtest, for example,
                    # we don't create a merge commit for a PR with multiple commits,
                    # according to the GitHub API, the last commit will be the merge commit,
                    # which is not what we need when backporting (we need all the commits).
                    # So here, we are validating the correct SHA for each commit so we can cherry-pick
                    if promoted_commit.commit.message.startswith(commit_title):
                        commits.append(promoted_commit.sha)

    elif pr.state == 'closed':
        events = pr.get_issue_events()
        for event in events:
            if event.event == 'closed':
                commits.append(event.commit_id)
    return commits


def create_pr_comment_and_remove_label(pr, comment_body):
    labels = pr.get_labels()
    pattern = re.compile(r"backport/\d+\.\d+$")
    for label in labels:
        if pattern.match(label.name):
            print(f"Removing label: {label.name}")
            comment_body += f'- {label.name}\n'
            pr.remove_from_labels(label)
    pr.create_issue_comment(comment_body)


def backport(repo, pr, version, commits, backport_base_branch):
    new_branch_name = f'backport/{pr.number}/to-{version}'
    backport_pr_title = f'[Backport {version}] {pr.title}'
    repo_url = f'https://scylladbbot:{github_token}@github.com/{repo.full_name}.git'
    fork_repo = f'https://scylladbbot:{github_token}@github.com/scylladbbot/{repo.name}.git'
    with (tempfile.TemporaryDirectory() as local_repo_path):
        try:
            repo_local = Repo.clone_from(repo_url, local_repo_path, branch=backport_base_branch)
            repo_local.git.checkout(b=new_branch_name)
            is_draft = False
            for commit in commits:
                try:
                    repo_local.git.cherry_pick(commit, '-x')
                except GitCommandError as e:
                    logging.warning(f'Cherry-pick conflict on commit {commit}: {e}')
                    is_draft = True
                    repo_local.git.add(A=True)
                    repo_local.git.cherry_pick('--continue')
            repo_local.git.push(fork_repo, new_branch_name, force=True)
            create_pull_request(repo, new_branch_name, backport_base_branch, pr, backport_pr_title, commits,
                                is_draft=is_draft)

        except GitCommandError as e:
            logging.warning(f"GitCommandError: {e}")


def with_github_keyword_prefix(repo, pr):
    pattern = rf"(?:fix(?:|es|ed))\s*:?\s*(?:(?:(?:{repo.full_name})?#)|https://github\.com/{repo.full_name}/issues/)(\d+)"
    match = re.findall(pattern, pr.body, re.IGNORECASE)
    if not match:
        print(f'No valid close reference for {pr.number}')
        comment = f':warning:  @{pr.user.login} PR body does not contain a Fixes reference to an issue '
        comment += ' and can not be backported\n\n'
        comment += 'The following labels were removed:\n'
        create_pr_comment_and_remove_label(pr, comment)
        return False
    else:
        return True


def main():
    args = parse_args()
    base_branch = args.base_branch.split('/')[2]
    promoted_label = 'promoted-to-master'
    repo_name = args.repo
    fork_repo_name = 'scylladbbot/scylladb'
    if 'scylla-enterprise' in args.repo:
        promoted_label = 'promoted-to-enterprise'
        fork_repo_name = 'scylladbbot/scylla-enterprise'
    stable_branch = base_branch
    backport_branch = 'branch-'

    backport_label_pattern = re.compile(r'backport/\d+\.\d+$')

    g = Github(github_token)
    repo = g.get_repo(repo_name)
    scylladbbot_repo = g.get_repo(fork_repo_name)
    closed_prs = []
    start_commit = None

    if args.commits:
        start_commit, end_commit = args.commits.split('..')
        commits = repo.compare(start_commit, end_commit).commits
        for commit in commits:
            match = re.search(rf"Closes .*#([0-9]+)", commit.commit.message, re.IGNORECASE)
            if match:
                pr_number = int(match.group(1))
                pr = repo.get_pull(pr_number)
                closed_prs.append(pr)
    if args.pull_request:
        start_commit = args.head_commit
        pr = repo.get_pull(args.pull_request)
        closed_prs = [pr]

    for pr in closed_prs:
        labels = [label.name for label in pr.labels]
        backport_labels = [label for label in labels if backport_label_pattern.match(label)]
        if promoted_label not in labels:
            print(f'no {promoted_label} label: {pr.number}')
            continue
        if not backport_labels:
            print(f'no backport label: {pr.number}')
            continue
        if args.commits and not with_github_keyword_prefix(repo, pr):
            continue
        if not repo.private and not scylladbbot_repo.has_in_collaborators(pr.user.login):
            logging.info(f"Sending an invite to {pr.user.login} to become a collaborator to {scylladbbot_repo.full_name} ")
            scylladbbot_repo.add_to_collaborators(pr.user.login)
            comment = f':warning:  @{pr.user.login} you have been added as collaborator to scylladbbot fork '
            comment += f'Please check your inbox and approve the invitation, once it is done, please add the backport labels again\n'
            create_pr_comment_and_remove_label(pr, comment)
            continue
        commits = get_pr_commits(repo, pr, stable_branch, start_commit)
        logging.info(f"Found PR #{pr.number} with commit {commits} and the following labels: {backport_labels}")
        for backport_label in backport_labels:
            version = backport_label.replace('backport/', '')
            backport_base_branch = backport_label.replace('backport/', backport_branch)
            backport(repo, pr, version, commits, backport_base_branch)


if __name__ == "__main__":
    main()
