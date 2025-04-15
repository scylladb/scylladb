import argparse
import re
import sys
import os
from github import Github
from github.GithubException import UnknownObjectException

try:
    github_token = os.environ["GITHUB_TOKEN"]
except KeyError:
    print("Please set the 'GITHUB_TOKEN' environment variable")
    sys.exit(1)


def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repository', type=str, required=True,
                        help='Github repository name (e.g., scylladb/scylladb)')
    parser.add_argument('--commits', type=str, required=True, help='Range of promoted commits.')
    parser.add_argument('--label', type=str, default='promoted-to-master', help='Label to use')
    parser.add_argument('--ref', type=str, required=True, help='PR target branch')
    return parser.parse_args()


def add_comment_and_close_pr(pr, comment):
    if pr.state == 'open':
        pr.create_issue_comment(comment)
        pr.edit(state="closed")


def mark_backport_done(repo, ref_pr_number, branch):
    pr = repo.get_pull(int(ref_pr_number))
    label_to_remove = f'backport/{branch}'
    label_to_add = f'{label_to_remove}-done'
    current_labels = [label.name for label in pr.get_labels()]
    if label_to_remove in current_labels:
        pr.remove_from_labels(label_to_remove)
    if label_to_add not in current_labels:
        pr.add_to_labels(label_to_add)


def main():
    # This script is triggered by a push event to either the master branch or a branch named branch-x.y (where x and y represent version numbers). Based on the pushed branch, the script performs the following actions:
    # - When ref branch is `master`, it will add the `promoted-to-master` label, which we need later for the auto backport process
    # - When ref branch is `branch-x.y` (which means we backported a patch), it will replace in the original PR the `backport/x.y` label with `backport/x.y-done` and will close the backport PR (Since GitHub close only the one referring to default branch)
    args = parser()
    pr_pattern = re.compile(r'Closes .*#([0-9]+)')
    target_branch = re.search(r'branch-(\d+\.\d+)', args.ref)
    g = Github(github_token)
    repo = g.get_repo(args.repository, lazy=False)
    start_commit, end_commit = args.commits.split('..')
    commits = repo.compare(start_commit, end_commit).commits
    processed_prs = set()
    # Print commit information
    for commit in commits:
        print(f'Commit sha is: {commit.sha}')
        pr_last_line = commit.commit.message.splitlines()
        for line in reversed(pr_last_line):
            match = pr_pattern.search(line)
            if match:
                pr_number = int(match.group(1))
                if pr_number in processed_prs:
                    continue
                if target_branch:
                    pr = repo.get_pull(pr_number)
                    branch_name = target_branch[1]
                    refs_pr = re.findall(r'Parent PR: (?:#|https.*?)(\d+)', pr.body)
                    if refs_pr:
                        print(f'branch-{target_branch.group(1)}, pr number is: {pr_number}')
                        # 1. change the backport label of the parent PR to note that
                        #    we've merged the corresponding backport PR
                        # 2. close the backport PR and leave a comment on it to note
                        #    that it has been merged with a certain git commit.
                        ref_pr_number = refs_pr[0]
                        mark_backport_done(repo, ref_pr_number, branch_name)
                        comment = f'Closed via {commit.sha}'
                        add_comment_and_close_pr(pr, comment)
                else:
                    try:
                        pr = repo.get_pull(pr_number)
                        pr.add_to_labels('promoted-to-master')
                        print(f'master branch, pr number is: {pr_number}')
                    except UnknownObjectException:
                        print(f'{pr_number} is not a PR but an issue, no need to add label')
                processed_prs.add(pr_number)


if __name__ == "__main__":
    main()
