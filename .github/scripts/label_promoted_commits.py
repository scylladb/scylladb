import requests
from github import Github
import argparse
import re
import sys
import os

try:
    github_token = os.environ["GITHUB_TOKEN"]
except KeyError:
    print("Please set the 'GITHUB_TOKEN' environment variable")
    sys.exit(1)


def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repository', type=str, required=True,
                        help='Github repository name (e.g., scylladb/scylladb)')
    parser.add_argument('--commit_before_merge', type=str, required=True, help='Git commit ID to start labeling from ('
                                                                               'newest commit).')
    parser.add_argument('--commit_after_merge', type=str, required=True,
                        help='Git commit ID to end labeling at (oldest '
                             'commit, exclusive).')
    parser.add_argument('--update_issue', type=bool, default=False, help='Set True to update issues when backport was '
                                                                         'done')
    parser.add_argument('--label', type=str, required=True, help='Label to use')
    return parser.parse_args()


def main():
    args = parser()
    pr_pattern = re.compile(r'Closes .*#([0-9]+)')
    g = Github(github_token)
    repo = g.get_repo(args.repository, lazy=False)

    commits = repo.compare(head=args.commit_after_merge, base=args.commit_before_merge)
    # Print commit information
    for commit in commits.commits:
        print(commit.sha)
        match = pr_pattern.search(commit.commit.message)
        if match:
            pr_number = match.group(1)
            url = f'https://api.github.com/repos/{args.repository}/issues/{pr_number}/labels'
            data = {
                "labels": [f'{args.label}']
            }
            headers = {
                "Authorization": f"token {github_token}",
                "Accept": "application/vnd.github.v3+json"
            }
            response = requests.post(url, headers=headers, json=data)
            if response.ok:
                print(f"Label added successfully to {url}")
            else:
                print(f"No label was added to {url}")


if __name__ == "__main__":
    main()
