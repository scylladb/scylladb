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
    parser.add_argument('--label', type=str, default='promoted-to-master', help='Label to use')
    parser.add_argument('--ref', type=str, required=True, help='PR target branch')
    return parser.parse_args()


def add_comment_and_close_pr(pr_number, repository, comment):
    pr = repository.get_pull(pr_number)
    if pr.state == open:
        pr.create_issue_comment(comment)
        pr.edit(state="closed")


def main():
    args = parser()
    pr_pattern = re.compile(r'Closes .*#([0-9]+)')
    g = Github(github_token)
    repo = g.get_repo(args.repository, lazy=False)
    commits = repo.compare(head=args.commit_after_merge, base=args.commit_before_merge)
    processed_prs = set()
    # Print commit information
    for commit in commits.commits:
        match = pr_pattern.search(commit.commit.message)
        if match:
            search_url = f'https://api.github.com/search/issues'
            query = f"repo:{args.repository} is:pr is:closed sha:{commit.sha}"
            params = {
                "q": query,
            }
            headers = {
                "Authorization": f"token {github_token}",
                "Accept": "application/vnd.github.v3+json"
            }
            response = requests.get(search_url, headers=headers, params=params)
            prs = response.json().get("items", [])
            for pr in prs:
                refs_match = re.findall(r'Refs (?:#|https.*?)(\d+)', pr["body"])
                ref = re.search(r'branch-(\d+\.\d+)', args.ref)
                if refs_match and ref:
                    pr_number = int(refs_match[0])
                    backport_pr_number = int(match.group(1))
                    if pr_number in processed_prs:
                        continue
                    label_to_add = f'backport/{ref.group(1)}-done'
                    label_to_remove = f'backport/{ref.group(1)}'
                    pr = repo.get_pull(pr_number)
                    current_labels = pr.get_labels()
                    current_label_names = [label.name for label in current_labels]
                    if label_to_remove in current_label_names:
                        pr.remove_from_labels(label_to_remove)
                    if label_to_add not in current_label_names:
                        pr.add_to_labels(label_to_add)
                    comment = f'Closed via {commit.sha}'
                    add_comment_and_close_pr(backport_pr_number, repo, comment)
                    processed_prs.add(pr_number)
                else:
                    assert re.match(r'/master', args.ref)
                    pr_number = pr["number"]
                    pr = repo.get_pull(pr_number)
                    label_to_add = args.label
                    current_labels = pr.get_labels()
                    current_label_names = [label.name for label in current_labels]
                    if label_to_add not in current_label_names:
                        pr.add_to_labels(label_to_add)
                processed_prs.add(pr_number)


if __name__ == "__main__":
    main()
