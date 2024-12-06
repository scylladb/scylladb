#!/usr/bin/env python3

import logging
import os
import subprocess
import sys
import re
import tempfile
from typing import Optional, Dict, Any
import requests

GITHUB_API_URL = "https://api.github.com"

def run_git_command(args: list[str]) -> str:
    try:
        logging.info("Running `git %s`", " ".join(args))
        result = subprocess.run(["git"] + args, capture_output=True, text=True, check=True)
        stripped = result.stdout.strip()
        logging.info("Result: %s", stripped)
        return stripped
    except subprocess.CalledProcessError as e:
        msg = e.stderr.strip()
        msg = f": {msg}" if msg else ". No error message was provided."
        logging.error("Git command `git %s` failed%s", " ".join(args), msg)
        sys.exit(1)

def get_current_branch() -> str:
    res = run_git_command(["rev-parse", "--abbrev-ref", "HEAD"])
    return res

def get_tracked_remote_and_branch() -> tuple[str, str]:
    symref = run_git_command(["symbolic-ref", "--short", "HEAD"])
    remote = run_git_command(["config", "--local", f"branch.{symref}.remote"])
    branch = run_git_command(["config", "--local", f"branch.{symref}.merge"])
    return remote, branch

def fetch_pr_data(repo: str, branch: str, token: str, author: str) -> Optional[Dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token}"}
    query = f"{author}:{branch}"
    response = requests.get(f"{GITHUB_API_URL}/repos/{repo}/pulls", headers=headers, params={"state": "open", "head": query})
    response.raise_for_status()
    prs = response.json()
    return prs[0] if prs else None

def get_new_version_and_branch(old_branch: str) -> tuple[int, str]:
    match = re.search(r"(.+)-v(\d+)$", old_branch)
    if match:
        old_branch_pref = str(match.group(1))
        new_version = int(match.group(2)) + 1
    else:
        logging.warning(f"Old branch name %s does not follow the pattern `branch-vN`. Will append `-v2` to new branch name", old_branch)
        old_branch_pref = old_branch
        new_version = 2

    new_branch = f"{old_branch_pref}-v{new_version}"
    logging.info("New branch name will be: %s", new_branch)
    return new_version, new_branch

def get_title_stripped(old_title: str) -> str:
    match = re.search(r"^\[v\d+\] *(.+)$", old_title)
    if not match:
        return old_title
    return match.group(1)

def get_git_editor() -> str:
    return run_git_command(["config", "core.editor"])

def checkout_new_branch(branch: str) -> None:
    run_git_command(["checkout", "-b", branch])

def edit_title_and_description(editor: str, title: str, description: str) -> tuple[str, str]:
    with tempfile.NamedTemporaryFile(suffix=".md", mode="w+", delete=False) as tmp_file:
        tmp_file.write(title + "\n\n" + description)
        tmp_file.flush()
        subprocess.run(editor.split() + [tmp_file.name])
        tmp_file.seek(0)
        title, _, description = tmp_file.read().partition("\n\n")
        return title, description

def create_pr(repo: str, base: str, head: str, title: str, body: str, token: str) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {token}"}
    payload = {
        "title": title,
        "head": head,
        "base": base,
        "body": body
    }
    response = requests.post(f"{GITHUB_API_URL}/repos/{repo}/pulls", headers=headers, json=payload)
    response.raise_for_status()
    return response.json()

def close_pr(repo: str, pr_number: int, token: str) -> None:
    headers = {"Authorization": f"Bearer {token}"}
    payload = {"state": "closed"}
    response = requests.patch(f"{GITHUB_API_URL}/repos/{repo}/pulls/{pr_number}", headers=headers, json=payload)
    response.raise_for_status()

def main() -> None:
    logging.basicConfig(
        level = logging.INFO,
        format = "%(asctime)s [%(levelname)s] %(message)s",
        handlers = [
            logging.StreamHandler(sys.stdout)
        ]
    )
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True

    token = os.getenv("GITHUB_TOKEN")
    if not token:
        logging.error("Environment variable GITHUB_TOKEN must be set.")
        sys.exit(1)

    repo = os.getenv("GITHUB_REPO")
    if not repo:
        logging.error("Environment variable GITHUB_REPO must be set to the repo you're opening PR against (e.g., 'owner/repo').")
        sys.exit(1)

    author = os.getenv("GITHUB_AUTHOR")
    if not author:
        logging.error("Need your github account name as GITHUB_AUTHOR env variable to identify your PR")
        sys.exit(1)

    current_branch = get_current_branch()
    # TODO might be detached
    logging.info("Current branch: %s", current_branch)

    remote, remote_branch = get_tracked_remote_and_branch()
    if not remote or not remote_branch:
        logging.error("Current branch is not tracking any remote branch.")
        sys.exit(1)

    logging.info(f"Tracked remote name: {remote}, branch name on the remote: {remote_branch}")

    pr_data = fetch_pr_data(repo, remote_branch, token, author)
    if not pr_data:
        logging.error(f"No open pull request found for branch %s.", remote_branch)
        sys.exit(1)

    logging.debug("PR data: %s", pr_data)

    old_pr_number = pr_data["number"]
    old_pr_description = pr_data["body"]
    base_branch = pr_data["base"]["ref"]
    old_pr_title = pr_data["title"]
    old_pr_url = pr_data["html_url"]

    logging.info(f"Found PR #{old_pr_number}: \"{old_pr_title}\"")
    title_stripped = get_title_stripped(old_pr_title)

    new_version, new_branch = get_new_version_and_branch(current_branch)

    new_title = f"[v{new_version}] {title_stripped}"
    #TODO: repo prefix e.g. scylladb/scylladb#X
    new_description = old_pr_description + f"\nPrevious version: #{old_pr_number}\n\nv{new_version}:\n**Describe the changes from previous version**"
    editor = get_git_editor()
    #TODO can be undefined?
    logging.info(f"Using editor `{editor}`")
    while True:
        new_title, new_description = edit_title_and_description(editor, new_title, new_description)
        inp = input(f"\nNew title will be:\n\n{new_title}\n---\n\nNew description will be:\n\n{new_description}\n---\n\nProceed? [(y)es/(E)dit again/(c)ancel] ")
        inp = inp.lower()
        if inp in ["yes", "y"]:
            break
        elif inp in ["c", "cancel"]:
            sys.exit(0)
        else:
            continue

    checkout_new_branch(new_branch)
    logging.warning("Switched to new branch `%s`. If the operation fails, return to the previous branch (e.g. `git checkout -`) and delete this one before retrying.", new_branch)

    inp = input(f"Will set upstream of current branch `{new_branch}` to `{remote}` and push. Continue? [y/N] ")
    if inp.lower() not in ["yes", "y"]:
        print("Exiting.")
        sys.exit(0)

    run_git_command(["push", "-u", remote, new_branch])

    new_pr = create_pr(repo, base_branch, new_branch, new_title, new_description, token)
    logging.info(f"New PR created: {new_pr['html_url']}. Closing old PR...")

    close_pr(repo, old_pr_number, token)
    logging.info(f"Old PR #{old_pr_number} closed.")

if __name__ == "__main__":
    main()
