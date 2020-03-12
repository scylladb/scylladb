#!/bin/bash

# Script for pulling a github pull request
# along with generating a merge commit message.
# Example usage for pull request #6007 and /next branch:
# git fetch
# git checkout origin/next
# ./scripts/pull_github_pr.sh 6007

set -e

if [[ $# != 1 ]]; then
	echo Please provide a github pull request number
	exit 1
fi

for required in jq curl; do
	if ! type $required >& /dev/null; then
		echo Please install $required first
		exit 1
	fi
done

PR_NUM=$1
PR_PREFIX=https://api.github.com/repos/scylladb/scylla/pulls

PR_DATA=$(curl -s $PR_PREFIX/$PR_NUM)
PR_TITLE=$(jq -r .title <<< $PR_DATA)
PR_DESCR=$(jq -r .body <<< $PR_DATA)
PR_REF=$(jq -r .head.ref <<< $PR_DATA)
PR_LOGIN=$(jq -r .head.user.login <<< $PR_DATA)
PR_REPO=$(jq -r .head.repo.html_url <<< $PR_DATA)
PR_LOCAL_BRANCH=$PR_LOGIN-$PR_REF

git fetch origin pull/$PR_NUM/head:$PR_LOCAL_BRANCH

git merge --no-ff --log $PR_LOCAL_BRANCH -m "Merge '$PR_TITLE' from $PR_LOGIN" -m "$PR_DESCR"
git commit --amend # for a manual double-check

