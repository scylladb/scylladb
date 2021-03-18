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

NL=$'\n'

PR_NUM=$1
# convert git@github.com:scylladb/scylla.git to just scylladb/scylla:
PROJECT=`git config --get remote.origin.url | sed 's/git@github.com://;s/\.git$//'`
PR_PREFIX=https://api.github.com/repos/$PROJECT/pulls

echo "Fetching info on PR #$PR_NUM... "
PR_DATA=$(curl -s $PR_PREFIX/$PR_NUM)
MESSAGE=$(jq -r .message <<< $PR_DATA)
if [ "$MESSAGE" != null ]
then
    # Error message, probably "Not Found".
    echo "$MESSAGE"
    exit 1
fi
PR_TITLE=$(jq -r .title <<< $PR_DATA)
echo "    $PR_TITLE"
PR_DESCR=$(jq -r .body <<< $PR_DATA)
PR_LOGIN=$(jq -r .head.user.login <<< $PR_DATA)
echo -n "Fetching full name of author $PR_LOGIN... "
USER_NAME=$(curl -s "https://api.github.com/users/$PR_LOGIN" | jq -r .name)
echo "$USER_NAME"

git fetch origin pull/$PR_NUM/head

nr_commits=$(git log --pretty=oneline HEAD..FETCH_HEAD | wc -l)

closes="${NL}${NL}Closes #${PR_NUM}${NL}"

if [[ $nr_commits == 1 ]]; then
	commit=$(git log --pretty=oneline HEAD..FETCH_HEAD | awk '{print $1}')
	message="$(git log -1 "$commit" --format="format:%s%n%n%b")"
	git cherry-pick $commit
	git commit --amend -m "${message}${closes}"
else
	git merge --no-ff --log=1000 FETCH_HEAD -m "Merge '$PR_TITLE' from $USER_NAME" -m "${PR_DESCR}${closes}"
fi
git commit --amend # for a manual double-check
