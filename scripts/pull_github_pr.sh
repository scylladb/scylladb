#!/bin/bash

# Script for pulling a github pull request
# along with generating a merge commit message.
# Example usage for pull request #6007 and /next branch:
# git fetch
# git checkout origin/next
# ./scripts/pull_github_pr.sh 6007

set -e

gh_hosts=~/.config/gh/hosts.yml

if [[ ( -z "$GITHUB_LOGIN" || -z "$GITHUB_TOKEN" ) && -f "$gh_hosts" ]]; then
	GITHUB_LOGIN=$(awk '/user:/ { print $2 }' "$gh_hosts")
	GITHUB_TOKEN=$(awk '/oauth_token:/ { print $2 }' "$gh_hosts")
fi

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

curl() {
    local opts=()
    if [[ -n "$GITHUB_LOGIN" && -n "$GITHUB_TOKEN" ]]; then
        opts+=(--user "${GITHUB_LOGIN}:${GITHUB_TOKEN}")
    fi
    command curl "${opts[@]}" "$@"
}

NL=$'\n'

PR_NUM=$1
# convert full repo URL to its project/repo part, in case of failure default to origin/master:
REMOTE_SLASH_BRANCH="$(git rev-parse --abbrev-ref --symbolic-full-name  @{upstream} \
     || git rev-parse --abbrev-ref --symbolic-full-name master@{upstream} \
     || echo 'origin/master')"
REMOTE="${REMOTE_SLASH_BRANCH%/*}"
REMOTE_URL="$(git config --get "remote.$REMOTE.url")"
PROJECT=`sed 's/git@github.com://;s#https://github.com/##;s/\.git$//;' <<<"${REMOTE_URL}"`
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

git fetch "$REMOTE" pull/$PR_NUM/head

nr_commits=$(git log --pretty=oneline HEAD..FETCH_HEAD | wc -l)

closes="${NL}${NL}Closes ${PROJECT}#${PR_NUM}${NL}"

if [[ $nr_commits == 1 ]]; then
	commit=$(git log --pretty=oneline HEAD..FETCH_HEAD | awk '{print $1}')
	message="$(git log -1 "$commit" --format="format:%s%n%n%b")"
	if ! git cherry-pick $commit
	then
		echo "Cherry-pick failed. You are now in a subshell. Either resolve with git cherry-pick --continue or git cherry-pick --abort, then exit the subshell"
		head_before=$(git rev-parse HEAD)
		bash
		head_after=$(git rev-parse HEAD)
		if [[ "$head_before" = "$head_after" ]]; then
			exit 1
		fi
	fi
	git commit --amend -m "${message}${closes}"
else
	git merge --no-ff --log=1000 FETCH_HEAD -m "Merge '$PR_TITLE' from $USER_NAME" -m "${PR_DESCR}${closes}"
fi
git commit --amend # for a manual double-check

# Check PR tests status
PR_HEAD_SHA=$(jq -r .head.sha <<< $PR_DATA)
PR_TESTS_STATUS=$(curl -s "https://api.github.com/repos/$PROJECT/commits/$PR_HEAD_SHA/status" | jq -r .state)
if [ "$PR_TESTS_STATUS" != "success" ]; then
  ORANGE='\033[0;33m'
  NC='\033[0m'
  echo -e "${ORANGE}\nWARNING:${NC} Some of the tests that ran for this PR were not completed successfully,\n" \
        "please make sure all tests are done successfully before merge this PR.\n"
fi
