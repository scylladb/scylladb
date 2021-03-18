# Maintainer's handbook

This document describes the mechanics of maintaining
Scylla - how to do things, rather than what do do
(e.g. accept or reject a patch).

## General git tips

### Enable reusing recorded resolutions

The command

    git config --global rerere.enabled true

will record merge conflict resolutions and replay them
when git encounters the same conflict. This is helpful
when managing multiple branches that see similar conflicts.


### Set merge.conflictstyle to diff3

The command

    git config --global diff.conflictstyle = diff3

will set the conflict markers to three-way diff style.
This records not only "ours" and "theirs", but also the
common ancestor. This allows the maintainer to see the
intent of each change and aids in resolution.


### `git submodule sync`

Avoid using `git submodule sync` as it sets internal state
that is easy to forget, and which can wreak havoc if you do
forget it.

## Applying patches and patch series

Patches can arrive via mailing lists and github pull
requests. Either way, they should be applied to the
`next` branch, not `master`.

Sometimes, patches and patch series have dependencies.
It's important to verify that those dependencies are
satisfied. In the case of patch series, make sure
the series base contains the dependency, otherwise
bisectability is compromised.

A series base can be found with the command

    git merge-base remote/series_branch_or_tag origin/master


### Applying patches and patch series from the mailing list

Before you begin, check out the `next` branch and pull
from scylla.git to be up to date. If the pull isn't clean,
abort the merge and use `git pull --rebase`. Examine the result
to see if you forgot to push a previously applied patch.

### Applying single patches from the mailing list

Save the patch(es) to some directory, and use the command

    git am -im3 /path/to/patches/*.eml

to apply the patches. `-i` makes the process interactive and
lets you edit the commit message, `-m` sets the Message-Id
tag (which is used by Commit Bot to set the Reply-To header,
so that the commit acknowledgement appears as a response to
the patch), and `-3` enables 3-way merging which is rumored
to reduce conflicts.

Use `git push` to publish the patches.

### Applying patch series from the mailing list

Indentify the git url and branch/tag identifier, and issue
the command

    git pull --log --no-ff --no-rebase <url> <branch/tag>

The `--log` flag generates a list of patches in the commit log,
while `--no-ff` and `--no-rebase` ensure a merge commit is created.

Copy the cover letter subject and body to the merge commit's
subject and body, respectively. Make sure the merge commit
supplies enough information to understand what the series is
doing without having to read individual commits.

Use `git push` to publish the patches.

### Applying patches and patch series from github pull requests

A common contributor mistake is to base patches on `next`
rather than on `master`. This results in random commits
appearing in the pull request, if `next` is edited for some
reason.

Ensure that the target branch in the pull request page is
set to `scylladb:next` (click Edit and change it if that's
not the case).

When merging, verify that github didn't mangle the commit
log, check both your and the contributor's email address
are correct (no @noreply.users.github.com or similar
address, or home vs. work addresses).

### Applying single patches from github pull requests

Select "Squash and merge" and follow through.

### Applying patch series from github pull requests

Select "Create a merge commit" and do NOT follow through -
github will attribute itself as the commiter. Instead, click
"view command line options", select the "git pull" line,
paste it to a terminal and add `--no-ff --log` and execute.

## Dequeuing bad patches

Sometimes, a patch fails promotion by Jenkins, or needs to
be dequeued for some other reason. This section explains how.

 1. Synchronize with origin by checking out `next` and
    issuing a `git pull`
 2. Issue `git rebase -i --rebase-merges origin/master`
 3. Identify the final section that contains the pick/merge
    command that will contain the result. Ignore any intermediate
    sections that describe branches.
 4. Delete pick/merge commands that correspond to bad commits
 5. Save the file and let `git rebase` do the work
 6. Publish your changes with `git push --force-with-lease`

Note: git contains a bug where branch descriptions with the
characters `['":\.]` confuse it. Best to search-and-replace those
characters with nothing.

## Updating submodule references

Submodules are maintained in separate repositories. For example, Seastar
is developed upstream independently of Scylla. We want to periodically
(and upon contributor request) refresh scylla.git to include the
latest submodules.

 1. Check out the `next` branch and synchronize it using `git pull`
 2. Run the `scripts/refresh-submodules.sh` script, which will open a git
    commit log editor for every submodule to show that commits are being
    updated.
 3. Edit the submodule update commits with any necessary additional
    imformation. For example, amend the message with `Fixes` tags.
 4. Use `git push` to publish your work.

## Backporting patches

To backport a patch, check out the next branch of the relevant
release branch (e.g. next-3.2), syncrhonize it with scylla.git,
and use the cherry-pick command:

    git cherry-pick -x <commit hash>

for individual commits, and

    git cherry-pick -x -m 1 <commit hash>

for merge commits. `-x` leaves a reference to the original commit
hash, and `-m 1` indicates which is the "mainline" parent of the
merge commit.

If conflicts cannot be resolved with reasonable effort, ask the
contributor for help.

## Backporting Seastar commits

The first time a release branch needs a Seastar backport requires
creating a Seastar branch. This is done in a separate repository:

 1. Check out the next branch for your release branch (e.g.
    `next-3.2`) and synchronize using `git pull`.
 2. Use `git submodule update` to syncrhonize the submodule
 3. Use `cd seastar` to enter the submodule
 4. Create a new branch (e.g. `git checkout -b branch-3.2`)
    corresponding to the release series you are backporting to.
    Note, the regular branch name is used, not the next branch.
 5. Use `git push -u scylla-seastar branch-3.2' to publish the
    branch. Note, scylla-seastar here is a git remote that refers
    to https://github.com/scylladb/scylla-seastar.git, a
    repository used for holding seastar backports for scylla.git.
 6. Use `cd ..` to return to scylla.git.
 7. Edit `.gitmodules` to change `../seastar` to `../scylla-seastar`.
    This points the seastar submodule at the backports repository.
 8. Commit with a descriptive message and push to the relevant next
    branch.

After this is done, backporting seastar patches can proceed:
 1. Check out the next branch for your release branch (e.g.
    `next-3.2`) and synchronize using `git pull`.
 2. Use `git submodule update` to syncrhonize the submodule
 3. Use `cd seastar` to enter the submodule
 4. Check out the relevant branch (`branch-3.2` in our example)
 5. Use `git cherry-pick -x <hash>`
    (or `git cherry-pick -x -m 1 <hash>`) to backport patches.
 6. Use `git push` to publish the scylla-seastar.git patches.
 7. Use `cd ..` to return to scylla.git.
 8. Use `git submodule summary seastar` to create a change log.
 9. Commit using `git commit seastar`, populate change log from
    step 8.
 10. Publish using `git push`.

