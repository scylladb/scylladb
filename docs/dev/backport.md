# Backport

Backport queue is implemented using github issues and github label.

1. All patches that fix things must come with a Fixes: #nnnn line; if a
patchset it should be present on the cover letter as well. All patches
with Fixes: should also include which branches need backport (perhaps
none) by including Branches: w.x, y.z or Branches: none

2. Maintainers will insist on rule 1

3. After a patch with a Fixes #nnnn line gets promoted, a bot wakes up
and assigns the label [Backport candidate] to that issue iff:
- no Branches: was added
- Branches: w.x was added 
In case the "Branches: none" was specified the label [Backport
candidate] is not set

4. Once a week or so a maintainer wakes up and filters the issue list of
open [Backport candidate]s.

5. The maintainer may take the following actions:

5.1. Decide no branches need the fix, and remove the label

5.2. Decide the fix does not merit a backport, and remove the label
(explaining why in the issue)

5.3. Backport to branches that require it, and remove the label

5.4. Backport to some branches, but not others, noting which and keeping
the label

5.5. Decide that more time is needed to gain confidence in the fix, and
keep the label

5.6. Ask someone else (from QA, or the patch author, or anyone relevant)
for assistance in selecting among the options

## Contributor instructions
When backporting the maintainer will try to cherry-pick the changes onto the branch with older version of Scylla,
but sometimes additional changes are needed to make the change work with old code.

In such case the maintainer can ask the contributor to prepare a change to work with this particular version.
Here are the instructions for a contributor on how to properly prepare these changes.

It's the easiest to show it by an example, so let's look at PR [#12031](https://github.com/scylladb/scylladb/pull/12031), which fixed issue [#12014](https://github.com/scylladb/scylladb/issues/12014).

### Wait for the original change to get merged
First the original PR should get merged.
In the example the author wanted to merge the branch `cvybhu:filter_multi_bug` into `scylladb:master`.
Each PR is submitted on its own branch, which the PR author wants to merge into the `master` branch.
Before the changes end up on `master`, they are first added to the `next` branch to run some additional tests. Later the `next` branch is merged into `master` and the PR is officially merged.

### A backport PR
Released versions of `Scylla` live on their own branches - `branch-5.0`, `branch-4.6` etc.
To backport the change we will have to put it on its own branch and merge it into the branch with the desired version.
In the example, the PR with the backport [#12086](https://github.com/scylladb/scylladb/pull/12086) wants to merge `cvybhu:filter_multi_bug_5.0` into `scylladb:branch-5.0`.
Before the changes end up on `branch-5.0`, they are first added to the `next-5.0` branch to run some additional tests. Later the `next-5.0` branch is merged into `branch-5.0` and the PR is officially backported.

### Preparing the backport PR
1. Go to the branch with the target backport version
```bash
git fetch origin branch-5.0
git checkout -t origin/branch-5.0
git submodule sync
git pull origin branch-5.0
git submodule update --recursive
```
2. Create a branch with the fix
```bash
git checkout -b my_fix_branch_5.0
```

3. Copy the changes to the branch, use the merge commit from `master`.  
This command will copy all commits to the branch at once, which might result in a lot conflicts to resolve. It's also possible to cherry-pick the commits individually in order to tackle the conflicts one at a time. Please use the `-x` option on cherry-pick so that's it's possible to tell where the commit came from.
```bash
git cherry-pick -m1 -x 2d2034ea28b8615a130146ad9780010d29d2fcc9
```
Resolve conflicts as necessary

4. Build and run the tests using `dbuild`
```bash
./tools/toolchain/dbuild ./configure.py --disable-dpdk
./tools/toolchain/dbuild ninja dev-test
```

4. Push the branch
```bash
git push <my-scylla-fork> my_fix_branch_5.0
```

5. Open the PR.  
Change the branch so that the merge target is the backport branch (e.g `branch-5.0`) instead of `master`
