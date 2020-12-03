# Backport

Backport queue is implemented using github issues and github label.

1. All patches that fix things must come with a Fixes: #nnnn line; if a
patchset it should be present on the cover letter as well. All patches
with Fixes: should also include which branches need backport (perhaps
none) by including Branches: w.x, y.z or Branches: none

2. Maintainers will insist on rule 1

3. After a patch with a Fixes #nnnn line gets promoted, a bot wakes up
and assignes the label [Backport candidate] to that issue iff:
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
