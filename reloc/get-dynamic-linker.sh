#!/bin/bash -e

# The relocatable package includes its own dynamic linker. We don't
# know the path it will be installed to, so for now use a very long
# path so that patchelf doesn't need to edit the program headers.  The
# kernel imposes a limit of 4096 bytes including the null. The other
# constraint is that the build-id has to be in the first page, so we
# can't use all 4096 bytes for the dynamic linker.
# In here we just guess that 2000 extra / should be enough to cover
# any path we get installed to but not so large that the build-id is
# pushed to the second page.
# At the end of the build we check that the build-id is indeed in the
# first page. At install time we check that patchelf doesn't modify
# the program headers.

# gdb has a SO_NAME_MAX_PATH_SIZE of 512, so limit the path size to
# that. The 512 includes the null at the end, hence the 511 bellow.

ORIGINAL_DYNAMIC_LINKER=$(gcc -### /dev/null -o t 2>&1 | perl -n  -e '/-dynamic-linker ([^ ]*) / && print $1')
DYNAMIC_LINKER=$(printf "%511s$ORIGINAL_DYNAMIC_LINKER" | sed 's| |/|g')

echo $DYNAMIC_LINKER
