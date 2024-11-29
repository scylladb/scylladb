#!/bin/bash

nodetool flush repair_test

# wait gc_grace_seconds
sleep 15

nodetool compact repair_test repair_test

directory="/var/lib/scylla/data/repair_test/repair_test_mv-*"
#  simulate the failure of GSI asynchronous writing to replica node A 
# (achieved by clearing the sstable in the GSI directory, like rm -rf me-*).
find $directory -type f -name "me-*" -exec rm {} \;
