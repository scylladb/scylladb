System Limits
-------------
The following system limits should be set or removed.

=======  ==============  ========================================
Name	 Required value	 Description
=======  ==============  ========================================
core	 unlimited	 maximum core dump size
-------  --------------  ----------------------------------------
memlock	 unlimited	 maximum locked-in-memory address space
-------  --------------  ----------------------------------------
nofile	 200000	         maximum number of open file descriptors
-------  --------------  ----------------------------------------
as	 unlimited       address space limit
-------  --------------  ----------------------------------------
nproc	 8096            maximum number of processes
=======  ==============  ========================================

This configuration is provided in the file :code:`/etc/security/limits.d/scylla.conf`.

