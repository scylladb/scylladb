"Address already in use" Messages
=================================

Phenomena
^^^^^^^^^
Spurious ``"Address already in use"`` messages originating from different contexts in the log, for example:

.. code-block:: none

                
                Dec 14 04:10:10 sjc-315.rfiserve.net scylla[370430]:  [shard 60] gossip - Fail to handle GOSSIP_DIGEST_SYN: std::system_error (error system:98, Address already in use)                       
                Dec 14 02:23:38 sjc-315.rfiserve.net scylla[370430]:  [shard 39] repair - repair 1 failed - std::system_error (error system:98, Address already in use)                                       

These errors may cause all sorts of actual problems, e.g., Gossip on a remote node determines that the node where these errors appear is DOWN.

Problem
^^^^^^^

The root cause for this issue is usually that the networking stack ran out of free ports due to having too many sockets in a ``TIME_WAIT/CLOSE_WAIT`` state.

To verify that, use:

.. code-block:: none

                netstat -np | grep -i "_wait"

Solution
^^^^^^^^

Usually, the situation described above is caused by some program malfunctioning. To find out which program it is, check which programs are the primary consumers of TCP sockets:

This is an example output on a machine that doesn't have an issue:

.. code-block:: none

   netstat -npt | tr -s ' ' | cut -d" " -f7- | sort | uniq -c

   (Not all processes could be identified, non-owned process info
   will not be shown, you would have to be root to see it all.)
                 
      1
      3 -
     18 13464/firefox
      3 13942/python3
     18 18603/chromium-brow
      4 19879/tusk
      2 20627/zoom
      3 2485/vladz --pid=24
      7 2525/vladz --pid=24
      1 2594/Preload.js --c
      1 26112/ssh
      8 32620/scylla
      8 376/scylla
      1 4549/gvfsd-smb-brow
      1 4755/python3
      1 4883/WhatsDesk
      8 571/scylla
      5 9359/thunderbird
      1 Address State PID/Program name


However, if one of the applications stands out and uses a lot of sockets, you may want to take a closer look at it.
Note that in the output above, we included the PID of the program, and as a result, each instance of the same program appears separately (e.g., scylla above).

If you see that some program appears too many times, you may want to aggregate all its appearances:
``$ netstat -npt | tr -s ' ' | cut -d" " -f7- | cut -d"/" -f2- | sort | uniq -c``
