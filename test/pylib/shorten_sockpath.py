# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# The address of a UNIX-domain socket is a file path. For historic reasons,
# the length of this path is limited to just a little over 100 bytes.
# In our test/pylib code, we prefered to put the socket file in the test
# output directory, which may have a very long path and result in socket
# paths that cannot be used.
#
# So the following code has a Linux-specific workaround inspired by a similar
# implementation in OpenvSwitch. It uses the fact that in Linux (and probably
# other Unix-like systems as well), the socket is looked up by inode, not
# name. So if we can use a shorter name to refer to the same file, it will
# work. What we do is open the long-named directory, and then refer to it
# using /proc/self/fd/NUM. The implementation is a class, which needs to be
# held alive while it is used as a os.PathLike (we need to keep the file
# descriptor open).

from contextlib import contextmanager
import os

class ShortenSockpath:
    def __init__(self, long_sockpath):
        self.dirname, self.basename = os.path.split(long_sockpath)
        self.dirfd = os.open(self.dirname, os.O_DIRECTORY | os.O_RDONLY)
    # __fspath__() is needed to make ShortenSockpath comply with os.PathLike
    def __fspath__(self):
        return f"/proc/self/fd/{self.dirfd}/{self.basename}"
    def __del__(self):
        os.close(self.dirfd)
    def orig(self):
        return f"{self.dirname}/{self.basename}"
    # hack: __str__() is used by test.py to print pass a "--manager-api" 
    # option to the individual test. Pass the original socket name - the test
    # will shorten it on its own to make the connection.
    def __str__(self):
        return self.orig()
