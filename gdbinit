# Recommended .gdbinit for debugging scylla
# See docs/dev/debugging.md for more information
handle SIG34 pass noprint
handle SIG35 pass noprint
handle SIGUSR1 pass noprint
set print pretty
set python print-stack full
set auto-load safe-path /opt/scylladb/libreloc
add-auto-load-safe-path /lib64
add-auto-load-safe-path /usr/lib64
set debug libthread-db 1

# Register pretty-printer helpers for printing common
# std-c++ stl containers.
python
import glob
sys.path.insert(0, glob.glob('/usr/share/gcc-*/python')[0])
from libstdcxx.v6.printers import register_libstdcxx_printers
register_libstdcxx_printers (None)
end
