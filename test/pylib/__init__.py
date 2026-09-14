from enum import StrEnum


class SeastarIOMetric(StrEnum):
    """Seastar reactor IO counters, scraped at the end of each test.

    They cover all file IO issued by Scylla, counted whether or not the kernel
    serves it from the page cache (tests run with --kernel-page-cache 1, so most
    of it never reaches a disk).  They are incremented on submission to the IO
    queue and carry only a shard label; the scylla_io_queue_total_* counters
    track the same submissions broken down per IO class and device, a breakdown
    that summing for a per-test total would only throw away.
    """
    READ_BYTES = 'scylla_reactor_aio_bytes_read'
    READ_OPS = 'scylla_reactor_aio_reads'
    WRITE_BYTES = 'scylla_reactor_aio_bytes_write'
    WRITE_OPS = 'scylla_reactor_aio_writes'
