from attrs import define, AttrsInstance


@define
class TestMetricRecord(AttrsInstance):

    architecture: str = None
    mode: str = None
    test_name: str = None
    run_id: int = None
    directory: str = None
    user_usec: int = None
    system_usec: int = None
    usage_usec: int = None
    memory_peak: int = None
    time_taken: int = None
    time_start: float = None
    time_end: float = None
    success: bool = False


@define
class ResourceTimelineRecord(AttrsInstance):
    memory: int = None
    cpu: int = None
    timestamp: str = None
