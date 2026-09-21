from enum import Enum


class CompactionStrategy(Enum):
    LEVELED = "LeveledCompactionStrategy"
    SIZE_TIERED = "SizeTieredCompactionStrategy"
    TIME_WINDOW = "TimeWindowCompactionStrategy"
    INCREMENTAL = "IncrementalCompactionStrategy"
    DATE_TIERED = "DateTieredCompactionStrategy"

    @classmethod
    def from_str(cls, output_str):
        try:
            return CompactionStrategy[CompactionStrategy(output_str).name]
        except AttributeError as attr_err:
            raise ValueError(f"Could not recognize compaction strategy value: {output_str} - {attr_err}")


KEYSPACE_NAME = "keyspace1"
TABLE_NAME = "standard1"
FULL_TABLE_NAME = ".".join([KEYSPACE_NAME, TABLE_NAME])
KB = 1024
MB = 1024 * 1024
