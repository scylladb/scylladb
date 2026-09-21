from dataclasses import dataclass

from cassandra.util import Duration


@dataclass
class ScyllaDuration:
    years: int = None
    months: int = None
    weeks: int = None
    days: int = None
    hours: int = None
    minutes: int = None
    seconds: int = None
    milliseconds: int = None
    microseconds: int = None
    nanoseconds: int = None

    @classmethod
    def from_duration(cls, duration: Duration) -> ScyllaDuration:
        return ScyllaDuration(months=duration.months, days=duration.days, nanoseconds=duration.nanoseconds)

    def get_duration(self) -> Duration:
        return Duration(months=self._get_total_months(), days=self._get_total_days(), nanoseconds=self._get_total_nanos())

    def _years_to_months(self) -> int:
        return self._convert_if_exists(self.years, 12)

    def _weeks_to_days(self) -> int:
        return self._convert_if_exists(self.weeks, 7)

    def _hours_to_nanos(self) -> int:
        return self._convert_if_exists(self.hours, 36e11)

    def _minutes_to_nanos(self) -> int:
        return self._convert_if_exists(self.minutes, 6e10)

    def _seconds_to_nanos(self) -> int:
        return self._convert_if_exists(self.seconds, 1e9)

    def _millis_to_nanos(self) -> int:
        return self._convert_if_exists(self.milliseconds, 1e6)

    def _micros_to_nanos(self) -> int:
        return self._convert_if_exists(self.microseconds, 1e3)

    def _get_total_months(self) -> int:
        return (self.months if self.months else 0) + self._years_to_months()

    def _get_total_days(self) -> int:
        return (self.days if self.days else 0) + self._weeks_to_days()

    def _get_total_nanos(self) -> int:
        return sum((self.nanoseconds if self.nanoseconds else 0, self._micros_to_nanos(), self._millis_to_nanos(), self._seconds_to_nanos(), self._minutes_to_nanos(), self._hours_to_nanos()))

    def to_query_string(self) -> str:
        query_string = ""
        if self.years is not None:
            query_string += f"{self.years}y"
        if self.months is not None:
            query_string += f"{self.months}mo"
        if self.weeks is not None:
            query_string += f"{self.weeks}w"
        if self.days is not None:
            query_string += f"{self.days}d"
        if self.hours is not None:
            query_string += f"{self.hours}h"
        if self.minutes is not None:
            query_string += f"{self.minutes}m"
        if self.seconds is not None:
            query_string += f"{self.seconds}s"
        if self.milliseconds is not None:
            query_string += f"{self.milliseconds}ms"
        if self.microseconds is not None:
            query_string += f"{self.microseconds}us"
        if self.nanoseconds is not None:
            query_string += f"{self.nanoseconds}ns"
        return query_string

    @staticmethod
    def _convert_if_exists(unit: int, multiplier: int | float) -> int:
        return int(unit * multiplier) if unit else 0
