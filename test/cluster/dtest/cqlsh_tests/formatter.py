#
# Copyright (C) 2015-present The Apache Software Foundation
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# code adapted from https://github.com/apache/cassandra/blob/trunk/pylib/cqlshlib/formatting.py
# removed all color related stuff, since we only need it for testing cqlsh copy from CSV


import binascii
import calendar
import datetime
import math
import os
import platform
import re
import sys
from collections import defaultdict
from datetime import timedelta, tzinfo

from cassandra.cqltypes import EMPTY
from cassandra.util import datetime_from_timestamp
from six import ensure_text

ZERO = timedelta(0)


class UTC(tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO


class FormattedValue:
    def __init__(self, strval, coloredval=None, displaywidth=None):
        self.strval = strval
        if coloredval is None:
            coloredval = strval
        self.coloredval = coloredval
        if displaywidth is None:
            displaywidth = len(strval)
        # displaywidth is useful for display of special unicode characters
        # with
        self.displaywidth = displaywidth

    def __len__(self):
        return len(self.strval)

    def _pad(self, width, fill=" "):
        if width > self.displaywidth:
            return fill * (width - self.displaywidth)
        else:
            return ""

    def ljust(self, width, fill=" ", color=False):
        """
        Similar to self.strval.ljust(width), but takes expected terminal
        display width into account for special characters, and does not
        take color escape codes into account.
        """
        if color:
            return self.color_ljust(width, fill=fill)
        return self.strval + self._pad(width, fill)

    def rjust(self, width, fill=" ", color=False):
        """
        Similar to self.strval.rjust(width), but takes expected terminal
        display width into account for special characters, and does not
        take color escape codes into account.
        """
        if color:
            return self.color_rjust(width, fill=fill)
        return self._pad(width, fill) + self.strval

    def color_rjust(self, width, fill=" "):
        """
        Similar to self.rjust(width), but uses this value's colored
        representation, and does not take color escape codes into account
        in determining width.
        """
        return self._pad(width, fill) + self.coloredval

    def color_ljust(self, width, fill=" "):
        """
        Similar to self.ljust(width), but uses this value's colored
        representation, and does not take color escape codes into account
        in determining width.
        """
        return self.coloredval + self._pad(width, fill)


def colorme(bval, colormap, colorkey):
    return bval


def get_str(val):
    if isinstance(val, FormattedValue):
        return val.strval
    return val


is_win = platform.system() == "Windows"

unicode_controlchars_re = re.compile(r"[\x00-\x31\x7f-\xa0]")
controlchars_re = re.compile(r"[\x00-\x31\x7f-\xff]")


def _show_control_chars(match):
    txt = repr(match.group(0))
    if txt.startswith("u"):
        txt = txt[2:-1]
    else:
        txt = txt[1:-1]
    return txt


bits_to_turn_red_re = re.compile(r"\\([^uUx]|u[0-9a-fA-F]{4}|x[0-9a-fA-F]{2}|U[0-9a-fA-F]{8})")


def _make_turn_bits_red_f(color1, color2):
    def _turn_bits_red(match):
        txt = match.group(0)
        if txt == "\\\\":
            return "\\"
        return color1 + txt + color2

    return _turn_bits_red


default_null_placeholder = "null"
default_float_precision = 3
empty_colormap = defaultdict(lambda: "")


def format_by_type(  # noqa: PLR0913
    val,
    cqltype,
    encoding,
    colormap=None,
    addcolor=False,
    nullval=None,
    date_time_format=None,
    float_precision=None,
    decimal_sep=None,
    thousands_sep=None,
    boolean_styles=None,
):
    if nullval is None:
        nullval = default_null_placeholder
    if val is None:
        return colorme(nullval, colormap, "error")
    if addcolor is False:
        colormap = empty_colormap
    elif colormap is None:
        colormap = empty_colormap
    if date_time_format is None:
        date_time_format = DateTimeFormat()
    if float_precision is None:
        float_precision = default_float_precision
    return format_value(
        val, cqltype=cqltype, encoding=encoding, colormap=colormap, date_time_format=date_time_format, float_precision=float_precision, nullval=nullval, decimal_sep=decimal_sep, thousands_sep=thousands_sep, boolean_styles=boolean_styles
    )


def color_text(bval, colormap, displaywidth=None):
    # note that here, we render natural backslashes as just backslashes,
    # in the same color as surrounding text, when using color. When not
    # using color, we need to double up the backslashes so it's not
    # ambiguous. This introduces the unique difficulty of having different
    # display widths for the colored and non-colored versions. To avoid
    # adding the smarts to handle that in to FormattedValue, we just
    # make an explicit check to see if a null colormap is being used or
    # not.
    if displaywidth is None:
        displaywidth = len(bval)
    tbr = _make_turn_bits_red_f(colormap["blob"], colormap["text"])
    coloredval = colormap["text"] + bits_to_turn_red_re.sub(tbr, bval) + colormap["reset"]
    if colormap["text"]:
        displaywidth -= bval.count(r"\\")
    return FormattedValue(bval, coloredval, displaywidth)


DEFAULT_NANOTIME_FORMAT = "%H:%M:%S.%N"
DEFAULT_DATE_FORMAT = "%Y-%m-%d"

DEFAULT_TIMESTAMP_FORMAT = os.environ.get("CQLSH_DEFAULT_TIMESTAMP_FORMAT", "")
if not DEFAULT_TIMESTAMP_FORMAT:
    DEFAULT_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f%z"


class DateTimeFormat:
    def __init__(
        self,
        timestamp_format=DEFAULT_TIMESTAMP_FORMAT,
        date_format=DEFAULT_DATE_FORMAT,
        nanotime_format=DEFAULT_NANOTIME_FORMAT,
        timezone=None,
        milliseconds_only=False,
    ):
        self.timestamp_format = timestamp_format
        self.date_format = date_format
        self.nanotime_format = nanotime_format
        self.timezone = timezone
        self.milliseconds_only = milliseconds_only  # the microseconds part, .NNNNNN, wil be rounded to .NNN


class CqlType:
    """
    A class for converting a string into a cql type name that can match a formatter
    and a list of its sub-types, if any.
    """

    pattern = re.compile("^([^<]*)<(.*)>$")  # *<*>

    def __init__(self, typestring, ksmeta=None):
        self.type_name, self.sub_types, self.formatter = self.parse(typestring, ksmeta)

    def __str__(self):
        return "{}{}".format(self.type_name, self.sub_types or "")

    __repr__ = __str__

    def get_n_sub_types(self, num):
        """
        Return the sub-types if the requested number matches the length of the sub-types (tuples)
        or the first sub-type times the number requested if the length of the sub-types is one (list, set),
        otherwise raise an exception
        """
        if len(self.sub_types) == num:
            return self.sub_types
        elif len(self.sub_types) == 1:
            return [self.sub_types[0]] * num
        else:
            raise Exception("Unexpected number of subtypes %d - %s" % (num, self.sub_types))

    def parse(self, typestring, ksmeta):
        """
        Parse the typestring by looking at this pattern: *<*>. If there is no match then the type
        is either a simple type or a user type, otherwise it must be a composite type
        for which we need to look-up the sub-types. For user types the sub types can be extracted
        from the keyspace metadata.
        """
        while True:
            m = self.pattern.match(typestring)
            if not m:  # no match, either a simple or a user type
                name = typestring
                if ksmeta and name in ksmeta.user_types:  # a user type, look at ks meta for sub types
                    sub_types = [CqlType(t, ksmeta) for t in ksmeta.user_types[name].field_types]
                    return name, sub_types, format_value_utype
                else:
                    return name, [], self._get_formatter(name)
            else:
                if m.group(1) == "frozen":  # ignore frozen<>
                    typestring = m.group(2)
                    continue

                name = m.group(1)  # a composite type, parse sub types
                return name, self.parse_sub_types(m.group(2), ksmeta), self._get_formatter(name)

    @staticmethod
    def _get_formatter(name):
        return _formatters.get(name.lower())

    @staticmethod
    def parse_sub_types(val, ksmeta):
        """
        Split val into sub-strings separated by commas but only if not within a <> pair
        Return a list of CqlType instances where each instance is initialized with the sub-strings
        that were found.
        """
        last = 0
        level = 0
        ret = []
        for i, c in enumerate(val):
            if c == "<":
                level += 1
            elif c == ">":
                level -= 1
            elif c == "," and level == 0:
                ret.append(val[last:i].strip())
                last = i + 1

        if last < len(val) - 1:
            ret.append(val[last:].strip())

        return [CqlType(r, ksmeta) for r in ret]


def format_value_default(val, colormap, **_):
    val = ensure_text(str(val))
    escapedval = val.replace("\\", "\\\\")
    bval = controlchars_re.sub(_show_control_chars, escapedval)
    return bval


# Mapping cql type base names ("int", "map", etc) to formatter functions,
# making format_value a generic function
_formatters = {}


def format_value(val, cqltype, **kwargs):
    if val == EMPTY:
        return format_value_default("", **kwargs)
    formatter = get_formatter(val, cqltype)
    return formatter(val, cqltype=cqltype, **kwargs)


def get_formatter(val, cqltype):
    if cqltype and cqltype.formatter:
        return cqltype.formatter

    return _formatters.get(type(val).__name__.lower(), format_value_default)


def formatter_for(typname):
    def registrator(f):
        _formatters[typname.lower()] = f
        return f

    return registrator


class BlobType:
    def __init__(self, val):
        self.val = val

    def __str__(self):
        return str(self.val)


@formatter_for("BlobType")
def format_value_blob(val, colormap, **_):
    bval = ensure_text("0x") + ensure_text(binascii.hexlify(val))
    return colorme(bval, colormap, "blob")


formatter_for("bytearray")(format_value_blob)
formatter_for("buffer")(format_value_blob)
formatter_for("blob")(format_value_blob)


def format_python_formatted_type(val, colormap, color, quote=False):
    bval = ensure_text(str(val))
    if quote:
        bval = "'%s'" % bval
    return colorme(bval, colormap, color)


@formatter_for("Decimal")
def format_value_decimal(val, float_precision, colormap, decimal_sep=None, thousands_sep=None, **_):
    if (decimal_sep and decimal_sep != ".") or thousands_sep:
        return format_floating_point_type(val, colormap, float_precision, decimal_sep, thousands_sep)
    return format_python_formatted_type(val, colormap, "decimal")


@formatter_for("UUID")
def format_value_uuid(val, colormap, **_):
    return format_python_formatted_type(val, colormap, "uuid")


formatter_for("timeuuid")(format_value_uuid)


@formatter_for("inet")
def formatter_value_inet(val, colormap, quote=False, **_):
    return format_python_formatted_type(val, colormap, "inet", quote=quote)


@formatter_for("bool")
def format_value_boolean(val, colormap, boolean_styles=None, **_):
    if boolean_styles:
        val = boolean_styles[0] if val else boolean_styles[1]
    return format_python_formatted_type(val, colormap, "boolean")


formatter_for("boolean")(format_value_boolean)


def format_floating_point_type(val, colormap, float_precision, decimal_sep=None, thousands_sep=None, **_):
    if math.isnan(val):
        bval = "NaN"
    elif math.isinf(val):
        bval = "Infinity" if val > 0 else "-Infinity"
    elif thousands_sep:
        dpart, ipart = math.modf(val)
        bval = format_integer_with_thousands_sep(ipart, thousands_sep)
        dpart_str = ("%.*f" % (float_precision, math.fabs(dpart)))[2:].rstrip("0")
        if dpart_str:
            bval += "{}{}".format("." if not decimal_sep else decimal_sep, dpart_str)
    else:
        exponent = int(math.log10(abs(val))) if abs(val) > sys.float_info.epsilon else -sys.maxsize - 1
        if -4 <= exponent < float_precision:
            # when this is true %g will not use scientific notation,
            # increasing precision should not change this decision
            # so we increase the precision to take into account the
            # digits to the left of the decimal point
            float_precision = float_precision + exponent + 1
        bval = "%.*g" % (float_precision, val)
        if decimal_sep:
            bval = bval.replace(".", decimal_sep)

    return colorme(bval, colormap, "float")


formatter_for("float")(format_floating_point_type)
formatter_for("double")(format_floating_point_type)


def format_integer_type(val, colormap, thousands_sep=None, **_):
    # base-10 only for now; support others?
    bval = format_integer_with_thousands_sep(val, thousands_sep) if thousands_sep else str(val)
    bval = ensure_text(bval)
    return colorme(bval, colormap, "int")


# We can get rid of this in cassandra-2.2
def format_integer_with_thousands_sep(val, thousands_sep=","):
    return f"{val:,.0f}".replace(",", thousands_sep)


formatter_for("long")(format_integer_type)
formatter_for("int")(format_integer_type)
formatter_for("bigint")(format_integer_type)
formatter_for("varint")(format_integer_type)
formatter_for("duration")(format_integer_type)


@formatter_for("datetime")
def format_value_timestamp(val, colormap, date_time_format, quote=False, **_):
    if isinstance(val, datetime.datetime):
        bval = strftime(date_time_format.timestamp_format, calendar.timegm(val.utctimetuple()), microseconds=val.microsecond, timezone=date_time_format.timezone)
        if date_time_format.milliseconds_only:
            bval = round_microseconds(bval)
    else:
        bval = ensure_text(str(val))

    if quote:
        bval = "'%s'" % bval
    return colorme(bval, colormap, "timestamp")


formatter_for("timestamp")(format_value_timestamp)


def strftime(time_format, seconds, microseconds=0, timezone=None):
    ret_dt = datetime_from_timestamp(seconds) + datetime.timedelta(microseconds=microseconds)
    ret_dt = ret_dt.replace(tzinfo=UTC())
    if timezone:
        ret_dt = ret_dt.astimezone(timezone)
    try:
        return ret_dt.strftime(time_format)
    except ValueError:
        # CASSANDRA-13185: if the date cannot be formatted as a string, return a string with the milliseconds
        # since the epoch. cqlsh does the exact same thing for values below datetime.MINYEAR (1) or above
        # datetime.MAXYEAR (9999). Some versions of strftime() also have problems for dates between MIN_YEAR and 1900.
        # cqlsh COPY assumes milliseconds from the epoch if it fails to parse a datetime string, and so it is
        # able to correctly import timestamps exported as milliseconds since the epoch.
        return "%d" % (seconds * 1000.0)


microseconds_regex = re.compile(r"(.*)(?:\.(\d{1,6}))(.*)")


def round_microseconds(val):
    """
    For COPY TO, we need to round microsecond to milliseconds because server side
    TimestampSerializer.dateStringPatterns only parses milliseconds. If we keep microseconds,
    users may try to import with COPY FROM a file generated with COPY TO and have problems if
    prepared statements are disabled, see CASSANDRA-11631.
    """
    m = microseconds_regex.match(val)
    if not m:
        return val

    milliseconds = int(m.group(2)) * pow(10, 3 - len(m.group(2)))
    return "%s.%03d%s" % (m.group(1), milliseconds, "" if not m.group(3) else m.group(3))


@formatter_for("Date")
def format_value_date(val, colormap, **_):
    return format_python_formatted_type(val, colormap, "date")


@formatter_for("Time")
def format_value_time(val, colormap, **_):
    return format_python_formatted_type(val, colormap, "time")


@formatter_for("Duration")
def format_value_duration(val, colormap, **_):
    return format_python_formatted_type(duration_as_str(val.months, val.days, val.nanoseconds), colormap, "duration")


def duration_as_str(months, days, nanoseconds):
    builder = list()
    if months < 0 or days < 0 or nanoseconds < 0:
        builder.append("-")

    remainder = append(builder, abs(months), MONTHS_PER_YEAR, "y")
    append(builder, remainder, 1, "mo")
    append(builder, abs(days), 1, "d")

    if nanoseconds != 0:
        remainder = append(builder, abs(nanoseconds), NANOS_PER_HOUR, "h")
        remainder = append(builder, remainder, NANOS_PER_MINUTE, "m")
        remainder = append(builder, remainder, NANOS_PER_SECOND, "s")
        remainder = append(builder, remainder, NANOS_PER_MILLI, "ms")
        remainder = append(builder, remainder, NANOS_PER_MICRO, "us")
        append(builder, remainder, 1, "ns")

    return "".join(builder)


def append(builder, dividend, divisor, unit):
    if dividend == 0 or dividend < divisor:
        return dividend

    builder.append(str(dividend / divisor))
    builder.append(unit)
    return dividend % divisor


def decode_vint(buf):
    return decode_zig_zag_64(decode_unsigned_vint(buf))


def decode_unsigned_vint(buf):
    """
    Cassandra vints are encoded differently than the varints used in protocol buffer.
    The Cassandra vints are encoded with the most significant group first. The most significant byte will contains
    the information about how many extra bytes need to be read as well as the most significant bits of the integer.
    The number extra bytes to read is encoded as 1 bits on the left side.
    For example, if we need to read 3 more bytes the first byte will start with 1110.
    """

    first_byte = next(buf)
    if (first_byte >> 7) == 0:
        return first_byte

    size = number_of_extra_bytes_to_read(first_byte)
    retval = first_byte & (0xFF >> size)
    for i in range(size):
        b = next(buf)
        retval <<= 8
        retval |= b & 0xFF

    return retval


def number_of_extra_bytes_to_read(b):
    return 8 - (~b & 0xFF).bit_length()


def decode_zig_zag_64(n):
    return (n >> 1) ^ -(n & 1)


@formatter_for("str")
def format_value_text(val, encoding, colormap, quote=False, **_):
    escapedval = val.replace("\\", "\\\\")
    if quote:
        escapedval = escapedval.replace("'", "''")
    escapedval = unicode_controlchars_re.sub(_show_control_chars, escapedval)
    bval = escapedval
    if quote:
        bval = f"'{bval}'"
    return bval


# name alias
formatter_for("unicode")(format_value_text)
formatter_for("text")(format_value_text)
formatter_for("ascii")(format_value_text)


def format_simple_collection(  # noqa: PLR0913
    val,
    cqltype,
    lbracket,
    rbracket,
    encoding,
    colormap,
    date_time_format,
    float_precision,
    nullval,
    decimal_sep,
    thousands_sep,
    boolean_styles,
):
    subs = [
        format_value(
            sval,
            cqltype=stype,
            encoding=encoding,
            colormap=colormap,
            date_time_format=date_time_format,
            float_precision=float_precision,
            nullval=nullval,
            quote=True,
            decimal_sep=decimal_sep,
            thousands_sep=thousands_sep,
            boolean_styles=boolean_styles,
        )
        for sval, stype in zip(val, cqltype.get_n_sub_types(len(val)))
    ]
    bval = lbracket + ", ".join(get_str(sval) for sval in subs) + rbracket
    return bval


@formatter_for("list")
def format_value_list(  # noqa: PLR0913
    val,
    cqltype,
    encoding,
    colormap,
    date_time_format,
    float_precision,
    nullval,
    decimal_sep,
    thousands_sep,
    boolean_styles,
    **_,
):
    return format_simple_collection(val, cqltype, "[", "]", encoding, colormap, date_time_format, float_precision, nullval, decimal_sep, thousands_sep, boolean_styles)


@formatter_for("tuple")
def format_value_tuple(  # noqa: PLR0913
    val,
    cqltype,
    encoding,
    colormap,
    date_time_format,
    float_precision,
    nullval,
    decimal_sep,
    thousands_sep,
    boolean_styles,
    **_,
):
    return format_simple_collection(val, cqltype, "(", ")", encoding, colormap, date_time_format, float_precision, nullval, decimal_sep, thousands_sep, boolean_styles)


@formatter_for("set")
def format_value_set(  # noqa: PLR0913
    val,
    cqltype,
    encoding,
    colormap,
    date_time_format,
    float_precision,
    nullval,
    decimal_sep,
    thousands_sep,
    boolean_styles,
    **_,
):
    return format_simple_collection(val, cqltype, "{", "}", encoding, colormap, date_time_format, float_precision, nullval, decimal_sep, thousands_sep, boolean_styles)


formatter_for("frozenset")(format_value_set)
formatter_for("sortedset")(format_value_set)
formatter_for("SortedSet")(format_value_set)


@formatter_for("dict")
def format_value_map(  # noqa: PLR0913
    val,
    cqltype,
    encoding,
    colormap,
    date_time_format,
    float_precision,
    nullval,
    decimal_sep,
    thousands_sep,
    boolean_styles,
    **_,
):
    def subformat(v, t):
        return format_value(
            v,
            cqltype=t,
            encoding=encoding,
            colormap=colormap,
            date_time_format=date_time_format,
            float_precision=float_precision,
            nullval=nullval,
            quote=True,
            decimal_sep=decimal_sep,
            thousands_sep=thousands_sep,
            boolean_styles=boolean_styles,
        )

    subs = [(subformat(k, cqltype.sub_types[0]), subformat(v, cqltype.sub_types[1])) for (k, v) in sorted(val.items())]
    bval = "{" + ", ".join(get_str(k) + ": " + get_str(v) for (k, v) in subs) + "}"
    return bval


formatter_for("OrderedDict")(format_value_map)
formatter_for("OrderedMap")(format_value_map)
formatter_for("OrderedMapSerializedKey")(format_value_map)
formatter_for("map")(format_value_map)


def format_value_utype(  # noqa: PLR0913
    val,
    cqltype,
    encoding,
    colormap,
    date_time_format,
    float_precision,
    nullval,
    decimal_sep,
    thousands_sep,
    boolean_styles,
    **_,
):
    def format_field_value(v, t):
        if v is None:
            return colorme(nullval, colormap, "error")
        return format_value(
            v,
            cqltype=t,
            encoding=encoding,
            colormap=colormap,
            date_time_format=date_time_format,
            float_precision=float_precision,
            nullval=nullval,
            quote=True,
            decimal_sep=decimal_sep,
            thousands_sep=thousands_sep,
            boolean_styles=boolean_styles,
        )

    def format_field_name(name):
        return format_value_text(name, encoding=encoding, colormap=colormap, quote=False)

    subs = [(format_field_name(k), format_field_value(v, t)) for ((k, v), t) in zip(list(val._asdict().items()), cqltype.sub_types)]
    bval = "{" + ", ".join(get_str(k) + ": " + get_str(v) for (k, v) in subs) + "}"
    return bval


NANOS_PER_MICRO = 1000
NANOS_PER_MILLI = 1000 * NANOS_PER_MICRO
NANOS_PER_SECOND = 1000 * NANOS_PER_MILLI
NANOS_PER_MINUTE = 60 * NANOS_PER_SECOND
NANOS_PER_HOUR = 60 * NANOS_PER_MINUTE
MONTHS_PER_YEAR = 12
