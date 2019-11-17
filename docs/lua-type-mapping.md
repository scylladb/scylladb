# CQL to Lua type mapping

This document describes how CQL types are mapped to and from Lua in UDF.

## ASCII, TEXT and VARCHAR

When converting to Lua, these types are mapped to Lua strings.

When converting to CQL, types that Lua coerces to string are
accepted. The values are verified to be valid ASCII or UTF8 as
appropriate.

## BLOB

Lua strings can hold arbitrary data, so blobs are converted to and
from Lua strings.

## TINYINT, SMALLINT, INT and BIGINT

Lua 5.3 has native support for 64 bit integers, so all these types are
converted to a native integer.

When converting to CQL, types that Lua coerces to integers are
supported. If the value is too large for the CQL type, it wraps.

## BOOLEAN

This maps exactly to and from a Lua boolean.

## COUNTER

Since UDF are pure functions, a counter is mapped to and from a native
integer.

## FLOAT and DOUBLE

When converting to Lua, these are mapped o Lua's native double.

When converting to CQL, a number is rounded to a float or double value
as appropriate. For example, it is valid to return a integer that is
too large to have an exact representation as a double.

## VARINT and DECIMAL

These are mapped to a userdata since there is no native support for
large numbers is Lua. We try to provide type coercions similar to
those Lua has. For example, a UDF returning the CQL type bigint, can
return the value 42 with Lua code that returns

* The integer 42
* The double 42
* The string "42"
* The varint 42
* The decimal 42

## DATE

When converting to Lua, a date is mapped to the number of days since
epoch + 2^31.

When converting to CQL, we support:
* integer (number of days since epoch + 2^31).
* CQL date literal in a string.
* A lua "date table", but with only year, month and day

## DURATION

When converting to Lua we produce the table { months = v1, days = v2, nanoseconds = v3 }.

When converting to CQL we support a table in the same format or a CQL
duration literal in a string.

## INET

This is converted to and from a string ("127.0.0.1" or "::1").

## TIME

When converting to Lua we pass an integer with the number of seconds
since midnight.

When converting to CQL we support an integer with the same meaning or
a CQL time literal in a string.

## TIMESTAMP

When converting to Lua we pass an integer with the number of
milliseconds since the epoch.

When converting to CQL we support integers with the same meaning or
CQL timestamp literal in a string.

## TIMEUUID
## UUID

These are converted to and from the string representation.

## LIST

A list is represented as a sequence in lua. A list with "foo" and
"bar" is represented as {"foo", "bar"}. It is an error to return a
table that is not a sequence.

## TUPLE

A tuple is represented the same way as a list, but on return we check
the number of elements and the types.

## MAP

These are converted to and from lua tables. A map from "foo" to 42 is
represented as {foo = 42}.

## UDT

A UDT is represented by a table like a map, but on return we check
that:
* There are no unexpected keys
* All expected keys are present
* The fields have valid types

## SET

A set is represented in lua by the keys of a table. A set with 1, 2
and 3 is represented by as {[1] = true, [2] = true, [3] = true}. On
return the values are checked to be true.

Note that, like every other Lua table, the set is underscored. It is
sorted when converting back to CQL.
