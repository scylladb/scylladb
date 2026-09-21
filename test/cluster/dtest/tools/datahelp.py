"""
This module is a data-creation utility which allows creating data using markdown-style tables.

For example, this 'data' string specifies data to be created in 5 rows.
            data = "
                |id| value          |
                |--+----------------|
                |1 |testing         |
                |2 |and more testing|
                |3 |and more testing|
                |4 |and more testing|
                |5 |and more testing|
                "

To take the markdown-stye string above and insert data, call create_rows:

expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': unicode})

create_rows returns a data structure which represents what the data _should_ be like in the database.
It's meant to be used in tests when comparing expected to actual data, for validation.

For more examples reference paging_test.py
"""

import random
import re
import string
import time
from uuid import uuid1, uuid4

from cassandra.concurrent import execute_concurrent_with_args


def strip(val):
    # remove spaces and pipes from beginning/end
    return val.strip().strip("|")


def parse_headers_into_list(data):
    # throw out leading/trailing space and pipes
    # so we can split on the data without getting
    # extra empty fields
    rows = list(map(strip, data.split("\n")))

    # remove any remaining empty lines (i.e. '') from data
    rows = [_f for _f in rows if _f]

    # separate headers from actual data and remove extra spaces from them
    headers = [str(h.strip()) for h in rows.pop(0).split("|")]
    return headers


def get_row_multiplier(row):
    # find prefix like *1234 meaning create 1,234 rows
    row_cells = [l.strip() for l in row.split("|")]
    m = re.findall(r"\*(\d+)$", row_cells[0])

    if m:
        return int(m[0])

    return None


def row_has_multiplier(row):
    if get_row_multiplier(row) is not None:
        return True

    return False


def parse_row_into_dict(row, headers, format_funcs=None):
    row_cells = [l.strip() for l in row.split("|")]

    if row_has_multiplier(row):
        row_multiplier = get_row_multiplier(row)
        row = "|".join(row_cells[1:])  # cram remainder of row back into foo|bar format
        multirows = []

        for i in range(row_multiplier):
            multirows.append(parse_row_into_dict(row, headers, format_funcs=format_funcs))
        return multirows

    row_map = dict(list(zip(headers, row_cells)))

    if format_funcs:
        for colname, value in list(row_map.items()):
            func = format_funcs.get(colname)

            if func is not None:
                row_map[colname] = func(value)

    return row_map


def row_describes_data(row):
    """
    Returns True if this appears to be a row describing data, otherwise False.

    Meant to be used in conjunction with filter to prune out those rows
    that don't actually describe data, such as empty strings or decorations
    that delimit headers from actual data (i.e. '+----|----|-----+')
    """
    if row:
        if row.startswith("+") and row.endswith("+"):
            return False

        return True

    return False


def parse_data_into_dicts(data, format_funcs=None):
    # throw out leading/trailing space and pipes
    # so we can split on the data without getting
    # extra empty fields
    rows = list(map(strip, data.split("\n")))

    # remove any remaining empty/decoration lines (i.e. '') from data
    rows = list(filter(row_describes_data, rows))

    # remove headers
    headers = parse_headers_into_list(rows.pop(0))

    values = []

    for row in rows:
        if row_has_multiplier(row):
            values.extend(parse_row_into_dict(row, headers, format_funcs=format_funcs))
        else:
            values.append(parse_row_into_dict(row, headers, format_funcs=format_funcs))

    return values


def create_rows(data, session, table_name, cl=None, format_funcs=None, prefix="", postfix=""):  # noqa: PLR0913
    """
    Creates db rows using given session, with table name provided,
    using data formatted like:

    |colname1|colname2|
    +--------+--------+
    |value2  |value2  |

    format_funcs should be a dictionary of {columnname: function} if data needs to be formatted
    before being included in CQL.

    Returns a list of maps describing the data created.
    """
    values = []
    dicts = parse_data_into_dicts(data, format_funcs=format_funcs)

    # use the first dictionary to build a prepared statement for all
    prepared = session.prepare(
        "{prefix} INSERT INTO {table} ({cols}) values ({vals}) {postfix}".format(prefix=prefix, table=table_name, cols=", ".join(list(dicts[0].keys())), vals=", ".join("?" for k in list(dicts[0].keys())), postfix=postfix)
    )
    if cl is not None:
        prepared.consistency_level = cl

    query_results = execute_concurrent_with_args(session, prepared, [list(d.values()) for d in dicts])

    for i, (status, result_or_exc) in enumerate(query_results):
        # should maybe check status here before appening to expected values
        values.append(dicts[i])

    return values


def flatten_into_set(iterable):
    # use flatten() then convert to a set for set comparisons
    return set(flatten(iterable))


def flatten(list_of_dicts):
    # flatten list of dicts into list of strings for easier comparison
    # and easier set membership testing (e.g. foo is subset of bar)
    flattened = []

    for _dict in list_of_dicts:
        sorted_keys = sorted(_dict)
        items = [f"{k}__{_dict[k]}" for k in sorted_keys]
        flattened.append("__".join(items))

    return flattened


class ColumnType:
    def __init__(self, _type, limits=None):
        self.type = _type
        self.limits = limits
        self.value = self.generate_value(self.type)

    def get_value(self):
        return self.value

    def gen_random_string(self, length=1, source=string.printable):
        return "".join(random.choices(source, k=length))

    def gen_random_number(self, length):
        return int(self.gen_random_string(length=length, source=string.digits), 10) if length > 0 else 0

    def gen_random_decimal_number(self, length):
        int_idx = random.randint(0, length - 1)
        dec_idx = length - int_idx
        int_num = self.gen_random_number(int_idx)
        dec_num = self.gen_random_number(dec_idx)

        return float(".".join([str(int_num), str(dec_num)]) if dec_idx > 0 else int_num)

    def generate_value(self, data_type):  # noqa: PLR0912
        """
        types are:
        [ascii, bigint, blob, boolean, date, decimal, double, float, inet, int, list, map, smallint, set, text, time,
        timestamp, timeuuid, tinyint, tuple, UDT, uuid, varchar, varint]
        :return: a random value by its type definition
        """
        if data_type.lower() == "uuid":
            value = uuid4()
        elif data_type.lower() in ["ascii", "text", "varchar"]:
            value = "'{}'".format(self.gen_random_string(length=10).replace("'", " "))
        elif data_type.lower() == "bigint":
            value = random.randint(-9223372036854775808, 9223372036854775807)
        elif data_type.lower() == "blob":
            value = hex(random.randint(0, 4294967295))
            if len(value) % 2 != 0:
                value = value[:-1]
        elif data_type.lower() == "boolean":
            value = random.choice([True, False])
        elif data_type.lower() == "date":
            value = f"'{random.randint(0, 2999)}-{random.randint(1, 12)}-{random.randint(1, 31)}'"
        elif data_type.lower() == "time":
            value = f"'{random.randint(0, 23)}:{random.randint(0, 59)}:{random.randint(0, 59)}'"
        elif data_type.lower() == "timestamp":
            value = int(time.time())
        elif data_type.lower() == "timeuuid":
            value = uuid1(int(time.time()))
        elif data_type.lower() in ["decimal", "double", "float"]:
            value = self.gen_random_decimal_number(length=random.randint(2, 15))
        elif data_type.lower() == "inet":
            value = f"'{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}'"
        elif data_type.lower() in ["int", "varint"]:
            value = random.randint(-2147483648, 2147483647)
        elif data_type.lower() == "smallint":
            value = random.randint(-32768, 32767)
        elif data_type.lower() == "tinyint":
            value = random.randint(-128, 127)
        elif data_type.lower() == "list":
            value = [self.generate_value("int") for _ in range(3)]
        elif data_type.lower() == "map":
            value = "".join(["{'", self.gen_random_string(5, source=string.ascii_letters), "': ", str(self.generate_value("int")) + "}"])
        elif data_type.lower() == "udt":
            value = "{a: '" + self.gen_random_string(5, source=string.ascii_letters) + "', b: '" + self.gen_random_string(5, source=string.ascii_letters) + "'}"
        elif data_type.lower() == "set":
            value = "{ " + str(self.generate_value("int")) + ", " + str(self.generate_value("int")) + " }"
        elif data_type.lower() == "tuple":
            value = "({})".format(self.generate_value("int"))
        else:
            value = None
        return value
