#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from cassandra import ConsistencyLevel                  # type: ignore
from cassandra.query import SimpleStatement             # type: ignore
import re
from tabulate import tabulate                           # type: ignore


def test_cql(request, cql, keyspace):
    # Comments allowed by CQL - -- and //
    comment_re = re.compile(r"^\s*((--|//).*)?$")
    # A comment is not a delimiter even if ends with one
    delimiter_re = re.compile(r"^(?!\s*(--|//)).*;\s*$")
    with open(request.config.getoption("--input"), "r") as ifile, \
            open(request.config.getoption("--output"), "a") as ofile:
        cql.set_keyspace(keyspace)

        while True:
            line = ifile.readline()
            statement = ""
            pretty_statement = "> "
            if not line:
                break
            # Handle multiline input and comments
            if comment_re.match(line):
                ofile.write("> {}".format(line))
                continue
            statement += line
            pretty_statement += line
            while not delimiter_re.match(line):
                # Read the rest of input until delimiter or EOF
                line = ifile.readline()
                if not line:
                    break
                statement += line
                pretty_statement += "> {}".format(line)
            ofile.write(pretty_statement)

            stmt = SimpleStatement(statement,
                                   consistency_level=ConsistencyLevel.ONE,
                                   serial_consistency_level=ConsistencyLevel.SERIAL)

            def prettify(rows):
                if rows is None:
                    return "null"
                elif isinstance(rows, (str, float, int, bool)) or not hasattr(rows, '__iter__'):
                    return rows
                return [prettify(row) for row in rows]

            try:
                result = cql.execute(stmt)
                if not result.column_names:
                    ofile.write("OK\n")
                else:
                    ofile.write(tabulate(prettify(result.current_rows), result.column_names,
                                         tablefmt="psql"))
                    ofile.write("\n")
            except Exception as e:
                # Avoid host name in the message - make output stable
                msg = re.sub(r"\d+\.\d+\.\d+\.\d+", "127.0.0.1", str(e))
                ofile.write("{}\n".format(msg))
