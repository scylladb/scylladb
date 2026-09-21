import itertools
import logging
import random
import string
import time
from collections import defaultdict
from copy import deepcopy
from uuid import UUID

import pytest
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement
from ccmlib.node import Node, NodetoolError

from dtest_class import wait_for
from tools.cluster import run_rest_api
from tools.data import rows_to_list
from tools.metrics import get_node_metrics
from tools.misc import flush_by_node

logger = logging.getLogger(__name__)


class TableManager:
    """Class provides interface to create and prefill tables and materialized views by user demand"""

    CLMN_PREFIX = "clmn"
    DEFAULT_MIN_LENGTH = 1
    DEFAULT_MAX_LENGTH = 10

    # TODO: add possibility for PK and CL order
    def __init__(  # noqa: PLR0913
        self,
        session,
        cluster,
        keyspace="ks",
        table_name="tm_table",
        columns=None,
        pk_columns=None,
        cl_columns=None,
        table_options=None,
        all_types_table=False,
    ):
        """
        Function initializes TableManager class
        :param columns: {<column type>: {  'amount': <how many columns with this type>,
                                           'names': [columns names, comma separated],
                                           'prefix': <prefix will be used for column names>,
                                           'frozen': <if frozen: True-frozen/False>},
                                           'value length': value length of this <column type> definition. Dictionary:
                                                        'min': dict defined minimum value length
                                                        'max': dict defined maximum value length
                                        }
                        If columns name is provided, expected:
                        - name for every column of this type(by <how many columns with this type> parameter). If not all names are proveded
                            (according to amount), default names will be used
                        OR
                        If prefix is provided, names of columns will be started from this string
                        OR
                        if 'names' AND 'prefix' are provided, 'names' will be used
                        OR
                        if 'names' or 'prefix' aren't provided, default prefix will be used
                EXAMPLES:
                    1) 'list<text>': {'amount': 1, 'frozen': False}
                    2) {'int': {'amount': 2, 'prefix': 'mypref', 'frozen': False, 'value length': {'min': 1, 'max': 10000}}
                    3) 'text': {'amount': 2, 'names': ['c1', 'c2'], 'frozen': False, 'value length': {'min': 1, 'max': 10000}}
                DEFAULT:
                    {'int': {'amount': 2, 'frozen': False, 'value length': {'min': 1, 'max': 10000}},
                    'text': {'amount': 1, 'frozen': False, 'value length': {'min': 1, 'max': 10000}}
                ***NOTE***: id column with type 'varint' will be aded automatically. Its value wil be unique.
        :type columns: dict
        :param pk_columns: columns for PRIMARY KEYS. Could by defined by:
                            - 'by name': list of explicitly defined names of this type
                            - 'by count': amount of columns of this type
                           {<column type>: {'by count': <how many columns with this type set to PK>}}
                           {<column type>: {'by name': [columns names, comma separated]}}
                      EXAMPLE:
                        {'int': {'by count': 1}, 'text': {'by name': ['c1']}}
                      DEFAULT:
                        {'int': {'by count': 1}}
        :type pk_columns: dict
        :param cl_columns: columns for CLUSTERING. Could by defined by:
                            - 'by name': list of explicitly defined names of this type
                            - 'by amount': amount of columns of this type
                           {<column type>: {'by count': <how many columns with this type set to CLUSTERING>}}
                           {<column type>: {'by name': [columns names, comma separated]}}
                      EXAMPLE:
                        {'int': {'by count': 1}, 'text': {'by name': ['c1']}}
                      DEFAULT:
                        {'text': {'by count': 1}}
                      ***NOTE***: if it shouldn't be clustering keys, send som definitions with 0 counts. For example: {'text': {'by count': 0}}
        :type cl_columns: dict
        :param table_options: WILL BE DEFINED
        :type table_options: dict
        :param all_types_table: create table with all knows column types: True/False.
                                In case True, columns/pk_columns/cl_columns values will be ignored
                                DEFAULT: False
        :type all_types_table: bool
        """
        self.session = session
        self.cluster = cluster
        self.columns_dict = columns or {
            "int": {"amount": 1, "frozen": False, "value length": {"min": self.DEFAULT_MIN_LENGTH, "max": self.DEFAULT_MAX_LENGTH}},
            "text": {"amount": 1, "frozen": False, "value length": {"min": self.DEFAULT_MIN_LENGTH, "max": self.DEFAULT_MAX_LENGTH}},
        }

        self.cl_columns_dict = cl_columns if cl_columns is not None else {"text": {"by count": 1}}
        self.pk_columns_dict = pk_columns if pk_columns is not None else {"int": {"by count": 1}}
        self.keyspace = keyspace
        self.table_name = table_name
        self.columns_list = ["id varint"]
        self.column_names_list = []
        self.pk_list = ["id"]
        self.cl_list = []
        self.table_options = table_options
        self.all_types_table = all_types_table
        self.materialized_views = {}

    def _convert_type_to_preffix(self, ctype, c_def):
        """
        Function defines prefix for the column name (by user request)
        :param ctype: current column type
        :type ctype: str
        :param c_def: dictionary with column definition (from self.columns)
        :type c_def: dict
        :return: column prefix
        :rtype: str
        """
        if "names" in c_def and len(c_def["names"]) == c_def["amount"]:
            return ""
        elif "names" in c_def and len(c_def["names"]) != c_def["amount"]:
            logger.debug(f"Names amount does not coincides with columns amount. Asked create {c_def['amount']} columns, supplied {len(c_def['names'])} names")
        return c_def["prefix"] if "prefix" in c_def else "{clmn_prefix}_{clmn_suffix}".format(clmn_prefix=self.CLMN_PREFIX, clmn_suffix="".join([s[0] for s in ctype.split("<")]) if "<" in ctype else ctype)

    def _set_column_as_key(self, definition_dict, set_list, c_type, clmn_name, exclude_list=None):
        """
        Function creates list of PRIMARY KEYS and CLUSTERING KEYS
        :param definition_dict: dictionary with columns for PRIMARY KEYS (as it described in the __init__ function)
        :type definition_dict: dict
        :param set_list: dictionary with columns for CLUSTERING KEYS (as it described in the __init__ function)
        :type set_list: list
        :param c_type: current column type
        :type c_type: str
        :param clmn_name: current column name
        :type clmn_name: str
        :param exclude_list: list with column names. If current column is already in this list, it won't be added to the set_list
        :type exclude_list: list
        :return: None
        """
        if not exclude_list or (exclude_list and clmn_name not in exclude_list):
            if c_type in definition_dict:
                if "by name" in definition_dict[c_type] and definition_dict[c_type]["by name"] and not set(definition_dict[c_type]["by name"]).issubset(set_list):
                    set_list.extend(definition_dict[c_type]["by name"])
                elif "by count" in definition_dict[c_type] and definition_dict[c_type]["by count"] > 0:
                    set_list.append(clmn_name)
                    definition_dict[c_type]["by count"] -= 1

    def create_table(self):
        # TODO: setup if all_types_table is True
        self._create_columns_list()
        # TODO: add options (like WITH CLUSTERING ORDER BY and others) to the table statement
        # TODO: decide if columns order is important for PK and CK
        statement = "CREATE TABLE IF NOT EXISTS {keyspace_name}.{table_name} ({columns_definition}, PRIMARY KEY(({pks}){cls}))".format(
            keyspace_name=self.keyspace,
            table_name=self.table_name,
            columns_definition=", ".join([s for s in self.columns_list]),
            pks=", ".join([s for s in self.pk_list]),
            cls=", {}".format(", ".join([s for s in self.cl_list])) if self.cl_list else "",
        )
        if self.table_options:
            statement = statement + " WITH"
            for op, value in self.table_options.items():
                statement = f"{statement} {op} = {value}"
        logger.debug(statement)
        self.session.execute(statement)

    def _create_columns_list(self):
        # TODO: add UDT
        for c_type, c_def in self.columns_dict.items():
            preffix = self._convert_type_to_preffix(c_type, c_def)
            for i in range(int(c_def["amount"])):
                clmn_name = f"{preffix}{i}" if preffix else c_def["names"][i]

                self.columns_list.append("{clmn_name} {frozen}{clmn_type}".format(clmn_name=clmn_name, clmn_type=c_type, frozen="frozen " if c_def["frozen"] else ""))
                # Select column as PRIMARY KEY
                self._set_column_as_key(self.pk_columns_dict, self.pk_list, c_type, clmn_name)
                # Select column as CLUSTERING
                self._set_column_as_key(self.cl_columns_dict, self.cl_list, c_type, clmn_name, exclude_list=self.pk_list)
        self.column_names_list = [c.split(" ")[0] for c in self.columns_list]

    def prefill_table(  # noqa: PLR0913
        self,
        rows,
        data=None,
        start_id_from=0,
        consistency=ConsistencyLevel.QUORUM,
        using=None,
        flush=True,
        delay=0,
    ):
        """
        Function pre-fill the table(named as value of self.table_name) with requested rows
        :param rows: how many rows should be in the table
        :type rows: varint
        :return:
        """
        time.sleep(delay)
        data_arr = self._create_data_array(rows, ready_data=data)
        using_str = f" USING {using.keys()[0]} {using[using.keys()[0]]}" if using else ""
        st = "INSERT INTO {ks}.{table_name} ({columns}) VALUES ({values}){using}".format(
            ks=self.keyspace, table_name=self.table_name, columns=", ".join([c.split(" ")[0] for c in self.columns_list]), values=("?," * len(self.columns_list))[:-1], using=using_str
        )
        logger.debug(st)
        statement = self.session.prepare(st)
        statement.consistency_level = consistency

        execute_concurrent_with_args(self.session, statement, map(lambda k: [k + start_id_from] + [data_arr[t][k] for t in range(len(data_arr))], [k for k in range(rows)]))
        if flush:
            flush_by_node(self.cluster)

        logger.debug("Finish prefill")

    def multiple_deletes(self, filters, delay=0):
        """
        :param filters: {<column_name1>: [<value1>,<value2>,..] <column_name2>: [<value1>,<value2>,..], ..}
        """
        time.sleep(delay)
        for i in range(len(next(iter(filters.values())))):
            _filter = {}
            for column, values in filters.items():
                _filter.update({column: values[i]})
            self.delete_row(_filter)

    def delete_row(self, _filter):
        """
        :param _filter: {<column_name1>: <value>, <column_name2>: <value>, ..}
        """
        where_statement = [f"{column}={self.prepare_value(str(value))}" for column, value in _filter.items()]
        query = "delete from {tbl} where {where}".format(tbl=self.table_name, where=" and ".join(s for s in where_statement))
        logger.debug(query)
        self.session.execute(query)

    def truncate_table(self):
        query = f"truncate table {self.table_name}"
        logger.debug(query)
        self.session.execute(query)

    def _create_data_array(self, rows, ready_data=None):
        def _get_random(dupl):  # noqa: PLR0912
            if ready_data and c_type in ready_data and dupl:
                return ready_data[c_type]
            data = []
            value = None
            # TODO: add UDT and collection types
            for i in range(dupl):
                if "int" in c_type:
                    value = random.randint(c_def["value length"]["min"], c_def["value length"]["max"])
                elif c_type in ["text", "ascii", "varchar"]:
                    value = "".join(random.choice(string.ascii_lowercase) for _ in range(c_def["value length"]["min"], c_def["value length"]["max"]))
                elif c_type in ["float", "decimal", "double"]:
                    value = random.uniform(c_def["value length"]["min"], c_def["value length"]["max"])
                elif c_type == "decimal":
                    value = random.uniform(c_def["value length"]["min"], c_def["value length"]["max"])
                elif c_type == "boolean":
                    value = "true"
                elif c_type == "blob":
                    pass
                elif c_type == "timestamp":
                    pass
                elif c_type == "timeuuid":
                    pass
                elif c_type == "uuid":
                    pass
                elif c_type == "time":
                    pass
                elif c_type == "date":
                    pass
                elif c_type == "inet":
                    pass
                data.append(value)
            return data

        data_array = {}
        dupl = 10 if not ready_data else len(ready_data)
        for c_type, c_def in self.columns_dict.items():
            if "value length" not in c_def:
                c_def["value length"] = {"min": self.DEFAULT_MIN_LENGTH, "max": self.DEFAULT_MAX_LENGTH}
            data_array[c_type] = _get_random(dupl) * (rows // dupl) + _get_random(rows % dupl)
        data_array = [data_array[i] for i in [c.split(" ")[1] for c in self.columns_list[1:]]]
        return data_array

    def update_table(  # noqa: PLR0913
        self,
        set_clause,
        where_filter,
        using_clause=None,
        consistency_level=None,
        delay=0,
        update_columns_exclude=None,
    ):
        """
        :param set_clause: which columns should by updated with wich value
                            {'by type': {'int': <new value>, 'text': '<new value>'},
                             'by name': {name1: value, name2: value}}
        :type set_clause: dict
        :param where_filter: dictionary. Will used to build WHERE clause fo the UPDATE statement.
                        [{'by type': {'int': {'value': <value>, 'operator': < =/!=/</>/in >}}},
                       {'by name': {name1: {'value': <value> or [<value>,<value> - in cas when operator in/between], 'operator': < =/!=/</>/in/between >}}}]
        :type where_filter: dict
        :param using_clause: {<option>: <value>}
        :type using_clause: dict
        :param consistency_level:
        :type consistency_level:
        :param delay: delay before start, in seconds. Default: 0
        :type delay: int
        :param update_columns_exclude: list with column names that should by excluded from set clause: [name1, name2]
        :type update_columns_exclude: list
        :return: (set_list, filter_list)
        :rtype: tuple of lists
        """
        # TODO: add - select columns from materialized views
        if delay:
            time.sleep(delay)
        set_dict = self._build_set_clause(set_clause, exclude_columns=update_columns_exclude or [])
        filter_dict = self._build_filter(where_filter)

        if filter_dict:
            set_str = " and ".join(f"{name} = {self.prepare_value(value)}" for name, value in set_dict.items())
            filter_str = " and ".join(
                "{} {} {}".format(
                    name, value["operator"], "({})".format(", ".join([self.prepare_value(str(i)) for i in value["value"]])) if isinstance(value["value"], list) and value["operator"] == "in" else self.prepare_value(value["value"])
                )
                for name, value in filter_dict.items()
            )
            using_str = f" USING {next(iter(using_clause.keys()))} {using_clause[next(iter(using_clause.keys()))]}" if using_clause else ""

            statement = "UPDATE {ks}.{table_name}{using} SET {set_clause} WHERE {filter}".format(ks=self.keyspace, table_name=self.table_name, using="" if not using_clause else using_str, set_clause=set_str, filter=filter_str)
            logger.debug(f"{statement}")
            self.session.execute(statement)
            return (set_dict, filter_str)
        return (None, None)

    def multiple_int_updates_by_id(  # noqa: PLR0913
        self,
        update_to_boundaries,
        filter_values=None,
        ids=None,
        updated_columns=None,
        updates=100,
        same_id=True,
        delay=0,
    ):
        if ids is None:
            ids = []
        if filter_values is None:
            filter_values = []
        time.sleep(delay)
        query = f"select * from {self.table_name}"
        updated_columns = updated_columns or [c for c in self.column_names_list if f"{c} int" in self.columns_list and c not in self.pk_list + self.cl_list]

        res = list(self.session.execute(query + " LIMIT 1"))
        updated_column = updated_columns[random.randint(0, len(updated_columns) - 1)]
        updated_column_index = next(i for i, clmn in enumerate(res[0]._fields) if clmn == updated_column)
        _id = None
        id_condition = False if not same_id else None
        res = list(self.session.execute(query))
        for _ in range(updates):
            # Select column for update
            if not ids:
                k = 0
                if not same_id:
                    res = list(self.session.execute(query))
                while not id_condition:
                    i = random.randint(0, len(res) - 1)
                    if (filter_values and res[i][updated_column_index] in filter_values) or not filter_values:
                        _id = res[i].id
                        id_condition = False if not same_id else _id
                        break
                    k += 1
                    if k > len(res):
                        break
            else:
                _id = ids[random.randint(0, len(ids) - 1)]

            if _id is not None:
                self.update_table(set_clause={"by name": {updated_column: random.randint(update_to_boundaries[0], update_to_boundaries[1])}}, where_filter={"by name": {"id": {"operator": "=", "value": _id}}})

        logger.debug("Updates finished")

    def select_all_mvs(self, reads=100, by_id=False):
        logger.debug("Start reads from MVs")
        statement_template = "select * from {0}"
        if by_id:
            max_id = self.get_max_id()
            statement_template = statement_template + " where id={1}"
        else:
            statement_template = statement_template + " LIMIT 10"

        for _ in range(reads):
            i = random.randint(0, len(self.materialized_views) - 1)
            mv_name = next(name for j, name in enumerate(self.materialized_views.keys()) if j == i)
            statement = statement_template.format(mv_name, random.randint(0, max_id)) if by_id else statement_template.format(mv_name)
            logger.debug(statement)
            self.session.execute(statement)
        logger.debug("Finish reads from MVs")

    def prepare_value(self, value):
        try:
            _ = int(value)
            return value
        except ValueError:
            return f"'{value}'"

    def _build_filter(self, where_filter):
        clause = {}
        f, by = (where_filter["by name"], "name") if "by name" in where_filter else (where_filter["by type"], "type")
        for clmn, value in f.items():
            name = ""
            if by == "name":
                if clmn in self.pk_list + self.cl_list:
                    name = clmn
            elif by == "type":
                name = self._get_column_by_type(clmn, include_list=self.pk_list + self.cl_list)

            if name:
                clause.update({name: value})

        return clause

    def _build_set_clause(self, set_clause, exclude_columns=None):
        clause = {}
        for utype, udef in set_clause.items():
            s, by = udef, utype.replace("by ", "")
            # s, by = (clause['by name'], 'name') if 'by name' in clause else (clause['by type'], 'type')
            for name, new_value in s.items():
                update_item = ""
                if by == "name":
                    if [n for n in self.columns_list if f"{name} " in n] and name not in self.pk_list + self.cl_list + list(clause.keys()):
                        update_item = name
                elif by == "type":
                    update_item = self._get_column_by_type(name, exclude_list=self.pk_list + self.cl_list + list(clause.keys()) + exclude_columns)
                if update_item:
                    clause.update({update_item: new_value})
        return clause

    def _get_column_by_type(self, _type, include_list=None, exclude_list=None):
        exclude_list = exclude_list or []
        include_list = include_list or []
        for clmn in self.columns_list:
            name = clmn.split(" ")[0]
            if f" {_type}" in clmn and name not in exclude_list and (not include_list or (include_list and name in include_list)):
                return name
        return None

    def get_value_for_filter(self, row_index=0):
        statement = "SELECT {pks} FROM {ks}.{table_name}".format(ks=self.keyspace, table_name=self.table_name, pks=", ".join(name for name in self.pk_list + self.cl_list))

        res = self.session.execute(statement)
        result = {}
        for i, name in enumerate(list(res.current_rows[row_index]._fields)):
            result.update({name: {"value": res.current_rows[row_index][i], "operator": "="}})
        return result

    def get_max_id(self):
        _id = self.session.execute(f"select max(id) as id from {self.table_name}").current_rows[0].id
        return 0 if not _id else _id

    def set_mv(self, mv_name, mv_self_arr):
        self.materialized_views[mv_name] = mv_self_arr

    def remove_mv(self, mv_name):
        del self.materialized_views[mv_name]


class MaterializedViewManager:
    """Class provides interface to create and manage materialized views"""

    TEMPLATE_MV_NAME = "{0}_mv_{1}"

    def __init__(self, parent_table, mv_name=None):
        self.parent_table = parent_table
        self.mv_name = mv_name or self.TEMPLATE_MV_NAME.format(self.parent_table.table_name, 0)
        self.mv_columns_list = None
        self.mv_pk_list = None
        self.mv_cl_list = None
        self.mv_where_clause = None
        self.mv_options = None
        if self.mv_name in self.parent_table.materialized_views:
            if mv_name:
                self.mv_columns_list = self.parent_table.materialized_views.mv_columns_list
                self.mv_pk_list = self.parent_table.materialized_views.mv_pk_list
                self.mv_cl_list = self.parent_table.materialized_views.mv_cl_list
                self.mv_where_clause = self.parent_table.materialized_views.mv_where_clause
                self.mv_options = self.parent_table.materialized_views.mv_options
            else:
                mv_index = max([int(mv.split("_")[-1]) for mv in self.parent_table.materialized_views.keys()]) + 1
                self.mv_name = self.TEMPLATE_MV_NAME.format(self.parent_table.table_name, mv_index)

    # TODO: add possibility for PK and CL order
    def create_materialized_view(  # noqa: PLR0913
        self,
        mv_columns=None,
        mv_pk_column=None,
        mv_cl_column=None,
        mv_where_restriction=None,
        options=None,
        wait_for_view_built=True,
    ):
        """
        :param mv_columns: {<column type>: {  'amount': <how many columns with this type>,
                                            'names': [columns names, comma separated]
                                           }
                                    If columns name is provided, expected:
                                    - name for existent column of this type(by <how many columns with this type> parameter).
                                    OR
                                    if 'names' isn't provided, random existent column of this type will be selected
                            EXAMPLES:
                                1) {'int': {'amount': 2}, 'float': {'amount': 1}}
                                2) {'text': {'names': ['c1', 'c2']}}
        :param mv_columns: dict
        :param mv_pk_column: define which column should be added to the table primary key
                            Expected dict structure - by type or by name:
                            {'type': <random column with this type will be selected from table columns>}
                            OR
                            {'names': <existent column name>}
                            EXAMPLE:
                            {'type': 'text'} OR {'name': 'c1'}
        :type mv_pk_column: dict
        :param mv_where_restriction: define restriction in the WHERE clause if it's NOT NULL condition.
                                    Expected dict structure:
                                    {'names': {<column name>: {'operator': 'operator: =/in',
                                                               'value': <value according to column type OR LIST of values in case "in" operator>}}}
                                    OR
                                    {'position': {<position in the PK+CL list>: {'operator': 'operator: =/in',
                                                               'value': <value according to column type OR LIST of values in case "in" operator>}}}
                                    EXAMPLE:
                                    {'names': {'clmn_int0: {'operator': '>', 'value': 1}}}
                                    OR
                                    {'position': {-1: {'operator': 'in', 'value': [1, 2, 3]}}}
        :type mv_where_restriction: dict
        :return:
        """
        if self.mv_name not in self.parent_table.materialized_views:
            self.mv_columns_list = self._create_mv_columns_list(mv_columns) or ["*"]
            self.mv_pk_list = self.create_mv_pk_list(mv_pk_column)
            self.mv_cl_list = self.parent_table.cl_list or []
            self.mv_where_restriction = self._restriction_list(mv_where_restriction)
            self.mv_where_clause = " and ".join([f"{c} IS NOT NULL" for c in self.mv_pk_list + self.mv_cl_list if c not in self.mv_where_restriction])
            if self.mv_where_restriction:
                where_str = " and ".join(
                    [
                        "{} {} {}".format(
                            name,
                            value["operator"],
                            "({})".format(", ".join([self.parent_table.prepare_value(str(i)) for i in value["value"]]) if isinstance(value["value"], list) else value["value"]) if value["operator"] == "in" else value["value"],
                        )
                        for name, value in self.mv_where_restriction.items()
                    ]
                )
                self.mv_where_clause = f"{self.mv_where_clause} and {where_str}"
            # TODO: add option to filter
            # TODO: ADD CLUSTERING OPTION
            # self.mv_options =
            statement = "CREATE MATERIALIZED VIEW {ks}.{mv_name} AS SELECT {mv_columns} FROM {ks}.{table_name} WHERE {where_clause} PRIMARY KEY ({pk}{cl})".format(
                mv_name=self.mv_name,
                ks=self.parent_table.keyspace,
                mv_columns=", ".join([k for k in self.mv_columns_list]),
                table_name=self.parent_table.table_name,
                where_clause=self.mv_where_clause,
                pk=", ".join([k for k in self.mv_pk_list]),
                cl="" if not self.parent_table.cl_list or set(self.parent_table.cl_list).issubset(self.mv_pk_list) else ", {}".format(", ".join([k for k in self.mv_cl_list])),
            )
            logger.debug(statement + ";")
            self.parent_table.session.execute(statement)

            if wait_for_view_built:
                wait_for_view(cluster=self.parent_table.cluster, session=self.parent_table.session, ks=self.parent_table.keyspace, view=self.mv_name)

            if options:
                for op, value in options.items():
                    self.parent_table.session.execute(f"ALTER MATERIALIZED VIEW {self.parent_table.keyspace}.{self.mv_name} WITH {op} = {value}")
            logger.debug(f"Materialized view {self.mv_name} has been created")
            self.parent_table.set_mv(self.mv_name, self)

    def drop_mv(self):
        logger.debug(f"Start drop materialized view {self.mv_name}")
        self.parent_table.session.execute(f"drop materialized view {self.mv_name}")
        logger.debug(f"Finish drop materialized view {self.mv_name}")
        self.parent_table.remove_mv(mv_name=self.mv_name)
        self.mv_name = ""
        self.mv_columns_list = None
        self.mv_pk_list = None
        self.mv_cl_list = None
        self.mv_where_clause = None
        self.mv_options = None

    def _restriction_list(self, mv_where_restriction):
        restriction_dict = {}
        if mv_where_restriction:
            pk_list = self.mv_pk_list + self.mv_cl_list
            if "names" in mv_where_restriction:
                restriction_dict.update(mv_where_restriction["names"])
            elif "position" in mv_where_restriction:
                for position, r_def in mv_where_restriction["position"].items():
                    if len(pk_list) > position:
                        restriction_dict.update({pk_list[position]: r_def})

        return restriction_dict

    def _create_mv_columns_list(self, mv_columns, exclude_list=None):
        """
        :param mv_columns: as described in create_materialized_view.mv_columns
        :param mv_columns: dict
        :param exclude_list: list of column that already selected and should be excluded from new list
        :param exclude_list: list
        :return:
        :rtype: list
        """
        # TODO: add UDT
        if not mv_columns:
            return mv_columns

        exclude_list = exclude_list or []
        mv_columns_list = []
        mv_columns_list.extend(list(itertools.chain.from_iterable([self._build_columns_list(c_type, c_def, mv_columns_list + exclude_list) for c_type, c_def in mv_columns.items()])))
        return mv_columns_list

    def _build_columns_list(self, c_type, c_def, exclude_list):
        column_names = []
        if "names" in c_def:
            column_names = [clmn for clmn in c_def["names"] if f"{clmn} {c_type}" in self.parent_table.columns_list and clmn not in column_names + exclude_list]
        else:
            for i in range(c_def["amount"]):
                column_names.append(next(clmn.split(" ")[0] for clmn in self.parent_table.columns_list if f" {c_type}" in clmn and clmn.split(" ")[0] not in column_names + exclude_list))
        return column_names

    def create_mv_pk_list(self, mv_pk_column):
        """
        Just one non-primary key column can be added to the PK in the materialized view
        :param mv_pk_column: define which column should be added to the table primary key
                            Expected dict structure - by type or by name:
                            {'type': <random column with this type will be selected from table columns>}
                            OR
                            {'names': <existent column name>}
                            EXAMPLE:
                            {'type': 'text'} OR {'name': 'c1'}
        :param mv_pk_column: dict
        :return:
        :rtype: list
        """
        mv_pk_column_list = deepcopy(self.parent_table.pk_list)
        if "names" in mv_pk_column:
            for name in mv_pk_column["names"]:
                if name in self.parent_table.column_names_list and name not in mv_pk_column_list:
                    mv_pk_column_list.append(name)
        elif "type" in mv_pk_column:
            clmns = [clmn.split(" ")[0] for clmn in self.parent_table.columns_list if " {}".format(mv_pk_column["type"]) in clmn and clmn.split(" ")[0] not in mv_pk_column_list + (self.parent_table.cl_list or [])]
            if not clmns:
                logger.debug("ERROR: new column for Materialized View PK is not found. Received parameters: {}".format(mv_pk_column["type"]))
            else:
                mv_pk_column_list.append(clmns[0])
        return mv_pk_column_list

    def my_count_query(self, _filter=None):
        # TODO: handle filter
        return "SELECT COUNT(*) FROM {my_name}{where_clause}".format(my_name=self.mv_name, where_clause=_filter or "")


# wait_for_view waits for the given materialized view to have been built on
# all *living* nodes.
# In this implementation, nodes which are not alive may or may not have
# finished building the view when wait_for_view returns. This was a deliberate
# implementation choice - we also know the state of the build for dead nodes,
# but waiting only for live nodes makes it easier to write tests which check
# how view building and dead nodes interact.
def wait_for_view(cluster, session, ks, view, raise_exception=True, timeout=600, cl=None):  # noqa: PLR0913
    num_nodes = len(cluster.nodelist())
    num_alive = len([node for node in cluster.nodelist() if node.is_live()])
    if cl is None:
        cl = ConsistencyLevel.ONE if num_alive == 1 else ConsistencyLevel.QUORUM
    cl_name = ConsistencyLevel.value_to_name[cl]
    logger.debug(f"Waiting for view {ks}.{view} to finish building: num_nodes={num_nodes} num_alive={num_alive} cl={cl_name}")

    query = SimpleStatement(view_built_status_query(ks, view, "host_id,status"), consistency_level=cl)

    def _view_build_finished_on_live_nodes():
        status = defaultdict(set)
        entries = rows_to_list(session.execute(query))
        for entry in entries:
            status[entry[1]].add(entry[0])
        logger.debug(f"wait_for_view {ks}.{view}: status={status}")
        done = status["SUCCESS"]
        for node in cluster.nodelist():
            try:
                if node.is_live() and not (UUID(node.hostid()) in done):
                    logger.debug(f"wait_for_view {ks}.{view}: node {node.hostid()}/{node.address()} is not done yet")
                    return False
            except NodetoolError:
                # If we decomissioned a node with "nodetool decommission"
                # the code above may temporarily think that node.is_alive()
                # is still true, but node.hostid(), which calls nodetool,
                # can fail with an exception. In this case we just need to
                # consider this node non-live.
                pass
        return True

    if hasattr(cluster, "scylla_mode") and cluster.scylla_mode == "debug":
        timeout *= 2

    deadline = time.time() + timeout
    while time.time() < deadline:
        if _view_build_finished_on_live_nodes():
            return True
        time.sleep(1)

    error_msg = f"View {ks}.{view} not built"
    if raise_exception:
        raise Exception(error_msg)
    else:
        logger.debug(error_msg)
        return False


def view_built_status_query(ks="", view="", select_column="status"):
    query = f"SELECT {select_column} FROM system_distributed.view_build_status"
    if ks or view:
        query = f"{query} WHERE "
        where = " AND ".join([f"{k} = '{v}'" for k, v in {"keyspace_name": ks, "view_name": view}.items() if v])
        if ks:
            query = f"{query} {where}"
    return query


def index_is_built(cluster, session, ks_name, table_name, index_name, raise_exception=True, timeout=600):  # noqa: PLR0913
    wait_for_view(cluster, session, ks_name, get_index_view_name(index_name), raise_exception=raise_exception, timeout=timeout)
    return len(list(session.execute(f"SELECT * FROM system_schema.indexes WHERE keyspace_name = '{ks_name}' and table_name ='{table_name}' AND index_name='{index_name}'"))) == 1


def get_index_view_name(index_name):
    return f"{index_name}_index"


def view_update_generation_metrics(node):
    return get_node_metrics(node.address(), metrics=["pending_registrations", "queued_batches_count", "sstables_to_move_count", "sstables_pending_work"])


def view_update_generation_done(node):
    metrics = view_update_generation_metrics(node)
    for v in metrics.values():
        if v:
            return False
    return True


def wait_for_view_update_generation(node, timeout=60):
    wait_for(view_update_generation_done, text="view_update_generation_done", timeout=timeout, node=node)


def sync_hinted_handoff(node, target_nodes: list[Node] | None = None, timeout=60) -> bool:
    """
    Waits for hints to be sent to `target_nodes` (all cluster nodes by default).
    Timeout in seconds after which the query returns even if hints are still being replayed.
    - No value or 0 will cause the query to return immediately.
    - A negative value will cause the query to wait until the sync point is reached
    Returns `True` if and only if hints were replayed successfully.
            `False` if `timeout` is 0 and hints are still being replayed, otherwise
            if timeout > 0, a `TimeoutError` is raised
    """
    params = dict()
    if target_nodes:
        params["target_hosts"] = ",".join([node.address() for node in target_nodes])
    sync_point_id = run_rest_api(node, "/hinted_handoff/sync_point", params=params, api_method="post").json()
    params = {"id": str(sync_point_id)}
    if timeout:
        params["timeout"] = str(timeout)
    result = run_rest_api(node, "/hinted_handoff/sync_point", params=params, api_method="get").json()
    if result == "DONE":
        return True
    assert result == "IN_PROGRESS"
    if timeout > 0:
        # we've waited for specified time, but hints are still being sent, so timeout
        raise TimeoutError(f"hinted_handoff_sync_point: id={sync_point_id} result={result} timeout={timeout}")
    elif timeout == 0:
        # we checked the status, and returned immediately
        return False
    else:
        # timeout < 0, we should wait forever before all hints are replayed, so
        # result should have been "DONE". but it is "IN_PROGRESS"
        assert False, f"sync_point api call is expected to wait forever when timeout={timeout} is negative"
