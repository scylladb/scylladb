#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import datetime
import logging

from concurrent.futures.thread import ThreadPoolExecutor

from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args


logger = logging.getLogger(__name__)



def rows_to_list(rows):
    new_list = [list(row) for row in rows]
    return new_list


def run_in_parallel(functions_list):
    """
    Runs the functions that are passed in proc_functions in parallel using threads.
    :param functions_list: variable holds list of dictionaries with threads definitions. Expected structure:
                           [{'func': <function pointer - the function will be runs from the thread>,
                             'args': (arg1, arg2, arg3), - explicit function arguments by order in the function
                             'kwargs': {<arg name1>: value, <arg name2>: value} - function arguments by name
                            }, - first thread definition
                            {{'func': <function pointer, 'args': (), 'kwargs': {}} - second thread, no arguments
                           ]
    :param functions_list: list
    :return: list of functions' return values
    :rtype: list
    """
    logger.debug(f"Threads start at {datetime.datetime.now()}")
    pool = ThreadPoolExecutor(max_workers=len(functions_list))
    tasks = []
    for func in functions_list:
        args = func["args"] if "args" in func else []
        kwargs = func["kwargs"] if "kwargs" in func else {}
        tasks.append(pool.submit(func["func"], *args, **kwargs))
    results = [task.result() for task in tasks]
    logger.debug(f"'{len(results)}' threads finished at {datetime.datetime.now()}")
    return results
