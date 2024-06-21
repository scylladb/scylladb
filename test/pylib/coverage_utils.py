#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2024-present ScyllaDB
#
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import argparse
import asyncio
import multiprocessing
from pathlib import Path, PurePath
from typing import (
    Union,
    List,
    Dict,
    Iterable,
    Optional,
    Coroutine,
    Mapping,
    Any,
    List,
    Self,
)
import re
from enum import Enum
from asyncio.subprocess import PIPE, DEVNULL
from asyncio import Semaphore
from collections.abc import Iterable as IterableType
import os
from collections import namedtuple
from itertools import repeat
from functools import wraps, partial
import logging
import sys
import inspect

# So the module can be imported outside of this directory
sys.path.insert(0, os.path.dirname(__file__))
import lcov_utils

del sys.path[0]
import concurrent.futures
from urllib.parse import quote, unquote

# NOTE: A lot of the functions in this file uses the form: func(*, param1, param2....)
# This was intentionally done in order to prevent calling those function with positional
# arguments until the API stabilizes.

# Type definitions for annotation
PathLike = Union[Path, str]
ConcurrencyParam = Optional[Union[int, Semaphore]]
LoggerType = Union[logging.Logger, logging.LoggerAdapter]

# Logging support for debugging
COVERAGE_TOOLS_LOGGER = logging.getLogger("coverage_utils.py")
COVERAGE_TOOLS_LOGGER.addHandler(logging.NullHandler())
COVERAGE_TOOLS_LOGGER.propagate = False

# Utility functions for tracing
_tid = 0


def unique_trace_id():
    global _tid
    _tid += 1
    return _tid


def trace_function_call(
    func, sig: inspect.Signature, __logger__: LoggerType, *args, **kwargs
):
    """Extracts the parameters of a function and traces the
    call into logger (With trace level)
    """
    logger = __logger__
    tid = unique_trace_id()
    bound = sig.bind(*args, **kwargs)
    non_defaults = list(bound.arguments.keys())
    bound.apply_defaults()
    log_str = f"Function call ({tid}):\n{func.__qualname__}("
    for param, value in bound.arguments.items():
        log_str += (
            f"\n\t{param} "
            + ("" if param in non_defaults else "(default)")
            + f"= {value},"
        )
    log_str += ")"
    logger.debug(log_str)
    return tid


def traced_func(f):
    """
    A tracing helper which logs function name and parameters so it
    is easy to debug this module.
    It is quite a modest one:
    If the function gets a logger parameter, it will use it to log the function name and params (if it is not None).
    Else it becomes a no-op.
    The logging is done with debug level for logs and error level for errors (only if debug is enabled in the logger)
    """
    sig = inspect.signature(f)
    has_logger_param = "logger" in sig.parameters
    # if the function takes a logger param
    # use it.
    if not has_logger_param:
        return f
    else:

        def extract_logger(*args, **kwargs):
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            return bound.arguments["logger"]

        if inspect.isasyncgenfunction(f):

            @wraps(f)
            async def trace_wrapper(*args, **kwargs):
                logger: LoggerType = extract_logger(*args, **kwargs)
                if not logger or not logger.isEnabledFor(logging.DEBUG):
                    return await f(*args, **kwargs)
                else:
                    try:
                        tid = trace_function_call(f, sig, logger, *args, **kwargs)
                        return await f(*args, **kwargs)
                    except Exception as e:
                        logger.error(
                            f"Function {f.__qualname__} ({tid})exception:\n{e}"
                        )
                        raise e

            return trace_wrapper
        else:

            @wraps(f)
            def trace_wrapper(*args, **kwargs):
                logger: LoggerType = extract_logger(*args, **kwargs)
                if not logger or not logger.isEnabledFor(logging.DEBUG):
                    return f(*args, **kwargs)
                else:
                    tid = trace_function_call(f, sig, logger, *args, **kwargs)
                    try:
                        return f(*args, **kwargs)
                    except Exception as e:
                        logger.error(
                            f"Function {f.__qualname__} ({tid})exception:\n{e}"
                        )
                        raise e

            return trace_wrapper


_create_subprocess_exec_sig = inspect.signature(asyncio.create_subprocess_exec)


@wraps(asyncio.create_subprocess_exec)
async def create_subprocess_exec(*args, logger = COVERAGE_TOOLS_LOGGER, **kwargs):
    if not logger or not logger.isEnabledFor(logging.DEBUG):
        return await asyncio.create_subprocess_exec(*args, **kwargs)
    tid = trace_function_call(
        asyncio.create_subprocess_exec,
        _create_subprocess_exec_sig,
        logger,
        *args,
        **kwargs,
    )
    bound = _create_subprocess_exec_sig.bind(*args, **kwargs)
    bound.apply_defaults()
    logger.debug(
        f"asyncio.create_subprocess_exec ({tid}) going to run:\n"
        f"{bound.arguments['program']} {' '.join([str(arg) for arg in bound.arguments['args']])}"
    )
    try:
        return await asyncio.create_subprocess_exec(*args, **kwargs)
    except Exception as e:
        logger.error(f"Function asyncio.create_subprocess_exec ({tid})exception:\n{e}")
        raise e


_create_subprocess_shell_sig = inspect.signature(asyncio.create_subprocess_shell)


@wraps(asyncio.create_subprocess_shell)
async def create_subprocess_shell(*args, logger = COVERAGE_TOOLS_LOGGER, **kwargs):
    if not logger or not logger.isEnabledFor(logging.DEBUG):
        return await asyncio.create_subprocess_shell(*args, **kwargs)
    tid = trace_function_call(
        asyncio.create_subprocess_shell,
        _create_subprocess_shell_sig,
        logger,
        *args,
        **kwargs,
    )
    bound = _create_subprocess_shell_sig.bind(*args, **kwargs)
    bound.apply_defaults()
    logger.debug(
        f"asyncio.create_subprocess_shell ({tid}) going to run:\n"
        f"{bound.arguments['cmd']}"
    )
    try:
        return await asyncio.create_subprocess_shell(*args, **kwargs)
    except Exception as e:
        logger.error(f"Function asyncio.create_subprocess_shell ({tid})exception:\n{e}")
        raise e


# A set of commands to be used by the FileType enumeration
CONSUME_FIRST_INPUT_FIELD = "cut -d ',' -f1"
IS_PROFILED_COMMAND = (
    "test `eu-readelf -S {} | grep -E '__llvm_cov(map|fun)*[ ]+PROGBITS' | wc -l` -ge 2"
)
IS_DEBUG_ONLY_COMMAND = "eu-readelf -S {} | grep '.text[ ]*NOBITS'"


class FileType(Enum):
    EXEC_BIN = (re.compile(r"ELF .* executable"), 0)
    # This will never match
    DEBUG_ONLY_EXEC_BIN = (re.compile(r"(?!x)x"), 1)
    # This will never match
    PROFILED_EXEC_BIN = (re.compile(r"(?!x)x"), 2)
    # This will never match
    DEBUG_ONLY_PROFILED_EXEC_BIN = (re.compile(r"(?!x)x"), 3)
    EXEC_SO = (re.compile(r"ELF .* shared object"), 4)
    # This will never match
    DEBUG_ONLY_EXEC_SO = (re.compile(r"(?!x)x"), 5)
    # This will never match
    PROFILED_EXEC_SO = (re.compile(r"(?!x)x"), 6)
    # This will never match
    DEBUG_ONLY_PROFILED_EXEC_SO = (re.compile(r"(?!x)x"), 7)
    RAW_PROFILE = (re.compile(r".*LLVM raw profile data.*"), 8)
    INDEXED_PROFILE = (re.compile(r".*LLVM indexed profile data.*"), 9)
    HTML = (re.compile(r".*HTML document.*"), 10)
    # keep this definition last so every unrecognized enum will get this
    UNRECOGNIZED = (re.compile(r".*"), 100)

    @staticmethod
    async def get_file_description(f: PathLike) -> str:
        """Gets the file type description from the shell `file` utility
        Args:
            f (PathLike): _description_

        Raises:
            RuntimeError: if `file` ends with an error

        Returns:
            str: file description
        """
        f = Path(f)
        proc = await create_subprocess_shell(
            " | ".join([f"file -b {f}", CONSUME_FIRST_INPUT_FIELD]),
            stdout = PIPE,
            stderr = PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(
                f"Type info for file {f} couldn't be retrieved: {stderr}"
            )
        return stdout.decode().strip()

    @staticmethod
    @traced_func
    async def is_profiled(f: PathLike) -> bool:
        """Checks if the file is profiled by poking into
        the sections in the elf

        Args:
            f (PathLike): The file to check for profile

        Returns:
            bool: True if the file is profiled, False otherwise
        """
        proc = await create_subprocess_shell(
            IS_PROFILED_COMMAND.format(f), stdout = DEVNULL, stderr = DEVNULL
        )
        await proc.wait()
        return proc.returncode == 0

    @staticmethod
    @traced_func
    async def is_debug_only(f: PathLike) -> bool:
        """Checks if the elf file is debug only (no code) by poking into
        the sections in the elf

        Args:
            f (PathLike): The file to check for profile

        Returns:
            bool: True if the file is profiled, False otherwise
        """
        proc = await create_subprocess_shell(
            IS_DEBUG_ONLY_COMMAND.format(f), stdout = DEVNULL, stderr = DEVNULL
        )
        await proc.wait()
        return proc.returncode == 0

    @staticmethod
    async def get_file_type(f: PathLike) -> Self:
        """Tries to classify the type of a file (one of FileType).

        Args:
            f (PathLike): The path to the file to be classified

        Returns:
            FileType: One of filetype, if the file couldn't be recognized,
            FileType.UNRECOGNIZED will be returned.
        """
        file_description = await FileType.get_file_description(f)
        for ft in FileType:
            if m := ft.value[0].fullmatch(file_description):
                if ft in ELF_TYPES:
                    profiled = "PROFILED_" if await FileType.is_profiled(f) else ""
                    debug_only = (
                        "DEBUG_ONLY_" if await FileType.is_debug_only(f) else ""
                    )
                    return FileType[debug_only + profiled + ft.name]
                return ft
        return FileType.UNRECOGNIZED


ELF_TYPES = [FileType.EXEC_BIN, FileType.EXEC_SO]
PROFILED_ELF_TYPES = [FileType.PROFILED_EXEC_BIN, FileType.PROFILED_EXEC_SO]

# Convenience functions

@traced_func
async def gather_limited_concurrency(
    *args,
    semaphore: Semaphore,
    logger: LoggerType = COVERAGE_TOOLS_LOGGER,
    **kwargs,
) -> Iterable[Any]:
    """A wrapper around asyncio.gather that limits concurrency for the submitted tasks.
    This function takes parameters as asyncio.gather with an additional mandatory `semaphore` param that
    is the semaphore that will limit the tasks concurrency.
    Note: The function looks a bit convoluted, but it is actually a nuance that arises from the semaphore.
    Since asyncio.gather, doesn't stop already submitted tasks, it can cause deadlock on the semaphore. So
    we should make sure that in case of a failure the all tasks are terminated unconditionally.
    """

    async def with_semaphore(coro: Coroutine):
        async with semaphore:
            return await coro

    coros = [asyncio.ensure_future(with_semaphore(coro)) for coro in args]
    try:
        return await asyncio.gather(*coros, **kwargs)
    except:
        raise
    finally:
        [coro.cancel() for coro in coros]

GET_BIN_ID_SHELL_CMD = "eu-readelf -n  {} | grep 'Build ID:' | head -1"
@traced_func
async def get_binary_id(
    *, path: PathLike, logger: LoggerType = COVERAGE_TOOLS_LOGGER
) -> str:
    """A function to get the binary id of an ELF file

    Args:
        path (PathLike): A path to the file who's id to extract
        logger (LoggerType, optional): The logger to which log information. Defaults to COVERAGE_TOOLS_LOGGER.

    Raises:
        RuntimeError: If the readelf command fails for some reason

    Returns:
        str: The found id if it exists else None
    """
    cmd = GET_BIN_ID_SHELL_CMD.format(path)
    proc = await create_subprocess_shell(cmd, stdout = PIPE, stderr = PIPE)
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"could not read {path} build id: {stderr.decode()}")
    id_fields = stdout.decode().splitlines()
    if len(id_fields) == 0:
        return None
    return id_fields[-1].strip().split(" ")[-1]

@traced_func
async def get_binary_ids_map(
    *,
    paths: Iterable[PathLike],
    filter: Optional[Iterable[FileType]] = None,
    with_types : bool = False,
    semaphore: Semaphore = Semaphore(1),
    logger: LoggerType = COVERAGE_TOOLS_LOGGER,
) -> Union[Mapping[Path, str], Mapping[Path, tuple[str, FileType]]]:
    """Maps given files to their build ids, for paths it recursively finds elfs contained in them
        and maps them.
    Args:
        paths (Iterable[PathLike]): paths for single files or directories or iterable of such:
            for every path given:
            1. if it is a file, then it's id will be be mapped
            2. if it is a directory, the directory will be scanned recursively, for elf files
               and their ids will be mapped
        semaphore (Semaphore, optional): A concurrency limiter of the operation. Defaults to Semaphore(1) (no concurrency).
        logger (LoggerType, optional): The logger into which the log information. Defaults to COVERAGE_TOOLS_LOGGER.
    Raises:
        FileNotFoundError: in case some of the paths for the id scanning
            doesn't exist.

    Returns:
        Mapping[Path, str]: A mapping from the elf files (Path) to their id. It is assumed the id is unique, however,
            duplicate files or stripped and unstriped versions of the same file will have the same build id.
    """

    paths = [Path(p) for p in paths]
    dont_exist = [f for f in paths if not f.exists()]
    if len(dont_exist) > 0:
        err = f"some of the paths for id mappings doesn't exist: {dont_exist}"
        logger.error(err)
        raise FileNotFoundError(err)
    dirs = list({path for path in paths if path.is_dir()})
    files = {path for path in paths if not path.is_dir()}
    files_per_dir = [[f for f in dir.rglob("*") if f.is_file() and os.access(f, os.X_OK)] for dir in dirs]
    files.update({f for dirfiles in files_per_dir for f in dirfiles})
    files = list(files)
    types = await gather_limited_concurrency(*(FileType.get_file_type(f) for f in files), semaphore = semaphore, logger = logger)
    if filter:
        filter = list(filter)
        files = [f for f,ft in zip(files, types) if ft in filter]
    build_ids = await gather_limited_concurrency(
        *(get_binary_id(path = f, logger = logger) for f in files),
        semaphore = semaphore,
        logger = logger,
    )
    if with_types:
        files_to_ids_map = {file: build_id for build_id, file in zip(zip(build_ids, types), files)}
    else :
        files_to_ids_map = {file: build_id for build_id, file in zip(build_ids, files)}

    return files_to_ids_map


GET_PROFILED_BINARIES_SHELL_CMD = (
    "llvm-profdata show --binary-ids {} | sed -n '/^Binary IDs:/,/^.*:/p' | tail -n +2"
)


@traced_func
async def get_profiled_binary_ids(
    *, path: PathLike, logger: LoggerType = COVERAGE_TOOLS_LOGGER
) -> List[str]:
    """For a given file (assumed to be llvm profile, either raw or indexed), get the profiled binary ids.
    The reason that can be more than one is if this is a merged profile.

    Args:
        path (PathLike): A path to the llvm profile
        logger (LoggerType, optional): logger to which log information. Defaults to COVERAGE_TOOLS_LOGGER.

    Raises:
        RuntimeError: If llvm-profdata fails for some reason, or, if no profiled binary id couldn't be found
        in the profile.

    Returns:
        List[str]: A list of binary ids profiled in this profile.
    """

    proc = await create_subprocess_shell(
        GET_PROFILED_BINARIES_SHELL_CMD.format(path),
        stdout = PIPE,
        stderr = PIPE,
        stdin = DEVNULL,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"Could not get profiled file ids from {path}: {stderr.decode()}"
        )
    info = [l.strip() for l in stdout.decode().splitlines()]
    if len(info) == 0:
        raise RuntimeError(
            f"Could not get profiled file ids from {path}: No values found"
        )
    return info


# The best way to merge profiles is by the file build id that they map, somewhen in the future,
# it might also be desirable to merge profiles from different binaries, but lcov format does it better
# as it is source dependent so for now we will stick to it.
# if more than one id is contained in one of the files, it is going to be merged only with files that contains
# the same composition of ids.
MergeProfilesResult = namedtuple(
    "MergeProfilesResult", ["generated_profiles", "error_undeleted_files", "errors"]
)


# TODO: Add a "smart" option which will remap symbols and avoid collision in the index
@traced_func
async def merge_profiles(
    *,
    profiles: Iterable[PathLike],
    path_for_merged: PathLike = Path(),
    sparse: bool = True,
    clear_on_success: bool = False,
    semaphore: Semaphore = Semaphore(1),
    logger: Union[logging.Logger, logging.LoggerAdapter] = COVERAGE_TOOLS_LOGGER,
) -> MergeProfilesResult:
    """A function which takes a list of profiles and merges them by exact profiled binaries match.
    The llvm toolchain collection already contains: "llvm profdata merge" however, this function is still necessary
    because merging profdata files that contains same named symbols but different binaries are
    ambiguous, the ambiguity is only "solved" when merging lcov files since lcov maps lines of code instead of symbols.

    Args:
        profiles (Iterable[PathLike]): A list of profiles to merge
        path_for_merged (PathLike, optional): A path to a directory for the merged files. Defaults to Path().
        clear_on_success (bool, optional): Remove the original profiles on success. Defaults to False.
        semaphore (ConcurrencyParam, optional): concurrency limitation for the operation. Defaults to Semaphore(1) (no concurrency).
        logger (Union[logging.Logger, logging.LoggerAdapter], optional): The logger to which log information. Defaults to COVERAGE_TOOLS_LOGGER.

    Returns:
        MergeProfilesResult: A result containing the new profiles list, undeleted files due to errors (if clear_on_success is True)
        and a list of errors that happened during merge.
        It is the user responsibility to the result for errors and act accordingly.
    """
    profiles = [Path(p) for p in profiles]
    path_for_merged = Path(path_for_merged)
    profile_ids = await gather_limited_concurrency(
        *(get_profiled_binary_ids(path = profile, logger = logger) for profile in profiles),
        semaphore = semaphore,
        logger = logger,
    )
    [ids.sort() for ids in profile_ids]
    profile_merge_map = {}
    for ids, profile in zip(profile_ids, profiles):
        profile_merge_map.setdefault(tuple(ids), set()).add(profile)

    async def do_merge_profile(
        ids: Iterable[str], profiles: Iterable[PathLike]
    ):
        destination_profile = path_for_merged / ("_".join(ids) + ".profdata")
        params = (
            ["merge"]
            + (["--sparse"] if sparse else [])
            + list(profiles)
            + ["-o", destination_profile]
        )
        logger.debug(
            f"running command: llvm-profdata {' '.join([str(p) for p in params])}"
        )
        proc = await create_subprocess_exec(
            "llvm-profdata", *params, stderr = PIPE, stdout = DEVNULL
        )
        _, stderr = await proc.communicate()
        error_undeleted_files = []
        if proc.returncode != 0:
            if clear_on_success:
                error_undeleted_files.extend(profiles)
            return MergeProfilesResult(
                [],
                error_undeleted_files,
                [RuntimeError(f"Could not merge {profiles}: {stderr.decode()}")],
            )
        if clear_on_success:
            [profile.unlink() for profile in profiles]
        return MergeProfilesResult([destination_profile], [], [])

    merging_tasks = [
        do_merge_profile(ids, profiles) for ids, profiles in profile_merge_map.items()
    ]
    merging_results = await gather_limited_concurrency(*merging_tasks, semaphore = semaphore)
    return MergeProfilesResult(
        sum([mpr.generated_profiles for mpr in merging_results], []),
        sum([mpr.error_undeleted_files for mpr in merging_results], []),
        sum([mpr.errors for mpr in merging_results], []),
    )


@traced_func
async def profdata_to_lcov(
    *,
    profiles: Iterable[PathLike],
    excludes: Iterable[str] = [],
    compilation_dir: Union[PathLike, None] = None,
    known_file_ids: Dict[PathLike, str] = {},
    id_search_paths: Iterable[PathLike] = [],
    clear_on_success: bool = False,
    update_known_ids: bool = True,
    semaphore: Semaphore = Semaphore(1),
    logger: LoggerType = COVERAGE_TOOLS_LOGGER,
):
    """A function to convert an indexed profiles to lcov files.

    Args:
        profiles (Iterable[PathLike]): The profiles to be converted to lcov format
        excludes (Iterable[str], optional): A list of regex file filter to exclude from the conversion,
        an example might be library source code that is not interesting and can bias the coverage
        metrics. Defaults to [].
        compilation_dir (Union[PathLike, None], optional): The path from which the compilation executed,
        this is for files that have been compiled with relative directories embedded. Defaults to None.
        known_file_ids (Dict[PathLike, str], optional): A map of known binary file ids to use for the
        conversion. Defaults to {}.
        id_search_paths (Iterable[PathLike], optional): A list of paths to search binaries in order
        to facilitate the conversion. Defaults to [].
        clear_on_success (bool, optional): Whether to remove the llvm profiles on successful conversion or not.
        Defaults to False.
        update_known_ids (bool, optional): Whether to update the known ids map given in `known_file_ids` by
        the user. Defaults to True.
        semaphore (Semaphore, optional): Concurrency limitation for the operation. Defaults to Semaphore(1) (no concurrency).
        logger (LoggerType, optional): logger to which log information. Defaults to COVERAGE_TOOLS_LOGGER.

    Raises:
        RuntimeError: If some of the binaries for the conversion couldn't be found (weren't present in `known_file_ids` nor
        weren't contained in any of `id_search_paths`), or, if the conversion itself failed for some reason.

    """
    found_ids = await get_binary_ids_map(
        paths = id_search_paths, filter = PROFILED_ELF_TYPES, semaphore = semaphore, logger = logger
    )
    if not update_known_ids:
        known_file_ids = dict(known_file_ids)

    known_file_ids.update(found_ids)
    excludes = list(excludes)
    exclude_params = sum(map(lambda exclude: ["-ignore-filename-regex", exclude], excludes), [])
    profiles = [Path(p) for p in profiles]
    # logger.debug(f"going to convert {profiles}")
    per_profile_ids = await gather_limited_concurrency(
        *(get_profiled_binary_ids(path = profile, logger = logger) for profile in profiles),
        semaphore = semaphore,
        logger = logger,
    )
    # validate that we know all of the files that created the profiles
    profile_ids_set = set()
    [profile_ids_set.update(ids) for ids in per_profile_ids]
    known_ids_set = set(known_file_ids.values())
    id_to_files_map = {v: k for k, v in known_file_ids.items()}
    if not profile_ids_set.issubset(known_ids_set):
        missing_ids = profile_ids_set.difference(known_ids_set)
        raise RuntimeError(
            f"Some of the profiles contain ids which their files are not known {missing_ids}"
        )
    constant_conversion_params = ["export", "--format", "lcov"] + exclude_params
    if compilation_dir is not None:
        constant_conversion_params += ["--compilation-dir", Path(compilation_dir)]
    constant_conversion_params += ["-instr-profile"]

    async def do_profdata_to_lcov(profile, profile_ids):
        objects_params = " -object ".join(
            [str(id_to_files_map[id]) for id in profile_ids]
        ).split(" ")
        conversion_params = constant_conversion_params + [profile] + objects_params
        with open(profile.with_suffix(".info"), "w") as lcov_file:
            logger.debug(
                f"command: llvm-cov {' '.join([str(p) for p in conversion_params])}"
            )
            proc = await create_subprocess_exec(
                "llvm-cov", *conversion_params, stdout = lcov_file, stderr = PIPE
            )
            _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(
                f"Failed to create {profile.with_suffix('.info')} : {stderr.decode()}"
            )
        if clear_on_success:
            profile.unlink()

    conversion_tasks = [
        do_profdata_to_lcov(profile, profile_ids)
        for profile, profile_ids in zip(profiles, per_profile_ids)
    ]
    await gather_limited_concurrency(*conversion_tasks, semaphore = semaphore)


LCOV_INCLUDE_BRANCH_DATA_PARAMS = ["--rc", "lcov_branch_coverage=1"]
LCOV_TAG_WITH_TEST_SHELL_CMD = (
    "sed 's/^TN:.*/TN:{test_name}/g' {input_lcov} > {output_lcov}"
)

@traced_func
async def lcov_combine_traces(
    *,
    lcovs: Iterable[PathLike],
    output_lcov: Optional[PathLike] = None,
    test_tag: Optional[str] = None,
    clear_on_success: bool = False,
    files_per_chunk: Union[int, None] = None,
    semaphore: Semaphore = Semaphore(1),
    logger: LoggerType = COVERAGE_TOOLS_LOGGER,
):
    """A function to combine lcov traces, the main advantage of this function over just running the lcov command
    from the command line is that this function can parallelize the process, especially when a lot of lcov files are
    merged.

    Args:
        lcovs (Iterable[PathLike]): A list of source lcov trace files to merge
        output_lcov (PathLike): the final output lcov file
        branch_coverage (bool, optional): Whether to include branch coverage data or not (if exists). Defaults to True.
        files_per_chunk (Union[int, None], optional): How many files to combine per parallel task. Defaults to None.
        concurrency (ConcurrencyParam, optional): A concurrency limiting parameter for the execution. Defaults to None.
        logger (LoggerType, optional): A logger to which log information. Defaults to COVERAGE_TOOLS_LOGGER.

    Raises:
        RuntimeError: If one of the parallel merges fails for any reason.
    """
    loop = asyncio.get_running_loop()

    lcovs = [Path(lcov) for lcov in lcovs]

    if files_per_chunk is None or files_per_chunk > len(lcovs):
        files_per_chunk = len(lcovs)

    def merge_lcovs(lcov_spec: List[Union[lcov_utils.LcovFile, Path]]):
        lcov_objs: List[lcov_utils.LcovFile] = []
        if (len(lcov_spec) == 1) and isinstance(lcov_spec[0], lcov_utils.LcovFile):
            return lcov_spec[0]
        for lcov in lcov_spec:
            if isinstance(lcov, Path):
                lcov_objs.append(lcov_utils.LcovFile(lcov))
            else:
                lcov_objs.append(lcov)
        lcov_result = lcov_utils.LcovFile()

        for lcov in lcov_objs:
            if test_tag:
                lcov.tag_with_test(test_tag)
            lcov_result.union(lcov)
        return lcov_result

    files_to_merge = lcovs
    # Consume all of the available concurrency in the semaphore
    concurrency = 0
    while not semaphore.locked():
        await semaphore.acquire()
        concurrency += 1
    try:
        with concurrent.futures.ThreadPoolExecutor(concurrency) as executor:
            while len(files_to_merge) > 1:
                files_to_merge = [
                    files_to_merge[i : i + files_per_chunk]
                    for i in range(0, len(files_to_merge), files_per_chunk)
                ]
                merge_funcs = [partial(merge_lcovs, chunk) for chunk in files_to_merge]
                # We use "normal" gather here since we have the concurrency limited by the executor
                files_to_merge = await asyncio.gather(
                    *(loop.run_in_executor(executor, func) for func in merge_funcs)
                )
            result: List[lcov_utils.LcovFile] = await loop.run_in_executor(
                executor, partial(merge_lcovs, files_to_merge)
            )
            if output_lcov:
                result.write(output_lcov)
                if clear_on_success:
                    for lcov in lcovs:
                        if isinstance(lcov, Path):
                            lcov.unlink()
            else:
                return result
    finally:
        # Release all consumed concurrency back into the semaphore
        for _ in range(concurrency):
            semaphore.release()

@traced_func
async def html_fixup(*, html_dir: Path):
    """Fix genhtml generated links, there is a bug in genhtml where it doesn't properly encode links to
    files names that contain url illegal characters
    """
    html_files = [f for f in html_dir.rglob("*.html") if f.is_file()]
    href_re = re.compile(r'href=".*?\.html">')
    for html_file in html_files:
        with open(html_file, "r") as f:
            content = f.read()
        hrefs = href_re.findall(content)
        hrefs_to_replace = {}
        for href in hrefs:
            if href in hrefs_to_replace:
                continue
            new_href = 'href="' + quote(unquote(href[6:-2])) + href[-2:]
            if new_href != href:
                hrefs_to_replace[href] = new_href
        if len(hrefs_to_replace) > 0:
            for old_href, new_href in hrefs_to_replace.items():
                content = content.replace(old_href, new_href)
            with open(html_file, "w") as f:
                f.write(content)


@traced_func
async def merge_profiles_cmd(args):
    profiles = [Path(p) for p in args.profiles]
    result_path = Path(args.result_path)
    if not result_path.exists():
        result_path.mkdir(parents = True, exist_ok = True)
    await merge_profiles(
        profiles = profiles,
        path_for_merged = result_path,
        clear_on_success = args.clear_on_success,
        semaphore = args.concurrency,
        logger = COVERAGE_TOOLS_LOGGER,
    )


@traced_func
async def prof_to_lcov_cmd(args):
    known_ids = await get_binary_ids_map(
        paths = [Path(bsp) for bsp in args.binary_search_path],
        filter = PROFILED_ELF_TYPES,
        semaphore = args.concurrency,
        logger = COVERAGE_TOOLS_LOGGER,
    )
    profiles = [Path(p) for p in args.profiles]
    excludes = set() if args.excludes is None else set(args.excludes)
    if args.excludes_file:
        excludes.update(
            {
                line
                for line in open(args.excludes_file, "r").read().split("\n")
                if line and not line.startswith("#")
            }
        )
    excludes = list(excludes)
    await profdata_to_lcov(
        profiles = profiles,
        excludes = excludes,
        compilation_dir = args.compilation_dir,
        known_file_ids = known_ids,
        clear_on_success = args.clear_on_success,
        semaphore = args.concurrency,
        logger = COVERAGE_TOOLS_LOGGER,
    )


@traced_func
async def merge_lcov_files_cmd(args):
    output_trace = Path(args.output_trace)
    lcovs = [Path(lcov) for lcov in args.lcov_files]
    merged: lcov_utils.LcovFile = await lcov_combine_traces(
        lcovs = lcovs,
        test_tag = args.testname,
        clear_on_success = args.clear_on_success,
        files_per_chunk = args.files_per_chunk,
        semaphore = args.concurrency,
        logger = COVERAGE_TOOLS_LOGGER,
    )
    if args.filter:
        exclude_line = args.exclude_line if args.exclude_line else None
        exclude_start = args.exclude_start if args.exclude_start else None
        exclude_end = args.exclude_end if args.exclude_end else None
        exclude_branch = args.exclude_branch if args.exclude_branch else None
        exclude_branch_start = (
            args.exclude_branch_start if args.exclude_branch_start else None
        )
        exclude_branch_end = (
            args.exclude_branch_end if args.exclude_branch_end else None
        )
        assert not (bool(exclude_start) ^ bool(exclude_end))
        assert not (bool(exclude_branch_start) ^ bool(exclude_branch_end))
        if exclude_start:
            assert exclude_start != exclude_end
        if exclude_branch_start:
            assert exclude_branch_start != exclude_branch_end
        merged.filter_by_source_tags(
            LCOV_EXCL_LINE = exclude_line,
            LCOV_EXCL_START = exclude_start,
            LCOV_EXCL_STOP = exclude_end,
            LCOV_EXCL_BR_LINE = exclude_branch,
            LCOV_EXCL_BR_START = exclude_branch_start,
            LCOV_EXCL_BR_STOP = exclude_branch_end,
        )
    merged.write(output_trace)
    if args.clear_on_success:
        [lcov.unlink() for lcov in lcovs if lcov.exists()]


@traced_func
async def list_build_ids_cmd(args):
    paths = [Path(p) for p in args.paths]
    file_to_id_map = await get_binary_ids_map(
        paths = paths,
        with_types = True,
        semaphore = args.concurrency,
        logger = COVERAGE_TOOLS_LOGGER
    )
    max_id_len = (
        max(
            *[
                len(id) if id is not None else len("Not found")
                for (id,_) in file_to_id_map.values()
            ]
        )
        + 5
    )
    fmt_str = f"{{id: <{max_id_len}}}{{file}}{' '*5}({{ftype}})"
    for file, (id, ftype) in file_to_id_map.items():
        print(
            fmt_str.format(
                id = id if id is not None else "Not Found",
                file = str(file),
                ftype = ftype.name,
            )
        )

@traced_func
async def coverage_diff_cmd(args):
    diff_trace = await lcov_combine_traces(
        lcovs = args.diff_tracefiles,
        files_per_chunk = args.files_per_chunk,
        clear_on_success = False,
        semaphore = args.concurrency,
        logger = COVERAGE_TOOLS_LOGGER,
    )
    base_lcov = lcov_utils.LcovFile(args.base_tracefile)
    base_lcov.difference(diff_trace)
    base_lcov.write(Path(args.output_trace))


@traced_func
async def coverage_intersection_cmd(args):
    trace_files = list(args.tracefiles)
    result = lcov_utils.LcovFile(trace_files[0])
    for trace_file in trace_files[1:]:
        result.intersection(lcov_utils.LcovFile(trace_file))
    result.write(args.output_trace)


@traced_func
async def coverage_symmetric_diff_cmd(args):
    result = lcov_utils.LcovFile(args.tracefiles[0])
    result.symmetric_difference(lcov_utils.LcovFile(args.tracefiles[1]))
    result.write(args.output_trace)


async def patch_coverage_cmd(args):
    base_commit = args.base_commit
    proc = await create_subprocess_shell("git diff --quiet")
    await proc.wait()
    output_dir: Path = args.output_dir
    dirty = bool(proc.returncode != 0)
    if base_commit:  # if we were given base commits we should generate the patches

        def get_next_numbered_patch_name():
            last_split = (list(sorted(output_dir.glob("*.patch")))[-1]).stem
            last_split_len = len(last_split)
            next_fn = str(int(last_split) + 1)
            next_fn = ("0" * (last_split_len - len(next_fn))) + next_fn
            return next_fn

        if args.merge:
            proc = await create_subprocess_shell(
                "git show --summary HEAD | grep -q ^Merge"
            )
            await proc.wait()
            assert (
                proc.returncode == 0
            ), "Head is not a merge commit but --merge/-m option was given"
        if dirty:
            assert (
                args.dirty
            ), "The git repository has modified files but --dirty option not given"
        output_dir.mkdir(parents = True, exist_ok = True)
        if args.clear_output_dir:
            [f.unlink() for f in list(output_dir.glob("*.patch"))]
        else:
            assert (
                len(list(output_dir.glob("*.patch"))) == 0
            ), f"{output_dir} is not empty, patches should be created in a directory that doesn't contain any *.patch files"
        coverage_commit = ""
        # if this is a merge commit we should:
        # 1. generate the commits leading up to HEAD^<parent> from the fork point
        # 2. generate the diff commit between HEAD^<parent> and HEAD
        if args.merge:
            coverage_commit = f"HEAD^{args.merge_parent}"
            proc = await create_subprocess_shell(
                f"git merge-base {base_commit} {coverage_commit}", stdout = PIPE
            )
            stdout, _ = await proc.communicate()
            assert proc.returncode == 0, "Couldn't determine the fork point of HEAD"
            base_commit = stdout.decode().strip()

        proc = await create_subprocess_shell(
            f"git log -m --first-parent -p -z --reverse --format=medium {base_commit}..{coverage_commit} | sed 's/\\x00/\\x01\\n/g' | csplit -s --prefix=\"{output_dir}/\" --suffix=\"%02d.patch\" --suppress-matched -  $'/\\x01/' '{{*}}'"
        )
        await proc.wait()
        proc = await create_subprocess_shell(
            f"git log -m --first-parent --reverse --format=medium {base_commit}..{coverage_commit} --format=\"%s\" | sed -E 's/(.{1,60}).*/\1/g'",
            stdout = PIPE,
        )
        stdout, _ = await proc.communicate()
        names = stdout.decode().splitlines()
        if args.merge:
            next_patch = output_dir / f"{get_next_numbered_patch_name()}.patch"
            proc = await create_subprocess_shell(
                f"git log HEAD -1 --format=medium > {next_patch}"
            )
            await proc.wait()
            proc = await create_subprocess_shell(
                f"git diff {coverage_commit}..HEAD >> {next_patch}"
            )
            await proc.wait()
            proc = await create_subprocess_shell(
                f"git log HEAD -1 --format=medium --format=\"%s\" | sed -E 's/(.{1,60}).*/\1/g'",
                stdout = PIPE,
            )
            stdout, _ = await proc.communicate()
            merge_name = stdout.decode().splitlines()[0]
            names.append(merge_name)

        # git log --format="%s" | sed -E 's/(.{1,60}).*/\1/g'
        names = [name.replace("/", "\\") for name in names]
        patches = list(output_dir.glob("*.patch"))
        patches.sort()
        dirty_name = get_next_numbered_patch_name()
        patches = [
            f.rename(f.with_stem(f.stem + " - " + p_name))
            for f, p_name in zip(patches, names)
        ]
        if dirty:
            dirty_name = dirty_name + " - uncommitted changes.patch"
            dirty_file = output_dir / dirty_name
            proc = await create_subprocess_shell(f"git diff > '{dirty_file}'")
            await proc.wait()
            patches.append(dirty_file)
    else:  # Patches already generated we should only map them
        patches = list(output_dir.glob("*.patch"))
        patches.sort()
    remapped_tracefile = lcov_utils.LcovFile(args.tracefile)
    remapped_tracefile.remap_to_patches(patches)
    if args.pseudo_patch_for_uncovered:
        covered_patches = {key[1] for key in remapped_tracefile.records.keys()}
        uncovered_patches = set(patches) - covered_patches
        for uncovered_patch in uncovered_patches:
            preamble = "This patch either touched only unprofiled code, was overridden entirely by a later patch or only removed lines"
            pseudo_patch = uncovered_patch.with_suffix(".patch.uncovered")
            proc = await create_subprocess_shell(
                f"cat <(echo '{preamble}') <(cat '{uncovered_patch}') > '{pseudo_patch}'"
            )
            await proc.wait()
            pseudo_record = lcov_utils.LcovRecord()
            pseudo_record.source_file = pseudo_patch
            pseudo_record.line_hits[1] = 1
            remapped_tracefile.add_record(pseudo_record)
    remapped_tracefile.write(args.output_trace)


async def html_fixup_cmd(args):
    await html_fixup(html_dir = args.html_dir)


async def genhtml_cmd(args):
    genhtml_options = ["genhtml", "--output-directory", f"'{args.output_dir}'"]
    if not args.verbose:
        genhtml_options.append("--quiet")
    if args.title is not None:
        genhtml_options.extend(["--title", f"'{args.title}'"])
    if not args.no_legend:
        genhtml_options.append("--legend")
    if not args.no_function_coverage:
        genhtml_options.append("--function-coverage")
    if not args.no_branch_coverage:
        genhtml_options.append("--branch-coverage")
    if not args.no_cpp_demangle:
        genhtml_options.append("--demangle-cpp")
    if args.ignore_errors:
        genhtml_options.extend(["--ignore-errors", "'source'"])
    if args.negate:
        genhtml_options.append("--missed")
    genhtml_options.extend(["--rc", f"'genhtml_hi_limit={args.high_limit}'"])
    genhtml_options.extend(["--rc", f"'genhtml_med_limit={args.med_limit}'"])
    genhtml_options.extend([str(f) for f in args.tracefiles])
    proc = await create_subprocess_shell(" ".join(genhtml_options))
    await proc.wait()
    await html_fixup(html_dir = args.output_dir)


def recursively_print_help(p: argparse.ArgumentParser, indent = 0):
    help_lines = p.format_help().splitlines()
    help_lines = [("\t" * indent) + l for l in help_lines]
    print("\n".join(help_lines))
    subparsers_actions = [
        action
        for action in p._actions
        if isinstance(action, argparse._SubParsersAction)
    ]
    sp_action: argparse._SubParsersAction
    for sp_action in subparsers_actions:
        for sp in sp_action.choices.values():
            recursively_print_help(sp, indent = indent + 1)


async def print_help(args):
    recursively_print_help(args.parser)


async def main():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter(
        "%(asctime)s - %(filename)s:%(lineno)d - %(module)s:%(funcName)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    COVERAGE_TOOLS_LOGGER.addHandler(handler)
    root.addHandler(handler)
    COVERAGE_TOOLS_LOGGER.setLevel(logging.DEBUG)
    parser = argparse.ArgumentParser(
        description = "A collection of tools to handle llvm coverage data",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--log-level",
        choices = logging._nameToLevel.keys(),
        action = "store",
        default = logging._levelToName[logging.INFO],
        help = f"logging level of the tool",
    )
    parser.add_argument(
        "--concurrency",
        action = "store",
        default = max(int(multiprocessing.cpu_count() * 0.75), 1),
        type = int,
        help = "The concurrency to use for parallel operations.",
    )
    parser.set_defaults(func = print_help, parser = parser)
    subparsers = parser.add_subparsers()
    llvm_profile_commands = subparsers.add_parser(
        "llvm-profiles", help = "Handle and manipulate llvm profiles"
    )
    llvm_profile_commands.set_defaults(func = print_help, parser = llvm_profile_commands)
    llvm_commands_subparsers = llvm_profile_commands.add_subparsers()

    # List build ids can help determine the right build id for parsing llvm profiles
    list_build_ids_parser = llvm_commands_subparsers.add_parser(
        "list-build-ids", help = "list build ids for the given paths"
    )
    list_build_ids_parser.add_argument(
        "paths",
        metavar = "PATH",
        help = "A folder to be searched for (recursively) or a specific file to get an build id for",
        action = "store",
        type = Path,
        nargs = "+",
    )
    list_build_ids_parser.set_defaults(func = list_build_ids_cmd)

    # Convert raw profiles into a profdata
    raw_to_indexed_parser = llvm_commands_subparsers.add_parser(
        "merge",
        help = "merge and convert raw and indexed profiles to a unified profile",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    raw_to_indexed_parser.add_argument(
        "profiles",
        metavar = "PROFILE",
        help = "a raw or indexed profile to merge into the output merged profile",
        action = "store",
        type = Path,
        nargs = "+",
    )
    raw_to_indexed_parser.add_argument(
        "result_path",
        metavar = "PROFDATA",
        type = Path,
        help = "The path to which the merged indexed data will saved",
    )
    raw_to_indexed_parser.add_argument(
        "-c",
        "--clear-on-success",
        action = "store_true",
        default = False,
        help = "Whether to clear the raw profiles on success",
    )
    raw_to_indexed_parser.set_defaults(func = merge_profiles_cmd)

    # Convert indexed profiles into lcov trace files
    prof_to_lcov_parser = llvm_commands_subparsers.add_parser(
        "to-lcov",
        help = "convert indexed profiles into lcov trace files",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    prof_to_lcov_parser.add_argument(
        "profiles",
        metavar = "PROFILE",
        help = "an indexed profile to be converted to an lcov trace file",
        action = "store",
        type = Path,
        nargs = "+",
    )
    prof_to_lcov_parser.add_argument(
        "-c",
        "--clear-on-success",
        action = "store_true",
        default = False,
        help = "Whether to clear the indexed profiles on success",
    )
    prof_to_lcov_parser.add_argument(
        "--exclude",
        "-e",
        action = "append",
        type = str,
        dest = "excludes",
        help = "Regex patterns for excluding files from coverage",
    )
    prof_to_lcov_parser.add_argument(
        "--excludes-file",
        "--ef",
        action = "store",
        type = Path,
        help = "A file containing a list of regexes to exclude",
    )
    prof_to_lcov_parser.add_argument(
        "-b",
        "--binary-search-path",
        type = Path,
        action = "append",
        required = True,
        help = "The path or paths to search for binaries to use in the conversion.",
    )
    prof_to_lcov_parser.add_argument(
        "--compilation-dir",
        action = "store",
        type = Path,
        default = Path(),
        help = "The compilation directory for the binaries (for files compiled with relative paths mapping)",
    )
    prof_to_lcov_parser.set_defaults(func = prof_to_lcov_cmd)

    lcov_trace_commands = subparsers.add_parser(
        "lcov-tools", help = "Handle and manipulate lcov tracefiles"
    )
    lcov_trace_commands_subparsers = lcov_trace_commands.add_subparsers()
    lcov_trace_commands.set_defaults(func = print_help, parser = lcov_trace_commands)
    merge_lcov_files_parser = lcov_trace_commands_subparsers.add_parser(
        "union",
        help = "Merges several (or single) lcov file into another trace file. If testname is given, the resulting lcov file will be tagged with "
        "this name, else if will just merge the files similarly to 'lcov -a...' command. Files can also be filtered (see 'man lcovrc')",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    merge_lcov_files_parser.add_argument(
        "lcov_files",
        metavar = "TRACE_FILE",
        help = "an lcov trace file to be merged into the output trace file",
        action = "store",
        type = Path,
        nargs = "+",
    )
    merge_lcov_files_parser.add_argument(
        "output_trace",
        metavar = "OUTPUT_TRACE_FILE",
        type = Path,
        help = "The path to which the merged lcov trace  data will saved",
    )
    merge_lcov_files_parser.add_argument(
        "-c",
        "--clear-on-success",
        action = "store_true",
        default = False,
        help = "Whether to clear the original lcov files upon success",
    )
    merge_lcov_files_parser.add_argument(
        "--testname",
        action = "store",
        type = str,
        default = None,
        help = "An optional testname to tag all records in the output",
    )
    merge_lcov_files_parser.add_argument(
        "--files-per-chunk",
        action = "store",
        type = int,
        default = 4,
        help = "The maximal number of files to merge at once (for performance tweaking)",
    )
    merge_lcov_files_parser.add_argument(
        "--filter",
        "-f",
        action = "store_true",
        help = "Apply filter to the result (see: 'man lcovrc')",
    )
    merge_lcov_files_parser.add_argument(
        "--exclude-line",
        "--el",
        action = "store",
        type = str,
        help = "Tag for line exclusion, empty string for None (when --filter is given)",
        default = lcov_utils.LcovFile.LCOV_EXCL_LINE_DEFAULT,
    )
    merge_lcov_files_parser.add_argument(
        "--exclude-start",
        "--es",
        action = "store",
        type = str,
        help = "Tag for line exclusion block start, empty string for None, (when --filter is given)",
        default = lcov_utils.LcovFile.LCOV_EXCL_START_DEFAULT,
    )
    merge_lcov_files_parser.add_argument(
        "--exclude-end",
        "--ee",
        action = "store",
        type = str,
        help = "Tag for line exclusion block end, empty string for None (when --filter is given)",
        default = lcov_utils.LcovFile.LCOV_EXCL_STOP_DEFAULT,
    )
    merge_lcov_files_parser.add_argument(
        "--exclude-branch",
        "--eb",
        action = "store",
        type = str,
        help = "Tag branch exclusion, empty string for None (when --filter is given)",
        default = lcov_utils.LcovFile.LCOV_EXCL_BR_LINE_DEFAULT,
    )
    merge_lcov_files_parser.add_argument(
        "--exclude-branch-start",
        "--ebs",
        action = "store",
        type = str,
        help = "Tag for branch exclusion block start, empty string for None (when --filter is given)",
        default = lcov_utils.LcovFile.LCOV_EXCL_BR_START_DEFAULT,
    )
    merge_lcov_files_parser.add_argument(
        "--exclude-branch-end",
        "--ebe",
        action = "store",
        type = str,
        help = "Tag for branch exclusion block end, empty string for None (when --filter is given)",
        default = lcov_utils.LcovFile.LCOV_EXCL_BR_STOP_DEFAULT,
    )
    merge_lcov_files_parser.set_defaults(func = merge_lcov_files_cmd)

    lcov_diff_parser = lcov_trace_commands_subparsers.add_parser(
        "diff",
        help = "computes the diff between two or more coverage files (lines that are covered by first but not others)",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    lcov_diff_parser.add_argument(
        "--output-trace",
        "-o",
        help = "The output file to write the result into",
        type = Path,
        required = True,
    )
    lcov_diff_parser.add_argument(
        "--files-per-chunk",
        action = "store",
        type = int,
        default = 4,
        help = "The max number of files to merge at once (for performance tweaking)",
    )
    lcov_diff_parser.add_argument(
        "base_tracefile",
        action = "store",
        type = Path,
        help = "The base line trace - the file which we want to diff with all others",
    )
    lcov_diff_parser.add_argument(
        "diff_tracefiles",
        action = "store",
        type = Path,
        nargs = "+",
        help = "The tracefiles to subtracted from the base trace",
    )
    lcov_diff_parser.set_defaults(func = coverage_diff_cmd)

    lcov_intersection_parser = lcov_trace_commands_subparsers.add_parser(
        "intersection",
        help = "computes the intersection between two or more coverage files (lines that are covered by all trace files)",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    lcov_intersection_parser.add_argument(
        "--output-trace",
        "-o",
        help = "The output file to write the result into",
        type = Path,
        required = True,
    )
    lcov_intersection_parser.add_argument(
        "--files-per-chunk",
        action = "store",
        type = int,
        default = 4,
        help = "The max number of files to merge at once (for performance tweaking)",
    )
    lcov_intersection_parser.add_argument(
        "tracefiles",
        action = "store",
        type = Path,
        nargs = "+",
        help = "The tracefiles to subtract from the base trace",
    )
    lcov_intersection_parser.set_defaults(func = coverage_intersection_cmd)

    lcov_symmetric_diff_parser = lcov_trace_commands_subparsers.add_parser(
        "symmetric-dff",
        help = "computes the symmetric difference between two traces (line covered by either trace but not both)",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    lcov_symmetric_diff_parser.add_argument(
        "--output-trace",
        "-o",
        help = "The output file to write the result into",
        type = Path,
        required = True,
    )
    lcov_symmetric_diff_parser.add_argument(
        "tracefiles",
        action = "store",
        type = Path,
        nargs = 2,
        help = "The tracefiles to be subtracted",
    )
    lcov_symmetric_diff_parser.set_defaults(func = coverage_symmetric_diff_cmd)

    patch_coverage_parser = lcov_trace_commands_subparsers.add_parser(
        "git-patch-coverage",
        help = "Transform a a source coverage tracefile into a patch coverage tracefile",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    patch_coverage_parser.add_argument(
        "--output-trace",
        "-o",
        help = "The output file to write the result into",
        type = Path,
        required = True,
    )
    patch_coverage_parser.add_argument(
        "--tracefile",
        "-t",
        type = Path,
        action = "store",
        help = "The tracefile to transform",
    )
    patch_coverage_parser.add_argument(
        "--base-commit",
        "-b",
        type = str,
        default = None,
        action = "store",
        help = "The base commit for the patch coverage",
    )
    patch_coverage_parser.add_argument(
        "--output-dir",
        "-d",
        type = Path,
        required = True,
        action = "store",
        help = "The directory to create the patches in",
    )
    patch_coverage_parser.add_argument(
        "--dirty",
        action = "store_true",
        help = "Whether to include a final meta patch which is the uncommitted changes to the environment,"
        "if this parameter is not given and there are uncommitted changes in the repo, the command will fail. (not including untracked files)",
    )
    patch_coverage_parser.add_argument(
        "--clear-output-dir",
        "-c",
        action = "store_true",
        help = "Remove any previous .patch file in output dir",
    )
    patch_coverage_parser.add_argument(
        "--pseudo-patch-for-uncovered",
        "-p",
        action = "store_true",
        help = "Create a pseudo patch for uncovered patch files and make them appear as 100%% covered",
    )
    patch_coverage_parser.add_argument(
        "--merge", "-m", action = "store_true", help = "Coverage report for merge commit"
    )
    patch_coverage_parser.add_argument(
        "--merge-parent",
        "--mp",
        type = int,
        default = 2,
        action = "store",
        choices = [1, 2],
        help = "The parent of the merge commit to generate coverege for",
    )
    patch_coverage_parser.set_defaults(func = patch_coverage_cmd)

    genhtml_parser = lcov_trace_commands_subparsers.add_parser(
        "genhtml",
        help = "Generate and fixup html pages",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    genhtml_parser.add_argument(
        "--output-dir",
        "-o",
        type = Path,
        required = True,
        action = "store",
        help = "The folder in which to generate the html report",
    )
    genhtml_parser.add_argument(
        "--verbose",
        "-v",
        action = "store_true",
        help = "Output everything instead of just warnings and errors",
    )
    genhtml_parser.add_argument(
        "--title",
        "-t",
        action = "store",
        type = str,
        default = None,
        help = "The title of the coverage run for example: 'Unit tests run'",
    )
    genhtml_parser.add_argument(
        "--no-legend", "--nl", action = "store_true", help = "Don't create legend"
    )
    genhtml_parser.add_argument(
        "--no-function-coverage",
        "--nf",
        action = "store_true",
        help = "Don't report function coverage",
    )
    genhtml_parser.add_argument(
        "--no-branch-coverage",
        "--nb",
        action = "store_true",
        help = "Don't report branch coverage",
    )
    genhtml_parser.add_argument(
        "--no-cpp-demangle",
        "--nd",
        action = "store_true",
        help = "Don't demangle function names",
    )
    genhtml_parser.add_argument(
        "--ignore-errors",
        "-i",
        action = "store_true",
        help = "Ignore 'source wasn't found' errors",
    )
    genhtml_parser.add_argument(
        "--negate",
        "-n",
        action = "store_true",
        help = "Make the report missed coverage centric instead of coverage centric",
    )
    genhtml_parser.add_argument(
        "--high-limit",
        "--hl",
        action = "store",
        type = int,
        default = 90,
        help = "The high limit for reporting (high coverage)",
    )
    genhtml_parser.add_argument(
        "--med-limit",
        "--ml",
        action = "store",
        type = int,
        default = 75,
        help = "The medium limit for reporting (medium coverage)",
    )
    genhtml_parser.add_argument(
        "tracefiles",
        metavar = "tracefile",
        action = "store",
        type = Path,
        nargs = "+",
        help = "The tracefiles to generate the reports from",
    )
    genhtml_parser.set_defaults(func = genhtml_cmd)
    html_fixup_parser = lcov_trace_commands_subparsers.add_parser(
        "html-fixup",
        help = "Fix genhtml broken links for non standard file name (for example that contains '#')",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    html_fixup_parser.add_argument(
        "--html-dir",
        "-d",
        type = Path,
        required = True,
        action = "store",
        help = "The folder containing the html report generated by htmlgen",
    )

    html_fixup_parser.set_defaults(func = html_fixup_cmd)

    help_parser = subparsers.add_parser("help", help = "Print a full help message")
    help_parser.set_defaults(func = print_help, parser = parser)
    args = parser.parse_args()
    args.concurrency = Semaphore(args.concurrency)
    loglevel = logging._nameToLevel[args.log_level]
    COVERAGE_TOOLS_LOGGER.setLevel(loglevel)
    [handler.setLevel(loglevel) for handler in COVERAGE_TOOLS_LOGGER.handlers]
    await args.func(args)


if __name__ == "__main__":
    asyncio.run(main())
