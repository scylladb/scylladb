# -*- coding: utf-8 -*-
#
# Copyright (C) 2024-present ScyllaDB
#
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import unidiff
from typing import (
    List,
    OrderedDict as OrderedDictType,
    Tuple,
    Callable,
    Union,
    TextIO,
    Self,
    Optional,
    Mapping,
)
from collections import OrderedDict
from pathlib import Path
import unidiff.patch
from unidiff import PatchSet, PatchedFile
from unidiff.patch import Hunk, Line
import copy
from itertools import repeat, accumulate

# TN: test name
# SF: source file path
# FN: line number,function name
# FNF:  number functions found
# FNH: number hit
# BRDA: branch data: line, block, (expressions,count)+
# BRF: branches found
# DA: line number, hit count
# LF: lines found
# LH:  lines hit.


# This monkey patching is done for two main reasons:
# 1. In order to allow deep copying of the objects (which is the main purpose)
# 2. For efficiency, instead of creating a function that will compare the strings
#    every time.
class MakeLcovRouter(type):
    def __new__(cls, name, bases, dct):
        routes = {}
        dct_keys = list(dct.keys())
        for match in dct["routes"]:
            keys = [key for key in dct_keys if key.endswith("_" + match)]
            assert len(keys) == 1
            routes[match] = dct[keys[0]]
        dct["routes"] = routes

        def route(self, route, *args, **kwargs):
            return self.routes[route](self, *args, **kwargs)

        dct["route"] = route
        return super().__new__(cls, name, bases, dct)


class LcovRecord(metaclass = MakeLcovRouter):
    routes = [
        "TN",
        "SF",
        "FN",
        "FNDA",
        "FNF",
        "FNH",
        "BRDA",
        "BRF",
        "BRH",
        "DA",
        "LF",
        "LH",
        "end_of_record",
    ]

    def __init__(self) -> None:
        self._test_name: Optional[str] = None
        self.source_file: Optional[Path] = None
        self.line_hits: dict[int, int] = dict()

        self.function_hits: dict[Tuple(int, str), int] = dict()
        self.functions_to_lines: dict[str, int] = dict()
        self.branch_hits: dict[Tuple[int, int, int], Optional[int]] = dict()
        self.sealed: bool = False
        self.FNF = None
        self.FNH = None
        self.BRF = None
        self.BRH = None
        self.LF = None
        self.LH = None

    def empty(self):
        return (
            len(self.line_hits) + len(self.function_hits) + len(self.branch_hits)
        ) == 0

    @property
    def test_name(self):
        return self._test_name if self._test_name else ""

    @property
    def functions_found(self):
        return len(self.function_hits)

    @property
    def functions_hit(self):
        return sum([int(hits and hits > 0) for hits in self.function_hits.values()])

    @property
    def branches_found(self):
        return len(self.branch_hits)

    @property
    def branches_hit(self):
        return sum([int(bool(hits and hits > 0)) for hits in self.branch_hits.values()])

    @property
    def lines_found(self):
        return len(self.line_hits)

    @property
    def lines_hit(self):
        return sum([int(hits and hits > 0) for hits in self.line_hits.values()])

    def add(self, type_str: str, fields: List[str]):
        assert not self.sealed
        return self.route(type_str, fields)

    def add_TN(self, fields: List[str]) -> bool:
        assert self._test_name is None
        self._test_name = fields[0]
        return False

    def add_SF(self, fields: List[str]) -> bool:
        assert self.source_file is None
        self.source_file = Path(fields[0])
        return False

    def add_FN(self, fields: List[str]) -> bool:
        line, func_name = fields
        line = int(line)
        assert func_name not in self.functions_to_lines
        self.functions_to_lines[func_name] = line
        self.function_hits.setdefault((line, func_name), 0)
        return False

    def add_FNDA(self, fields: List[str]) -> bool:
        hits, func_name = fields
        line = self.functions_to_lines[func_name]
        hits = int(hits)
        self.function_hits[(line, func_name)] = hits
        return False

    def add_FNF(self, fields: List[str]) -> bool:
        self.FNF = int(fields[0])
        return False

    def add_FNH(self, fields: List[str]) -> bool:
        self.FNH = int(fields[0])
        return False

    def add_BRDA(self, fields: List[str]) -> bool:
        line, block, branch, count = fields
        line = int(line)
        block = int(block)
        branch = int(branch)
        count = int(count) if count != "-" else None
        self.branch_hits.setdefault((line, block, branch), count)
        return False

    def add_BRF(self, fields: List[str]) -> bool:
        self.BRF = int(fields[0])
        return False

    def add_BRH(self, fields: List[str]) -> bool:
        self.BRH = int(fields[0])
        return False

    def add_DA(self, fields: List[str]) -> bool:
        line, hits = fields
        line = int(line)
        hits = int(hits)
        self.line_hits.setdefault(line, hits)
        return False

    def add_LF(self, fields: List[str]) -> bool:
        self.LF = int(fields[0])
        return False

    def add_LH(self, fields: List[str]) -> bool:
        self.LH = int(fields[0])
        return False

    def add_end_of_record(self, fields: List[str]) -> bool:
        self.sealed = True
        self.validate_integrity()
        self._refresh_functions_to_lines()
        return True

    def remove_lines(self, line_numbers: List[int]):
        self.validate_integrity()
        for line_number in line_numbers:
            if line_number in self.line_hits:
                del self.line_hits[line_number]
        functions_to_remove = list(
            {
                (line, func_name)
                for func_name, line in self.functions_to_lines.items()
                if line in line_numbers
            }
        )
        for key in functions_to_remove:
            del self.function_hits[key]
            del self.functions_to_lines[key[1]]
        branches_to_remove = [
            branch for branch in self.branch_hits if branch[0] in line_numbers
        ]
        for branch in branches_to_remove:
            del self.branch_hits[branch]
        self.validate_integrity()

    def remove_line(self, line_number: int):
        self.remove_lines([line_number])

    def remove_branches(self, branch_line_numbers: List[int]):
        branch_keys_to_remove = {
            key for key in self.branch_hits.keys() if key[0] in branch_line_numbers
        }
        for branch_to_remove in branch_keys_to_remove:
            del self.branch_hits[branch_to_remove]

    def remove_branch(self, branch_line):
        self.remove_branches([branch_line])

    def validate_integrity(self):
        assert (
            len(set(self.functions_to_lines.values()) - set(self.line_hits.keys())) == 0
        )
        assert (
            len(
                set([x[0] for x in self.branch_hits.keys()])
                - set(self.line_hits.keys())
            )
            == 0
        )

    def get_lines(self) -> set[int]:
        return set(self.line_hits.keys())

    def filter_lines(self, lines: List[int]):
        self.remove_lines(self.get_lines().difference(set(lines)))
        self._refresh_functions_to_lines()

    def remap_lines(self, lines_mapping: Mapping[int, int]):
        # Validate that no two lines are mapped to the same target
        assert len(lines_mapping) == len(set(lines_mapping.values()))
        # First filter all the None mapped lines
        lines_to_keep = self.get_lines().intersection(set(lines_mapping.keys()))
        self.filter_lines(lines_to_keep)
        line_hits = self.line_hits
        self.line_hits = dict()
        for line, hits in line_hits.items():
            new_line = lines_mapping[line]
            self.line_hits[new_line] = hits
        function_hits = self.function_hits
        self.function_hits = dict()
        for (line, func_name), hits in function_hits.items():
            new_key = (lines_mapping[line], func_name)
            self.function_hits[new_key] = hits
        branch_hits = self.branch_hits
        self.branch_hits = dict()
        for (line, block, branch), count in branch_hits.items():
            new_key = (lines_mapping[line], block, branch)
            self.branch_hits[new_key] = count

    def transform_line_hitrates(self, transform: Callable[[Optional[int]], int]):
        for line in self.line_hits.keys():
            self.line_hits[line] = transform(self.line_hits[line])

    def transform_function_hitrates(self, transform: Callable[[Optional[int]], int]):
        for key in self.function_hits.keys():
            self.function_hits[key] = transform(self.function_hits[key])

    def transform_branch_hitrates(self, transform: Callable[[Optional[int]], int]):
        for key in self.branch_hits.keys():
            self.branch_hits[key] = transform(self.branch_hits[key])

    def transform_hitrates(self, transform: Callable[[Optional[int]], int]):
        self.transform_line_hitrates(transform)
        self.transform_function_hitrates(transform)
        self.transform_branch_hitrates(transform)

    def _get_branches_line_hitrate(self) -> Mapping[int, int]:
        this_branch_line_hits = dict()
        for key in self.branch_hits.keys():
            this_branch_line_hits.setdefault(key[0], 0)
            if self.branch_hits[key] is not None:
                this_branch_line_hits[key[0]] += self.branch_hits[key]
        return this_branch_line_hits

    def _refresh_functions_to_lines(self):
        self.functions_to_lines = {
            func: line for line, func in self.function_hits.keys()
        }

    def union(self, other: Self) -> Self:
        """an in place version of the union operation,
           the semantics are that the hitrates are combined for
           shared lines, functions and branches.
        Arguments:
            other {Self} -- the other component to union with
        """
        for line in other.line_hits.keys():
            if line not in self.line_hits:
                self.line_hits[line] = other.line_hits[line]
            else:
                self.line_hits[line] += other.line_hits[line]
        for key in other.function_hits.keys():
            if key not in self.function_hits:
                self.function_hits[key] = other.function_hits[key]
            else:
                self.function_hits[key] += other.function_hits[key]
        self._refresh_functions_to_lines()
        for key in other.branch_hits.keys():
            if key not in self.branch_hits or self.branch_hits[key] is None:
                self.branch_hits[key] = other.branch_hits[key]
            elif other.branch_hits[key] is not None:
                self.branch_hits[key] += other.branch_hits[key]
        return self

    def intersection(self, other: Self) -> Self:
        """an in place version of the intersection operation.
        The semantics are, everything that is covered by both components
        is kept and merged, everything that is covered by only one component is
        removed from coverage completely (not just the hitrate).
        The coverage is measured in lines, and functions but for brunches it is
        measured in lines since there is not much meaning to only partial branch
        instead the specific branch that should be removed is set to None.
        Note: Self intersection is not an identity, it will remove any uncovered
        lines, functions and (line) branches completely.
        Arguments:
            other {Self} -- the other component to intersect with
        """
        covered_lines = dict()
        lines_to_merge = self.get_lines().intersection(other.get_lines())
        lines_to_merge = [
            line
            for line in lines_to_merge
            if self.line_hits[line] > 0 and other.line_hits[line] > 0
        ]
        if other == self:
            for line in lines_to_merge:
                covered_lines[line] = self.line_hits[line]
        else:
            for line in lines_to_merge:
                covered_lines[line] = self.line_hits[line] + other.line_hits[line]
        self.line_hits = covered_lines
        covered_functions = dict()
        functions_to_merge = set(self.function_hits.keys()).intersection(
            set(other.function_hits.keys())
        )
        functions_to_merge = [
            (line, func_name)
            for line, func_name in functions_to_merge
            if self.function_hits[(line, func_name)] > 0
            and other.function_hits[(line, func_name)] > 0
        ]
        if other == self:
            for key in functions_to_merge:
                covered_functions[key] = self.function_hits[key]
        else:
            for key in functions_to_merge:
                covered_functions[key] = (
                    self.function_hits[key] + other.function_hits[key]
                )
        self.function_hits = covered_functions
        self._refresh_functions_to_lines()
        covered_branches = dict()
        # for branches, count hits per line
        this_branch_line_hits = dict()
        for key in self.branch_hits.keys():
            if self.branch_hits[key] is not None:
                this_branch_line_hits.setdefault(key[0], 0)
                this_branch_line_hits[key[0]] += self.branch_hits[key]
        other_branch_line_hits = dict()
        for key in other.branch_hits.keys():
            if other.branch_hits[key] is not None:
                other_branch_line_hits.setdefault(key[0], 0)
                other_branch_line_hits[key[0]] += other.branch_hits[key]
        this_branch_line_hits = {
            key
            for key in this_branch_line_hits.keys()
            if this_branch_line_hits[key] > 0
        }
        other_branch_line_hits = {
            key
            for key in other_branch_line_hits.keys()
            if other_branch_line_hits[key] > 0
        }
        branches_lines_to_merge = this_branch_line_hits.intersection(
            other_branch_line_hits
        )
        for key in self.branch_hits.keys():
            if key[0] not in branches_lines_to_merge:
                continue
            this_hits = self.branch_hits[key]
            other_hits = other.branch_hits[key]
            if this_hits is None and other_hits is None:
                covered_branches[key] = None
            elif this_hits is None:
                covered_branches[key] = other_hits
            elif other_hits is None:
                covered_branches[key] = this_hits
            else:
                covered_branches[key] = this_hits + other_hits
        self.branch_hits = covered_branches
        return self

    def difference(self, other: Self) -> Self:
        """an in place version of the difference operation
           The semantics are everything that is covered by this component
           but not the other. For branches it is calculated per line, not
           per branch so it can be that some uncovered branches are still
           indicated but not complete lines that are not evaluated.
           For branches:
           If the branch is only covered by this component, the hit rate will
           be preserved, if it is covered by both or neither it will be None and if only covered
           by other it will be 0.
        Arguments:
            other {Self} -- the other component to intersect with
        """
        # first remove every line that is not covered by self (at all)
        self.remove_lines([line for line, hits in self.line_hits.items() if hits <= 0])
        # remove every line that is covered by both
        this_covered_lines = {
            key for key in self.line_hits.keys() if self.line_hits[key] > 0
        }
        other_covered_lines = {
            key for key in other.line_hits.keys() if other.line_hits[key] > 0
        }
        lines_to_remove = this_covered_lines.intersection(other_covered_lines)
        self.remove_lines(lines_to_remove)
        self._refresh_functions_to_lines()
        # first remove every function that is not covered by self (at all)
        for key in list(self.function_hits.keys()):
            if self.function_hits[key] <= 0:
                del self.function_hits[key]
        this_covered_functions = {
            key for key in self.function_hits.keys() if self.function_hits[key] > 0
        }
        other_covered_functions = {
            key for key in other.function_hits.keys() if other.function_hits[key] > 0
        }
        # the remove all functions that are covered by both
        functions_to_remove = this_covered_functions.intersection(
            other_covered_functions
        )
        for key in functions_to_remove:
            del self.function_hits[key]
        self._refresh_functions_to_lines()
        # first remove every line that is not hit at all
        branch_line_hits = self._get_branches_line_hitrate()
        branches_lines_with_hits = {
            line for key, line in branch_line_hits.items() if branch_line_hits[key] > 0
        }
        for key in list(self.branch_hits.keys()):
            if key[0] not in branch_line_hits:
                del self.branch_hits[key]
        for key, hits in list(self.branch_hits.items()):
            covered_by_this = bool(self.branch_hits[key])
            covered_by_other = (
                bool(other.branch_hits[key]) if key in other.branch_hits else False
            )
            covered_by_both = covered_by_this and covered_by_other
            # covered by both
            if covered_by_both:
                self.branch_hits[key] = None
            elif covered_by_this:  # Only covered by this
                pass
            elif covered_by_other:
                self.branch_hits[key] = 0
            else:  # covered by neither
                self.branch_hits[key] = None
        return self

    def symmetric_difference(self, other: Self) -> Self:
        """an in place version of the symmetric difference operation
        Arguments:
            other {Self} -- the other component to intersect with
        """
        intersection = self.__and__(other)
        self.difference(intersection).union(other.__sub__(intersection))
        return self

    def __and__(self, other: Self):
        new_component = copy.deepcopy(self)
        new_component.intersection(other)
        return new_component

    def __or__(self, other: Self):
        new_component = copy.deepcopy(self)
        new_component.union(other)
        return new_component

    def __sub__(self, other: Self):
        new_component = copy.deepcopy(self)
        new_component.difference(other)
        return new_component

    def __xor__(self, other: Self):
        new_component = copy.deepcopy(self)
        new_component.symmetric_difference(other)
        return new_component

    def write(self, f: TextIO):
        # Test Name
        f.write("TN:" + self.test_name + "\n")
        # Source File
        f.write("SF:" + str(self.source_file) + "\n")
        # functions
        if self.functions_found > 0:
            functions_and_lines = list(self.function_hits.keys())
            functions_and_lines.sort()
            [f.write(f"FN:{line},{func}\n") for line, func in functions_and_lines]
            [
                f.write(f"FNDA:{int(self.function_hits[key])},{key[1]}\n")
                for key in functions_and_lines
            ]
        # Function data is outputted regardless to function mapping (?)
        f.write(f"FNF:{self.functions_found}\n")
        f.write(f"FNH:{self.functions_hit}\n")
        # branches
        if self.branches_found > 0:
            sorted_branches = list(self.branch_hits.keys())
            sorted_branches.sort()
            for key in sorted_branches:
                line, block, branch = key
                count = self.branch_hits[key]
                f.write(
                    f"BRDA:{line},{block},{branch},{count if count is not None else '-'}\n"
                )
            f.write(f"BRF:{self.branches_found}\n")
            f.write(f"BRH:{self.branches_hit}\n")
        # lines
        if self.lines_found > 0:
            [f.write(f"DA:{line},{count}\n") for line, count in self.line_hits.items()]
            f.write(f"LF:{self.lines_found}\n")
            f.write(f"LH:{self.lines_hit}\n")
        f.write("end_of_record\n")

    @staticmethod
    def get_type_and_fields(line: str) -> Tuple[str, List[str]]:
        parts = line.split(":", maxsplit = 1)
        if len(parts) == 1:
            parts.append("")
        type_str = parts[0].strip()
        fields = [parts[1].strip()] if type_str == "SF" else [field.strip() for field in parts[1].split(",")]
        return type_str, fields

    def add_line(self, line: str) -> bool:
        type_str, fields = self.get_type_and_fields(line)
        return self.add(type_str, fields)

    def __eq__(self, other: Self):
        if isinstance(other, type(self)):
            if self.test_name != other.test_name:
                return False
            if self.source_file != other.source_file:
                return False
            elif self.line_hits != other.line_hits:
                return False
            elif self.function_hits != other.function_hits:
                return False
            elif self.branch_hits != other.branch_hits:
                return False
            else:
                return True
        else:
            return False


class LcovFile:
    LCOV_EXCL_LINE_DEFAULT = "LCOV_EXCL_LINE"
    LCOV_EXCL_START_DEFAULT = "LCOV_EXCL_START"
    LCOV_EXCL_STOP_DEFAULT = "LCOV_EXCL_STOP"
    LCOV_EXCL_BR_LINE_DEFAULT = "LCOV_EXCL_BR_LINE"
    LCOV_EXCL_BR_START_DEFAULT = "LCOV_EXCL_BR_START"
    LCOV_EXCL_BR_STOP_DEFAULT = "LCOV_EXCL_BR_STOP"
    EMPTY_LCOV_PSEUDO_FILE = Path("this_lcov_is_empty")

    def __init__(
        self,
        coverage_file: Optional[Path] = None,
        filter_by_tags: bool = False,
        LCOV_EXCL_LINE = LCOV_EXCL_LINE_DEFAULT,
        LCOV_EXCL_START = LCOV_EXCL_START_DEFAULT,
        LCOV_EXCL_STOP = LCOV_EXCL_STOP_DEFAULT,
        LCOV_EXCL_BR_LINE = LCOV_EXCL_BR_LINE_DEFAULT,
        LCOV_EXCL_BR_START = LCOV_EXCL_BR_START_DEFAULT,
        LCOV_EXCL_BR_STOP = LCOV_EXCL_BR_STOP_DEFAULT,
    ):
        self.records: OrderedDictType[Tuple[str, Path], LcovRecord] = OrderedDict()
        self.filter_by_tags = filter_by_tags
        self.LCOV_EXCL_LINE = LCOV_EXCL_LINE
        self.LCOV_EXCL_START = LCOV_EXCL_START
        self.LCOV_EXCL_STOP = LCOV_EXCL_STOP
        self.LCOV_EXCL_BR_LINE = LCOV_EXCL_BR_LINE
        self.LCOV_EXCL_BR_START = LCOV_EXCL_BR_START
        self.LCOV_EXCL_BR_STOP = LCOV_EXCL_BR_STOP
        if coverage_file:
            self.load(coverage_file)

    def __eq__(self, other: Self):
        if isinstance(other, type(self)):
            return dict(self.records.items()) == dict(other.records.items())
        else:
            return False

    def load(self, coverage_file: Path):
        with open(coverage_file, "r") as f:
            current_record = None
            while l := f.readline():
                if not l.strip():
                    continue
                if current_record is None:
                    current_record = LcovRecord()
                try:
                    if current_record.add_line(l):
                        if (
                            current_record.source_file
                            != LcovFile.EMPTY_LCOV_PSEUDO_FILE
                        ):
                            self._add_record(current_record)
                        current_record = None
                except AssertionError as e:
                    raise RuntimeError(
                        f"assertion in loading {coverage_file}, {current_record.source_file}",
                        e,
                    )
        if self.filter_by_tags:
            self.filter_by_source_tags(
                self.LCOV_EXCL_LINE,
                self.LCOV_EXCL_START,
                self.LCOV_EXCL_STOP,
                self.LCOV_EXCL_BR_LINE,
                self.LCOV_EXCL_BR_START,
                self.LCOV_EXCL_BR_STOP,
            )
        return self

    # This copy of the function is to avoid deep copy
    # when we know that the record is going not to be
    # used anywhere else after this call.
    def _add_record(self, record: LcovRecord):
        if record.empty():
            return
        key = (record.test_name, record.source_file)
        if key in self.records:
            self.records[key].union(record)
        else:
            self.records[key] = record

    def add_record(self, record: LcovRecord):
        if record.empty():
            return
        key = (record.test_name, record.source_file)
        if key in self.records:
            self.records[key].union(record)
        else:
            self.records[key] = copy.deepcopy(record)

    @staticmethod
    def write_empty(target_file: Path, as_covered = False):
        empty_lcov = LcovFile()
        record = LcovRecord()
        record.source_file = LcovFile.EMPTY_LCOV_PSEUDO_FILE
        record.line_hits[1] = int(as_covered)
        empty_lcov._add_record(record)
        empty_lcov.write(target_file)

    def write(
        self,
        target_file: Path,
        generate_empty = False,
        incompatible_empty = False,
        as_covered = False,
    ):
        """Writes the content of this object to an lcov trace file.
        target_file - The target file to write into
        generate_empty - If to write even if the content is empty
        incompatible_empty - entirely empty lcov traces are not compatible with lcov and htmlgen commands, this is fine
                             for library operations that use only this object, however, if lcov or htmlgen are to run on
                             an entirely empty file, they are going to fail on format issues, those tools expect at least
                             one legal record in a trace file. If True, will generate a truly empty file, else, will generate
                             a file with a record pointing to a non existing pseudo file.
        as_covered - this parameter is only meaningful when incompatible_empty is False and generate_empty is True,
                    if True, will generate a pseudo record that appears as fully covered (100%), else, the pseudo record will
                    be fully uncovered (0%)
        """
        self.prune()
        if not generate_empty:
            assert (
                not self.empty()
            ), "Writing an empty lcov trace will result in a trace which is incompatible with lcov tools"
        if generate_empty and (not incompatible_empty) and self.empty():
            LcovFile.write_empty(target_file = target_file, as_covered = as_covered)
        else:
            with open(target_file, "w") as f:
                [record.write(f) for record in self.records.values()]

    def filter_files(self, files_to_keep: List[Path]):
        for key_to_remove in [
            key for key in self.records.keys() if key[1] not in files_to_keep
        ]:
            del self.records[key_to_remove]

    def filter_lines(self, file: Path, lines_to_keep: List[int]):
        for record in [
            record for key, record in self.records.items() if key[1] == file
        ]:
            record.filter_lines(lines_to_keep)

    def _remap_to_patch(
        self, patch_file: Union[Path, unidiff.PatchSet], patch_fn: Path
    ):
        patch: unidiff.PatchSet = (
            unidiff.PatchSet.from_filename(patch_file)
            if isinstance(patch_file, Path)
            else patch_file
        )
        patched_files: List[unidiff.PatchedFile] = (
            patch.added_files + patch.modified_files
        )
        # 1. remove all files that were not patched
        self.filter_files(
            [Path(pf.target_file).relative_to("b/") for pf in patched_files]
        )
        # 2. for every file keep only the lines that were patched
        # There is going to be only one new record because all lines belongs to this file.
        record_by_source = dict()
        for record in self.records.values():
            record_by_source.setdefault(record.source_file, []).append(record)
        self.records.clear()
        for patched_file in patched_files:
            source_file = Path(patched_file.target_file).relative_to("b/")
            if not source_file in record_by_source:
                continue
            lines_remap = {
                line.target_line_no: line.diff_line_no
                for hunk in patched_file
                for line in hunk
                if line.is_added
            }
            for record in record_by_source[source_file]:
                record.remap_lines(lines_remap)
                if not record.empty():
                    record.source_file = patch_fn
                    self._add_record(record)

    def remap_to_patches(self, patch_files: List[Path]):
        patches = prepare_patches_for_lcov(patch_files)
        prototype = copy.deepcopy(self)
        self.records.clear()
        for patch, patch_fn in zip(patches, patch_files):
            remapped = copy.deepcopy(prototype)
            remapped._remap_to_patch(patch, patch_fn)
            self.union(remapped)
        self.prune()
        return self

    def coverage_report(
        self,
        f: TextIO,
        include_branches: bool = True,
        files_to_include: Union[Path, List[Path], None] = None,
        colors = False,
    ):
        if isinstance(files_to_include, Path):
            files_to_include = [files_to_include]
        for test, files_map in self.records.items():
            for file, record in files_map.items():
                if files_to_include is not None and not file in files_to_include:
                    continue
                if record.empty():
                    continue
                # We would like to print the following:
                # |Line number|Line data|line text
                # This will maintain conformance with the html presentation
                LINE_NUMBERS_WIDTH = 10
                LINE_DATA_WIDTH = 12
                table_head_format = f"| {{: <{LINE_NUMBERS_WIDTH}}}| {{: <{LINE_DATA_WIDTH}}}| Source code"
                table_line_format = (
                    f"|{{: >{LINE_NUMBERS_WIDTH}}} |{{: >{LINE_DATA_WIDTH}}} : {{}}"
                )
                with open(file, "r") as f_src:
                    line = 0
                    while l := f_src.readline():
                        line += 1
                        if line == 1:
                            headline = f"coverage data for: {file}, test: {test}"
                            f.write(headline + "\n")
                            f.write("-" * len(headline) + "\n\n")
                            f.write(
                                table_head_format.format("Line", "Line Data") + "\n"
                            )
                        if line in record.lines.line_hits:
                            f.write(
                                table_line_format.format(
                                    line, record.lines.line_hits[line], l
                                )
                            )
                        else:
                            f.write(table_line_format.format(line, "", l))
                f.write("\n\n")
                f.write("Summary\n-------\n")
                f.write("Lines Hit: ")
                if record.lines.lines_found > 0:
                    f.write(
                        f"{record.lines.lines_hit}/{record.lines.lines_found} ({(record.lines.lines_hit/record.lines.lines_found)*100:.2f}%)\n"
                    )
                else:
                    f.write("No information found.\n")
                f.write("Functions Hit: ")
                if record.functions.functions_found > 0:
                    f.write(
                        f"{record.functions.functions_hit}/{record.functions.functions_found} ({(record.functions.functions_hit/record.functions.functions_found)*100:.2f}%)\n"
                    )
                else:
                    f.write("No information found.\n")
                f.write("Branches Hit: ")
                if record.branches.branches_found > 0:
                    f.write(
                        f"{record.branches.branches_hit}/{record.branches.branches_found} ({(record.branches.branches_hit/record.branches.branches_found)*100:.2f}%)\n"
                    )
                else:
                    f.write("No information found.\n")

    # Defaults are set according to `man geninfo`:
    #  The following markers are recognized by geninfo:

    #    LCOV_EXCL_LINE
    #           Lines containing this marker will be excluded.
    #    LCOV_EXCL_START
    #           Marks the beginning of an excluded section. The current line is part of this section.
    #    LCOV_EXCL_STOP
    #           Marks the end of an excluded section. The current line not part of this section.
    #    LCOV_EXCL_BR_LINE
    #           Lines containing this marker will be excluded from branch coverage.
    #    LCOV_EXCL_BR_START
    #           Marks the beginning of a section which is excluded from branch coverage. The current line is part of this section.
    #    LCOV_EXCL_BR_STOP
    #           Marks the end of a section which is excluded from branch coverage. The current line not part of this section.
    def filter_by_source_tags(
        self,
        LCOV_EXCL_LINE = "LCOV_EXCL_LINE",
        LCOV_EXCL_START = "LCOV_EXCL_START",
        LCOV_EXCL_STOP = "LCOV_EXCL_STOP",
        LCOV_EXCL_BR_LINE = "LCOV_EXCL_BR_LINE",
        LCOV_EXCL_BR_START = "LCOV_EXCL_BR_START",
        LCOV_EXCL_BR_STOP = "LCOV_EXCL_BR_STOP",
    ):
        assert not (bool(LCOV_EXCL_START) ^ bool(LCOV_EXCL_STOP))
        assert not (bool(LCOV_EXCL_BR_START) ^ bool(LCOV_EXCL_BR_STOP))

        # for each source file, create a map of excludes and then apply them to the file.
        def make_exclusion_lists(f: Path):
            lines_to_exclude = set()  # line numbers to exclude
            branches_to_exclude = set()  # branch line numbers to exclude
            line_section_open = None
            branch_section_open = None
            with open(f, "r") as src:
                current_line = 1
                while l := src.readline():
                    if LCOV_EXCL_LINE and LCOV_EXCL_LINE in l:
                        lines_to_exclude.add(current_line)
                    if LCOV_EXCL_BR_LINE and LCOV_EXCL_BR_LINE in l:
                        branches_to_exclude.add(current_line)
                    if (
                        LCOV_EXCL_START
                        and (LCOV_EXCL_START in l)
                        and not line_section_open
                    ):
                        line_section_open = current_line
                    if LCOV_EXCL_STOP and (LCOV_EXCL_STOP in l) and line_section_open:
                        lines_to_exclude.update(range(line_section_open, current_line))
                        line_section_open = None
                    if (
                        LCOV_EXCL_BR_START
                        and (LCOV_EXCL_BR_START in l)
                        and not branch_section_open
                    ):
                        branch_section_open = current_line
                    if (
                        LCOV_EXCL_BR_STOP
                        and (LCOV_EXCL_BR_STOP in l)
                        and branch_section_open
                    ):
                        branches_to_exclude.update(
                            range(branch_section_open, current_line)
                        )
                        branch_section_open = None
                    current_line += 1
            return lines_to_exclude, branches_to_exclude

        records_by_source = {}
        for key, record in self.records.items():
            records_by_source.setdefault(key[1], [])
            records_by_source[key[1]].append(record)

        for f, records in records_by_source.items():
            lines_to_exclude, branches_to_exclude = make_exclusion_lists(f)
            record: LcovRecord
            for record in records:
                record.remove_lines(lines_to_exclude)
                record.remove_branches(branches_to_exclude)
        self.prune()

    def intersection(self, other: Self):
        common_records = set(self.records.keys()).intersection(
            set(other.records.keys())
        )
        old_records = self.records()
        self.records = OrderedDict()
        for key in common_records():
            self.records[key] = old_records[key].intersection(other.records[key])
        self.prune()
        return self

    def union(self, other: Self):
        for key, val in other.records.items():
            if key not in self.records:
                self.records[key] = copy.deepcopy(val)
            else:
                self.records[key].union(val)
        self.prune()
        return self

    def difference(self, other: Self):
        common_records = set(self.records.keys()).intersection(
            set(other.records.keys())
        )
        for key in common_records:
            self.records[key].difference(other.records[key])
        self.prune()
        return self

    def symmetric_difference(self, other: Self) -> Self:
        """an in place version of the symmetric difference operation
        Arguments:
            other {Self} -- the other component to intersect with
        """
        intersection = self.__and__(other)
        self.difference(intersection).union(other.__sub__(intersection))
        return self

    def __and__(self, other: Self):
        new_component = copy.deepcopy(self)
        new_component.intersection(other)
        return new_component

    def __or__(self, other: Self):
        new_component = copy.deepcopy(self)
        new_component.union(other)
        return new_component

    def __sub__(self, other: Self):
        new_component = copy.deepcopy(self)
        new_component.difference(other)
        return new_component

    def __xor__(self, other: Self):
        new_component = copy.deepcopy(self)
        new_component.symmetric_difference(other)
        return new_component

    def prune(self):
        keys_to_prune = [key for key, val in self.records.items() if val.empty()]
        for key in keys_to_prune:
            del self.records[key]

    def empty(self):
        if len(self.records) == 0:
            return True
        else:
            if not next(iter(self.records.values())).empty():
                return False
            else:
                # this is a little bit dangerous because of the recursion,
                # if prune() doesn't clear every empty record due to a bug
                # we might get into an infinite recursion.
                # However, this is the cleanest way to code it.
                self.prune()
                return self.empty()

    def tag_with_test(self, test_name: str, from_test: Optional[str] = None):
        # TODO: we should probably error out or normalize the string in order to preserve
        # lcov compatibility
        if test_name == from_test:
            return
        all_compatible_records = [
            key for key in self.records.keys() if not from_test or key[0] == test_name
        ]
        records_to_change = [self.records[key] for key in all_compatible_records]
        for key in all_compatible_records:
            del self.records[key]
        for record in records_to_change:
            record._test_name = test_name
            self._add_record(record)

    def tag_with_test_map(self, tests_map: Mapping[str, str]):
        for from_test, to_test in tests_map.items():
            self.tag_with_test(from_test, to_test)


def prepare_patches_for_lcov_old(patches: List[Path]) -> List[unidiff.PatchSet]:
    """Takes a list of patches paths that are assumed to be applied in their
    order of appearance in the list and adjusts them to have target_line_no and
    files in the final source code.

    Args:
        patches (List[Path]): An ordered list of patches paths
    """

    patchsets: List[unidiff.PatchSet] = [
        unidiff.PatchSet.from_filename(f) for f in patches
    ]

    def eliminate_rename(
        source_file: str, dest_file: str, patchsets: List[unidiff.PatchSet]
    ):
        new_src_file = "a/" + dest_file[2:]
        old_dest_file = "b/" + source_file[2:]
        for patch in patchsets:
            for patched_file in patch:
                patched_file: unidiff.PatchedFile = patched_file
                if patched_file.target_file == old_dest_file:
                    patched_file.source_file = new_src_file
                    patched_file.target_file = dest_file

    def remove_file(f: str, patchsets: List[unidiff.PatchSet]):
        target_to_remove = "b/" + f[2:]
        for patch in patchsets:
            files_to_remove = []
            for patched_file in patch:
                patched_file: unidiff.PatchedFile = patched_file
                if patched_file.target_file == target_to_remove:
                    files_to_remove.append(patched_file)
            [patch.remove(f) for f in files_to_remove]

    def update_line_removed(line_num, source_file, patchsets):
        for patch in patchsets:
            for patched_file in patch:
                patched_file: unidiff.PatchedFile = patched_file
                if patched_file.source_file == source_file:
                    for line in patched_file:
                        line: unidiff.patch.Line = line
                        if line.target_line_no > line_num:
                            line.target_line_no -= 1

    def update_line_added(line_num, target_file, patchsets):
        for patch in patchsets:
            for patched_file in patch:
                patched_file: unidiff.PatchedFile = patched_file
                if patched_file.target_file == target_file:
                    for line in patched_file:
                        line: unidiff.patch.Line = line
                        if line.target_line_no >= line_num:
                            line.target_line_no += 1

    for idx, patch in enumerate(patchsets):
        for patched_file in patch:
            patched_file: unidiff.PatchedFile = patched_file
            if patched_file.is_removed_file:
                remove_file(patched_file.source_file, patchsets[:idx])
            elif patched_file.is_rename:
                # Make the semantics as if the file was actually never renamed (since in the final source it will be as if the file is
                # already in it renamed form)
                eliminate_rename(
                    patched_file.source_file,
                    patched_file.target_file,
                    patchsets[: idx + 1],
                )
            for hunk in patched_file:
                hunk: unidiff.Hunk = hunk
                for line in hunk:
                    line: unidiff.patch.Line = line
                    if line.is_removed:
                        update_line_removed(
                            line.source_line_no,
                            patched_file.source_file,
                            patchsets[:idx],
                        )
                    if line.is_added:
                        update_line_added(
                            line.target_line_no,
                            patched_file.target_file,
                            patchsets[:idx],
                        )
    return patchsets


def prepare_patches_for_lcov(patches: List[Path]) -> List[unidiff.PatchSet]:
    """Takes a list of patches paths that are assumed to be applied in their
    order of appearance in the list and adjusts them to have target_line_no and
    files in the final source code.

    Args:
        patches (List[Path]): An ordered list of patches paths
    """
    # rough algorithm-
    # incrementally map patches to the latest file so eventually only contain
    # patch data of the latest file. each file is getting handled separately.
    # it calls for some kind of induction - assume that patches until n-1 are referring to
    # the source file of patch n and make them refer to the target file of patch n.

    patchsets: List[unidiff.PatchSet] = [
        unidiff.PatchSet.from_filename(f) for f in patches
    ]
    for n in range(1, len(patchsets)):
        patches_adjustment_step(patchsets[: n + 1])
    return patchsets


def patches_adjustment_step(patchlist: List[PatchSet]):
    main_patch = patchlist[-1]
    patches = patchlist[:-1]
    # 1. if file is removed remove it from all patches
    removed_files: list[PatchedFile] = list(main_patch.removed_files)
    removed_files = [x.path for x in removed_files]
    for patch in patches:
        files_to_remove = [
            f
            for f in (list(patch.modified_files) + list(patch.added_files))
            if f.path in removed_files
        ]
        [patch.remove(f) for f in files_to_remove]
    renamed_files = [f for f in main_patch.modified_files if f.is_rename]

    def maybe_remove_prefix(path: str):
        if path.startswith("a/") or path.startswith("b/"):
            return path[2:]
        return path

    renames = {maybe_remove_prefix(f.source_file): f.target_file for f in renamed_files}
    # 2. if file was renamed, change the patches targets to the new file name
    for patch in patches:
        files: List[PatchedFile] = list(patch.added_files) + list(patch.modified_files)
        for f in files:
            target = maybe_remove_prefix(f.target_file)
            if target in renames:
                f.target_file = renames[target]
    # 3. invariant, every patch in the previous patches only contain the most recent change,
    # make the invariant true also for the patches with this patch applied.
    patched_files_by_target = {}
    for patch in patches:
        for file in patch:
            file: PatchedFile = file
            patched_files_by_target.setdefault(file.target_file, [])
            patched_files_by_target[file.target_file].append(file)
    for file in list(main_patch.modified_files):
        removed_lines: List[int] = [
            line.source_line_no for hunk in file for line in hunk if line.is_removed
        ]
        added_lines: List[int] = [
            line.target_line_no for hunk in file for line in hunk if line.is_added
        ]
        removed_lines.sort()
        added_lines.sort()
        hunks: List[Hunk] = [
            hunk
            for patched_file in patched_files_by_target.get(file.target_file, [])
            for hunk in patched_file
        ]
        # remove replaced/removed lines from previous patches
        [
            hunk.remove(line)
            for hunk in hunks
            for line in list(hunk)
            if line.target_line_no in removed_lines
        ]
        lines: List[Line] = [line for hunk in hunks for line in hunk if line.is_added]
        lines.sort(key = lambda l: l.target_line_no)

        def apply_transform(transform):
            transform_iter = iter(transform)
            current_transform = next(transform_iter, (0, 0))
            for line in lines[::-1]:
                while current_transform[0] > line.target_line_no:
                    current_transform = next(transform_iter, (0, 0))
                if current_transform == (0, 0):
                    break
                line.target_line_no += current_transform[1]

        removed_transform = list(zip(removed_lines, accumulate(repeat(-1))))[::-1]
        apply_transform(removed_transform)
        added_transform = list(zip(added_lines, accumulate(repeat(1))))
        added_transform = list([(x - y + 1, y) for x, y in added_transform])[::-1]
        apply_transform(added_transform)
        # 4. TODO: Cleanup
        # Remove hunks that don't have added or deleted lines, remove files that don't
        # have any hunks. It is a performance optimization hence it is not implemented right
        # now.
