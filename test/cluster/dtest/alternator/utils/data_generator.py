#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import random
import string
from decimal import Decimal, getcontext
from enum import Enum, auto

import boto3.dynamodb.types


class TypeMode(Enum):
    NUMBER = auto()
    BOOL = auto()
    NONE = auto()
    STRING = auto()
    BINARY = auto()
    LIST = auto()
    DICT = auto()
    MIXED = auto()


class AlternatorDataGenerator:
    def __init__(self, primary_key, primary_key_format):
        self.primary_key = primary_key
        self.primary_key_format = primary_key_format
        self.items_count = 0
        self._precision = Decimal("0" + str(10 ** (getcontext().prec - 1)) + "1")

    @staticmethod
    def _random_string():
        password_characters = string.ascii_letters + string.digits + string.punctuation
        return ''.join(random.choice(password_characters) for _ in range(random.randint(1, 30)))

    @staticmethod
    def get_mode_name(mode):  # noqa: PLR0911
        if not isinstance(mode, TypeMode):
            raise TypeError(f"The mode variable should be '{TypeMode.__name__}' class")
        if mode == TypeMode.NUMBER:
            return "number"
        elif mode == TypeMode.BOOL:
            return "bool"
        elif mode == TypeMode.NONE:
            return "none"
        elif mode == TypeMode.STRING:
            return "string"
        elif mode == TypeMode.BINARY:
            return "binary"
        elif mode == TypeMode.LIST:
            return "list"
        elif mode == TypeMode.DICT:
            return "dict"

    def create_random_number_item(self):
        self.items_count += 1
        mode_name = self.get_mode_name(mode=TypeMode.NUMBER)
        if self.items_count % 2 == 0:
            return {self.primary_key: self.primary_key_format.format(self.items_count - 1),
                    mode_name: random.randint(0, 1000)}
        return {self.primary_key: self.primary_key_format.format(self.items_count - 1),
                mode_name: Decimal.from_float(random.random()).quantize(self._precision)}

    def create_random_bool_item(self):
        self.items_count += 1
        return {self.primary_key: self.primary_key_format.format(self.items_count - 1),
                self.get_mode_name(mode=TypeMode.BOOL): bool(random.randint(0, 1))}

    def create_none_item(self):
        self.items_count += 1
        return {self.primary_key: self.primary_key_format.format(self.items_count - 1),
                self.get_mode_name(mode=TypeMode.NONE): None}

    def create_random_string_item(self):
        self.items_count += 1
        return {self.primary_key: self.primary_key_format.format(self.items_count - 1),
                self.get_mode_name(mode=TypeMode.STRING): self._random_string()}

    def create_random_binary_item(self):
        self.items_count += 1
        random_str = self._random_string()
        return {self.primary_key: self.primary_key_format.format(self.items_count - 1),
                self.get_mode_name(mode=TypeMode.BINARY): boto3.dynamodb.types.Binary(random_str.encode())}

    def create_random_list_item(self):
        item = []
        original_item_count = self.items_count + 1
        for _ in range(random.randint(0, 30)):
            if self.items_count % 5 == 0:
                item.append(self.create_random_number_item())
            elif self.items_count % 5 == 1:
                item.append(self.create_random_bool_item())
            elif self.items_count % 5 == 2:
                item.append(self.create_none_item())
            elif self.items_count % 5 == 3:
                item.append(self.create_random_string_item())
            elif self.items_count % 5 == 4:
                item.append(self.create_random_binary_item())
        self.items_count = original_item_count
        return {self.primary_key: self.primary_key_format.format(self.items_count - 1),
                self.get_mode_name(mode=TypeMode.LIST): item}

    def create_random_dict_item(self):
        original_item_count = self.items_count + 1
        item = {self.primary_key: self.primary_key_format.format(original_item_count - 1),
                self.get_mode_name(mode=TypeMode.NUMBER): self.create_random_number_item(),
                self.get_mode_name(mode=TypeMode.BOOL): self.create_random_bool_item(),
                self.get_mode_name(mode=TypeMode.NONE): self.create_none_item(),
                self.get_mode_name(mode=TypeMode.STRING): self.create_random_string_item(),
                self.get_mode_name(mode=TypeMode.BINARY): self.create_random_binary_item(),
                self.get_mode_name(mode=TypeMode.LIST): self.create_random_list_item()
                }
        self.items_count = original_item_count
        return item

    def create_multiple_items(self, num_of_items, mode):
        """
        Create multiple items for some type
        Example:
            * Create list of 10 number items: self.create_multiple_items(num_of_items=10, mode=TypeMode.NUMBER)
            * Create list of 5 string items: self.create_multiple_items(num_of_items=10, mode=TypeMode.STRING)
        """

        def _create_multiple_items(_mode, _num_of_items):  # noqa: PLR0911
            if _mode == TypeMode.NUMBER:
                return [self.create_random_number_item() for _ in range(_num_of_items)]
            elif _mode == TypeMode.BOOL:
                return [self.create_random_bool_item() for _ in range(_num_of_items)]
            elif _mode == TypeMode.NONE:
                return [self.create_none_item() for _ in range(_num_of_items)]
            elif _mode == TypeMode.STRING:
                return [self.create_random_string_item() for _ in range(_num_of_items)]
            elif _mode == TypeMode.BINARY:
                return [self.create_random_binary_item() for _ in range(_num_of_items)]
            elif _mode == TypeMode.LIST:
                return [self.create_random_list_item() for _ in range(_num_of_items)]
            elif _mode == TypeMode.DICT:
                return [self.create_random_dict_item() for _ in range(_num_of_items)]
            raise TypeError(f"The following type '{_mode}' not supported")

        if mode != TypeMode.MIXED:
            return _create_multiple_items(_mode=mode, _num_of_items=num_of_items)

        result = []
        options = list(TypeMode)
        options.pop(TypeMode.MIXED.value - 1)  # The ENUM value started from 1
        while len(result) != num_of_items:
            result.append(_create_multiple_items(_mode=options[len(result) % len(options)], _num_of_items=1)[0])

        self.items_count = 0
        return result
