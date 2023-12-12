#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2023-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import argparse
import datetime
import re
import string
import uuid

# If the option of "uuid_sstable_identifiers_enabled" is set, scylla and
# Cassandra 4.x generate sstables using timeuuid as their identifiers. But the
# string representation does not follow the formal definition defined by
# rfc4122. Instead, it uses a base36-based presentation which is used by
# Cassandra. The filename of a Data component file might look like
# "nb-3fw2_0tj4_46w3k2cpidnirvjy7k-big-Data.db". where
# "3fw2_0tj4_46w3k2cpidnirvjy7k" is the identifier for the SSTable. If this
# option is disabled, a positive integer is used instead. In comparison to
# the rfc4122 representation, the base36 encoded UUID is shorter.
#
# This tool encodes the given 64-bit least significant bits and most significant
# bits to the base36-based string representation, and decodes the string. Because
# the timeuuid encodes the timestamp in it, this tool also prints the human readable
# time encoded in the timeuuid.

# base36 alphabet
ALPHABET = string.digits + string.ascii_lowercase
# 1/10 of a microsecond
DECIMICRO_RATIO = 10_000_000


def decode(s: str) -> int:
    '''
    decode a string represented using base36

    the input should have the most significant bit first
    '''

    alphabet_len = len(ALPHABET)
    output = 0
    for char in s:
        output *= alphabet_len
        try:
            output += ALPHABET.index(char)
        except ValueError:
            raise ValueError(f'"{s}" contains a character "{char}" not in alphabet')
    return output


def encode(n: int) -> str:
    '''
    encode an integer represented using base36

    the output has the most significant bit first
    '''
    assert len(ALPHABET) > 1

    if n < 0:
        raise ValueError(f'{n} is negative')

    output = ''
    alphabet_len = len(ALPHABET)
    while n:
        n, index = divmod(n, alphabet_len)
        output += ALPHABET[index]
    return output[::-1]


class TimeUuid:
    def __init__(self, msb: int, lsb: int) -> None:
        bytes = msb.to_bytes(8, byteorder='big') + lsb.to_bytes(8, byteorder='big')
        self.uuid = uuid.UUID(bytes=bytes)

    @staticmethod
    def decode_with_base36(s: str) -> 'TimeUuid':
        '''decode an string represented using base36'''
        matched = re.match(r'''(?P<days>\w{4})_
                               (?P<seconds>\w{4})_
                               (?P<decimicrosecs>\w{5})
                               (?P<lsb>\w{13})''', s, re.ASCII | re.VERBOSE)
        if matched is None:
            raise ValueError(f'malformatted uuid: "{s}"')
        days = decode(matched.group('days'))
        seconds = decode(matched.group('seconds'))
        decimicrosecs = decode(matched.group('decimicrosecs'))
        lsb = decode(matched.group('lsb'))
        delta = datetime.timedelta(days=days, seconds=seconds)
        timestamp = decimicrosecs + int(delta.total_seconds()) * DECIMICRO_RATIO
        msb = TimeUuid._time_to_msb(timestamp)
        return TimeUuid(msb, lsb)

    def encode_with_base36(self) -> str:
        '''encode an integer using base36 representation'''
        seconds, decimicro = divmod(self.uuid.time, DECIMICRO_RATIO)
        delta = datetime.timedelta(seconds=seconds)
        encoded_days = encode(delta.days)
        encoded_seconds = encode(delta.seconds)
        encoded_decimicro = encode(decimicro)
        encoded_lsb = encode(self.lsb)
        return (f'{encoded_days:0>4}_'
                f'{encoded_seconds:0>4}_'
                f'{encoded_decimicro:0>5}'
                f'{encoded_lsb:0>13}')

    @staticmethod
    def _time_to_msb(time: int) -> int:
        # see https://datatracker.ietf.org/doc/html/rfc4122.html
        time_low = (2 ** 32 - 1) & time
        time >>= 32
        time_mid = (2 ** 16 - 1) & time
        time >>= 16
        if time >> 12 != 0:
            raise ValueError(f'time "{time:#016x}" is too large to fit in')
        time_hi = (2 ** 12 - 1) & time
        # sets the version to 1
        time_hi_version = 1 << 12 | time_hi
        return (time_low << 32 |
                time_mid << 16 |
                time_hi_version)

    @property
    def msb(self) -> int:
        return int.from_bytes(self.uuid.bytes[:8], byteorder='big')

    @property
    def lsb(self) -> int:
        return int.from_bytes(self.uuid.bytes[8:], byteorder='big')

    # the duration between 00:00 15 Oct 1582 and UNIX epoch
    # see also utils/UUID_gen.hh
    UNIX_EPOCH_SINCE_GREGORIAN_DAY0 = 122192928000000000

    @property
    def timestamp(self) -> (datetime.datetime, int):
        # UUID v1 uses a timestamp epoch derived from Gregorian calendar, so we
        # need to translate the timestamp to the UNIX time
        unix_time = self.uuid.time - self.UNIX_EPOCH_SINCE_GREGORIAN_DAY0
        seconds, decimicro_seconds = divmod(unix_time, DECIMICRO_RATIO)
        return datetime.datetime.fromtimestamp(seconds), decimicro_seconds

    def print_field(self, field: str, print_in_hex: bool) -> None:
        def print_num(n: int, bits: int) -> str:
            if print_in_hex:
                # each hex char represents 4 bits
                width = bits // 4
                print(f'{field} = 0x{n:0{width}x}')
            else:
                print(f'{field} = {n}')

        if field == 'lsb':
            print_num(self.lsb, 64)
        elif field == 'msb':
            print_num(self.msb, 64)
        elif field == 'date':
            datetime, _ = self.timestamp
            print(f'date = {datetime}')
        elif field == 'decimicro_seconds':
            _, decimicro_seconds = self.timestamp
            print_num(decimicro_seconds, DECIMICRO_RATIO.bit_length())
        elif field == 'time':
            print_num(self.uuid.time, 60)
        elif field == 'node':
            print_num(self.uuid.node, 48)
        else:
            assert False, f'unknown field: {field}'

    def __str__(self) -> str:
        return self.encode_with_base36()


def test_dencode_base36() -> None:
    # a minimal pytest, run it with 'pytest base36-uuid.py'
    # the dataset comes from test/boost/sstable_generation_test.cc
    encoded_uuid = "3fw2_0tj4_46w3k2cpidnirvjy7k"
    expected_msb = 0x6636ac00da8411ec
    expected_lsb = 0x9abaf56e1443def0
    timeuuid = TimeUuid.decode_with_base36(encoded_uuid)
    assert timeuuid.msb == expected_msb
    assert timeuuid.lsb == expected_lsb
    assert timeuuid.encode_with_base36() == encoded_uuid

    timestamp, decimicro_seconds = timeuuid.timestamp
    assert timestamp == datetime.datetime(2022, 5, 23, 18, 37, 52)
    assert decimicro_seconds == 7040000


def main():
    parser = argparse.ArgumentParser(
        description="Encode and decode timeuuid using base36 representation.")
    dencode_parser = parser.add_mutually_exclusive_group(required=True)
    dencode_parser.add_argument('-d', '--decode',
                                help='Decode base36-encoded timeuuid',
                                metavar='UUID')
    dencode_parser.add_argument('-e', '--encode',
                                nargs=2,
                                type=lambda x: int(x, 0),
                                help='Encode 64-bit hex MSB and LSB using base36',
                                metavar='N')
    default_fields = ['date', 'decimicro_seconds', 'lsb']
    parser.add_argument('--field',
                        action='append',
                        choices=['lsb', 'msb', 'date', 'decimicro_seconds' 'time', 'node'],
                        help='Field to be printed (default: {})'.format(
                            ", ".join(default_fields)),
                        dest='fields')
    parser.add_argument('--hex',
                        action=argparse.BooleanOptionalAction,
                        help='Format numbers in hex',
                        default=True,
                        dest='print_in_hex')

    args = parser.parse_args()
    if args.decode:
        uuid = TimeUuid.decode_with_base36(args.decode.lower())
        if args.fields:
            fields = args.fields
        else:
            fields = default_fields
        for field in fields:
            uuid.print_field(field, args.print_in_hex)
    else:
        print(TimeUuid(*args.encode))


if __name__ == '__main__':
    main()
