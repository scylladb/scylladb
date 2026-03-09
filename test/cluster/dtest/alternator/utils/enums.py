#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from enum import Enum


class StreamViewType(Enum):
    KEYS_ONLY = 'KEYS_ONLY'
    NEW_IMAGE = 'NEW_IMAGE'
    OLD_IMAGE = 'OLD_IMAGE'
    NEW_AND_OLD_IMAGES = 'NEW_AND_OLD_IMAGES'


class StreamSpecification(Enum):
    KEYS_ONLY = {'StreamSpecification': {'StreamEnabled': True, 'StreamViewType': StreamViewType.KEYS_ONLY.value}}
    NEW_IMAGE = {'StreamSpecification': {'StreamEnabled': True, 'StreamViewType': StreamViewType.NEW_IMAGE.value}}
    OLD_IMAGE = {'StreamSpecification': {'StreamEnabled': True, 'StreamViewType': StreamViewType.OLD_IMAGE.value}}
    NEW_AND_OLD_IMAGE = {'StreamSpecification': {
        'StreamEnabled': True, 'StreamViewType': StreamViewType.NEW_AND_OLD_IMAGES.value}}
