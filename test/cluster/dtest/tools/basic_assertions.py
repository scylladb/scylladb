"""
Basic assertion utilities with no dependencies on other tools modules.
This module exists to break circular dependencies.
"""


def assert_length_equal(object_with_length, expected_length):
    """
    Assert an object has a specific length.
    @param object_with_length The object whose length will be checked
    @param expected_length The expected length of the object

    Examples:
    assert_length_equal(res, nb_counter)
    """
    assert len(object_with_length) == expected_length, f"Expected {object_with_length} to have length {expected_length}, but instead is of length {len(object_with_length)}"
