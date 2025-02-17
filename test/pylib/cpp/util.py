from types import SimpleNamespace


def make_test_object(test_name: str, suite: str, mode: str = 'no_mode', original_name: str = None) -> SimpleNamespace:
    test = SimpleNamespace()
    test.time_end = 0
    test.id = test_name
    test.mode = mode
    test.success = False
    test.shortname = original_name or test_name

    test.suite = SimpleNamespace()
    test.suite.mode = mode or 'no_mode'
    test.suite.name = suite

    test_name_parts = test_name.split('.')
    test_name_parts.remove(original_name)
    if mode in test_name_parts:
        test_name_parts.remove(mode)
    if len(test_name_parts) > 0:
        test.suite.id = test_name_parts[0]
    else:
        test.suite.id = 1
    return test
