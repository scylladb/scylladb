import os
import tempfile
import pathlib
from test.pylib.util import read_last_line

def test_read_last_line():
    test_cases = [
        (b"This is the first line.\nThis is the second line.\nThis is the third line.", 'This is the third line.'),
        (b"This is another file.\nIt has a few lines.\nThe last line is what we're interested in.", 'The last line is what we\'re interested in.'),
        (b"This file has only one line.", 'This file has only one line.'),
        (b"\n", ""),
        (b"\n\n\n", ""),
        (b"", ""),
        (b"abc\n", 'abc'),
        (b"abc", '...bc', 2),
        (b"lalala\nbububu", "bububu"),
        (b"line1\nline2\nline3\n", "...line3", 6),
        (b"line1\nline2\nline3", "line3", 6),
        (b"line1\nline2\nline3\n", "line3", 7),
        (b"\xbe\xbe\xbe\xbebububu\n", "bububu")
    ]
    for test_case in test_cases:
        with tempfile.NamedTemporaryFile(dir=os.getenv('TMPDIR', '/tmp')) as f:
            f.write(test_case[0])
            f.flush()
            file_path = pathlib.Path(f.name)
            actual = read_last_line(file_path, test_case[2]) if len(test_case) == 3 else read_last_line(file_path)
            assert(actual == test_case[1])
