#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Fix llms.txt generation for every version of the multiversion documentation.

sphinx-llm generates the Markdown files referenced by llms.txt by rerunning
Sphinx for each version. By default, each run uses that version's conf.py and
extensions. Custom extensions (/ext dir) can crash the Markdown builder,
preventing llms.txt from being generated.

Pass `-c` to each run so it uses the current checkout's conf.py, matching the
configuration used by the rest of the build.
"""


import subprocess

try:
    from sphinx_llm import txt as sphinx_llm_txt
except ImportError:
    sphinx_llm_txt = None

_real_popen = subprocess.Popen


class _SubprocessWithConfdir:
    def __init__(self, confdir):
        self._confdir = str(confdir)

    def Popen(self, cmd, *args, **kwargs):
        if isinstance(cmd, (list, tuple)) and "-c" not in cmd:
            cmd = list(cmd)
            try:
                i = cmd.index("-b")
                if cmd[i + 1] == "markdown":
                    cmd[i + 2 : i + 2] = ["-c", self._confdir]
            except (ValueError, IndexError):
                pass
        return _real_popen(cmd, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(subprocess, name)


def forward_confdir(app, *_):
    if sphinx_llm_txt is not None:
        sphinx_llm_txt.subprocess = _SubprocessWithConfdir(app.confdir)


def setup(app):
    app.connect("config-inited", forward_confdir)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
