#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import argparse
import importlib.machinery
import importlib.util
import json
import os
import stat
import textwrap

import pytest

_SCRIPT_PATH = os.path.join(
    os.path.dirname(__file__), '..', '..', 'dist', 'common', 'scripts', 'scylla_gnutls_setup',
)


def _load_script():
    loader = importlib.machinery.SourceFileLoader('scylla_gnutls_setup', _SCRIPT_PATH)
    spec = importlib.util.spec_from_loader('scylla_gnutls_setup', loader)
    mod = importlib.util.module_from_spec(spec)
    loader.exec_module(mod)
    return mod


gs = _load_script()


def _result(override, mbps_by_case, verified=True):
    return {
        'gnutls_version': '3.8.13',
        'cpuid_override': override,
        'verified': verified,
        'results': [{'cipher': c, 'op': op, 'size': size, 'mbps': mbps}
                    for (c, op, size), mbps in mbps_by_case.items()],
    }


CASES = [('AES-256-GCM', op, size) for op in ('enc', 'dec') for size in (128, 8192)]


def _uniform(mbps):
    return {k: mbps for k in CASES}


class TestDecision:
    def test_best_of_takes_max_per_case(self):
        a = _result(None, _uniform(100))
        b = _result(None, {k: 100 + i for i, k in enumerate(CASES)})
        best = gs.best_of([a, b])
        assert best == {k: max(100, 100 + i) for i, k in enumerate(CASES)}

    def test_clear_win_is_chosen(self):
        base = _uniform(1000)
        assert gs.choose(base, {'0x58': _uniform(4000)}, 1.2, 0.05) == '0x58'

    def test_small_gain_is_rejected(self):
        base = _uniform(1000)
        assert gs.choose(base, {'0x58': _uniform(1100)}, 1.2, 0.05) is None

    def test_slowdown_is_rejected(self):
        base = _uniform(3000)
        assert gs.choose(base, {'0x1': _uniform(1000)}, 1.2, 0.05) is None

    def test_regression_in_one_case_is_rejected(self):
        base = _uniform(1000)
        cand = _uniform(4000)
        cand[CASES[0]] = 900
        assert gs.choose(base, {'0x58': cand}, 1.2, 0.05) is None

    def test_best_candidate_wins(self):
        base = _uniform(1000)
        assert gs.choose(base, {'0x1': _uniform(3000), '0x58': _uniform(4000)}, 1.2, 0.05) == '0x58'

    def test_mismatched_cases_raise(self):
        base = _uniform(1000)
        cand = dict(base)
        del cand[CASES[0]]
        with pytest.raises(RuntimeError):
            gs.compare(base, cand)


class TestCandidates:
    def test_neoverse_v1_keeps_sha512(self):
        flags = set('fp asimd evtstrm aes pmull sha1 sha2 crc32 atomics fphp asimdhp cpuid asimdrdm jscvt fcma '
                    'lrcpc dcpop sha3 sm3 sm4 asimddp sha512 sve asimdfhm dit uscat ilrcpc flagm ssbs paca pacg '
                    'dcpodp svei8mm svebf16 i8mm bf16 dgh rng'.split())
        assert gs.default_candidates('aarch64', flags) == ['0x58']

    def test_neoverse_n1_has_no_sha512(self):
        flags = set('fp asimd evtstrm aes pmull sha1 sha2 crc32 atomics fphp asimdhp cpuid asimdrdm lrcpc '
                    'dcpop asimddp ssbs'.split())
        assert gs.default_candidates('aarch64', flags) == ['0x18']

    def test_no_sha_extensions_gives_empty_set(self):
        assert gs.default_candidates('aarch64', {'fp', 'asimd'}) == ['0x1']

    def test_x86_never_gets_aarch64_bits(self):
        assert gs.default_candidates('x86_64', {'aes', 'pclmulqdq', 'avx'}) == ['0x1']

    def test_unknown_architecture_has_no_candidates(self):
        assert gs.default_candidates('s390x', set()) == []


class TestConfFile:
    def test_write_is_idempotent_and_removal_works(self, tmp_path):
        path = gs.conf_path(tmp_path)
        path.parent.mkdir()
        content = gs.render_conf('0x58', 'aarch64 CPU part 0xd4f')
        assert gs.apply(path, content)
        assert path.read_text() == content
        assert stat.S_IMODE(path.stat().st_mode) == 0o644
        assert 'GNUTLS_CPUID_OVERRIDE=0x58\n' in content
        assert gs.is_managed(path)
        assert not gs.apply(path, content)
        assert gs.apply(path, None)
        assert not path.exists()
        assert not gs.apply(path, None)

    def test_hand_written_file_is_not_managed(self, tmp_path):
        path = gs.conf_path(tmp_path)
        path.parent.mkdir()
        path.write_text('GNUTLS_CPUID_OVERRIDE=0x1\n')
        assert not gs.is_managed(path)


@pytest.fixture
def fake_scylla(tmp_path):
    """A stand-in for `scylla perf-tls` that reports 1000 MB/s, or 4000 MB/s
    with GNUTLS_CPUID_OVERRIDE=0x58, and fails with 0xbad."""
    script = tmp_path / 'scylla'
    script.write_text(textwrap.dedent(f'''\
        #!/usr/bin/env python3
        import json, os, sys
        args = sys.argv[1:]
        assert args[0] == 'perf-tls'
        override = os.environ.get('GNUTLS_CPUID_OVERRIDE')
        if override == '0xbad':
            sys.exit(1)
        mbps = 4000 if override == '0x58' else 1000
        result = {{'cpuid_override': override, 'verified': True, 'gnutls_version': '3.8.13',
                   'results': [{{'cipher': 'AES-256-GCM', 'op': 'enc', 'size': 8192, 'mbps': mbps}}]}}
        with open(args[args.index('--json-result') + 1], 'w') as f:
            json.dump(result, f)
        '''))
    script.chmod(0o755)
    return script


class TestRunPerfTls:
    def test_baseline_runs_without_inherited_override(self, fake_scylla, monkeypatch):
        monkeypatch.setenv('GNUTLS_CPUID_OVERRIDE', '0x58')
        r = gs.run_perf_tls(fake_scylla, None, [])
        assert r['cpuid_override'] is None
        assert r['results'][0]['mbps'] == 1000

    def test_candidate_runs_with_override(self, fake_scylla):
        r = gs.run_perf_tls(fake_scylla, '0x58', [])
        assert r['cpuid_override'] == '0x58'
        assert r['results'][0]['mbps'] == 4000

    def test_failed_run_raises(self, fake_scylla):
        with pytest.raises(RuntimeError):
            gs.run_perf_tls(fake_scylla, '0xbad', [])


class TestMeasureAndChoose:
    def _args(self, scylla):
        return argparse.Namespace(scylla=str(scylla), rounds=2, duration=0.01, min_speedup=1.2, max_regression=0.05)

    def test_faster_value_is_chosen_and_failing_one_skipped(self, fake_scylla):
        content = gs.measure_and_choose(self._args(fake_scylla), ['0xbad', '0x2', '0x58'])
        assert content is not None
        assert content.endswith('GNUTLS_CPUID_OVERRIDE=0x58\n')

    def test_no_gain_gives_defaults(self, fake_scylla):
        assert gs.measure_and_choose(self._args(fake_scylla), ['0x2']) is None

    def test_no_candidates_gives_defaults(self, fake_scylla):
        assert gs.measure_and_choose(self._args(fake_scylla), []) is None
