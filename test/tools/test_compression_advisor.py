# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Unit tests for tools/compression_advisor.py.

These are pure-Python tests that require no running ScyllaDB cluster.
Run with:  python -m pytest test/tools/test_compression_advisor.py -v
"""

import re
import pytest
import sys
import os
import types
import json
import urllib.error

# Make tools/ importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "tools"))
import compression_advisor as ca


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_result(
    algo: str, chunk_kb: int, level: int, dict_mode: str, ratio: float
) -> dict:
    """Build one estimation result entry."""
    return {
        "sstable_compression": algo,
        "chunk_length_in_kb": chunk_kb,
        "level": level,
        "dict": dict_mode,
        "ratio": ratio,
    }


def _lz4(chunk_kb, dict_mode, ratio):
    return make_result("LZ4WithDictsCompressor", chunk_kb, 1, dict_mode, ratio)


def _zstd(chunk_kb, level, dict_mode, ratio):
    return make_result("ZstdWithDictsCompressor", chunk_kb, level, dict_mode, ratio)


def make_full_result_set(
    *,
    # Base ratios for LZ4, no dict, per chunk size
    lz4_none_1k=0.82,
    lz4_none_4k=0.78,
    lz4_none_16k=0.67,
    # LZ4 with future dict
    lz4_future_1k=0.45,
    lz4_future_4k=0.41,
    lz4_future_16k=0.38,
    # LZ4 with past dict
    lz4_past_1k=0.55,
    lz4_past_4k=0.52,
    lz4_past_16k=0.48,
    # ZSTD base ratio scale: each level improves by this factor
    zstd_level_factors=None,
    # ZSTD no-dict base (4K level 1)
    zstd_none_base=0.76,
    # ZSTD future-dict base (4K level 1)
    zstd_future_base=0.38,
    # ZSTD past-dict base (4K level 1)
    zstd_past_base=0.50,
    # Chunk size factors relative to 4K
    chunk_1k_factor=1.05,  # 1K is 5% worse than 4K
    chunk_16k_factor=0.92,  # 16K is 8% better than 4K
):
    """Build a complete 54-entry result set with configurable parameters."""
    if zstd_level_factors is None:
        # Default: diminishing returns — levels 1-3 meaningful, 4-5 marginal
        zstd_level_factors = {1: 1.0, 2: 0.96, 3: 0.93, 4: 0.925, 5: 0.92}

    results = []

    # LZ4 entries (1 level × 3 chunks × 3 dicts = 9)
    for chunk_kb, none_r, past_r, future_r in [
        (1, lz4_none_1k, lz4_past_1k, lz4_future_1k),
        (4, lz4_none_4k, lz4_past_4k, lz4_future_4k),
        (16, lz4_none_16k, lz4_past_16k, lz4_future_16k),
    ]:
        results.append(_lz4(chunk_kb, "none", none_r))
        results.append(_lz4(chunk_kb, "past", past_r))
        results.append(_lz4(chunk_kb, "future", future_r))

    # ZSTD entries (5 levels × 3 chunks × 3 dicts = 45)
    for level in range(1, 6):
        lf = zstd_level_factors[level]
        for chunk_kb, chunk_f in [
            (1, chunk_1k_factor),
            (4, 1.0),
            (16, chunk_16k_factor),
        ]:
            none_r = zstd_none_base * lf * chunk_f
            past_r = zstd_past_base * lf * chunk_f
            future_r = zstd_future_base * lf * chunk_f
            results.append(_zstd(chunk_kb, level, "none", round(none_r, 4)))
            results.append(_zstd(chunk_kb, level, "past", round(past_r, 4)))
            results.append(_zstd(chunk_kb, level, "future", round(future_r, 4)))

    assert len(results) == 54
    return results


# ---------------------------------------------------------------------------
# TestParseCurrentConfig
# ---------------------------------------------------------------------------


class TestParseCurrentConfig:
    def test_lz4_compressor(self):
        cfg = ca.parse_current_config(
            {
                "sstable_compression": "LZ4Compressor",
                "chunk_length_in_kb": "4",
            }
        )
        assert cfg["algorithm"] == "LZ4WithDictsCompressor"
        assert cfg["chunk_length_kb"] == 4
        assert cfg["level"] == 1
        assert cfg["dict_aware"] is False

    def test_lz4_with_dicts(self):
        cfg = ca.parse_current_config(
            {
                "sstable_compression": "LZ4WithDictsCompressor",
                "chunk_length_in_kb": "16",
            }
        )
        assert cfg["algorithm"] == "LZ4WithDictsCompressor"
        assert cfg["chunk_length_kb"] == 16

    def test_zstd_with_level(self):
        cfg = ca.parse_current_config(
            {
                "sstable_compression": "ZstdWithDictsCompressor",
                "chunk_length_in_kb": "4",
                "compression_level": "3",
            }
        )
        assert cfg["algorithm"] == "ZstdWithDictsCompressor"
        assert cfg["level"] == 3

    def test_fully_qualified_java_name(self):
        cfg = ca.parse_current_config(
            {
                "sstable_compression": "org.apache.cassandra.io.compress.LZ4Compressor",
                "chunk_length_in_kb": "4",
            }
        )
        assert cfg["algorithm"] == "LZ4WithDictsCompressor"
        assert cfg["dict_aware"] is False

    def test_dict_capable_compressor_is_dict_aware(self):
        cfg = ca.parse_current_config(
            {
                "sstable_compression": "LZ4WithDictsCompressor",
                "chunk_length_in_kb": "4",
            }
        )
        assert cfg["dict_aware"] is True

    def test_default_chunk_length(self):
        cfg = ca.parse_current_config(
            {
                "sstable_compression": "LZ4WithDictsCompressor",
            }
        )
        assert cfg["chunk_length_kb"] == 4

    def test_default_level(self):
        cfg = ca.parse_current_config(
            {
                "sstable_compression": "ZstdWithDictsCompressor",
            }
        )
        assert cfg["level"] == 1

    def test_snappy_compressor(self):
        cfg = ca.parse_current_config(
            {
                "sstable_compression": "SnappyCompressor",
                "chunk_length_in_kb": "4",
            }
        )
        # Snappy is recognized but not dict-aware (not in _TO_DICT_AWARE)
        assert cfg["dict_aware"] is False
        assert cfg["algorithm"] == "SnappyCompressor"

    def test_unknown_compressor(self):
        cfg = ca.parse_current_config(
            {
                "sstable_compression": "SomeFutureCompressor",
            }
        )
        assert cfg["algorithm"] == "SomeFutureCompressor"
        assert cfg["dict_aware"] is False

    def test_invalid_chunk_length_defaults_to_4(self):
        cfg = ca.parse_current_config(
            {
                "sstable_compression": "LZ4WithDictsCompressor",
                "chunk_length_in_kb": "banana",
            }
        )
        assert cfg["chunk_length_kb"] == 4

    def test_invalid_level_defaults_to_1(self):
        cfg = ca.parse_current_config(
            {
                "sstable_compression": "ZstdWithDictsCompressor",
                "compression_level": "high",
            }
        )
        assert cfg["level"] == 1


# ---------------------------------------------------------------------------
# TestFindCurrentRatio
# ---------------------------------------------------------------------------


class TestFindCurrentRatio:
    def test_exact_match_with_past_dict(self):
        results = [
            _lz4(4, "none", 0.78),
            _lz4(4, "past", 0.52),
            _lz4(4, "future", 0.41),
        ]
        cfg = {"algorithm": "LZ4WithDictsCompressor", "chunk_length_kb": 4, "level": 1}
        assert ca.find_current_ratio(results, cfg) == 0.52

    def test_prefers_past_over_none(self):
        results = [
            _lz4(4, "none", 0.78),
            _lz4(4, "past", 0.52),
        ]
        cfg = {"algorithm": "LZ4WithDictsCompressor", "chunk_length_kb": 4, "level": 1}
        assert ca.find_current_ratio(results, cfg) == 0.52

    def test_fallback_to_none_when_no_past(self):
        results = [
            _lz4(4, "none", 0.78),
            _lz4(4, "future", 0.41),
        ]
        cfg = {"algorithm": "LZ4WithDictsCompressor", "chunk_length_kb": 4, "level": 1}
        assert ca.find_current_ratio(results, cfg) == 0.78

    def test_returns_zero_when_no_match(self):
        results = [
            _lz4(1, "none", 0.82),
        ]
        cfg = {"algorithm": "LZ4WithDictsCompressor", "chunk_length_kb": 4, "level": 1}
        assert ca.find_current_ratio(results, cfg) == 0.0

    def test_zstd_match(self):
        results = [
            _zstd(4, 3, "past", 0.35),
            _zstd(4, 3, "none", 0.50),
        ]
        cfg = {"algorithm": "ZstdWithDictsCompressor", "chunk_length_kb": 4, "level": 3}
        assert ca.find_current_ratio(results, cfg) == 0.35

    def test_plain_compressor_prefers_none_over_past(self):
        results = [
            _lz4(4, "none", 0.78),
            _lz4(4, "past", 0.52),
        ]
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": False,
        }
        assert ca.find_current_ratio(results, cfg) == 0.78

    def test_dict_capable_compressor_prefers_past(self):
        results = [
            _lz4(4, "none", 0.78),
            _lz4(4, "past", 0.52),
        ]
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        assert ca.find_current_ratio(results, cfg) == 0.52


# ---------------------------------------------------------------------------
# TestSelectBestForFamily
# ---------------------------------------------------------------------------


class TestSelectBestForFamily:
    # --- Chunk size preferences ---

    def test_prefers_4k_when_1k_marginally_better(self):
        """1K is only ~2% better than 4K -> prefer 4K."""
        results = [
            _lz4(1, "future", 0.40),  # absolute best, but only 2.4% better than 4K
            _lz4(4, "future", 0.41),  # within 5% of best
            _lz4(16, "future", 0.45),  # worse than 4K
            _lz4(1, "none", 0.80),
            _lz4(4, "none", 0.82),
            _lz4(16, "none", 0.85),
        ]
        best = ca.select_best_for_family(results, "LZ4")
        assert best["config"]["chunk_length_kb"] == 4

    def test_picks_1k_when_significantly_better(self):
        """1K is 20% better than 4K -> pick 1K."""
        results = [
            _lz4(1, "future", 0.30),
            _lz4(4, "future", 0.50),  # 67% worse
            _lz4(16, "future", 0.48),
            _lz4(1, "none", 0.60),
            _lz4(4, "none", 0.80),
            _lz4(16, "none", 0.75),
        ]
        best = ca.select_best_for_family(results, "LZ4")
        # absolute best is 1K=0.30, 4K=0.50 is not within 5%
        assert best["config"]["chunk_length_kb"] != 4

    def test_prefers_4k_over_16k_when_close(self):
        """16K is only 3% better than 4K -> prefer 4K."""
        results = [
            _lz4(1, "future", 0.50),
            _lz4(4, "future", 0.41),
            _lz4(16, "future", 0.40),  # 2.4% better than 4K
            _lz4(1, "none", 0.80),
            _lz4(4, "none", 0.70),
            _lz4(16, "none", 0.68),
        ]
        best = ca.select_best_for_family(results, "LZ4")
        assert best["config"]["chunk_length_kb"] == 4

    # --- Dictionary preferences ---

    def test_prefers_no_dict_when_marginal_improvement(self):
        """Dict provides only 3% improvement -> prefer no dict."""
        results = [
            _lz4(4, "none", 0.42),
            _lz4(4, "future", 0.41),  # 2.4% better
            _lz4(1, "none", 0.50),
            _lz4(1, "future", 0.48),
            _lz4(16, "none", 0.40),
            _lz4(16, "future", 0.39),
        ]
        best = ca.select_best_for_family(results, "LZ4")
        assert best["config"]["dict"] == "none"

    def test_recommends_dict_when_significant(self):
        """Dict provides 33% improvement -> recommend dict."""
        results = [
            _lz4(4, "none", 0.60),
            _lz4(4, "future", 0.40),  # 33% better
            _lz4(1, "none", 0.65),
            _lz4(1, "future", 0.42),
            _lz4(16, "none", 0.55),
            _lz4(16, "future", 0.38),
        ]
        best = ca.select_best_for_family(results, "LZ4")
        assert best["config"]["dict"] == "future"

    # --- Level preferences (ZSTD) ---

    def test_prefers_level1_when_level5_marginally_better(self):
        """ZSTD level 5 is only 2% better than level 1 -> pick level 1."""
        results = []
        for level, ratio in [(1, 0.40), (2, 0.395), (3, 0.393), (4, 0.392), (5, 0.391)]:
            results.append(_zstd(4, level, "future", ratio))
            results.append(_zstd(4, level, "none", ratio + 0.2))
        # Add other chunks to avoid issues
        for ck in [1, 16]:
            for level in range(1, 6):
                results.append(_zstd(ck, level, "future", 0.50))
                results.append(_zstd(ck, level, "none", 0.70))
        best = ca.select_best_for_family(results, "Zstd")
        assert best["config"]["level"] == 1

    def test_picks_level3_when_diminishing_returns(self):
        """Levels 1-3 have meaningful improvement, 4-5 are marginal."""
        results = []
        for level, ratio in [(1, 0.50), (2, 0.45), (3, 0.42), (4, 0.415), (5, 0.41)]:
            results.append(_zstd(4, level, "future", ratio))
            results.append(_zstd(4, level, "none", ratio + 0.15))
        for ck in [1, 16]:
            for level in range(1, 6):
                results.append(_zstd(ck, level, "future", 0.60))
                results.append(_zstd(ck, level, "none", 0.80))
        best = ca.select_best_for_family(results, "Zstd")
        # Level 3 at 0.42 vs best 0.41: within 5%. Level 1 at 0.50 vs 0.41: not within 5%
        # Level 2 at 0.45 vs 0.41: 9.7% — not within 5%
        # Level 3 at 0.42 vs 0.41: 2.4% — within 5%!
        assert best["config"]["level"] == 3

    def test_picks_level5_when_each_level_significant(self):
        """Each level has >5% improvement over the previous."""
        results = []
        for level, ratio in [(1, 0.60), (2, 0.50), (3, 0.40), (4, 0.30), (5, 0.20)]:
            results.append(_zstd(4, level, "future", ratio))
            results.append(_zstd(4, level, "none", ratio + 0.15))
        for ck in [1, 16]:
            for level in range(1, 6):
                results.append(_zstd(ck, level, "future", 0.70))
                results.append(_zstd(ck, level, "none", 0.85))
        best = ca.select_best_for_family(results, "Zstd")
        # Each level far apart from best=0.20. Only level 5 is within 5% of 0.20
        assert best["config"]["level"] == 5

    # --- LZ4 always level 1 ---

    def test_lz4_always_level_1(self):
        results = [
            _lz4(4, "future", 0.40),
            _lz4(4, "none", 0.70),
            _lz4(1, "future", 0.42),
            _lz4(1, "none", 0.75),
            _lz4(16, "future", 0.38),
            _lz4(16, "none", 0.65),
        ]
        best = ca.select_best_for_family(results, "LZ4")
        assert best["config"]["level"] == 1

    # --- Edge cases ---

    def test_all_ratios_identical_picks_simplest(self):
        """When all ratios are the same, pick the simplest config."""
        results = []
        for ck in [1, 4, 16]:
            results.append(_lz4(ck, "none", 0.50))
            results.append(_lz4(ck, "future", 0.50))
        best = ca.select_best_for_family(results, "LZ4")
        # Should prefer 4K and no dict (simplest)
        assert best["config"]["chunk_length_kb"] == 4
        assert best["config"]["dict"] == "none"

    def test_generates_notes(self):
        """Check that notes are generated for preference decisions."""
        results = [
            _lz4(1, "future", 0.39),
            _lz4(4, "future", 0.41),
            _lz4(16, "future", 0.38),
            _lz4(1, "none", 0.80),
            _lz4(4, "none", 0.82),
            _lz4(16, "none", 0.75),
        ]
        best = ca.select_best_for_family(results, "LZ4")
        assert len(best["notes"]) > 0

    def test_no_results_returns_none_config(self):
        """If no results match the family, config is None."""
        results = [
            _lz4(4, "future", 0.40),
        ]
        best = ca.select_best_for_family(results, "Zstd")
        assert best["config"] is None

    def test_no_4k_entries_picks_best_available_chunk(self):
        """If no 4K entries exist, pick the best available chunk size."""
        results = [
            _lz4(1, "future", 0.45),
            _lz4(16, "future", 0.38),
            _lz4(1, "none", 0.80),
            _lz4(16, "none", 0.65),
        ]
        best = ca.select_best_for_family(results, "LZ4")
        assert best["config"] is not None
        # Should pick the best chunk (16K with 0.38)
        assert best["config"]["chunk_length_kb"] in (1, 16)


# ---------------------------------------------------------------------------
# TestEdgeCases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_parse_current_config_empty_dict(self):
        """parse_current_config({}) should return sensible defaults."""
        cfg = ca.parse_current_config({})
        assert cfg["chunk_length_kb"] == 4
        assert cfg["level"] == 1
        assert cfg["algorithm"] == ""
        assert cfg["dict_aware"] is False

    def test_find_current_ratio_empty_results(self):
        """find_current_ratio([], cfg) should return 0.0."""
        cfg = {"algorithm": "LZ4WithDictsCompressor", "chunk_length_kb": 4, "level": 1}
        assert ca.find_current_ratio([], cfg) == 0.0

    def test_select_best_empty_results(self):
        """select_best_for_family([], ...) should return None config."""
        best = ca.select_best_for_family([], "LZ4")
        assert best["config"] is None

    def test_visible_len_no_ansi(self):
        """_visible_len on plain text returns normal length."""
        assert ca._visible_len("hello") == 5

    def test_visible_len_with_ansi(self):
        """_visible_len strips ANSI codes."""
        s = "\033[1;32m-15.3%\033[0m"
        assert ca._visible_len(s) == 6  # "-15.3%"

    def test_ratio_zero_shown_as_zero(self):
        """Ratio 0.0 should be shown as '0.00', not as a dash."""
        # This tests fix #2: cur["ratio"] == 0.0 should not be treated as falsy
        results = [_lz4(4, "none", 0.0), _lz4(4, "past", 0.0)]
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        summary = ca.generate_summary(results, cfg)
        table = ca.format_summary_table(summary, use_color=False)
        # The ratio for current should be "0.000", not "—"
        assert "0.000" in table


# ---------------------------------------------------------------------------
# TestGenerateSummary
# ---------------------------------------------------------------------------


class TestGenerateSummary:
    def test_summary_has_all_keys(self):
        results = make_full_result_set()
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        summary = ca.generate_summary(results, cfg)
        assert "current" in summary
        assert "best_lz4" in summary
        assert "best_zstd" in summary

    def test_current_ratio_matches(self):
        results = make_full_result_set()
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        summary = ca.generate_summary(results, cfg)
        assert summary["current"]["ratio"] == 0.52  # lz4_past_4k default

    def test_improvement_pct_calculation(self):
        results = make_full_result_set()
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        summary = ca.generate_summary(results, cfg)
        # Both recommendations should be better (positive improvement)
        assert summary["best_lz4"]["improvement_pct"] > 0
        assert summary["best_zstd"]["improvement_pct"] > 0

    def test_recommendations_not_worse_than_current(self):
        results = make_full_result_set()
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        summary = ca.generate_summary(results, cfg)
        cur_ratio = summary["current"]["ratio"]
        if summary["best_lz4"]["config"]:
            assert summary["best_lz4"]["config"]["ratio"] <= cur_ratio
        if summary["best_zstd"]["config"]:
            assert summary["best_zstd"]["config"]["ratio"] <= cur_ratio

    def test_negative_improvement_when_current_is_optimal(self):
        """If current config is better than recommendations, improvement is negative."""
        # Make current config very good: past dict at 4K is 0.10
        results = make_full_result_set(
            lz4_past_4k=0.10,
            lz4_future_4k=0.41,
            zstd_future_base=0.38,
        )
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        summary = ca.generate_summary(results, cfg)
        # Current ratio is very low (0.10), recommendations are higher
        assert summary["best_lz4"]["improvement_pct"] < 0

    def test_with_full_54_entry_dataset(self):
        results = make_full_result_set()
        cfg = {
            "algorithm": "ZstdWithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        summary = ca.generate_summary(results, cfg)
        assert summary["current"]["algorithm"] == "ZstdWithDictsCompressor"
        assert summary["best_lz4"]["config"] is not None
        assert summary["best_zstd"]["config"] is not None


# ---------------------------------------------------------------------------
# TestFormatHeatmapGrid
# ---------------------------------------------------------------------------


class TestFormatHeatmapGrid:
    def _make_grid(self, use_color=False):
        results = make_full_result_set()
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        summary = ca.generate_summary(results, cfg)
        return ca.format_heatmap_grid(results, cfg, summary, use_color)

    def test_grid_has_header_row(self):
        grid = self._make_grid(use_color=False)
        assert "no dict" in grid
        assert "future dict" in grid

    def test_grid_has_chunk_size_columns(self):
        grid = self._make_grid(use_color=False)
        assert "1K" in grid
        assert "4K" in grid
        assert "16K" in grid

    def test_grid_has_algorithm_rows(self):
        grid = self._make_grid(use_color=False)
        assert "LZ4" in grid
        assert "ZSTD" in grid

    def test_grid_has_all_zstd_levels(self):
        grid = self._make_grid(use_color=False)
        for lvl in range(1, 6):
            assert f"level {lvl}" in grid

    def test_grid_has_6_data_rows(self):
        """1 LZ4 row + 5 ZSTD rows = 6 data rows."""
        grid = self._make_grid(use_color=False)
        lvl_count = sum(
            1
            for line in grid.split("\n")
            if "level " in line
            and any(c.isdigit() for c in line.split("level ")[-1][:1])
        )
        assert lvl_count == 6

    def test_current_config_marked(self):
        grid = self._make_grid(use_color=False)
        assert "*" in grid

    def test_recommended_config_marked(self):
        grid = self._make_grid(use_color=False)
        assert "\u25c4" in grid  # ◄

    def test_no_recommended_marker_on_regression(self):
        """If a family's best is worse than current, no ◄ for that family."""
        # Current is ZSTD with very good past-dict ratio;
        # LZ4 recommendations will all be worse -> no ◄ on LZ4 cells
        results = make_full_result_set(
            lz4_future_4k=0.60,
            lz4_future_1k=0.65,
            lz4_future_16k=0.58,
            zstd_past_base=0.20,  # current ratio is very good
            zstd_future_base=0.15,  # ZSTD still gets ◄
        )
        cfg = {
            "algorithm": "ZstdWithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        summary = ca.generate_summary(results, cfg)
        grid = ca.format_heatmap_grid(results, cfg, summary, use_color=False)
        lines = grid.split("\n")
        lz4_lines = [l for l in lines if l.strip().startswith("LZ4")]
        # No ◄ on LZ4 lines (it's a regression)
        for line in lz4_lines:
            assert "\u25c4" not in line, f"LZ4 regression should not have ◄: {line}"
        # But ZSTD should still have ◄
        zstd_lines = [l for l in lines if l.strip().startswith("ZSTD")]
        assert any("\u25c4" in l for l in zstd_lines), "ZSTD improvement should have ◄"

    def test_no_color_mode_no_ansi(self):
        grid = self._make_grid(use_color=False)
        assert "\033[" not in grid

    def test_color_mode_has_ansi(self):
        grid = self._make_grid(use_color=True)
        assert "\033[" in grid

    def test_legend_present(self):
        grid = self._make_grid(use_color=False)
        assert "current" in grid.lower()
        assert "recommended" in grid.lower()


# ---------------------------------------------------------------------------
# TestFormatSummaryTable
# ---------------------------------------------------------------------------


class TestFormatSummaryTable:
    def _make_table(self, use_color=False):
        results = make_full_result_set()
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        summary = ca.generate_summary(results, cfg)
        return ca.format_summary_table(summary, use_color)

    def test_three_columns(self):
        tbl = self._make_table(use_color=False)
        assert "Current" in tbl
        assert "Best LZ4" in tbl
        assert "Best ZSTD" in tbl

    def test_contains_algorithm_names(self):
        tbl = self._make_table(use_color=False)
        assert "LZ4WithDicts" in tbl
        assert "ZstdWithDicts" in tbl

    def test_contains_ratio_values(self):
        tbl = self._make_table(use_color=False)
        # Should contain some decimal ratios
        assert re.search(r"0\.\d{3}", tbl)

    def test_contains_improvement_pct(self):
        tbl = self._make_table(use_color=False)
        assert "%" in tbl

    def test_small_improvement_uses_size_change_sign(self):
        results = make_full_result_set(
            lz4_past_4k=0.50,
            lz4_none_4k=0.485,
            lz4_none_1k=0.60,
            lz4_none_16k=0.60,
            lz4_future_1k=0.60,
            lz4_future_4k=0.60,
            lz4_future_16k=0.60,
            zstd_future_base=0.30,
        )
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        summary = ca.generate_summary(results, cfg)
        tbl = ca.format_summary_table(summary, use_color=False)

        assert 0 < summary["best_lz4"]["improvement_pct"] < 5
        assert "-3.0%" in tbl

    def test_small_regression_uses_size_change_sign(self):
        results = make_full_result_set(
            lz4_past_4k=0.50,
            lz4_none_4k=0.515,
            lz4_none_1k=0.60,
            lz4_none_16k=0.60,
            lz4_future_1k=0.60,
            lz4_future_4k=0.60,
            lz4_future_16k=0.60,
            zstd_future_base=0.30,
        )
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        summary = ca.generate_summary(results, cfg)
        tbl = ca.format_summary_table(summary, use_color=False)

        assert -5 < summary["best_lz4"]["improvement_pct"] < 0
        assert "+3.0%" in tbl

    def test_notes_section(self):
        tbl = self._make_table(use_color=False)
        assert "Notes" in tbl

    def test_no_color_mode(self):
        tbl = self._make_table(use_color=False)
        assert "\033[" not in tbl

    def test_color_mode(self):
        tbl = self._make_table(use_color=True)
        assert "\033[" in tbl


# ---------------------------------------------------------------------------
# TestFormatAlterTable
# ---------------------------------------------------------------------------


class TestFormatAlterTable:
    def _make_summary(self, **kwargs):
        results = make_full_result_set(**kwargs)
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        return ca.generate_summary(results, cfg)

    def test_contains_alter_table(self):
        summary = self._make_summary()
        output = ca.format_alter_table(summary, "myks", "mytbl", use_color=False)
        assert 'ALTER TABLE "myks"."mytbl"' in output

    def test_contains_compression_map(self):
        summary = self._make_summary()
        output = ca.format_alter_table(summary, "myks", "mytbl", use_color=False)
        assert "sstable_compression" in output
        assert "chunk_length_in_kb" in output

    def test_zstd_recommendation_includes_level(self):
        """When ZSTD is recommended at level > 1, include compression_level."""
        # Make ZSTD dramatically better so it's picked, and ensure higher levels win
        summary = self._make_summary(
            zstd_future_base=0.20,
            zstd_none_base=0.80,
            zstd_level_factors={1: 1.0, 2: 0.80, 3: 0.60, 4: 0.40, 5: 0.30},
        )
        output = ca.format_alter_table(summary, "ks", "tbl", use_color=False)
        assert "ZstdWithDictsCompressor" in output, "ZSTD should be recommended"
        assert "compression_level" in output

    def test_zstd_level1_recommendation_includes_level(self):
        """When ZSTD level 1 is recommended, include compression_level explicitly."""
        summary = self._make_summary(
            lz4_none_1k=0.82,
            lz4_none_4k=0.81,
            lz4_none_16k=0.80,
            lz4_future_1k=0.79,
            lz4_future_4k=0.78,
            lz4_future_16k=0.77,
            lz4_past_1k=0.82,
            lz4_past_4k=0.80,
            lz4_past_16k=0.79,
            zstd_future_base=0.60,
            zstd_none_base=0.95,
            zstd_level_factors={1: 1.0, 2: 0.98, 3: 0.97, 4: 0.965, 5: 0.96},
        )
        output = ca.format_alter_table(summary, "ks", "tbl", use_color=False)
        assert "ZstdWithDictsCompressor" in output, "ZSTD should be recommended"
        assert "'compression_level': '1'" in output

    def test_lz4_recommendation_no_level(self):
        """LZ4 is always level 1, so no compression_level in the ALTER."""
        # Make LZ4 the best option by making it very good and ZSTD worse
        summary = self._make_summary(
            lz4_future_4k=0.10,
            lz4_past_4k=0.80,
            zstd_future_base=0.90,
            zstd_none_base=0.95,
        )
        output = ca.format_alter_table(summary, "ks", "tbl", use_color=False)
        assert "LZ4WithDictsCompressor" in output, "LZ4 should be recommended"
        assert "compression_level" not in output

    def test_prefers_lz4_when_both_improve_but_lz4_improves_more(self):
        summary = self._make_summary(
            lz4_future_4k=0.20,
            lz4_past_4k=0.50,
            zstd_future_base=0.47,
            zstd_past_base=0.55,
            zstd_none_base=0.80,
            zstd_level_factors={1: 1.0, 2: 0.99, 3: 0.98, 4: 0.97, 5: 0.96},
        )
        assert (
            summary["best_lz4"]["improvement_pct"]
            > summary["best_zstd"]["improvement_pct"]
        )

        output = ca.format_alter_table(summary, "ks", "tbl", use_color=False)
        assert "LZ4WithDictsCompressor" in output
        assert "ZstdWithDictsCompressor" not in output

    def test_dict_retrain_hint_when_future(self):
        """When future dict is recommended, show the retrain hint."""
        summary = self._make_summary(
            lz4_none_4k=0.80,
            lz4_future_4k=0.30,
            lz4_past_4k=0.78,
        )
        output = ca.format_alter_table(summary, "ks", "tbl", use_color=False)
        # With these params, the best recommendation should use a future dict
        best_lz4_dict = summary["best_lz4"]["config"].get("dict", "")
        best_zstd_dict = (
            summary["best_zstd"]["config"].get("dict", "")
            if summary["best_zstd"]["config"]
            else ""
        )
        assert best_lz4_dict == "future" or best_zstd_dict == "future", (
            "At least one recommendation should use future dict"
        )
        assert "retrain" in output.lower()

    def test_quotes_identifiers(self):
        summary = self._make_summary()
        output = ca.format_alter_table(summary, "select", "table-name", use_color=False)
        assert 'ALTER TABLE "select"."table-name"' in output

    def test_retrain_hint_urlencodes_identifiers(self):
        summary = self._make_summary(
            lz4_none_4k=0.80,
            lz4_future_4k=0.30,
            lz4_past_4k=0.78,
        )
        output = ca.format_alter_table(summary, "my ks", "tbl/name", use_color=False)
        assert "keyspace=my+ks" in output
        assert "cf=tbl%2Fname" in output

    def test_no_change_when_already_optimal(self):
        """When current is already best, say so."""
        summary = self._make_summary(
            lz4_past_4k=0.10,
            lz4_future_4k=0.41,
            zstd_future_base=0.38,
        )
        output = ca.format_alter_table(summary, "ks", "tbl", use_color=False)
        assert "optimal" in output.lower() or "no change" in output.lower()

    def test_no_color_mode(self):
        summary = self._make_summary()
        output = ca.format_alter_table(summary, "ks", "tbl", use_color=False)
        assert "\033[" not in output

    def test_color_mode(self):
        summary = self._make_summary()
        output = ca.format_alter_table(summary, "ks", "tbl", use_color=True)
        assert "\033[" in output


# ---------------------------------------------------------------------------
# TestFormatOutput (full pipeline)
# ---------------------------------------------------------------------------


class TestFormatOutput:
    def test_full_output_structure(self):
        results = make_full_result_set()
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        output = ca.format_output(results, cfg, "myks", "mytbl", use_color=False)
        assert "myks.mytbl" in output
        assert "lower is better" in output
        assert "Recommendation Summary" in output
        assert "no dict" in output
        assert "future dict" in output
        assert "ALTER TABLE" in output

    def test_full_output_no_crash_with_all_zeros(self):
        """Degenerate case: all ratios are 0."""
        results = []
        for ck in [1, 4, 16]:
            results.append(_lz4(ck, "none", 0.0))
            results.append(_lz4(ck, "past", 0.0))
            results.append(_lz4(ck, "future", 0.0))
        for ck in [1, 4, 16]:
            for lvl in range(1, 6):
                results.append(_zstd(ck, lvl, "none", 0.0))
                results.append(_zstd(ck, lvl, "past", 0.0))
                results.append(_zstd(ck, lvl, "future", 0.0))
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        # Should not crash
        output = ca.format_output(results, cfg, "ks", "tbl", use_color=False)
        assert "ks.tbl" in output


# ---------------------------------------------------------------------------
# TestI/OHelpers
# ---------------------------------------------------------------------------


class TestIOHelpers:
    def test_call_estimate_api_exits_on_non_list_json(self, monkeypatch, capsys):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return b'{"message": "table not found"}'

        monkeypatch.setattr(
            ca.urllib.request, "urlopen", lambda *args, **kwargs: FakeResponse()
        )

        with pytest.raises(SystemExit) as exc:
            ca.call_estimate_api("127.0.0.1", "ks", "tbl")

        assert exc.value.code == 1
        assert "unexpected JSON structure" in capsys.readouterr().err

    def test_call_estimate_api_returns_list_json(self, monkeypatch):
        entry = make_result("LZ4WithDictsCompressor", 4, 1, "none", 0.5)

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return json.dumps([entry]).encode()

        monkeypatch.setattr(
            ca.urllib.request, "urlopen", lambda *args, **kwargs: FakeResponse()
        )

        assert ca.call_estimate_api("127.0.0.1", "ks", "tbl") == [entry]

    def test_call_estimate_api_exits_on_malformed_entry(self, monkeypatch, capsys):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                # Missing the required keys (only "ratio" present).
                return b'[{"ratio": 0.5}]'

        monkeypatch.setattr(
            ca.urllib.request, "urlopen", lambda *args, **kwargs: FakeResponse()
        )

        with pytest.raises(SystemExit) as exc:
            ca.call_estimate_api("127.0.0.1", "ks", "tbl")

        assert exc.value.code == 1
        assert "malformed entry at index 0" in capsys.readouterr().err

    def test_call_estimate_api_exits_on_non_utf8(self, monkeypatch, capsys):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return b"\xff\xfe not valid utf-8"

        monkeypatch.setattr(
            ca.urllib.request, "urlopen", lambda *args, **kwargs: FakeResponse()
        )

        with pytest.raises(SystemExit) as exc:
            ca.call_estimate_api("127.0.0.1", "ks", "tbl")

        assert exc.value.code == 1
        assert "invalid JSON" in capsys.readouterr().err

    def test_call_estimate_api_exits_on_read_timeout(self, monkeypatch, capsys):
        # A read timeout raises TimeoutError (an OSError), which is *not* a
        # URLError subclass; it must still be handled gracefully.
        def raise_timeout(*args, **kwargs):
            raise TimeoutError("timed out")

        monkeypatch.setattr(ca.urllib.request, "urlopen", raise_timeout)

        with pytest.raises(SystemExit) as exc:
            ca.call_estimate_api("127.0.0.1", "ks", "tbl")

        assert exc.value.code == 1
        assert "Error calling estimation API" in capsys.readouterr().err

    def test_call_estimate_api_exits_on_invalid_json(self, monkeypatch, capsys):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return b"<html>Server Error</html>"

        monkeypatch.setattr(
            ca.urllib.request, "urlopen", lambda *args, **kwargs: FakeResponse()
        )

        with pytest.raises(SystemExit) as exc:
            ca.call_estimate_api("127.0.0.1", "ks", "tbl")

        assert exc.value.code == 1
        assert "invalid JSON" in capsys.readouterr().err

    def test_call_estimate_api_exits_on_url_error(self, monkeypatch, capsys):
        def raise_urlerror(*args, **kwargs):
            raise urllib.error.URLError("boom")

        monkeypatch.setattr(ca.urllib.request, "urlopen", raise_urlerror)

        with pytest.raises(SystemExit) as exc:
            ca.call_estimate_api("127.0.0.1", "ks", "tbl")

        assert exc.value.code == 1
        assert "Error calling estimation API" in capsys.readouterr().err

    def test_get_current_config_shuts_down_cluster_on_connect_error(self, monkeypatch):
        class FakeCluster:
            last_instance = None

            def __init__(self, hosts, port):
                self.hosts = hosts
                self.port = port
                self.shutdown_called = False
                FakeCluster.last_instance = self

            def connect(self):
                raise RuntimeError("connect failed")

            def shutdown(self):
                self.shutdown_called = True

        cassandra_mod = types.ModuleType("cassandra")
        cluster_mod = types.ModuleType("cassandra.cluster")
        setattr(cluster_mod, "Cluster", FakeCluster)

        monkeypatch.setitem(sys.modules, "cassandra", cassandra_mod)
        monkeypatch.setitem(sys.modules, "cassandra.cluster", cluster_mod)

        with pytest.raises(SystemExit) as exc:
            ca.get_current_config_via_cql("127.0.0.1", "ks", "tbl")

        assert exc.value.code == 1
        assert FakeCluster.last_instance is not None
        assert FakeCluster.last_instance.shutdown_called is True

    def test_get_current_config_handles_empty_compression_map(self, monkeypatch):
        """An uncompressed table (compression is None) must not crash."""

        class FakeRow:
            compression = None

        class FakeResult:
            def one(self):
                return FakeRow()

        class FakeSession:
            def execute(self, *args, **kwargs):
                return FakeResult()

        class FakeCluster:
            def __init__(self, hosts, port):
                self.shutdown_called = False

            def connect(self):
                return FakeSession()

            def shutdown(self):
                self.shutdown_called = True

        cassandra_mod = types.ModuleType("cassandra")
        cluster_mod = types.ModuleType("cassandra.cluster")
        setattr(cluster_mod, "Cluster", FakeCluster)
        monkeypatch.setitem(sys.modules, "cassandra", cassandra_mod)
        monkeypatch.setitem(sys.modules, "cassandra.cluster", cluster_mod)

        result = ca.get_current_config_via_cql("127.0.0.1", "ks", "tbl")
        assert result == {}

    def test_full_output_no_crash_zstd_current(self):
        """Current config is ZSTD."""
        results = make_full_result_set()
        cfg = {
            "algorithm": "ZstdWithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 3,
            "dict_aware": True,
        }
        output = ca.format_output(results, cfg, "ks", "tbl", use_color=False)
        assert "ks.tbl" in output

    def test_get_current_config_returns_none_without_driver(self, monkeypatch, capsys):
        """If the ScyllaDB driver is missing, return None instead of exiting."""
        # Ensure importing cassandra.cluster raises ImportError.
        monkeypatch.setitem(sys.modules, "cassandra", None)
        monkeypatch.setitem(sys.modules, "cassandra.cluster", None)

        result = ca.get_current_config_via_cql("127.0.0.1", "ks", "tbl")

        assert result is None
        err = capsys.readouterr().err
        assert "scylla-driver" in err
        assert "continuing without" in err


class TestUnknownCurrentConfig:
    """Behavior when the current compression config cannot be determined."""

    def test_generate_summary_handles_none(self):
        results = make_full_result_set()
        summary = ca.generate_summary(results, None)
        assert summary["current"]["ratio"] is None
        assert summary["current"]["algorithm"] is None
        # Recommendations are still produced, but improvement is unknown.
        assert summary["best_lz4"]["config"] is not None
        assert summary["best_lz4"]["improvement_pct"] is None
        assert summary["best_zstd"]["improvement_pct"] is None

    def test_format_output_no_crash_with_none(self):
        results = make_full_result_set()
        output = ca.format_output(results, None, "ks", "tbl", use_color=False)
        assert "ks.tbl" in output
        # Unknown current values are rendered as a dash, not "None".
        assert "None" not in output
        # The best absolute config is still recommended via ALTER TABLE.
        assert "ALTER TABLE" in output

    def test_format_heatmap_grid_no_crash_with_none(self):
        results = make_full_result_set()
        summary = ca.generate_summary(results, None)
        grid = ca.format_heatmap_grid(results, None, summary, use_color=False)
        assert "no dict" in grid


# ---------------------------------------------------------------------------
# TestEndToEnd: scenario-based integration tests (no I/O)
# ---------------------------------------------------------------------------


class TestEndToEnd:
    def test_dictionary_helps_significantly(self):
        """Scenario: dictionary provides huge improvement."""
        results = make_full_result_set(
            lz4_none_4k=0.80,
            lz4_future_4k=0.40,
            lz4_past_4k=0.78,
            zstd_none_base=0.78,
            zstd_future_base=0.35,
            zstd_past_base=0.75,
        )
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        summary = ca.generate_summary(results, cfg)
        # Dictionary should be recommended for both
        assert summary["best_lz4"]["config"]["dict"] == "future"
        assert summary["best_zstd"]["config"]["dict"] == "future"
        # Large improvements expected
        assert summary["best_lz4"]["improvement_pct"] > 40
        assert summary["best_zstd"]["improvement_pct"] > 40

    def test_dictionary_not_worth_it(self):
        """Scenario: dictionary provides negligible improvement."""
        results = make_full_result_set(
            lz4_none_4k=0.40,
            lz4_future_4k=0.39,
            lz4_past_4k=0.40,
            zstd_none_base=0.38,
            zstd_future_base=0.37,
            zstd_past_base=0.38,
        )
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        summary = ca.generate_summary(results, cfg)
        # No-dict should be preferred (dict is < 5% better)
        assert summary["best_lz4"]["config"]["dict"] == "none"

    def test_already_optimal(self):
        """Scenario: current config has the best ratio of all."""
        # Make past dict extraordinarily good
        results = make_full_result_set(
            lz4_past_4k=0.10,
            lz4_future_4k=0.41,
            zstd_future_base=0.38,
        )
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        summary = ca.generate_summary(results, cfg)
        # Recommendations should have negative improvement
        assert summary["best_lz4"]["improvement_pct"] < 0

    def test_zstd_much_better_than_lz4(self):
        """Scenario: ZSTD provides dramatically better compression."""
        results = make_full_result_set(
            lz4_future_4k=0.60,
            zstd_future_base=0.25,
            lz4_past_4k=0.65,
            zstd_past_base=0.30,
        )
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        summary = ca.generate_summary(results, cfg)
        # ZSTD should show much more improvement than LZ4
        assert (
            summary["best_zstd"]["improvement_pct"]
            > summary["best_lz4"]["improvement_pct"]
        )

    def test_small_chunk_big_win(self):
        """Scenario: 1K chunk is dramatically better (e.g. very small rows)."""
        results = make_full_result_set(
            lz4_future_1k=0.20,
            lz4_future_4k=0.50,
            lz4_future_16k=0.48,
            lz4_past_4k=0.55,
        )
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        summary = ca.generate_summary(results, cfg)
        # 1K should be chosen despite 4K preference (>5% better)
        assert summary["best_lz4"]["config"]["chunk_length_kb"] == 1


# ---------------------------------------------------------------------------
# TestUncompressedTable: an uncompressed table must still get recommendations
# ---------------------------------------------------------------------------


class TestUncompressedTable:
    """An uncompressed table is treated as ratio 1.0 (raw) so it still gets
    recommendations."""

    def test_parse_marks_uncompressed(self):
        cfg = ca.parse_current_config({})
        assert cfg["uncompressed"] is True
        assert cfg["algorithm"] == ""

    def test_parse_compressed_not_marked_uncompressed(self):
        cfg = ca.parse_current_config(
            {"sstable_compression": "LZ4Compressor", "chunk_length_in_kb": "4"}
        )
        assert cfg["uncompressed"] is False

    def test_find_current_ratio_is_one_for_uncompressed(self):
        results = make_full_result_set()
        cfg = ca.parse_current_config({})
        assert ca.find_current_ratio(results, cfg) == 1.0

    def test_summary_reports_uncompressed_current(self):
        results = make_full_result_set()
        cfg = ca.parse_current_config({})
        summary = ca.generate_summary(results, cfg)
        assert summary["current"]["ratio"] == 1.0
        assert summary["current"]["algorithm"] == "uncompressed"
        assert summary["best_lz4"]["improvement_pct"] > 0
        assert summary["best_zstd"]["improvement_pct"] > 0

    def test_uncompressed_recommends_a_change(self):
        # Regression: an uncompressed table must not be reported as optimal.
        results = make_full_result_set()
        cfg = ca.parse_current_config({})
        summary = ca.generate_summary(results, cfg)
        output = ca.format_alter_table(summary, "ks", "tbl", use_color=False)
        assert "ALTER TABLE" in output
        assert "optimal" not in output.lower()

    def test_full_output_no_crash_for_uncompressed(self):
        results = make_full_result_set()
        cfg = ca.parse_current_config({})
        output = ca.format_output(results, cfg, "ks", "tbl", use_color=False)
        assert "ks.tbl" in output
        assert "uncompressed" in output
        assert "None" not in output

    def test_heatmap_no_star_for_uncompressed(self):
        # The table uses none of the listed compressors, so no cell is current.
        results = make_full_result_set()
        cfg = ca.parse_current_config({})
        summary = ca.generate_summary(results, cfg)
        grid = ca.format_heatmap_grid(results, cfg, summary, use_color=False)
        data_rows = [
            l for l in grid.split("\n")
            if l.strip().startswith("LZ4") or l.strip().startswith("ZSTD")
        ]
        assert all("*" not in row for row in data_rows)


# ---------------------------------------------------------------------------
# TestHeatmapCurrentCellRatio: starred cell agrees with the summary
# ---------------------------------------------------------------------------


class TestHeatmapCurrentCellRatio:
    """For a dict-capable current compressor, the starred cell shows the active
    (``past``) ratio that the summary reports, not the no-dict ratio."""

    def _setup(self):
        # past (0.52) differs from none (0.78) at 4K so a mismatch would show.
        results = make_full_result_set(
            lz4_none_4k=0.78,
            lz4_past_4k=0.52,
            lz4_future_4k=0.41,
        )
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": True,
        }
        summary = ca.generate_summary(results, cfg)
        grid = ca.format_heatmap_grid(results, cfg, summary, use_color=False)
        return summary, grid

    def test_starred_cell_shows_past_ratio_not_none_ratio(self):
        summary, grid = self._setup()
        assert summary["current"]["ratio"] == 0.52
        star_line = next(l for l in grid.split("\n") if "*" in l and l.strip().startswith("LZ4"))
        assert "0.520*" in star_line
        assert "0.780*" not in star_line

    def test_summary_and_starred_cell_agree(self):
        summary, grid = self._setup()
        cur = f"{summary['current']['ratio']:.3f}"
        star_line = next(l for l in grid.split("\n") if "*" in l and l.strip().startswith("LZ4"))
        assert f"{cur}*" in star_line

    def test_plain_current_cell_unchanged(self):
        # A non-dict-aware current compressor still shows its no-dict ratio.
        results = make_full_result_set(lz4_none_4k=0.78, lz4_past_4k=0.52)
        cfg = {
            "algorithm": "LZ4WithDictsCompressor",
            "chunk_length_kb": 4,
            "level": 1,
            "dict_aware": False,
        }
        summary = ca.generate_summary(results, cfg)
        grid = ca.format_heatmap_grid(results, cfg, summary, use_color=False)
        star_line = next(l for l in grid.split("\n") if "*" in l and l.strip().startswith("LZ4"))
        assert "0.780*" in star_line
