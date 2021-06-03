#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2019-present ScyllaDB
#
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
#

import argparse
import json
import matplotlib.pyplot as plt

cmdline_parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
cmdline_parser.add_argument('results', nargs=1, help='JSON file with full perf_fast_forward results')
cmdline_parser.add_argument('-o', '--output', help='name of the output file')
cmdline_parser.add_argument('--histogram', action='store_true', help='plot a histogram of the frag/s results')
cmdline_parser.add_argument('--histogram-bins-count', default=50, help='number of histogram bins')
cmdline_parser.add_argument('--histogram-stats', default='frag/s', help='comma-separated list of result statistic to prepare histograms of')

args = cmdline_parser.parse_args()

results = json.loads(open(args.results[0]).read())

if args.histogram:
    histogram_stats = args.histogram_stats.split(',')

    stats = results['results']['stats']

    dpi = 96
    width = 1200
    height = 400 * len(histogram_stats)
    plt.figure(figsize=(width/dpi, height/dpi), dpi=dpi)

    count = 0
    for histogram_stat in histogram_stats:
        values = [stat[histogram_stat] for stat in stats]

        count = count + 1

        ax = plt.subplot(len(histogram_stats), 1, count)
        if count == 1:
            plt.title(args.results[0])

        ax.set_axisbelow(True)
        plt.grid(True)
        plt.ylabel('count')
        plt.xlabel(histogram_stat)
        plt.hist(values, int(args.histogram_bins_count))

    if args.output:
        plt.savefig(args.output)
    else:
        plt.show()
else:
    print('No action chosen. Doing nothing.')
