#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-

import io
import time
import subprocess
import sys
import re
from terminaltables import AsciiTable
import argparse

# https://stackoverflow.com/questions/287871/print-in-terminal-with-colors-using-python
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

parser = argparse.ArgumentParser(description='Parse the output of a JMH run and convert it to a nice ASCII table.')
parser.add_argument('--stats', type=str,
                    default="benchmark,max", dest='stats',
                    help='A comma separated list of stats to print for each measurement. TODO - list')
parser.add_argument('--secondary', type=str,
                    default="gc.count,gc.time", dest='secondary',
                    help='A comma separated list of secondary metrics to print for each benchmark. TODO - list')
args = parser.parse_args()

stats = args.stats.split(",")
secondary = args.secondary.split(",")

# Variables to fill up
benchmarks = {}
last_benchmark = None
last_secondary = None
parsing_result = False

for line in sys.stdin:
    # Some funky characters to clean out
    line = line.replace("\xc2\xb7", "")
    #print parsing_result, "  ", last_benchmark, " : ", last_secondary, "**** ", line
    
    if len(line.strip()) < 1:
        parsing_result = False
        last_secondary = None
    
    elif line.startswith("Result \"") or line.startswith("Secondary result \""):
        match = re.search("Result \"(?P<bench>.*)\"", line)
        if match:
            parsing_result = True
            last_benchmark = match.group('bench')
            benchmarks[last_benchmark] = {}
            benchmarks[last_benchmark]["benchmark"] = {}
            continue
        
        match = re.search("Secondary result \".*:(?P<secondary>.*)\"", line)
        if match:
            parsing_result = True
            last_secondary = match.group('secondary')
            benchmarks[last_benchmark][last_secondary] = {}
            continue
        
    elif parsing_result:
        cols = None
        
        if last_secondary is not None:
            cols = benchmarks[last_benchmark][last_secondary]
        else:
            cols = benchmarks[last_benchmark]["benchmark"]
        
        # Straight up result
        match = re.search("(?P<benchmark>\d+\.\d+).*\((?P<err>\d+\.\d+)%\) (?P<dev>\d+\.\d+) (?P<units>.*)\s+\[(?P<agg>.*)\]", line)
        if match:
            cols['benchmark'] = match.group('benchmark')
            cols['benchmark_err'] = match.group('err')
            cols['err_dev'] = match.group('dev')
            cols['units'] = match.group('units')
            cols['agg'] = match.group('agg')
            continue
        
        # Min, max, avg and stdev. Min and Max are REALLY useful!
        match = re.search(".*\((?P<min>\d+\.\d+), (?P<avg>\d+\.\d+), (?P<max>\d+\.\d+)\), .* = (?P<stdev>\d+\.\d+)", line)
        if match:
           cols['min'] = match.group('min')
           cols['avg'] = match.group('avg')
           cols['max'] = match.group('max')
           cols['stdev'] = match.group('stdev')
           continue
        
        # Confidence interval
        match = re.search("(?P<ci_percent>\d+\.\d+)%\): \[(?P<ci_lower>\d+\.\d+), (?P<ci_upper>\d+\.\d+)\]", line)
        if match:
           cols['ci_percent'] = match.group('ci_percent')
           cols['ci_lower'] = match.group('ci_lower')
           cols['ci_upper'] = match.group('ci_upper')
           continue

# All done so lets format!!
keys = []
headers = ["Benchmark"]
b = benchmarks.itervalues().next()
for key in b:
    if key in secondary or key == "benchmark":
        for stat in b[key].keys():
            if stat in stats:
                keys.append(stat)
                if stat == "benchmark":
                    headers.append(key + " " + stat + " " + b[key]["agg"] + " " + b[key]["units"])
                else:
                    headers.append(key + " " + stat + " " + b[key]["units"])
                print "Gonna trace: ", stat

table_data = []
table_data.append(headers)
# TODO - sorting
for bench in benchmarks:
    data = [bench]
    for key in benchmarks[bench]:
        if key in secondary or key == "benchmark":
            for stat in benchmarks[bench][key].keys():
                if stat in stats:
                    data.append(benchmarks[bench][key][stat])
    table_data.append(data)

table = AsciiTable(table_data)
print table.table