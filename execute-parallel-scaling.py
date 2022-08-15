#!/usr/bin/env python3
import itertools
import math
from pathlib import Path

seeds = [1235345, 5487697398, 12346127834, 347589323]
nodes = [(int(1e7), "weak"), (int(1e8), "strong")]
degrees = [1, 10]
exponent = [0.5, 1.0, 1.5, 2.0]
algos = ["par-polypa"]
num_threads_list = list(range(1, 65))

for (seed, (node, scaling), deg, expon, simple, resample,  algo, num_threads) in itertools.product(seeds, nodes, degrees, exponent, [True], [False], algos, num_threads_list):
    if deg == 1 and simple:
        continue

    if scaling == "weak":
        node = node * num_threads

    filename = Path("logs") / algo / scaling / ("n%d_d%d_s%d_e%d_%d_l%d_t%d.log" % (node, deg, simple, math.floor(expon * 10), seed, resample, num_threads))

    if algo == "dyn" or algo == "polypa" or algo == "polypa-prefetch" or algo == "par-polypa":
        cmd = "target/release/rust-nlpa -a %s -s %d -n %d -d %d -e %f -t %d" % (algo, seed, node, deg, expon, num_threads)
        if simple:
            cmd += " -p"

        if resample:
            cmd += " -l"
    else:
        assert(False)

    print("/usr/bin/time -av %s > %s 2>&1" % (cmd, filename))