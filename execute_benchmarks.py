#!/usr/bin/env python3
import itertools
import math
from pathlib import Path

seeds = [1235345, 5487697398, 12346127834, 347589323]
nodes = [1 << x for x in range(16, 32)]
degrees = [1, 2, 3, 5, 10]
exponent = [0.0, 0.5, 0.8, 1.0, 1.2, 1.5, 2.0]
algos = ["dyn", "polypa", "polypa-cpp"]

algo = "dyn"

for (seed, node, deg, expon, simple) in itertools.product(seeds, nodes, degrees, exponent, [False, True]):
    if deg == 1 and simple:
        continue

    filename = Path("logs") / algo / ("n%d_d%d_s%d_e%d-%d.log" % (node, deg, simple, math.floor(expon * 10), seed))

    if algo == "dyn" or algo == "polypa":
        cmd = "target/release/rust-nlpa -a %s -s %d -n %d -d %d -e %f" % (algo, seed, node, deg, expon)
        if simple:
            cmd += " -p"

    print("/usr/bin/time -av %s > %s 2>&1" % (cmd, filename))