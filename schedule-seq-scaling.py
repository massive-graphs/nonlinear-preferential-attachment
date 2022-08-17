#!/usr/bin/env python3
import itertools
from pathlib import Path

seeds = [1235345, 5487697398, 12346127834, 347589323]
nodes = [1 << x for x in range(16, 29, 2)]
degrees = [1, 10]
exponent = [0.5, 1.0, 1.5]
algos = ["polypa", "par-polypa", "dyn", "dyn-resample"]
num_threads=1

for (seed, node, deg, expon, algo) in itertools.product(seeds, nodes, degrees, exponent, algos):
    expon_s = int(expon * 10)

    seed = (node * seed) % ((1 << 63) - 1)

    filename = Path("logs") / "node_scaling" / f"a{algo}-n{node}-d{deg}-e{expon_s}-t{num_threads}-s{seed}.log"

    if algo == "dyn-resample":
        if deg == 1:
            continue

        algo = "dyn -l"

    cmd = f"target/release/rust-nlpa -a {algo} -s {seed} -n {node} -d {deg} -e {expon} -t {num_threads} -p"

    print("/usr/bin/time -av %s > %s 2>&1" % (cmd, filename))
