#!/usr/bin/env python3
import itertools
import math
from pathlib import Path

seeds = [5487697398, 12346127, 3475323, 17563412]
nodes = [(int(1e9), "strong")]
degrees = [1]
exponent = [0.5, 1.0, 1.5]
algos = ["par-polypa", "polypa", "dyn"]
num_threads_list = list(range(1, 64, 2))

for (seed, (node, scaling), deg, expon, algo, num_threads) in itertools.product(seeds, nodes, degrees, exponent, algos, num_threads_list):
    if scaling == "weak":
        node = node * num_threads

    if num_threads > 1 and algo != "par-polypa":
        continue

    seed = seed * num_threads
    expon_s = int(expon * 10)
    filename = Path("logs") / "par-scaling" / scaling / f"a{algo}-n{node}-d{deg}-e{expon_s}-t{num_threads}-s{seed}.log"

    if algo == "dyn" or algo == "polypa" or algo == "polypa-prefetch" or algo == "par-polypa":
        cmd = f"target/release/rust-nlpa -a {algo} -s {seed} -n {node} -d {deg} -e {expon} -t {num_threads} -p"
    else:
        assert(False)

    print("/usr/bin/time -av %s > %s 2>&1" % (cmd, filename))
