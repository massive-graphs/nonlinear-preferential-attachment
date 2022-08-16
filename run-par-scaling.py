#/usr/bin/env bash
rm -rf logs/par_scaling/
mkdir -p logs/par_scaling/{strong,weak}
export RUSTFLAGS='-C target-cpu=native'
cargo b --release
./schedule-par-scaling.py | parallel -j 1