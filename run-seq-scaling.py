#/usr/bin/env bash
rm -rf logs/node_scaling
mkdir -p logs/node_scaling
export RUSTFLAGS='-C target-cpu=native'
cargo +nightly b --release
./schedule-seq-scaling.py | parallel -j 8