#/usr/bin/env bash
rm -rf logs/node_scaling
mkdir -p logs/node_scaling
./schedule-seq-scaling.py | parallel -j 8