#!/usr/bin/env bash

rm -rf anon-sub
git clone . anon-sub

cd anon-sub
rm -rf .git .gitignore
rm make-anon.sh

cd ext/dynamic-weighted-index
rm .gitignore LICENSE
cat ../../../ext/dynamic-weighted-index/Cargo.toml | grep -v authors > Cargo.toml
cd ../..

tar -xf ../logs.tar.bz2
rm -rf logs/par-scaling/strong/.ipynb_checkpoints
rm -rf logs/node_scaling/.ipynb_checkpoints

rm -rf ../internal-memory.tar.bz2
tar -cjf ../internal-memory.tar.bz2 *

cargo b --release