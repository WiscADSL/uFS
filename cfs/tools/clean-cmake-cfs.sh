#!/bin/bash

set -e
cd "$(git rev-parse --show-toplevel)/cfs"
rm -rf build
mkdir -p build build/logs
cd build/
cmake ..
echo "Configure options using ccmake ."
echo "Build using make -j <num>"
