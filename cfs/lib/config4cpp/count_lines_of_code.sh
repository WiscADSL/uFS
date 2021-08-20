#!/bin/sh
cat src/*.h src/*.cpp include/config4cpp/*.h | grep -v '[ \t]*//' | grep -v '^$' | wc -l
