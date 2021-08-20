#! /bin/bash

set -e

for JOB in 'read' 'randread' 'write' 'randwrite' 'rw' 'randrw' 'rw' 'randrw'
do
    ./test_ra.py $JOB > bench_log_$JOB
    sleep 10
done

