#! /bin/bash

if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root"
   exit 1
fi


## reserve super pages
# to avoid failure because of lack of super pages, we need to do this immediate
# after rebooting
source ./bench_common.sh
../../cfs_bench/exprs/fsp_microbench_suite.py --fs fsp --devonly

## disable hyperthreading
echo off > /sys/devices/system/cpu/smt/control

## change cpu freqency to fixed value

sudo apt-get install cpufrequtils
cpufreq-info
# NOTE THE default max is 4000000 @bumble
TARGET_FREQ="2900000"
for x in /sys/devices/system/cpu/*/cpufreq/
do
    # NOTE: This will report error, but while verifying via `cat`, it has its effect there.
    echo "$TARGET_FREQ" | sudo tee "$x/scaling_max_freq"
    #echo "$TARGET_FREQ" > "$x/scaling_max_freq"
done
sudo /etc/init.d/cpufrequtils restart
# verify cpu freuqency
sudo cpufreq-info
# for reading cpu performance counter
modprobe msr
sudo sysctl kernel.nmi_watchdog=0

