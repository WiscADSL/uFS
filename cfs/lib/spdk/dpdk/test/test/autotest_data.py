# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2010-2014 Intel Corporation

# Test data for autotests

from glob import glob
from autotest_test_funcs import *


# quick and dirty function to find out number of sockets
def num_sockets():
    result = len(glob("/sys/devices/system/node/node*"))
    if result == 0:
        return 1
    return result


# Assign given number to each socket
# e.g. 32 becomes 32,32 or 32,32,32,32
def per_sockets(num):
    return ",".join([str(num)] * num_sockets())

# groups of tests that can be run in parallel
# the grouping has been found largely empirically
parallel_test_group_list = [
    {
        "Prefix":    "group_1",
        "Memory":    per_sockets(8),
        "Tests":
        [
            {
                "Name":    "Cycles autotest",
                "Command": "cycles_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Timer autotest",
                "Command": "timer_autotest",
                "Func":    timer_autotest,
                "Report":   None,
            },
            {
                "Name":    "Debug autotest",
                "Command": "debug_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Errno autotest",
                "Command": "errno_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Meter autotest",
                "Command": "meter_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Common autotest",
                "Command": "common_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Resource autotest",
                "Command": "resource_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
        ]
    },
    {
        "Prefix":    "group_2",
        "Memory":    "16",
        "Tests":
        [
            {
                "Name":    "Memory autotest",
                "Command": "memory_autotest",
                "Func":    memory_autotest,
                "Report":  None,
            },
            {
                "Name":    "Read/write lock autotest",
                "Command": "rwlock_autotest",
                "Func":    rwlock_autotest,
                "Report":  None,
            },
            {
                "Name":    "Logs autotest",
                "Command": "logs_autotest",
                "Func":    logs_autotest,
                "Report":  None,
            },
            {
                "Name":    "CPU flags autotest",
                "Command": "cpuflags_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Version autotest",
                "Command": "version_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "EAL filesystem autotest",
                "Command": "eal_fs_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "EAL flags autotest",
                "Command": "eal_flags_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Hash autotest",
                "Command": "hash_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
        ],
    },
    {
        "Prefix":    "group_3",
        "Memory":    per_sockets(512),
        "Tests":
        [
            {
                "Name":    "LPM autotest",
                "Command": "lpm_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "LPM6 autotest",
                "Command": "lpm6_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Memcpy autotest",
                "Command": "memcpy_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Memzone autotest",
                "Command": "memzone_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "String autotest",
                "Command": "string_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Alarm autotest",
                "Command": "alarm_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
        ]
    },
    {
        "Prefix":    "group_4",
        "Memory":    per_sockets(128),
        "Tests":
        [
            {
                "Name":    "PCI autotest",
                "Command": "pci_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Malloc autotest",
                "Command": "malloc_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Multi-process autotest",
                "Command": "multiprocess_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Mbuf autotest",
                "Command": "mbuf_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Per-lcore autotest",
                "Command": "per_lcore_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Ring autotest",
                "Command": "ring_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
        ]
    },
    {
        "Prefix":    "group_5",
        "Memory":    "32",
        "Tests":
        [
            {
                "Name":    "Spinlock autotest",
                "Command": "spinlock_autotest",
                "Func":    spinlock_autotest,
                "Report":  None,
            },
            {
                "Name":    "Byte order autotest",
                "Command": "byteorder_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "TAILQ autotest",
                "Command": "tailq_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Command-line autotest",
                "Command": "cmdline_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Interrupts autotest",
                "Command": "interrupt_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
        ]
    },
    {
        "Prefix":    "group_6",
        "Memory":    per_sockets(512),
        "Tests":
        [
            {
                "Name":    "Function reentrancy autotest",
                "Command": "func_reentrancy_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Mempool autotest",
                "Command": "mempool_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Atomics autotest",
                "Command": "atomic_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Prefetch autotest",
                "Command": "prefetch_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Red autotest",
                "Command": "red_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
        ]
    },
    {
        "Prefix":    "group_7",
        "Memory":    "64",
        "Tests":
        [
            {
                "Name":    "PMD ring autotest",
                "Command": "ring_pmd_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Access list control autotest",
                "Command": "acl_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
            {
                "Name":    "Sched autotest",
                "Command": "sched_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
        ]
    },
]

# tests that should not be run when any other tests are running
non_parallel_test_group_list = [

    {
        "Prefix":    "eventdev",
        "Memory":    "512",
        "Tests":
        [
            {
                "Name":    "Eventdev common autotest",
                "Command": "eventdev_common_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
        ]
    },
    {
        "Prefix":    "eventdev_sw",
        "Memory":    "512",
        "Tests":
        [
            {
                "Name":    "Eventdev sw autotest",
                "Command": "eventdev_sw_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
        ]
    },
    {
        "Prefix":    "kni",
        "Memory":    "512",
        "Tests":
        [
            {
                "Name":    "KNI autotest",
                "Command": "kni_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
        ]
    },
    {
        "Prefix":    "mempool_perf",
        "Memory":    per_sockets(256),
        "Tests":
        [
            {
                "Name":    "Mempool performance autotest",
                "Command": "mempool_perf_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
        ]
    },
    {
        "Prefix":    "memcpy_perf",
        "Memory":    per_sockets(512),
        "Tests":
        [
            {
                "Name":    "Memcpy performance autotest",
                "Command": "memcpy_perf_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
        ]
    },
    {
        "Prefix":    "hash_perf",
        "Memory":    per_sockets(512),
        "Tests":
        [
            {
                "Name":    "Hash performance autotest",
                "Command": "hash_perf_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
        ]
    },
    {
        "Prefix":      "power",
        "Memory":      "16",
        "Tests":
        [
            {
                "Name":       "Power autotest",
                "Command":    "power_autotest",
                "Func":       default_autotest,
                "Report":      None,
            },
        ]
    },
    {
        "Prefix":      "power_acpi_cpufreq",
        "Memory":      "16",
        "Tests":
        [
            {
                "Name":       "Power ACPI cpufreq autotest",
                "Command":    "power_acpi_cpufreq_autotest",
                "Func":       default_autotest,
                "Report":     None,
            },
        ]
    },
    {
        "Prefix":      "power_kvm_vm",
        "Memory":      "16",
        "Tests":
        [
            {
                "Name":       "Power KVM VM  autotest",
                "Command":    "power_kvm_vm_autotest",
                "Func":       default_autotest,
                "Report":     None,
            },
        ]
    },
    {
        "Prefix":    "timer_perf",
        "Memory":    per_sockets(512),
        "Tests":
        [
            {
                "Name":    "Timer performance autotest",
                "Command": "timer_perf_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
        ]
    },

    #
    # Please always make sure that ring_perf is the last test!
    #
    {
        "Prefix":    "ring_perf",
        "Memory":    per_sockets(512),
        "Tests":
        [
            {
                "Name":    "Ring performance autotest",
                "Command": "ring_perf_autotest",
                "Func":    default_autotest,
                "Report":  None,
            },
        ]
    },
]
