/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

/**
 * @file
 * Holds the structures for the eal internal configuration
 */

#ifndef EAL_INTERNAL_CFG_H
#define EAL_INTERNAL_CFG_H

#include <rte_eal.h>
#include <rte_pci_dev_feature_defs.h>

#define MAX_HUGEPAGE_SIZES 3  /**< support up to 3 page sizes */

/*
 * internal configuration structure for the number, size and
 * mount points of hugepages
 */
struct hugepage_info {
	uint64_t hugepage_sz;   /**< size of a huge page */
	const char *hugedir;    /**< dir where hugetlbfs is mounted */
	uint32_t num_pages[RTE_MAX_NUMA_NODES];
				/**< number of hugepages of that size on each socket */
	int lock_descriptor;    /**< file descriptor for hugepage dir */
};

/**
 * internal configuration
 */
struct internal_config {
	volatile size_t memory;           /**< amount of asked memory */
	volatile unsigned force_nchannel; /**< force number of channels */
	volatile unsigned force_nrank;    /**< force number of ranks */
	volatile unsigned no_hugetlbfs;   /**< true to disable hugetlbfs */
	unsigned hugepage_unlink;         /**< true to unlink backing files */
	volatile unsigned no_pci;         /**< true to disable PCI */
	volatile unsigned no_hpet;        /**< true to disable HPET */
	volatile unsigned vmware_tsc_map; /**< true to use VMware TSC mapping
										* instead of native TSC */
	volatile unsigned no_shconf;      /**< true if there is no shared config */
	volatile unsigned create_uio_dev; /**< true to create /dev/uioX devices */
	volatile enum rte_proc_type_t process_type; /**< multi-process proc type */
	/** true to try allocating memory on specific sockets */
	volatile unsigned force_sockets;
	volatile uint64_t socket_mem[RTE_MAX_NUMA_NODES]; /**< amount of memory per socket */
	uintptr_t base_virtaddr;          /**< base address to try and reserve memory from */
	volatile unsigned single_file_segments;
	/**< true if storing all pages within single files (per-page-size,
	 * per-node). */
	volatile int syslog_facility;	  /**< facility passed to openlog() */
	/** default interrupt mode for VFIO */
	volatile enum rte_intr_mode vfio_intr_mode;
	const char *hugefile_prefix;      /**< the base filename of hugetlbfs files */
	const char *hugepage_dir;         /**< specific hugetlbfs directory to use */
	const char *user_mbuf_pool_ops_name;
			/**< user defined mbuf pool ops name */
	unsigned num_hugepage_sizes;      /**< how many sizes on this system */
	struct hugepage_info hugepage_info[MAX_HUGEPAGE_SIZES];
};
extern struct internal_config internal_config; /**< Global EAL configuration. */

void eal_reset_internal_config(struct internal_config *internal_cfg);

#endif /* EAL_INTERNAL_CFG_H */
