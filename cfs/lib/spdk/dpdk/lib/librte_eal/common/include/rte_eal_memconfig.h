/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef _RTE_EAL_MEMCONFIG_H_
#define _RTE_EAL_MEMCONFIG_H_

#include <rte_config.h>
#include <rte_tailq.h>
#include <rte_memory.h>
#include <rte_memzone.h>
#include <rte_malloc_heap.h>
#include <rte_rwlock.h>
#include <rte_pause.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * the structure for the memory configuration for the RTE.
 * Used by the rte_config structure. It is separated out, as for multi-process
 * support, the memory details should be shared across instances
 */
struct rte_mem_config {
	volatile uint32_t magic;   /**< Magic number - Sanity check. */

	/* memory topology */
	uint32_t nchannel;    /**< Number of channels (0 if unknown). */
	uint32_t nrank;       /**< Number of ranks (0 if unknown). */

	/**
	 * current lock nest order
	 *  - qlock->mlock (ring/hash/lpm)
	 *  - mplock->qlock->mlock (mempool)
	 * Notice:
	 *  *ALWAYS* obtain qlock first if having to obtain both qlock and mlock
	 */
	rte_rwlock_t mlock;   /**< only used by memzone LIB for thread-safe. */
	rte_rwlock_t qlock;   /**< used for tailq operation for thread safe. */
	rte_rwlock_t mplock;  /**< only used by mempool LIB for thread-safe. */

	uint32_t memzone_cnt; /**< Number of allocated memzones */

	/* memory segments and zones */
	struct rte_memseg memseg[RTE_MAX_MEMSEG];    /**< Physmem descriptors. */
	struct rte_memzone memzone[RTE_MAX_MEMZONE]; /**< Memzone descriptors. */

	struct rte_tailq_head tailq_head[RTE_MAX_TAILQ]; /**< Tailqs for objects */

	/* Heaps of Malloc per socket */
	struct malloc_heap malloc_heaps[RTE_MAX_NUMA_NODES];

	/* address of mem_config in primary process. used to map shared config into
	 * exact same address the primary process maps it.
	 */
	uint64_t mem_cfg_addr;
} __attribute__((__packed__));


inline static void
rte_eal_mcfg_wait_complete(struct rte_mem_config* mcfg)
{
	/* wait until shared mem_config finish initialising */
	while(mcfg->magic != RTE_MAGIC)
		rte_pause();
}

#ifdef __cplusplus
}
#endif

#endif /*__RTE_EAL_MEMCONFIG_H_*/
