/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef MALLOC_HEAP_H_
#define MALLOC_HEAP_H_

#include <rte_malloc.h>
#include <rte_malloc_heap.h>

#ifdef __cplusplus
extern "C" {
#endif

static inline unsigned
malloc_get_numa_socket(void)
{
	unsigned socket_id = rte_socket_id();

	if (socket_id == (unsigned)SOCKET_ID_ANY)
		return 0;

	return socket_id;
}

void *
malloc_heap_alloc(struct malloc_heap *heap,	const char *type, size_t size,
		unsigned flags, size_t align, size_t bound);

int
malloc_heap_get_stats(struct malloc_heap *heap,
		struct rte_malloc_socket_stats *socket_stats);

int
rte_eal_malloc_heap_init(void);

#ifdef __cplusplus
}
#endif

#endif /* MALLOC_HEAP_H_ */
