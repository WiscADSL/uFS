
#ifndef FSUTILS_BUF_RING_H
#define FSUTILS_BUF_RING_H


#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <sys/types.h>
#include <stdio.h>
#include <errno.h>


#include "util/util_common.h"
#include "util/util_ring_c11_mem.h"


// the default enqueue/dequeue is single producer/consumer
#define RING_F_SP_ENQ 0x0001
#define RING_F_SC_DEQ 0x0002

#define RING_F_EXACT_SZ 0x0004
#define UTIL_RING_SZ_MASK (0x7fffffffU)

#define __IS_SP 1
#define __IS_MP 0
#define __IS_SC 1
#define __IS_MC 0

// count must be power of 2
ssize_t util_ring_get_memsize(uint32_t count);
int util_ring_init(struct util_ring *r, const char *name, uint32_t count, unsigned flags);
void util_ring_dump(FILE *f, const struct util_ring *r);


/*
 * the actual enqueue of pointers on the ring
 * single and multi producer enqueue functions
 */

#define ENQUEUE_PTRS(r, ring_start, prod_head, obj_table, n, obj_type) do { \
    unsigned int i; \
    const uint32_t size = (r)->size; \
    uint32_t idx = prod_head & (r)->mask; \
    obj_type *ring = (obj_type *)ring_start; \
    if (likely(idx + n < size)) { \
        for (i = 0; i < (n & ((~(unsigned)0x3))); i+=4, idx+=4) { \
            ring[idx] = obj_table[i]; \
            ring[idx+1] = obj_table[i+1]; \
            ring[idx+2] = obj_table[i+2]; \
            ring[idx+3] = obj_table[i+3]; \
        } \
        switch (n & 0x3) { \
        case 3: \
            ring[idx++] = obj_table[i++]; /* fallthrough */ \
        case 2: \
            ring[idx++] = obj_table[i++]; /* fallthrough */ \
        case 1: \
            ring[idx++] = obj_table[i++]; \
        } \
    } else { \
        for (i = 0; idx < size; i++, idx++)\
            ring[idx] = obj_table[i]; \
        for (idx = 0; i < n; i++, idx++) \
            ring[idx] = obj_table[i]; \
    } \
} while (0)


/*
 * the actual copy of pointers on the ring to obj_table.
 * Placed here since identical code needed in both
 * single and multi consumer dequeue functions
 */
#define DEQUEUE_PTRS(r, ring_start, cons_head, obj_table, n, obj_type) do { \
    unsigned int i; \
    uint32_t idx = cons_head & (r)->mask; \
    const uint32_t size = (r)->size; \
    obj_type *ring = (obj_type *)ring_start; \
    if (likely(idx + n < size)) { \
        for (i = 0; i < (n & (~(unsigned)0x3)); i+=4, idx+=4) {\
            obj_table[i] = ring[idx]; \
            obj_table[i+1] = ring[idx+1]; \
            obj_table[i+2] = ring[idx+2]; \
            obj_table[i+3] = ring[idx+3]; \
        } \
        switch (n & 0x3) { \
        case 3: \
            obj_table[i++] = ring[idx++]; /* fallthrough */ \
        case 2: \
            obj_table[i++] = ring[idx++]; /* fallthrough */ \
        case 1: \
            obj_table[i++] = ring[idx++]; \
        } \
    } else { \
        for (i = 0; idx < size; i++, idx++) \
            obj_table[i] = ring[idx]; \
        for (idx = 0; i < n; i++, idx++) \
            obj_table[i] = ring[idx]; \
    } \
} while (0)



/**
 * @internal Enqueue several objects on the ring
 *
  * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects).
 * @param n
 *   The number of objects to add in the ring from the obj_table.
 * @param behavior
 *   UTIL_RING_QUEUE_FIXED:    Enqueue a fixed number of items from a ring
 *   UTIL_RING_QUEUE_VARIABLE: Enqueue as many items as possible from ring
 * @param is_sp
 *   Indicates whether to use single producer or multi-producer head update
 * @param free_space
 *   returns the amount of space after the enqueue operation has finished
 * @return
 *   Actual number of objects enqueued.
 *   If behavior == UTIL_RING_QUEUE_FIXED, this will be 0 or n only.
 */
static __util_always_inline unsigned int
__util_ring_do_enqueue(struct util_ring *r, void * const *obj_table,
                      unsigned int n, enum util_ring_queue_behavior behavior,
                      unsigned int is_sp, unsigned int *free_space)
{
    uint32_t prod_head, prod_next;
    uint32_t free_entries;

    n = __util_ring_move_prod_head(r, is_sp, n, behavior,
                                  &prod_head, &prod_next, &free_entries);
    if (n == 0)
        goto end;

    ENQUEUE_PTRS(r, &r[1], prod_head, obj_table, n, void *);

    update_tail(&r->producer, prod_head, prod_next, is_sp, 1);
    end:
    if (free_space != NULL)
        *free_space = free_entries - n;
    return n;
}

/**
 * @internal Dequeue several objects from the ring
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects).
 * @param n
 *   The number of objects to pull from the ring.
 * @param behavior
 *   UTIL_RING_QUEUE_FIXED:    Dequeue a fixed number of items from a ring
 *   UTIL_RING_QUEUE_VARIABLE: Dequeue as many items as possible from ring
 * @param is_sc
 *   Indicates whether to use single consumer or multi-consumer head update
 * @param available
 *   returns the number of remaining ring entries after the dequeue has finished
 * @return
 *   - Actual number of objects dequeued.
 *     If behavior == UTIL_RING_QUEUE_FIXED, this will be 0 or n only.
 */
static __util_always_inline unsigned int
__util_ring_do_dequeue(struct util_ring *r, void **obj_table,
                      unsigned int n, enum util_ring_queue_behavior behavior,
                      unsigned int is_sc, unsigned int *available)
{
    uint32_t cons_head, cons_next;
    uint32_t entries;

    n = __util_ring_move_cons_head(r, (int)is_sc, n, behavior,
                                  &cons_head, &cons_next, &entries);
    if (n == 0)
        goto end;

    DEQUEUE_PTRS(r, &r[1], cons_head, obj_table, n, void *);

    update_tail(&r->consumer, cons_head, cons_next, is_sc, 0);

    end:
    if (available != NULL)
        *available = entries - n;
    return n;
}

/**
 * Enqueue several objects on the ring (multi-producers safe).
 *
 * This function uses a "compare and set" instruction to move the
 * producer index atomically.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects).
 * @param n
 *   The number of objects to add in the ring from the obj_table.
 * @param free_space
 *   if non-NULL, returns the amount of space in the ring after the
 *   enqueue operation has finished.
 * @return
 *   The number of objects enqueued, either 0 or n
 */
static __util_always_inline unsigned int
util_ring_mp_enqueue_bulk(struct util_ring *r, void * const *obj_table,
                         unsigned int n, unsigned int *free_space)
{
    return __util_ring_do_enqueue(r, obj_table, n, UTIL_RING_QUEUE_FIXED,
                                 __IS_MP, free_space);
}

/**
 * Enqueue several objects on a ring (NOT multi-producers safe).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects).
 * @param n
 *   The number of objects to add in the ring from the obj_table.
 * @param free_space
 *   if non-NULL, returns the amount of space in the ring after the
 *   enqueue operation has finished.
 * @return
 *   The number of objects enqueued, either 0 or n
 */
static __util_always_inline unsigned int
util_ring_sp_enqueue_bulk(struct util_ring *r, void * const *obj_table,
                         unsigned int n, unsigned int *free_space)
{
    return __util_ring_do_enqueue(r, obj_table, n, UTIL_RING_QUEUE_FIXED,
                                 __IS_SP, free_space);
}

/**
 * Enqueue several objects on a ring.
 *
 * This function calls the multi-producer or the single-producer
 * version depending on the default behavior that was specified at
 * ring creation time (see flags).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects).
 * @param n
 *   The number of objects to add in the ring from the obj_table.
 * @param free_space
 *   if non-NULL, returns the amount of space in the ring after the
 *   enqueue operation has finished.
 * @return
 *   The number of objects enqueued, either 0 or n
 */
static __util_always_inline unsigned int
util_ring_enqueue_bulk(struct util_ring *r, void * const *obj_table,
                      unsigned int n, unsigned int *free_space)
{
    return __util_ring_do_enqueue(r, obj_table, n, UTIL_RING_QUEUE_FIXED,
                                 r->producer.single, free_space);
}

/**
 * Enqueue one object on a ring (multi-producers safe).
 *
 * This function uses a "compare and set" instruction to move the
 * producer index atomically.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj
 *   A pointer to the object to be added.
 * @return
 *   - 0: Success; objects enqueued.
 *   - -ENOBUFS: Not enough room in the ring to enqueue; no object is enqueued.
 */
static __util_always_inline int
util_ring_mp_enqueue(struct util_ring *r, void *obj)
{
    return util_ring_mp_enqueue_bulk(r, &obj, 1, NULL) ? 0 : -ENOBUFS;
}

/**
 * Enqueue one object on a ring (NOT multi-producers safe).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj
 *   A pointer to the object to be added.
 * @return
 *   - 0: Success; objects enqueued.
 *   - -ENOBUFS: Not enough room in the ring to enqueue; no object is enqueued.
 */
static __util_always_inline int
util_ring_sp_enqueue(struct util_ring *r, void *obj)
{
    return util_ring_sp_enqueue_bulk(r, &obj, 1, NULL) ? 0 : -ENOBUFS;
}

/**
 * Enqueue one object on a ring.
 *
 * This function calls the multi-producer or the single-producer
 * version, depending on the default behaviour that was specified at
 * ring creation time (see flags).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj
 *   A pointer to the object to be added.
 * @return
 *   - 0: Success; objects enqueued.
 *   - -ENOBUFS: Not enough room in the ring to enqueue; no object is enqueued.
 */
static __util_always_inline int
util_ring_enqueue(struct util_ring *r, void *obj)
{
    return util_ring_enqueue_bulk(r, &obj, 1, NULL) ? 0 : -ENOBUFS;
}

/**
 * Dequeue several objects from a ring (multi-consumers safe).
 *
 * This function uses a "compare and set" instruction to move the
 * consumer index atomically.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects) that will be filled.
 * @param n
 *   The number of objects to dequeue from the ring to the obj_table.
 * @param available
 *   If non-NULL, returns the number of remaining ring entries after the
 *   dequeue has finished.
 * @return
 *   The number of objects dequeued, either 0 or n
 */
static __util_always_inline unsigned int
util_ring_mc_dequeue_bulk(struct util_ring *r, void **obj_table,
                         unsigned int n, unsigned int *available)
{
    return __util_ring_do_dequeue(r, obj_table, n, UTIL_RING_QUEUE_FIXED,
                                 __IS_MC, available);
}

/**
 * Dequeue several objects from a ring (NOT multi-consumers safe).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects) that will be filled.
 * @param n
 *   The number of objects to dequeue from the ring to the obj_table,
 *   must be strictly positive.
 * @param available
 *   If non-NULL, returns the number of remaining ring entries after the
 *   dequeue has finished.
 * @return
 *   The number of objects dequeued, either 0 or n
 */
static __util_always_inline unsigned int
util_ring_sc_dequeue_bulk(struct util_ring *r, void **obj_table,
                         unsigned int n, unsigned int *available)
{
    return __util_ring_do_dequeue(r, obj_table, n, UTIL_RING_QUEUE_FIXED,
                                 __IS_SC, available);
}

/**
 * Dequeue several objects from a ring.
 *
 * This function calls the multi-consumers or the single-consumer
 * version, depending on the default behaviour that was specified at
 * ring creation time (see flags).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects) that will be filled.
 * @param n
 *   The number of objects to dequeue from the ring to the obj_table.
 * @param available
 *   If non-NULL, returns the number of remaining ring entries after the
 *   dequeue has finished.
 * @return
 *   The number of objects dequeued, either 0 or n
 */
static __util_always_inline unsigned int
util_ring_dequeue_bulk(struct util_ring *r, void **obj_table, unsigned int n,
                      unsigned int *available)
{
    return __util_ring_do_dequeue(r, obj_table, n, UTIL_RING_QUEUE_FIXED,
                                 r->consumer.single, available);
}

/**
 * Dequeue one object from a ring (multi-consumers safe).
 *
 * This function uses a "compare and set" instruction to move the
 * consumer index atomically.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_p
 *   A pointer to a void * pointer (object) that will be filled.
 * @return
 *   - 0: Success; objects dequeued.
 *   - -ENOENT: Not enough entries in the ring to dequeue; no object is
 *     dequeued.
 */
static __util_always_inline int
util_ring_mc_dequeue(struct util_ring *r, void **obj_p)
{
    return util_ring_mc_dequeue_bulk(r, obj_p, 1, NULL)  ? 0 : -ENOENT;
}

/**
 * Dequeue one object from a ring (NOT multi-consumers safe).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_p
 *   A pointer to a void * pointer (object) that will be filled.
 * @return
 *   - 0: Success; objects dequeued.
 *   - -ENOENT: Not enough entries in the ring to dequeue, no object is
 *     dequeued.
 */
static __util_always_inline int
util_ring_sc_dequeue(struct util_ring *r, void **obj_p)
{
    return util_ring_sc_dequeue_bulk(r, obj_p, 1, NULL) ? 0 : -ENOENT;
}

/**
 * Dequeue one object from a ring.
 *
 * This function calls the multi-consumers or the single-consumer
 * version depending on the default behaviour that was specified at
 * ring creation time (see flags).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_p
 *   A pointer to a void * pointer (object) that will be filled.
 * @return
 *   - 0: Success, objects dequeued.
 *   - -ENOENT: Not enough entries in the ring to dequeue, no object is
 *     dequeued.
 */
static __util_always_inline int
util_ring_dequeue(struct util_ring *r, void **obj_p)
{
    return util_ring_dequeue_bulk(r, obj_p, 1, NULL) ? 0 : -ENOENT;
}

/**
 * Return the number of entries in a ring.
 *
 * @param r
 *   A pointer to the ring structure.
 * @return
 *   The number of entries in the ring.
 */
static inline unsigned
util_ring_count(const struct util_ring *r)
{
    uint32_t prod_tail = r->producer.tail;
    uint32_t cons_tail = r->consumer.tail;
    uint32_t count = (prod_tail - cons_tail) & r->mask;
    return (count > r->capacity) ? r->capacity : count;
}

/**
 * Return the number of free entries in a ring.
 *
 * @param r
 *   A pointer to the ring structure.
 * @return
 *   The number of free entries in the ring.
 */
static inline unsigned
util_ring_free_count(const struct util_ring *r)
{
    return r->capacity - util_ring_count(r);
}

/**
 * Test if a ring is full.
 *
 * @param r
 *   A pointer to the ring structure.
 * @return
 *   - 1: The ring is full.
 *   - 0: The ring is not full.
 */
static inline int
util_ring_full(const struct util_ring *r)
{
    return util_ring_free_count(r) == 0;
}

/**
 * Test if a ring is empty.
 *
 * @param r
 *   A pointer to the ring structure.
 * @return
 *   - 1: The ring is empty.
 *   - 0: The ring is not empty.
 */
static inline int
util_ring_empty(const struct util_ring *r)
{
    return util_ring_count(r) == 0;
}

/**
 * Return the size of the ring.
 *
 * @param r
 *   A pointer to the ring structure.
 * @return
 *   The size of the data store used by the ring.
 *   NOTE: this is not the same as the usable space in the ring. To query that
 *   use ``util_ring_get_capacity()``.
 */
static inline unsigned int
util_ring_get_size(const struct util_ring *r)
{
    return r->size;
}

/**
 * Return the number of elements which can be stored in the ring.
 *
 * @param r
 *   A pointer to the ring structure.
 * @return
 *   The usable size of the ring.
 */
static inline unsigned int
util_ring_get_capacity(const struct util_ring *r)
{
    return r->capacity;
}

/**
 * Dump the status of all rings on the console
 *
 * @param f
 *   A pointer to a file for output
 */
void util_ring_list_dump(FILE *f);

/**
 * Search a ring from its name
 *
 * @param name
 *   The name of the ring.
 * @return
 *   The pointer to the ring matching the name, or NULL if not found,
 *   with util_errno set appropriately. Possible util_errno values include:
 *    - ENOENT - required entry not available to return.
 */
struct util_ring *util_ring_lookup(const char *name);

/**
 * Enqueue several objects on the ring (multi-producers safe).
 *
 * This function uses a "compare and set" instruction to move the
 * producer index atomically.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects).
 * @param n
 *   The number of objects to add in the ring from the obj_table.
 * @param free_space
 *   if non-NULL, returns the amount of space in the ring after the
 *   enqueue operation has finished.
 * @return
 *   - n: Actual number of objects enqueued.
 */
static __util_always_inline unsigned
util_ring_mp_enqueue_burst(struct util_ring *r, void * const *obj_table,
                          unsigned int n, unsigned int *free_space)
{
    return __util_ring_do_enqueue(r, obj_table, n,
                                 UTIL_RING_QUEUE_VARIABLE, __IS_MP, free_space);
}

/**
 * Enqueue several objects on a ring (NOT multi-producers safe).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects).
 * @param n
 *   The number of objects to add in the ring from the obj_table.
 * @param free_space
 *   if non-NULL, returns the amount of space in the ring after the
 *   enqueue operation has finished.
 * @return
 *   - n: Actual number of objects enqueued.
 */
static __util_always_inline unsigned
util_ring_sp_enqueue_burst(struct util_ring *r, void * const *obj_table,
                          unsigned int n, unsigned int *free_space)
{
    return __util_ring_do_enqueue(r, obj_table, n,
                                 UTIL_RING_QUEUE_VARIABLE, __IS_SP, free_space);
}

/**
 * Enqueue several objects on a ring.
 *
 * This function calls the multi-producer or the single-producer
 * version depending on the default behavior that was specified at
 * ring creation time (see flags).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects).
 * @param n
 *   The number of objects to add in the ring from the obj_table.
 * @param free_space
 *   if non-NULL, returns the amount of space in the ring after the
 *   enqueue operation has finished.
 * @return
 *   - n: Actual number of objects enqueued.
 */
static __util_always_inline unsigned
util_ring_enqueue_burst(struct util_ring *r, void * const *obj_table,
                       unsigned int n, unsigned int *free_space)
{
    return __util_ring_do_enqueue(r, obj_table, n, UTIL_RING_QUEUE_VARIABLE,
                                 r->producer.single, free_space);
}

/**
 * Dequeue several objects from a ring (multi-consumers safe). When the request
 * objects are more than the available objects, only dequeue the actual number
 * of objects
 *
 * This function uses a "compare and set" instruction to move the
 * consumer index atomically.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects) that will be filled.
 * @param n
 *   The number of objects to dequeue from the ring to the obj_table.
 * @param available
 *   If non-NULL, returns the number of remaining ring entries after the
 *   dequeue has finished.
 * @return
 *   - n: Actual number of objects dequeued, 0 if ring is empty
 */
static __util_always_inline unsigned
util_ring_mc_dequeue_burst(struct util_ring *r, void **obj_table,
                          unsigned int n, unsigned int *available)
{
    return __util_ring_do_dequeue(r, obj_table, n,
                                 UTIL_RING_QUEUE_VARIABLE, __IS_MC, available);
}

/**
 * Dequeue several objects from a ring (NOT multi-consumers safe).When the
 * request objects are more than the available objects, only dequeue the
 * actual number of objects
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects) that will be filled.
 * @param n
 *   The number of objects to dequeue from the ring to the obj_table.
 * @param available
 *   If non-NULL, returns the number of remaining ring entries after the
 *   dequeue has finished.
 * @return
 *   - n: Actual number of objects dequeued, 0 if ring is empty
 */
static __util_always_inline unsigned
util_ring_sc_dequeue_burst(struct util_ring *r, void **obj_table,
                          unsigned int n, unsigned int *available)
{
    return __util_ring_do_dequeue(r, obj_table, n,
                                 UTIL_RING_QUEUE_VARIABLE, __IS_SC, available);
}

/**
 * Dequeue multiple objects from a ring up to a maximum number.
 *
 * This function calls the multi-consumers or the single-consumer
 * version, depending on the default behaviour that was specified at
 * ring creation time (see flags).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects) that will be filled.
 * @param n
 *   The number of objects to dequeue from the ring to the obj_table.
 * @param available
 *   If non-NULL, returns the number of remaining ring entries after the
 *   dequeue has finished.
 * @return
 *   - Number of objects dequeued
 */
static __util_always_inline unsigned
util_ring_dequeue_burst(struct util_ring *r, void **obj_table,
                       unsigned int n, unsigned int *available)
{
    return __util_ring_do_dequeue(r, obj_table, n,
                                 UTIL_RING_QUEUE_VARIABLE,
                                 r->consumer.single, available);
}


#ifdef __cplusplus
}
#endif


#endif //FSUTILS_BUF_RING_H
