
#ifndef FSUTILS_COMMON_H
#define FSUTILS_COMMON_H

#include <stdint.h>
#include "util_param.h"

#define __util_always_inline inline __attribute__((always_inline))
#define __util_aligned(a) __attribute__((__aligned__(a)))
#define __util_cache_aligned __util_aligned(UTIL_CACHE_LINE_SIZE)
/*
 * definitation to mark a variable or function parameter as used so
 * as to avoid a compiler warning
 */
#define UTIL_SET_USED(x) (void)(x)

#define likely(x)       __builtin_expect(!!(x), 1)
#define unlikely(x)     __builtin_expect(!!(x), 0)

/*
 * $ cat /proc/cpuinfo | grep sse2 # to see if support this feature
 * Provide a hint to the processor that the code sequence is a spin-wait loop.
 * This can help improve the performance and power consumption of spin-wait loops.
 */
#include <emmintrin.h>
static inline void util_pause(void)
{
    _mm_pause();
}


/* ------------------------------------------------------------------------------------------------------------ */
/*
 * Macros for doing pointer arithmetic
 */

#define UTIL_PTR_ADD(ptr, x) ((void*)((uintptr_t)(ptr) + (x)))

/* ------------------------------------------------------------------------------------------------------------ */
/*
 * Macros for doing alignment
 */


/**
 * Macro to align a value to a given power-of-two. The resultant value
 * will be of the same type as the first parameter, and will be no
 * bigger than the first parameter. Second parameter must be a
 * power-of-two value.
 */
#define UTIL_ALIGN_FLOOR(val, align) \
    (__typeof__(val))((val) & (~((__typeof__(val))((align) - 1))))


/**
* Macro to align a value to a given power-of-two. The resultant value
* will be of the same type as the first parameter, and will be no lower
* than the first parameter. Second parameter must be a power-of-two
* value.
*/
#define UTIL_ALIGN_CEIL(val, align) \
    UTIL_ALIGN_FLOOR(((val) + ((__typeof__(val)) (align) - 1)), align)


/**
 * Macro to align a value to a given power-of-two. The resultant
 * value will be of the same type as the first parameter, and
 * will be no lower than the first parameter. Second parameter
 * must be a power-of-two value.
 * This function is the same as UTIL_ALIGN_CEIL
 */
#define UTIL_ALIGN(val, align) UTIL_ALIGN_CEIL(val, align)


/* ------------------------------------------------------------------------------------------------------------ */

/*
 * Data structures for buf_ring
 */

enum util_ring_queue_behavior {
    UTIL_RING_QUEUE_FIXED = 0,    // Enq/Deq a fixed number of items from a ring
    UTIL_RING_QUEUE_VARIABLE,     // Enq/Deq as many items as possible from ring
};

struct util_ring_headtail {
    volatile uint32_t head;
    volatile uint32_t tail;
    uint32_t single;            // true if single prod/cons
};


#define UTIL_MEMZONE_NAMESIZE 32
struct util_ring_memzone {
    char name[UTIL_MEMZONE_NAMESIZE];
    char *addr;
    uint32_t flags;
}__attribute__((__packed__));


/*
 * NOTE: I do not fully understand, but, copy the note from util_ring.h
 * ---------
 * The producer and the consumer have a head and a tail index. The particularity
 * of these index is that they are not between 0 and size(ring). These indexes
 * are between 0 and 2^32, and we mask their value when we access the ring[]
 * field. Thanks to this assumption, we can do subtractions between 2 index
 * values in a modulo-32bit base: that's why the overflow of the indexes is not
 * a problem.
 */

#define UTIL_RING_NAME_SIZE 32

struct util_ring {
    char name[UTIL_RING_NAME_SIZE] __util_cache_aligned;
    int flags;
    struct util_ring_memzone *memzone;  // memzone, if any containing the rte_ring
    uint32_t size;                      // size of ring
    uint32_t mask;                      // mask of the ring
    uint32_t capacity;                  // usable capacity of the ring
    char pad0 __util_cache_aligned;     // empty cache line

    // producer status
    struct util_ring_headtail producer __util_cache_aligned;
    char pad1 __util_cache_aligned;
    // consumer status
    struct util_ring_headtail consumer __util_cache_aligned;
    char pad2 __util_cache_aligned;

};

/* ------------------------------------------------------------------------------------------------------------ */
/*
 * Macros to help aligh to power
 */


/**
 * Combines 32b inputs most significant set bits into the least
 * significant bits to construct a value with the same MSBs as x
 * but all 1's under it.
 *
 * @param x
 *    The integer whose MSBs need to be combined with its LSBs
 * @return
 *    The combined value.
 */
static inline uint32_t
util_combine32ms1b(uint32_t x)
{
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;

    return x;
}

/**
 * Aligns input parameter to the next power of 2
 *
 * @param x
 *   The integer value to algin
 *
 * @return
 *   Input parameter aligned to the next power of 2
 */
static inline uint32_t
util_align32pow2(uint32_t x)
{
    x--;
    x = util_combine32ms1b(x);

    return x + 1;
}



/* ------------------------------------------------------------------------------------------------------------ */


#endif //FSUTILS_COMMON_H
