
#include <sys/queue.h>
#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include "util/util_buf_ring.h"

/* true if x is a power of 2 */
#define POWEROF2(x) ((((x)-1) & (x)) == 0)

/* return the size of memory occupied by a ring */
ssize_t
util_ring_get_memsize(unsigned count)
{
    ssize_t sz;

    /* count must be a power of 2 */
    if ((!POWEROF2(count)) || (count > UTIL_RING_SZ_MASK )) {
        fprintf(stderr,
                "Requested size is invalid, must be power of 2, and "
                "do not exceed the size limit %u\n", UTIL_RING_SZ_MASK);
        return -EINVAL;
    }

    sz = sizeof(struct util_ring) + count * sizeof(void *);
    sz = UTIL_ALIGN(sz, UTIL_CACHE_LINE_SIZE);
    return sz;
}

int
util_ring_init(struct util_ring *r, const char *name, unsigned count,
              unsigned flags)
{
    int ret;

    /* init the ring structure */
    memset(r, 0, sizeof(*r));
    ret = snprintf(r->name, sizeof(r->name), "%s", name);
    if (ret < 0 || ret >= (int)sizeof(r->name))
        return -ENAMETOOLONG;
    r->flags = flags;
    r->producer.single = (flags & RING_F_SP_ENQ) ? __IS_SP : __IS_MP;
    r->consumer.single = (flags & RING_F_SC_DEQ) ? __IS_SC : __IS_MC;

    if (flags & RING_F_EXACT_SZ) {
        r->size = util_align32pow2(count + 1);
        r->mask = r->size - 1;
        r->capacity = count;
    } else {
        if ((!POWEROF2(count)) || (count > UTIL_RING_SZ_MASK)) {
            fprintf(stderr,
                    "Requested size is invalid, must be power of 2, and not exceed the size limit %u\n",
                    UTIL_RING_SZ_MASK);
            return -EINVAL;
        }
        r->size = count;
        r->mask = count - 1;
        r->capacity = r->mask;
    }
    r->producer.head = r->consumer.head = 0;
    r->producer.tail = r->consumer.tail = 0;

    return 0;
}


/* dump the status of the ring on the console */
void
util_ring_dump(FILE *f, const struct util_ring *r)
{
    fprintf(f, "ring <%s>@%p\n", r->name, r);
    fprintf(f, "  flags=%x\n", r->flags);
    fprintf(f, "  size=%"PRIu32"\n", r->size);
    fprintf(f, "  capacity=%"PRIu32"\n", r->capacity);
    fprintf(f, "  ct=%"PRIu32"\n", r->consumer.tail);
    fprintf(f, "  ch=%"PRIu32"\n", r->consumer.head);
    fprintf(f, "  pt=%"PRIu32"\n", r->producer.tail);
    fprintf(f, "  ph=%"PRIu32"\n", r->producer.head);
    fprintf(f, "  used=%u\n", util_ring_count(r));
    fprintf(f, "  avail=%u\n", util_ring_free_count(r));
}


