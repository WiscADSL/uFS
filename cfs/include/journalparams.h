#ifndef CFS_INCLUDE_JOURNAL_PARAMS__H
#define CFS_INCLUDE_JOURNAL_PARAMS__H

#include "cfs_feature_macros.h"
#include "param.h"

#if CFS_JOURNAL(GLOBAL_JOURNAL)
#define PER_WORKER_JOURNAL_SIZE GLOBAL_JOURNAL_SIZE
#else
#define PER_WORKER_JOURNAL_SIZE LOCAL_JOURNAL_SIZE
#endif

#define PER_WORKER_JOURNAL_NBLOCKS (PER_WORKER_JOURNAL_SIZE / BSIZE)
// Every worker will allocate some amount of pinned memory for writing journal
// entries.
#define MAX_JOURNAL_ENTRY_SIZE (128UL * KB)
// Maximum number of concurrent journal transactions per worker so we can
// preallocate the memory from spdk.
#define MAX_INFLIGHT_JOURNAL_TRANSACTIONS (100)
// Maximum number of bytes that can differ between journal superblock and
// entries in the journal. The OfflineCheckpointer will read these many
// additional bytes from the journal to see if there are entries it did not
// capture.
#define MAX_UNACCOUNTED_JOURNAL_BYTES (200UL * MB)

#endif  // CFS_INCLUDE_JOURNAL_PARAMS__H
