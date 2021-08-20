#ifndef CFS_INCLUDE_FSPROC_JOURNAL_H_
#define CFS_INCLUDE_FSPROC_JOURNAL_H_

#include "cfs_feature_macros.h"

#if CFS_JOURNAL(ON)
#include "FsProc__JournalEnabled.h"
#else
#include "FsProc__JournalDisabled.h"
#endif

#endif  // CFS_INCLUDE_FSPROC_JOURNAL_H_
