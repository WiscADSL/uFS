#ifndef _CFS_DEFS_
#define _CFS_DEFS_

// NOTE: this is used for common definition that is
// used for both client and server side
// especially, this could be included into fsapi.h
// And we avoid include this fsapi.h to be included into the server

#define FS_REASSIGN_ALL 0
#define FS_REASSIGN_PAST 1
#define FS_REASSIGN_FUTURE 2

#endif