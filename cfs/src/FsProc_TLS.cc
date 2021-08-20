#include "FsProc_TLS.h"

// initialization of thread local storage variables
thread_local int FsProcTLS::wid_ = -1;
