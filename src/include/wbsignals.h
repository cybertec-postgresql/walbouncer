#ifndef	_WB_SIGNALS_H
#define _WB_SIGNALS_H 1

#include "wbglobals.h"
#include <signal.h>

extern sig_atomic_t stopRequested;
void WbInitializeSignals();

#endif
