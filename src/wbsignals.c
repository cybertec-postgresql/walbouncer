#include <signal.h>

#include "wbsignals.h"

sig_atomic_t stopRequested = false;

static void RequestStopHandler(int signum);

static void
RequestStopHandler(int signum)
{
	stopRequested = true;
}

void WbInitializeSignals()
{
	signal(SIGINT, RequestStopHandler);
}
