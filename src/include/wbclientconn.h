#ifndef	_WB_CLIENTCONN_H
#define _WB_CLIENTCONN_H 1

#include "wbsocket.h"

void WbCCInitConnection(WbConn conn);
void WbCCPerformAuthentication(WbConn conn);
void WbCCCommandLoop(WbConn conn);

#endif
