#ifndef	_WB_CLIENTCONN_H
#define _WB_CLIENTCONN_H 1

#include "wbsocket.h"

void WbCCInitConnection(XfConn conn);
void WbCCPerformAuthentication(XfConn conn);
void WbCCCommandLoop(XfConn conn);

#endif
