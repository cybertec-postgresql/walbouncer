#ifndef	_WB_CLIENTCONN_H
#define _WB_CLIENTCONN_H 1

#include "xfsocket.h"

void WbCCInitConnection(XfConn conn);
void WbCCPerformAuthentication(XfConn conn);
void WbCCCommandLoop(XfConn conn);

#endif
