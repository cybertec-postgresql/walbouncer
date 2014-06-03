#ifndef	_WB_CLIENTCONN_H
#define _WB_CLIENTCONN_H 1

#include "xfsocket.h"

void XfInitConnection(XfConn conn);
void XfPerformAuthentication(XfConn conn);
void XfCommandLoop(XfConn conn);

#endif
