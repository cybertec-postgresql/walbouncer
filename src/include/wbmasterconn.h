#ifndef	_WB_MASTERCONN_H
#define _WB_MASTERCONN_H 1

#include "xfglobals.h"
#include "libpq-fe.h"

typedef enum {
	MSG_WAL_DATA,
	MSG_KEEPALIVE
} WalMsgType;

typedef struct {
	WalMsgType type;
	XLogRecPtr walEnd;
	TimestampTz sendTime;
	bool replyRequested;
	XLogRecPtr dataStart;

	int dataPtr;
	int dataLen;
	int nextPageBoundary;

	char *data;

} ReplMessage;

PGconn* xlf_open_connection(const char *conninfo);
bool xlf_startstreaming(PGconn *mc, XLogRecPtr pos, TimeLineID tli);
void xlf_endstreaming(PGconn *mc, TimeLineID *next_tli);
void xlf_process_message(PGconn *mc, char *buf, size_t len, ReplMessage *msg);
bool xlf_identify_system(PGconn* mc,
		char** primary_sysid, char** primary_tli, char** primary_xpos);
Oid * xlf_find_tablespace_oids(const char *conninfo, const char* tablespace_names);

#endif
