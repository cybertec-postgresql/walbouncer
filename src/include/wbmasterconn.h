#ifndef	_WB_MASTERCONN_H
#define _WB_MASTERCONN_H 1

#include "xfglobals.h"

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

typedef struct MasterConn MasterConn;

MasterConn* xlf_open_connection(const char *conninfo);
bool xlf_startstreaming(MasterConn *master, XLogRecPtr pos, TimeLineID tli);
void xlf_endstreaming(MasterConn *master, TimeLineID *next_tli);
void xlf_process_message(MasterConn *master, char *buf, size_t len, ReplMessage *msg);
bool xlf_identify_system(MasterConn* master,
		char** primary_sysid, char** primary_tli, char** primary_xpos);
Oid * xlf_find_tablespace_oids(const char *conninfo, const char* tablespace_names);
const char *xlf_parameter_status(MasterConn *master, char *name);
#endif
