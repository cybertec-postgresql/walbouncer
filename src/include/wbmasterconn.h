#ifndef	_WB_MASTERCONN_H
#define _WB_MASTERCONN_H 1

#include "wbglobals.h"
#include "wbsocket.h"

typedef enum {
	MSG_NOTHING,
	MSG_END_OF_WAL,
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

typedef struct {
	char *filename;
	size_t contentLen;
	char *content;
} TimelineHistory;

typedef enum {
	OID_RESOLVE_TABLESPACES,
	OID_RESOLVE_DATABASES
} OidResolveKind;

typedef struct MasterConn MasterConn;

MasterConn* WbMcOpenConnection(const char *conninfo);
void WbMcCloseConnection(MasterConn *master);
int WbMcGetSocket(MasterConn *master);
bool WbMcStartStreaming(MasterConn *master, XLogRecPtr pos, TimeLineID tli);
void WbMcEndStreaming(MasterConn *master, TimeLineID *nextTli, char** nextTliStart);
bool WbMcReceiveWalMessage(MasterConn *master, ReplMessage *msg);
void WbMcSendReply(MasterConn *master, StandbyReplyMessage *reply, bool force, bool requestReply);
void WbMcSendFeedback(MasterConn *master, HSFeedbackMessage *feedback);
bool WbMcIdentifySystem(MasterConn* master,
		char** primary_sysid, char** primary_tli, char** primary_xpos);
bool WbMcGetTimelineHistory(MasterConn* master, TimeLineID timeline,
		TimelineHistory *history);
char *WbMcShowVariable(MasterConn* master, char *varname);
Oid * WbMcResolveOids(MasterConn *master, OidResolveKind kind, bool include, char** names, int n_items);
const char *WbMcParameterStatus(MasterConn *master, char *name);
#endif
