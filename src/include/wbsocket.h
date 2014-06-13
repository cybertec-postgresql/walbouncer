#ifndef	_WB_SOCKET_H
#define _WB_SOCKET_H 1

#include "wbglobals.h"
#include "wbproto.h"

typedef struct {
	int fd;
} XfSocketStruct;
typedef XfSocketStruct* XfSocket;

typedef struct {
	XLogRecPtr writePtr;
	XLogRecPtr flushPtr;
	XLogRecPtr applyPtr;
	TimestampTz sendTime;
	bool		replyRequested;
} StandbyReplyMessage;

typedef struct {
	int fd;
	char *recvBuffer;
	int recvPointer;
	int recvLength;

	// Sending buffer management
	char *sendBuffer;
	int sendBufSize;
	int sendBufLen;
	int sendBufMsgLenPtr;
	int sendBufFlushPtr;

	ProtocolVersion proto;

	char *master_host;
	char *master_port;

	char *database_name;
	char *user_name;
	char *cmdline_options;
	char *guc_options;
	int gucs_len;

	// Filtering info
	char *include_tablespaces;

	// Sending state
	XLogRecPtr sentPtr;
	TimestampTz lastSend;
	bool copyDoneSent;
	bool copyDoneReceived;

	// Receive state
	StandbyReplyMessage lastReply;
	bool	replyForwarded;
} XfPortStruct;
typedef XfPortStruct* XfConn;

typedef struct {
	int32 len;
	char data[1];
} XfMessage;

typedef enum {
	FLUSH_IMMEDIATE,
	FLUSH_ASYNC
} ConnFlushMode;

XfSocket
OpenServerSocket(char *port);

XfConn
ConnCreate(XfSocket server);

bool
ConnHasDataToFlush(XfConn conn);

int
ConnFlush(XfConn conn, ConnFlushMode mode);

void
CloseConn(XfConn);

void
CloseSocket(XfSocket sock);

int
ConnGetSocket(XfConn conn);

void
ConnBeginMessage(XfConn conn, char type);

void
ConnSendInt(XfConn conn, int i, int b);

void
ConnSendInt64(XfConn conn, int64 i);

void
ConnSendString(XfConn conn, const char *str);

void
ConnSendBytes(XfConn conn, const char *str, int n);

void
ConnEndMessage(XfConn conn);

int
ConnGetByte(XfConn conn);

int
ConnGetBytes(XfConn conn, char *s, size_t len);

int
ConnGetByteIfAvailable(XfConn conn, char *c);

int
ConnGetMessage(XfConn conn, XfMessage **msg);

void
ConnFreeMessage(XfMessage *msg);


void
hexdump(char *buf, int amount);

#endif
