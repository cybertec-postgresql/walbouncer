#ifndef	_WB_SOCKET_H
#define _WB_SOCKET_H 1

#include <sys/socket.h>

#include "wbglobals.h"
#include "wbproto.h"
#include "wbconfig.h"

typedef struct {
	int fd;
} WbSocketStruct;
typedef WbSocketStruct* WbSocket;

typedef struct {
	XLogRecPtr writePtr;
	XLogRecPtr flushPtr;
	XLogRecPtr applyPtr;
	TimestampTz sendTime;
	bool		replyRequested;
} StandbyReplyMessage;

typedef struct {
	TimestampTz sendTime;
	TransactionId xmin;
	uint32		xmin_epoch;
	uint32		catalog_xmin;
	uint32		catalog_xmin_epoch;
} HSFeedbackMessage;

typedef struct {
	int fd;
	char *recvBuffer;
	int recvPointer;
	int recvLength;

	struct {
		uint32 addr;
		uint16 port;
	} client;

	// Matched configuration entry
	wb_config_entry *configEntry;

	// Sending buffer management
	char *sendBuffer;
	int sendBufSize;
	int sendBufLen;
	int sendBufMsgLenPtr;
	int sendBufFlushPtr;

	ProtocolVersion proto;

	char *master_host;
	int master_port;

	char *database_name;
	char *user_name;
	char *application_name;
	char *cmdline_options;
	char *guc_options;
	int gucs_len;

	// Sending state
	XLogRecPtr sentPtr;
	TimestampTz lastSend;
	bool copyDoneSent;
	bool copyDoneReceived;

	// Receive state
	StandbyReplyMessage lastReply;
	bool	replyForwarded;
	HSFeedbackMessage lastFeedback;
	bool	feedbackForwarded;
} WbPortStruct;
typedef WbPortStruct* WbConn;

typedef struct {
	int32 len;
	char data[1];
} WbMessage;

typedef enum {
	FLUSH_IMMEDIATE,
	FLUSH_ASYNC
} ConnFlushMode;

WbSocket
OpenServerSocket(int port);

WbConn
ConnCreate(WbSocket server);

bool
ConnHasDataToFlush(WbConn conn);

int
ConnFlush(WbConn conn, ConnFlushMode mode);

void
CloseConn(WbConn);

void
CloseSocket(WbSocket sock);

int
ConnGetSocket(WbConn conn);

void
ConnBeginMessage(WbConn conn, char type);

void
ConnSendInt(WbConn conn, int i, int b);

void
ConnSendInt64(WbConn conn, int64 i);

void
ConnSendString(WbConn conn, const char *str);

void
ConnSendBytes(WbConn conn, const char *str, int n);

void
ConnEndMessage(WbConn conn);

int
ConnGetByte(WbConn conn);

int
ConnGetBytes(WbConn conn, char *s, size_t len);

int
ConnGetByteIfAvailable(WbConn conn, char *c);

int
ConnGetMessage(WbConn conn, WbMessage **msg);

void
ConnFreeMessage(WbMessage *msg);


void
hexdump(char *buf, int amount);

void InitDeathWatchHandle();
void CloseDeathwatchPort();
bool DaemonIsAlive();

#endif
