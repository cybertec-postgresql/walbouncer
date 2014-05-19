#include<stdlib.h>
#include<stdio.h>
#include<errno.h>
#include<string.h>

#include<poll.h>

#include <getopt.h>


#include "libpq-fe.h"

#include "xfglobals.h"
#include "xfutils.h"
#include "xfsocket.h"
#include "xfproto.h"
#include "parser/parser.h"
#include "xfpgtypes.h"

#define MAXCONNINFO 1024
#define NAPTIME 100

#define WAL_BLOCK_SIZE 8192
#define WAL_BLOCK_MASK (WAL_BLOCK_SIZE-1)



/*typedef struct Port {

} Port;*/
/*
int
ProcessStartupPacket(Port port, bool SSLdone)
{
	return 1;
}*/

int
ensure_atoi(char *s)
{
	char *endptr;
	int result;
	result = strtol(s, &endptr, 0);

	//FIXME: check for error here

	return result;
}

xlogfilter_identify_system(PGconn *mc)
{
	PGresult *result = NULL;
	char *primary_sysid;
	TimeLineID primary_tli;

	result = PQexec(mc, "IDENTIFY_SYSTEM");
	if (PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		PQclear(result);
		error(PQerrorMessage(mc));
	}
	if (PQnfields(result) < 3 || PQntuples(result) != 1)
	{
		error("Invalid response");
	}

	primary_sysid = strdup(PQgetvalue(result, 0, 0));
	primary_tli = ensure_atoi(PQgetvalue(result, 0, 1));

	xf_info("System identifier: %s\n", primary_sysid);
	xf_info("TLI: %d\n", primary_tli);

	PQclear(result);
}

bool xlf_startstreaming(PGconn *mc, XLogRecPtr pos, TimeLineID tli)
{
	char cmd[256];
	PGresult *res;

	xf_info("Start streaming from master at %X/%X", FormatRecPtr(pos));

	snprintf(cmd, sizeof(cmd),
			"START_REPLICATION %X/%X TIMELINE %u",
			(uint32) (pos>>32), (uint32) pos, tli);
	res = PQexec(mc, cmd);

	if (PQresultStatus(res) == PGRES_COMMAND_OK)
	{
		PQclear(res);
		return false;
	}
	else if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		PQclear(res);
		error(PQerrorMessage(mc));
	}
	PQclear(res);
	return true;
}

void
xlf_endstreaming(PGconn *mc, TimeLineID *next_tli)
{
	PGresult   *res;
	int i = 0;

	if (PQputCopyEnd(mc, NULL) <= 0 || PQflush(mc))
		error(PQerrorMessage(mc));

	/*
	 * After COPY is finished, we should receive a result set indicating the
	 * next timeline's ID, or just CommandComplete if the server was shut
	 * down.
	 *
	 * If we had not yet received CopyDone from the backend, PGRES_COPY_IN
	 * would also be possible. However, at the moment this function is only
	 * called after receiving CopyDone from the backend - the walreceiver
	 * never terminates replication on its own initiative.
	 */
	res = PQgetResult(mc);
	while (PQresultStatus(res) == PGRES_COPY_OUT)
	{
		int copyresult;
		do {
			char *buf;
			copyresult = PQgetCopyData(mc, &buf, 0);
			if (buf)
				PQfreemem(buf);

		} while (copyresult >= 0);
		res = PQgetResult(mc);
	}

	if (PQresultStatus(res) == PGRES_TUPLES_OK)
	{
		/*
		 * Read the next timeline's ID. The server also sends the timeline's
		 * starting point, but it is ignored.
		 */
		if (PQnfields(res) < 2 || PQntuples(res) != 1)
			error("unexpected result set after end-of-streaming");
		*next_tli = ensure_atoi(PQgetvalue(res, 0, 0));
		PQclear(res);

		/* the result set should be followed by CommandComplete */
		res = PQgetResult(mc);
	}
	else
		*next_tli = 0;

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		error(PQerrorMessage(mc));

	/* Verify that there are no more results */
	res = PQgetResult(mc);
	while (res!=NULL) {
		xf_info("Status: %d", PQresultStatus(res));
		res = PQgetResult(mc);
	}

	if (res != NULL)
		error("unexpected result after CommandComplete: %s", PQerrorMessage(mc));

}

static char *recvBuf = NULL;

/*
 * Wait until we can read WAL stream, or timeout.
 *
 * Returns true if data has become available for reading, false if timed out
 * or interrupted by signal.
 *
 * This is based on pqSocketCheck.
 */
static bool
libpq_select(PGconn *mc, int timeout_ms)
{
	int			ret;

	Assert(mc != NULL);
	if (PQsocket(mc) < 0)
		error("socket not open");

	/* We use poll(2) if available, otherwise select(2) */
	{
		struct pollfd input_fd;

		input_fd.fd = PQsocket(mc);
		input_fd.events = POLLIN | POLLERR;
		input_fd.revents = 0;

		ret = poll(&input_fd, 1, timeout_ms);
	}

	if (ret == 0 || (ret < 0 && errno == EINTR))
		return false;
	if (ret < 0)
		error("select() failed: %m");
	return true;
}

int
xlf_receive(PGconn *mc, int timeout, char **buffer)
{
	int			rawlen;

	if (recvBuf != NULL)
		PQfreemem(recvBuf);
	recvBuf = NULL;

	/* Try to receive a CopyData message */
	rawlen = PQgetCopyData(mc, &recvBuf, 1);
	if (rawlen == 0)
	{
		/*
		 * No data available yet. If the caller requested to block, wait for
		 * more data to arrive.
		 */
		if (timeout > 0)
		{
			if (!libpq_select(mc, timeout))
				return 0;
		}

		if (PQconsumeInput(mc) == 0)
			showPQerror(mc, "could not receive data from WAL stream");

		/* Now that we've consumed some input, try again */
		rawlen = PQgetCopyData(mc, &recvBuf, 1);
		if (rawlen == 0)
			return 0;
	}
	if (rawlen == -1)			/* end-of-streaming or error */
	{
		PGresult   *res;

		res = PQgetResult(mc);
		if (PQresultStatus(res) == PGRES_COMMAND_OK ||
			PQresultStatus(res) == PGRES_COPY_IN)
		{
			PQclear(res);
			return -1;
		}
		else
		{
			PQclear(res);
			showPQerror(mc, "could not receive data from WAL stream");
		}
	}
	if (rawlen < -1)
		showPQerror(mc, "could not receive data from WAL stream");

	/* Return received messages to caller */
	*buffer = recvBuf;
	return rawlen;
}

uint64
fromnetwork64(char *buf)
{
	// XXX: unaligned reads
	uint32 h = *((uint32*) buf);
	uint32 l = *(((uint32*) buf)+1);
	return ((uint64)ntohl(h) << 32) | ntohl(l);
}

uint32
fromnetwork32(char *buf)
{
	// XXX: unaligned read
	return ntohl(*((uint32*) buf));
}

void
write64(char *buf, uint64 v)
{
	int i;
	for (i = 7; i>= 0; i--)
		*buf++ = (char)(v >> i*8);
}

XLogRecPtr latestWalEnd = 0;
TimestampTz latestSendTime = 0;

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

static void
process_walsender_message(ReplMessage *msg)
{
	latestWalEnd = msg->walEnd;
	latestSendTime = msg->sendTime;
}

/*
 * Send a message to XLOG stream.
 *
 * ereports on error.
 */
static void
xlf_send(PGconn *mc, const char *buffer, int nbytes)
{
	if (PQputCopyData(mc, buffer, nbytes) <= 0 ||
		PQflush(mc))
		showPQerror(mc, "could not send data to WAL stream");
}


xlf_send_reply(PGconn *mc, bool force, bool requestReply)
{
	XLogRecPtr writePtr = latestWalEnd;
	XLogRecPtr flushPtr = latestWalEnd;
	XLogRecPtr	applyPtr = latestWalEnd;
	TimestampTz sendTime = latestSendTime;
	TimestampTz now;
	char reply_message[1+4*8+1+1];

	/*
	 * If the user doesn't want status to be reported to the master, be sure
	 * to exit before doing anything at all.

	if (!force && wal_receiver_status_interval <= 0)
		return;*/

	/* Get current timestamp. */
	now = latestSendTime;//GetCurrentTimestamp();

	/*
	 * We can compare the write and flush positions to the last message we
	 * sent without taking any lock, but the apply position requires a spin
	 * lock, so we don't check that unless something else has changed or 10
	 * seconds have passed.  This means that the apply log position will
	 * appear, from the master's point of view, to lag slightly, but since
	 * this is only for reporting purposes and only on idle systems, that's
	 * probably OK.
	 */
	/*if (!force
		&& writePtr == LogstreamResult.Write
		&& flushPtr == LogstreamResult.Flush
		&& !TimestampDifferenceExceeds(sendTime, now,
									   wal_receiver_status_interval * 1000))
		return;*/
	sendTime = now;

	/* Construct a new message */

	//resetStringInfo(&reply_message);
	memset(reply_message, 0, sizeof(reply_message));
	//pq_sendbyte(&reply_message, 'r');
	reply_message[0] = 'r';
	//pq_sendint64(&reply_message, writePtr);
	write64(&(reply_message[1]), writePtr);
	//pq_sendint64(&reply_message, flushPtr);
	write64(&(reply_message[9]), flushPtr);
	//pq_sendint64(&reply_message, applyPtr);
	write64(&(reply_message[17]), applyPtr);
	//pq_sendint64(&reply_message, GetCurrentIntegerTimestamp());
	write64(&(reply_message[25]), sendTime);
	//pq_sendbyte(&reply_message, requestReply ? 1 : 0);
	reply_message[33] = requestReply ? 1 : 0;

	/* Send it */
	/*elog(DEBUG2, "sending write %X/%X flush %X/%X apply %X/%X%s",
		 (uint32) (writePtr >> 32), (uint32) writePtr,
		 (uint32) (flushPtr >> 32), (uint32) flushPtr,
		 (uint32) (applyPtr >> 32), (uint32) applyPtr,
		 requestReply ? " (reply requested)" : "");*/

	xf_info("Send reply: %lu %lu %lu %d\n", writePtr, flushPtr, applyPtr, requestReply);
	xlf_send(mc, reply_message, 34);
}

void
xlf_process_message(PGconn *mc, char *buf, size_t len,
		ReplMessage *msg)
{
	switch (buf[0])
	{
		case 'w':
			{
				msg->type = MSG_WAL_DATA;
				msg->dataStart = fromnetwork64(buf+1);
				msg->walEnd = fromnetwork64(buf+9);
				msg->sendTime = fromnetwork64(buf+17);
				msg->replyRequested = 0;

				msg->dataPtr = 0;
				msg->dataLen = len - 25;
				msg->data = buf+25;
				msg->nextPageBoundary = ((WAL_BLOCK_SIZE - msg->dataStart) & WAL_BLOCK_MASK);

				xf_info("Received %lu byte WAL block\n", len-25);
				xf_info("   dataStart: %X/%X\n", FormatRecPtr(msg->dataStart));
				xf_info("   walEnd: %lu\n", msg->walEnd);
				xf_info("   sendTime: %lu\n", msg->sendTime);

				process_walsender_message(msg);
				break;
			}
		case 'k':
			{
				xf_info("Keepalive message\n");
				msg->type = MSG_KEEPALIVE;
				msg->walEnd = fromnetwork64(buf+1);
				msg->sendTime = fromnetwork64(buf+9);
				msg->replyRequested = *(buf+17);

				xf_info("   walEnd: %lu\n", msg->walEnd);
				xf_info("   sendTime: %lu\n", msg->sendTime);
				xf_info("   replyRequested: %d\n", msg->replyRequested);
				process_walsender_message(msg);

				if (msg->replyRequested)
					xlf_send_reply(mc, true, false);
				break;
			}
	}
}

#define MAX_CONNINFO_LEN 4000

Oid *
xlf_find_tablespace_oids(XfConn conn)
{
	PGconn* masterConn;
	// TODO: take in other options
	char conninfo[MAX_CONNINFO_LEN+1];
	char *buf = conninfo;
	char *buf_end = &(conninfo[MAX_CONNINFO_LEN]);
	Oid *oids;

	memset(conninfo, 0, sizeof(conninfo));

	if (conn->master_host) {
		buf += snprintf(buf, buf_end - buf, "host=%s ", conn->master_host);
	}

	if (conn->master_port)
		buf +=  snprintf(buf, buf_end - buf, "port=%s ", conn->master_port);

	if (conn->user_name)
		buf += snprintf(buf, buf_end - buf, "user=%s ", conn->user_name);

	buf += snprintf(buf, buf_end - buf, "dbname=postgres application_name=walbouncer");//"dbname=replication replication=true application_name=walbouncer");

	xf_info("Start connecting to %s\n", conninfo);
	masterConn = PQconnectdb(conninfo);
	if (PQstatus(masterConn) != CONNECTION_OK)
		error(PQerrorMessage(masterConn));
	xf_info("Connected to master\n");

	{
		PGresult *res;
		int oidcount;
		int i;

		const char *paramValues[1] = {conn->include_tablespaces};
		res = PQexecParams(masterConn,
			"SELECT oid FROM pg_tablespace WHERE spcname = "
			"ANY (string_to_array($1, ',')) "
			"OR spcname IN ('pg_default', 'pg_global')",
			1, NULL, paramValues,
			NULL, NULL, 0);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			error("Could not retrieve tablespaces: %s", PQerrorMessage(masterConn));

		oidcount = PQntuples(res);
		oids = xfalloc0(sizeof(Oid)*(oidcount+1));

		for (i = 0; i < oidcount; i++)
		{
			char *oid = PQgetvalue(res, i, 0);
			xf_info("Found tablespace oid %s %d", oid, atoi(oid));
			oids[i] = atoi(oid);
		}
		PQclear(res);
	}
	PQfinish(masterConn);

	return oids;
}

int
XfProcessStartupPacket(XfConn conn, bool SSLdone)
{
	int32 len;
	void *buf;
	ProtocolVersion proto;

	if (ConnGetBytes(conn, (char*) &len, 4) == EOF)
	{
		log_error("Incomplete startup packet");
		return STATUS_ERROR;
	}

	len = ntohl(len);
	len -= 4;

	if (len < (int32) sizeof(ProtocolVersion) ||
		len > MAX_STARTUP_PACKET_LENGTH)
	{
		log_error("Invalid length of startup packet");
		return STATUS_ERROR;
	}

	/*
	 * Allocate at least the size of an old-style startup packet, plus one
	 * extra byte, and make sure all are zeroes.  This ensures we will have
	 * null termination of all strings, in both fixed- and variable-length
	 * packet layouts.
	 */
	if (len <= (int32) sizeof(StartupPacket))
		buf = xfalloc0(sizeof(StartupPacket) + 1);
	else
		buf = xfalloc0(len + 1);

	if (ConnGetBytes(conn, buf, len) == EOF)
	{
		log_error("incomplete startup packet");
		return STATUS_ERROR;
	}

	/*
	 * The first field is either a protocol version number or a special
	 * request code.
	 */
	conn->proto = proto = ntohl(*((ProtocolVersion *) buf));

	if (proto == CANCEL_REQUEST_CODE)
	{
		error("Cancel not supported");
		/* Not really an error, but we don't want to proceed further */
		return STATUS_ERROR;
	}

	if (proto == NEGOTIATE_SSL_CODE && !SSLdone)
	{
		char		SSLok;

#ifdef USE_SSL
		/* No SSL when disabled or on Unix sockets */
		if (!EnableSSL || IS_AF_UNIX(port->laddr.addr.ss_family))
			SSLok = 'N';
		else
			SSLok = 'S';		/* Support for SSL */
#else
		SSLok = 'N';			/* No support for SSL */
#endif

retry1:
		if (send(conn->fd, &SSLok, 1, 0) != 1)
		{
			if (errno == EINTR)
				goto retry1;	/* if interrupted, just retry */
			error("failed to send SSL negotiation response");
			return STATUS_ERROR;	/* close the connection */
		}

#ifdef USE_SSL
		if (SSLok == 'S' && secure_open_server(port) == -1)
			return STATUS_ERROR;
#endif
		/* regular startup packet, cancel, etc packet should follow... */
		/* but not another SSL negotiation request */
		return XfProcessStartupPacket(conn, true);
	}

	/* Could add additional special packet types here */

	/* Check we can handle the protocol the frontend is using. */

	if (PG_PROTOCOL_MAJOR(proto) < PG_PROTOCOL_MAJOR(PG_PROTOCOL_EARLIEST) ||
		PG_PROTOCOL_MAJOR(proto) > PG_PROTOCOL_MAJOR(PG_PROTOCOL_LATEST) ||
		(PG_PROTOCOL_MAJOR(proto) == PG_PROTOCOL_MAJOR(PG_PROTOCOL_LATEST) &&
		 PG_PROTOCOL_MINOR(proto) > PG_PROTOCOL_MINOR(PG_PROTOCOL_LATEST)))
		error("Unsupported frontend protocol");

	/*
	 * Now fetch parameters out of startup packet and save them into the Port
	 * structure.  All data structures attached to the Port struct must be
	 * allocated in TopMemoryContext so that they will remain available in a
	 * running backend (even after PostmasterContext is destroyed).  We need
	 * not worry about leaking this storage on failure, since we aren't in the
	 * postmaster process anymore.
	 */
	//oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	conn->guc_options = xfalloc0(len);
	conn->gucs_len = 0;
	conn->database_name = NULL;
	conn->user_name = NULL;
	conn->cmdline_options = NULL;
	conn->include_tablespaces = NULL;

	{
		int32		offset = sizeof(ProtocolVersion);
		bool am_walsender = false;
		/*
		 * Scan packet body for name/option pairs.  We can assume any string
		 * beginning within the packet body is null-terminated, thanks to
		 * zeroing extra byte above.
		 */
		while (offset < len)
		{
			char	   *nameptr = ((char *) buf) + offset;
			int32		valoffset;
			char	   *valptr;

			if (*nameptr == '\0')
				break;			/* found packet terminator */
			valoffset = offset + strlen(nameptr) + 1;
			if (valoffset >= len)
				break;			/* missing value, will complain below */
			valptr = ((char *) buf) + valoffset;

			if (strcmp(nameptr, "database") == 0)
				conn->database_name = xfstrdup(valptr);
			else if (strcmp(nameptr, "user") == 0)
				conn->user_name = xfstrdup(valptr);
			else if (strcmp(nameptr, "options") == 0)
				conn->cmdline_options = xfstrdup(valptr);
			else if (strcmp(nameptr, "replication") == 0)
			{
				/*
				 * Due to backward compatibility concerns the replication
				 * parameter is a hybrid beast which allows the value to be
				 * either boolean or the string 'database'. The latter
				 * connects to a specific database which is e.g. required for
				 * logical decoding.
				 */
				if (strcmp(valptr, "database") == 0
					|| strcasecmp(valptr, "on")
					|| strcasecmp(valptr, "yes")
					|| strcasecmp(valptr, "true")
					|| strcmp(valptr, "1"))
				{
					am_walsender = true;
				}
				else if (strcasecmp(valptr, "off")
						|| strcasecmp(valptr, "no")
						|| strcasecmp(valptr, "0"))
					error("This is a WAL proxy that only accepts replication connections");
				else
					error("invalid value for parameter \"replication\"");
			}
			else if (strcmp(nameptr, "application_name") == 0)
			{
				conn->include_tablespaces = xfstrdup(valptr);
			}
			else
			{
				int guclen = (valoffset - offset) + strlen(valptr) + 1;
				/* Assume it's a generic GUC option */
				memcpy(conn->guc_options + conn->gucs_len, nameptr,
						guclen);
				conn->gucs_len += guclen;

			}
			offset = valoffset + strlen(valptr) + 1;
		}

		if (!am_walsender)
			error("This is a WAL proxy that only accepts replication connections");

		/*
		 * If we didn't find a packet terminator exactly at the end of the
		 * given packet length, complain.
		 */
		if (offset != len - 1)
			error("invalid startup packet layout: expected terminator as last byte");
	}

	/* Check a user name was given. */
	if (conn->user_name == NULL || conn->user_name[0] == '\0')
		error("no PostgreSQL user name specified in startup packet");

	return STATUS_OK;
}

void
XfInitConnection(XfConn conn)
{
	xf_info("Received conn on fd %d", conn->fd);

	//FIXME: need to timeout here
	// setup error log destination
	// copy socket info out here
	if (XfProcessStartupPacket(conn, false) != STATUS_OK)
		error("Error while processing startup packet");
}

void
ReadyForQuery(XfConn conn)
{
	ConnBeginMessage(conn, 'Z');
	ConnSendInt(conn, 'I', 1);
	ConnEndMessage(conn);
	ConnFlush(conn);
}

typedef struct {
	int qtype;
	XfMessage *msg;
} XfCommand;

int
ReadCommand(XfConn conn, XfCommand *cmd)
{
	int qtype;
	cmd->qtype = qtype = ConnGetByte(conn);

	if (qtype == EOF)
		return EOF;

	if (ConnGetMessage(conn, &(cmd->msg)))
		return EOF;

	xf_info("Command %c payload %d\n", qtype, cmd->msg->len);

	return qtype;
}

#define TEXTOID 25
#define INT4OID 23

void
ExecIdentifySystem(XfConn conn, PGconn *mc)
{
	// query master server, pass through data
	PGresult *result = NULL;
	char *primary_sysid;
	char *primary_tli;
	char *primary_xpos;
	char *dbname = NULL;

	result = PQexec(mc, "IDENTIFY_SYSTEM");
	if (PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		PQclear(result);
		error(PQerrorMessage(mc));
	}
	if (PQnfields(result) < 3 || PQntuples(result) != 1)
	{
		error("Invalid response");
	}

	primary_sysid = strdup(PQgetvalue(result, 0, 0));
	primary_tli = strdup(PQgetvalue(result, 0, 1));
	primary_xpos = strdup(PQgetvalue(result, 0, 2));

	xf_info("System identifier: %s\n", primary_sysid);
	xf_info("TLI: %s\n", primary_tli);
	xf_info("Xpos: %s\n", primary_xpos);
	xf_info("dbname: %s\n", dbname);

	//TODO: parse out tli and xpos for our use

	/* Send a RowDescription message */
	ConnBeginMessage(conn, 'T');
	ConnSendInt(conn, 4, 2);		/* 4 fields */

	/* first field */
	ConnSendString(conn, "systemid");	/* col name */
	ConnSendInt(conn, 0, 4);		/* table oid */
	ConnSendInt(conn, 0, 2);		/* attnum */
	ConnSendInt(conn, TEXTOID, 4);		/* type oid */
	ConnSendInt(conn, -1, 2);	/* typlen */
	ConnSendInt(conn, 0, 4);		/* typmod */
	ConnSendInt(conn, 0, 2);		/* format code */

	/* second field */
	ConnSendString(conn, "timeline");	/* col name */
	ConnSendInt(conn, 0, 4);		/* table oid */
	ConnSendInt(conn, 0, 2);		/* attnum */
	ConnSendInt(conn, INT4OID, 4);		/* type oid */
	ConnSendInt(conn, 4, 2);		/* typlen */
	ConnSendInt(conn, 0, 4);		/* typmod */
	ConnSendInt(conn, 0, 2);		/* format code */

	/* third field */
	ConnSendString(conn, "xlogpos");		/* col name */
	ConnSendInt(conn, 0, 4);		/* table oid */
	ConnSendInt(conn, 0, 2);		/* attnum */
	ConnSendInt(conn, TEXTOID, 4);		/* type oid */
	ConnSendInt(conn, -1, 2);	/* typlen */
	ConnSendInt(conn, 0, 4);		/* typmod */
	ConnSendInt(conn, 0, 2);		/* format code */

	/* fourth field */
	ConnSendString(conn, "dbname");		/* col name */
	ConnSendInt(conn, 0, 4);		/* table oid */
	ConnSendInt(conn, 0, 2);		/* attnum */
	ConnSendInt(conn, TEXTOID, 4);		/* type oid */
	ConnSendInt(conn, -1, 2);	/* typlen */
	ConnSendInt(conn, 0, 4);		/* typmod */
	ConnSendInt(conn, 0, 2);		/* format code */
	ConnEndMessage(conn);

	/* Send a DataRow message */
	ConnBeginMessage(conn, 'D');
	ConnSendInt(conn, 4, 2);		/* # of columns */
	ConnSendInt(conn, strlen(primary_sysid), 4); /* col1 len */
	ConnSendBytes(conn, primary_sysid, strlen(primary_sysid));
	ConnSendInt(conn, strlen(primary_tli), 4);	/* col2 len */
	ConnSendBytes(conn, (char *) primary_tli, strlen(primary_tli));
	ConnSendInt(conn, strlen(primary_xpos), 4);	/* col3 len */
	ConnSendBytes(conn, (char *) primary_xpos, strlen(primary_xpos));
	/* send NULL if not connected to a database */
	if (dbname)
	{
		ConnSendInt(conn, strlen(dbname), 4);	/* col4 len */
		ConnSendBytes(conn, (char *) dbname, strlen(dbname));
	}
	else
	{
		ConnSendInt(conn, -1, 4);	/* col4 len, NULL */
	}

	ConnEndMessage(conn);

	PQclear(result);
}

void
ExecTimeline()
{
	error("Timeline");
	// query master server, write out copy data
}

void
XfSendWALRecord(XfConn conn, char *data, int len, XLogRecPtr sentPtr, TimestampTz lastSend)
{
	xf_info("Sending out %d bytes of WAL\n", len);
	ConnBeginMessage(conn, 'd');
	ConnSendBytes(conn, data, len);
	ConnEndMessage(conn);
	conn->sentPtr = sentPtr;
	conn->lastSend = lastSend;
	ConnFlush(conn);
}

void
XfSendEndOfWal(XfConn conn)
{
	ConnBeginMessage(conn, 'c');
	ConnEndMessage(conn);
	ConnFlush(conn);
}

void
XfSndKeepalive(XfConn conn, bool request_reply)
{
	xf_info("sending keepalive message %X/%X%s\n",
			(uint32) (conn->sentPtr>>32),
			(uint32) conn->sentPtr,
			request_reply ? " (reply requested)" : "");

	ConnBeginMessage(conn, 'd');
	ConnSendInt(conn, 'k', 1);
	ConnSendInt64(conn, conn->sentPtr);
	ConnSendInt64(conn, conn->lastSend);
	ConnSendInt(conn, request_reply ? 1 : 0, 1);
	ConnEndMessage(conn);
	ConnFlush(conn);
}

void
XfProcessStandbyReplyMessage(XfConn conn, XfMessage *msg)
{
	XLogRecPtr	writePtr,
				flushPtr,
				applyPtr;
	bool		replyRequested;

	/* the caller already consumed the msgtype byte */
	writePtr = fromnetwork64(msg->data + 1);
	flushPtr = fromnetwork64(msg->data + 9);
	applyPtr = fromnetwork64(msg->data + 17);
	(void) fromnetwork64(msg->data + 25);		/* sendTime; not used ATM */
	replyRequested = msg->data[33];

	xf_info("Standby reply msg: write %X/%X flush %X/%X apply %X/%X%s\n",
		 (uint32) (writePtr >> 32), (uint32) writePtr,
		 (uint32) (flushPtr >> 32), (uint32) flushPtr,
		 (uint32) (applyPtr >> 32), (uint32) applyPtr,
		 replyRequested ? " (reply requested)" : "");

	/* Send a reply if the standby requested one. */
	if (replyRequested)
		XfSndKeepalive(conn, false);

	//TODO: send reply message forward
}

void
XfProcessStandbyHSFeedbackMessage(XfConn conn, XfMessage *msg)
{
	TransactionId nextXid;
	uint32 nextEpoch;
	TransactionId feedbackXmin;
	uint32		feedbackEpoch;
	/*
	 * Decipher the reply message. The caller already consumed the msgtype
	 * byte.
	 */
	(void) fromnetwork64(msg->data + 1);		/* sendTime; not used ATM */
	feedbackXmin = fromnetwork32(msg->data + 9);
	feedbackEpoch = fromnetwork32(msg->data + 13);

	xf_info("hot standby feedback xmin %u epoch %u\n",
		 feedbackXmin,
		 feedbackEpoch);

	// TODO: arrange for the feedback to be forwarded to the master
}

void
XfProcessReplyMessage(XfConn conn)
{
	XfMessage *msg;
	if (ConnGetMessage(conn, &msg))
		error("unexpected EOF from receiver");

	switch (msg->data[0])
	{
		case 'r':
			XfProcessStandbyReplyMessage(conn, msg);
			break;
		case 'h':
			XfProcessStandbyHSFeedbackMessage(conn, msg);
			break;
		default:
			error("Unexpected message type");
	}

	ConnFreeMessage(msg);
}

void
XfProcessRepliesIfAny(XfConn conn)
{
	unsigned char firstchar;
	int r;

	for (;;)
	{
		r = ConnGetByteIfAvailable(conn, &firstchar);
		if (r < 0)
		{
			error("Unexpected EOF from receiver");
		}
		if (r == 0)
			break;

		// 		if (streamingDoneReceiving && firstchar != 'X')
		/*ereport(FATAL,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected standby message type \"%c\", after receiving CopyDone",
						firstchar)));*/

		switch (firstchar)
		{
			case 'd':
				XfProcessReplyMessage(conn);
				break;
			case 'c':
				error("Received CopyDone");
				/*if (!streamingDoneSending)
				{
					pq_putmessage_noblock('c', NULL, 0);
					streamingDoneSending = true;
				}

				/* consume the CopyData message
				resetStringInfo(&reply_message);
				if (pq_getmessage(&reply_message, 0))
				{
					ereport(COMMERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("unexpected EOF on standby connection")));
					proc_exit(0);
				}

				streamingDoneReceiving = true;
				received = true;*/
				break;
			case 'X':
				error("Standby is closing the socket");
			default:
				error("Invalid standby message");
		}
	}
}

void SendCopyBothResponse(XfConn conn)
{
	/* Send a CopyBothResponse message, and start streaming */
	ConnBeginMessage(conn, 'W');
	ConnSendInt(conn, 0, 1);
	ConnSendInt(conn, 0, 2);
	ConnEndMessage(conn);
	ConnFlush(conn);
}

void
ExecStartPhysical(XfConn conn, PGconn *mc, ReplicationCommand *cmd)
{
	TimeLineID next_tli;
	ReplMessage msg;

	xf_info("Starting streaming from %x/%x on TLI %u", (uint32) (cmd->startpoint>>32), (uint32) cmd->startpoint, cmd->timeline);
	xlf_startstreaming(mc, cmd->startpoint, cmd->timeline);

	/* Send a CopyBothResponse message, and start streaming */
	SendCopyBothResponse(conn);
	{
		bool endofwal = false;
		while (!endofwal)
		{
			char *buf;
			int	len;
			XfProcessRepliesIfAny(conn);
			len = xlf_receive(mc, NAPTIME, &buf);
			if (len != 0)
			{
				for (;;)
				{
					if (len > 0)
					{
						xlf_process_message(mc, buf, len, &msg);
						if (buf[0] == 'w')
						{
							//XfSendWALRecord(conn, buf, len, msg->walEnd, msg->sendTime);
						}
					} else if (len == 0)
						break;
					else if (len < 0)
					{
						printf("End of WAL\n");
						endofwal = true;
						break;
					}
					len = xlf_receive(mc, 0, &buf);
				}
			}
			else
			{
				printf(".");
			}

		}
	}

	XfSendEndOfWal(conn);

	xlf_endstreaming(mc, &next_tli);
}

static bool
IsAtWalPageBoundary(ReplMessage *msg)
{
	return msg->dataPtr == msg->nextPageBoundary;
}

static char *
ReplMessageConsume(ReplMessage *msg, size_t amount)
{
	int data_offset = msg->dataPtr;
	msg->dataPtr += amount;
	Assert(msg->dataPtr < msg->dataLen);
	return msg->data + data_offset;
}

typedef enum {
	FS_SYNCHRONIZING,
	FS_COPY_SWITCH,
	FS_COPY_NORMAL,
	FS_COPY_ZERO,
	FS_BUFFER_RECORD,
	FS_BUFFER_FILENODE
} FilterState;

#define FL_BUFFER_LEN 64

typedef struct {
	FilterState state;
	int dataNeeded;
	int recordRemaining;

	bool synchronized;
	XLogRecPtr requestedStartPos;

	int recordStart;
	int headerPos;
	int headerLen;

	int bufferLen;
	char buffer[FL_BUFFER_LEN];

	int unsentBufferLen;
	char unsentBuffer[FL_BUFFER_LEN];

	Oid *include_tablespaces;
} FilterData;

static void
ReplMessageBuffer(FilterData *fl, ReplMessage *msg, int amount)
{
	Assert(fl->bufferLen + amount <= FL_BUFFER_LEN);
	Assert(msg->dataPtr + amount <= msg->dataLen);
	memcpy(fl->buffer + fl->bufferLen, msg->data + msg->dataPtr, amount);
	fl->bufferLen += amount;
	msg->dataPtr += amount;
	fl->dataNeeded -= amount;
}

static void
ReplMessageCopy(FilterData *fl, ReplMessage *msg, int amount)
{
	Assert(msg->dataPtr + amount <= msg->dataLen);
	msg->dataPtr += amount;
	fl->dataNeeded -= amount;
}

static void
ReplMessageZero(FilterData *fl, ReplMessage *msg, int amount)
{
	Assert(msg->dataPtr + amount <= msg->dataLen);
	memset(msg->data + msg->dataPtr, 0, amount);
	msg->dataPtr += amount;
	fl->dataNeeded -= amount;
}

static void
ReplMessageAlign(ReplMessage *msg)
{
	XLogRecPtr curPos = msg->dataStart + msg->dataPtr;
	int alignAmount =  MAXALIGN(curPos) - curPos;
	Assert(msg->dataPtr + alignAmount <= msg->dataLen);
	msg->dataPtr += alignAmount;
}

static int
ReplDataRemainingInSegment(ReplMessage *msg)
{
	XLogRecPtr curPos = msg->dataStart + msg->dataPtr;
	XLogRecPtr segEnd = (curPos + XLogSegSize) & (~(XLogSegSize - 1));
	xf_info("Curpos %X/%X segEnd %X/%X", FormatRecPtr(curPos), FormatRecPtr(segEnd));
	return segEnd - curPos;
}

#define REC_HEADER_LEN 32

static pg_crc32
CalculateCRC32(char *buffer, int len, int total_len)
{
	pg_crc32 crc;
	INIT_CRC32(crc);
	COMP_CRC32_ZERO(crc, total_len - REC_HEADER_LEN);
	COMP_CRC32(crc, buffer, offsetof(XLogRecord, xl_crc));
	FIN_CRC32(crc);
	return crc;
}

static void
WriteNoopRecord(FilterData *fl, ReplMessage *msg)
{
	XLogRecord *rec = (XLogRecord*) fl->buffer;
	RelFileNode *node = (RelFileNode*) (fl->buffer + sizeof(XLogRecord));

	Assert(fl->bufferLen == REC_HEADER_LEN + sizeof(RelFileNode));

	// tot_len, xid stay the same
	// len is the whole record, without any backup blocks
	rec->xl_len = rec->xl_tot_len - REC_HEADER_LEN;
	rec->xl_info = XLOG_NOOP;
	rec->xl_rmid = RM_XLOG_ID;
	// xl_prev stays the same
	rec->xl_crc = 0;
	node->dbNode = 0;
	node->relNode = 0;
	node ->spcNode = 0;
	rec->xl_crc = CalculateCRC32(fl->buffer, fl->bufferLen, rec->xl_tot_len);

	{
		// We scribble over data in the message buffer
		// some of the data may be in the filter unsent buffer.


		int targetpos = fl->recordStart;
		int copied = 0;
		int amount;
		int toCopy = fl->bufferLen;

		if (targetpos == -1)
		{
			// Rewrite first part in the unsentbuffer
			amount = fl->unsentBufferLen;
			memcpy(fl->unsentBuffer, fl->buffer + copied, amount);
			copied += amount;
			toCopy -= amount;
			targetpos = 0;
		}
		// Copy up to next pageheader or to end of buffer
		amount = fl->headerPos >= 0 ? fl->headerPos - targetpos: toCopy;
		memcpy(msg->data + targetpos, fl->buffer + copied, amount);

		// Skip over page header in the target buffer
		toCopy -= amount;
		copied += amount;

		if (!toCopy)
			return;
		targetpos += fl->headerLen;

		// Copy the rest if any
		amount = toCopy;
		memcpy(msg->data + targetpos, fl->buffer + copied, amount);
	}
}

void
FilterClearBuffer(FilterData *fl)
{
	fl->bufferLen = 0;
}

bool
NeedToFilter(FilterData *fl, RelFileNode *node)
{
	Oid *tblspc_oid;

	if (!fl->include_tablespaces)
		return false;

	tblspc_oid = fl->include_tablespaces;
	for (; *tblspc_oid; tblspc_oid++)
	{
		if (node->spcNode == *tblspc_oid)
			return false;
	}
	xf_info("Filtering data in tablespace %d", node->spcNode);
	return true;
}

void
FilterBufferRecordHeader(FilterData* fl, ReplMessage* msg)
{
	fl->state = FS_BUFFER_RECORD;
	fl->recordStart = msg->dataPtr;
	fl->dataNeeded = REC_HEADER_LEN;
	fl->headerPos = -1;
	fl->headerLen = 0;
	fl->bufferLen = 0;
}

void
XfSendWalBlock(XfConn conn, ReplMessage *msg, FilterData *fl)
{
	XLogRecPtr dataStart;
	int msgOffset = 0;
	int buffered = 0;
	int unsentLen = 0;
	char unsentBuf[FL_BUFFER_LEN];

	// Take a local copy of the unsent buffer in case we need to rewrite it
	if (fl->unsentBufferLen) {
		unsentLen = fl->unsentBufferLen;
		memcpy(unsentBuf, fl->unsentBuffer, unsentLen);
		xf_info("Sending %d bytes of unbuffered data", unsentLen);
	}

	//'d' 'w' l(dataStart) l(walEnd) l(sendTime) s[WALdata]
	if (fl->state == FS_BUFFER_RECORD || fl->state == FS_BUFFER_FILENODE)
	{
		// Chomp the buffered data off of what we send
		buffered = fl->bufferLen;
		// Stash it away into fl state, we will send it with the next block
		fl->unsentBufferLen = fl->bufferLen;
		memcpy(fl->unsentBuffer, fl->buffer, fl->bufferLen);
		// Make note that record starts in the unsent buffer for rewriting
		fl->recordStart = -1;
		xf_info("Buffering %d bytes of data", buffered);

	} else {
		// Clear out unsent buffer
		fl->unsentBufferLen = 0;
	}

	// Don't send anything if we are not synchronized, we will see this data again after replication restart
	if (!fl->synchronized)
	{
		xf_info("Skipping sending data.");
		return;
	}

	// Include the previously unsent data
	dataStart = msg->dataStart - unsentLen;

	if (fl->requestedStartPos > dataStart) {
		if (fl->requestedStartPos > (msg->dataStart + msg->dataLen))
		{
			xf_info("Skipping whole WAL message as not requested");
			return;
		}
		msgOffset = fl->requestedStartPos - dataStart;
		dataStart = fl->requestedStartPos;
		Assert(msgOffset < (msg->dataLen + unsentLen));
		xf_info("Chomping WAL message down to size at %d", msgOffset);
	}

	xf_info("Sending data start %X/%X", FormatRecPtr(dataStart));

	ConnBeginMessage(conn, 'd');
	ConnSendInt(conn, 'w', 1);
	ConnSendInt64(conn, dataStart);
	ConnSendInt64(conn, msg->walEnd - buffered);
	ConnSendInt64(conn, msg->sendTime);
	xf_info("Sending out %d bytes of WAL\n", msg->dataLen - msgOffset - buffered);

	if (unsentLen && msgOffset < unsentLen) {
		xf_info("Unsent data contents at offset %d, %d bytes:", msgOffset, unsentLen-msgOffset);
		hexdump(unsentBuf + msgOffset, unsentLen);
		ConnSendBytes(conn, unsentBuf + msgOffset, unsentLen-msgOffset);
		msgOffset = msgOffset < unsentLen ? 0 : msgOffset - unsentLen;
	}

	ConnSendBytes(conn, msg->data + msgOffset, msg->dataLen - msgOffset - buffered);
	ConnEndMessage(conn);

	conn->sentPtr = msg->dataStart + msg->dataLen - buffered;
	conn->lastSend = msg->sendTime;
	ConnFlush(conn);
}


/*#define parse_debug(...) do{\
	fprintf (stderr, __VA_ARGS__);\
	fprintf (stderr, "\n");\
}while(0)*/

#define parse_debug(...)

static bool
ProcessWalDataBlock(ReplMessage* msg, FilterData* fl, XLogRecPtr *retryPos)
{
	// consume the whole message
	while (msg->dataPtr < msg->dataLen)
	{
		parse_debug("Parse loop at %d", msg->dataPtr);
		if (IsAtWalPageBoundary(msg))
		{
			/* We assume here that walsender will not split WAL page headers
			 * accross multiple messages.
			 */
			int headerPos = msg->dataPtr;
			parse_debug(" - Found page header at %d", headerPos);

			XLogPageHeader header = (XLogPageHeader) ReplMessageConsume(msg,
					SizeOfXLogShortPHD);
			if (header->xlp_magic != XLOG_PAGE_MAGIC)
				error("Received page with invalid page magic");

			if (header->xlp_info & XLP_LONG_HEADER)
				ReplMessageConsume(msg, SizeOfXLogLongPHD - SizeOfXLogShortPHD);

			msg->nextPageBoundary += WAL_BLOCK_SIZE;

			switch (fl->state)
			{
			case FS_SYNCHRONIZING:
				if (header->xlp_info & XLP_FIRST_IS_CONTRECORD)
				{
					/*
					 * We are starting on a continuation record. Need to figure
					 * out if we need to zero out the record. Resync by buffering
					 * the next record header, find xl_prev and restart there.
					 **/

					// Skip rest of the continuation record for now.
					fl->state = FS_COPY_NORMAL;
					fl->dataNeeded = header->xlp_rem_len;
					parse_debug("Unsynchronized at start pos, skipping %d to next record header", fl->dataNeeded);

					break;
				}
				fl->synchronized = true;
				FilterBufferRecordHeader(fl, msg);
				// fall through to buffering
			case FS_BUFFER_RECORD:
			case FS_BUFFER_FILENODE:
			case FS_COPY_NORMAL:
			case FS_COPY_ZERO:
				// We just take note of the header pos to skip over it when
				// replacing data in the buffer
				fl->headerPos = headerPos;
				fl->headerLen = XLogPageHeaderSize(header);
				break;
			case FS_COPY_SWITCH:
				fl->dataNeeded -= XLogPageHeaderSize(header);
			}
		}
		else
		{
			int amountAvailable = msg->nextPageBoundary - msg->dataPtr;
			if (msg->dataPtr + amountAvailable > msg->dataLen)
				amountAvailable = msg->dataLen - msg->dataPtr;
			switch (fl->state)
			{
			case FS_SYNCHRONIZING:
				// We assume that we start streaming at record boundary
				//XXX: figure out a way to verify this, maybe rewind to page
				// boundary
				parse_debug("Synchronizing at record header");
				fl->synchronized = true;
				FilterBufferRecordHeader(fl, msg);
				// Fall through to the correct handler
			case FS_BUFFER_RECORD:
				if (fl->dataNeeded <= amountAvailable)
					ReplMessageBuffer(fl, msg, fl->dataNeeded);
				else
					ReplMessageBuffer(fl, msg, amountAvailable);
				if (!fl->dataNeeded)
				{
					XLogRecord *rec = (XLogRecord*) fl->buffer;
					parse_debug(" - Record buffered at %d", msg->dataPtr);

					Assert(fl->bufferLen == REC_HEADER_LEN);

					if (!rec->xl_tot_len)
						error("Received invalid WAL record");

					if ((msg->dataStart + msg->dataPtr - fl->bufferLen) == 0xA46F1C8) {
						xf_info("Record with tot_len %d at %X/%X", rec->xl_tot_len, FormatRecPtr(msg->dataStart + msg->dataPtr - fl->bufferLen));
						xf_info("Msg ptr %X, len %X", msg->dataPtr, msg->dataLen);
						xf_info("Record with crc %X and first words %X %X", rec->xl_crc,
									*((uint32*)(msg->data + msg->dataPtr)),
									*((uint32*)(msg->data + msg->dataPtr + 4)));
					}

					if (!fl->synchronized)
					{
						// We are not synchronized, restart at previous record
						*retryPos = rec->xl_prev;
						fl->state = FS_SYNCHRONIZING;
						parse_debug("Found next record, requesting restart at xlog pos %X/%X",
								(uint32)(*retryPos >> 32), (uint32)*retryPos);
						return false;
					}

					fl->recordRemaining = rec->xl_tot_len - REC_HEADER_LEN;
					switch (rec->xl_rmid)
					{
						case RM_SMGR_ID:
						case RM_HEAP_ID:
						case RM_HEAP2_ID:
						case RM_BTREE_ID:
						case RM_GIN_ID:
						case RM_GIST_ID:
						case RM_SEQ_ID:
						case RM_SPGIST_ID:
							fl->state = FS_BUFFER_FILENODE;
							fl->dataNeeded = sizeof(RelFileNode);
							parse_debug(" - Data record, buffering %d bytes for filenode", fl->dataNeeded);
							break;
						case RM_XLOG_ID:
							{
								uint8 info = rec->xl_info & 0xF0;
								if (info == XLOG_SWITCH)
								{
									// Stream out data until end of buffer
									fl->state = FS_COPY_SWITCH;
									fl->dataNeeded = ReplDataRemainingInSegment(msg);
									parse_debug(" - Xlog switch, copying %d bytes ", fl->dataNeeded);
								}
								else if (info == XLOG_FPI)
								{
									fl->state = FS_BUFFER_FILENODE;
									fl->dataNeeded = sizeof(RelFileNode);
									parse_debug(" - FPI, buffering %d bytes for filenode", fl->dataNeeded);
								} else {
									fl->state = FS_COPY_NORMAL;
									fl->dataNeeded = fl->recordRemaining;
									parse_debug(" - Other XLOG record, copying %d bytes ", fl->dataNeeded);
								}
							}
							break;
						default:
							// No need for filtering
							fl->state = FS_COPY_NORMAL;
							fl->dataNeeded = fl->recordRemaining;
							parse_debug(" - Other record, copying %d bytes ", fl->dataNeeded);
					}
				}
				break;
			case FS_BUFFER_FILENODE:
				if (fl->dataNeeded <= amountAvailable)
					ReplMessageBuffer(fl, msg, fl->dataNeeded);
				else
					ReplMessageBuffer(fl, msg, amountAvailable);
				if (!fl->dataNeeded)
				{
					parse_debug(" - Filenode buffered at %d", msg->dataPtr);
					fl->recordRemaining -= sizeof(RelFileNode);
					if (NeedToFilter(fl,
							(RelFileNode*) (fl->buffer + REC_HEADER_LEN)))
					{
						WriteNoopRecord(fl, msg);
						fl->state = FS_COPY_ZERO;
						parse_debug(" - Filter record");
					}
					else
					{
						fl->state = FS_COPY_NORMAL;
						parse_debug(" - Passthrough record");
					}
					fl->dataNeeded = fl->recordRemaining;
					parse_debug(" - Copying %d bytes until next record", fl->dataNeeded);
				}
				break;
			case FS_COPY_NORMAL:
				if (fl->dataNeeded <= amountAvailable)
					ReplMessageCopy(fl, msg, fl->dataNeeded);
				else
					ReplMessageCopy(fl, msg, amountAvailable);
				if (!fl->dataNeeded)
				{
					ReplMessageAlign(msg);
					FilterBufferRecordHeader(fl, msg);
					parse_debug(" - Copy done, buffer %d bytes", fl->dataNeeded);
				}
				break;
			case FS_COPY_ZERO:
				if (fl->dataNeeded <= amountAvailable)
					ReplMessageZero(fl, msg, fl->dataNeeded);
				else
					ReplMessageZero(fl, msg, amountAvailable);
				if (!fl->dataNeeded)
				{
					ReplMessageAlign(msg);
					FilterBufferRecordHeader(fl, msg);
					parse_debug(" - Copy done, buffer %d bytes", fl->dataNeeded);
				}
				break;
			case FS_COPY_SWITCH:
				if (fl->dataNeeded <= amountAvailable)
					ReplMessageCopy(fl, msg, fl->dataNeeded);
				else
					ReplMessageCopy(fl, msg, amountAvailable);
				if (!fl->dataNeeded)
				{
					FilterBufferRecordHeader(fl, msg);
					parse_debug(" - Copy done, buffer %d bytes", fl->dataNeeded);
				}
			}
		}
	}
	return true;
}

void
ExecStartPhysical2(XfConn conn, PGconn *mc, ReplicationCommand *cmd)
{
	bool endofwal = false;
	char *buf;
	int len;
	XLogRecPtr startReceivingFrom;
	ReplMessage *msg = xfalloc(sizeof(ReplMessage));
	FilterData fl;


	fl.state = FS_SYNCHRONIZING;
	fl.dataNeeded = 0;
	fl.recordRemaining = 0;
	fl.synchronized = false;
	fl.requestedStartPos = cmd->startpoint;
	fl.recordStart = 0;
	fl.headerPos = -1;
	fl.headerLen = 0;
	fl.bufferLen = 0;
	fl.unsentBufferLen = 0;

	if (conn->include_tablespaces)
	{
		xf_info("Including tablespaces: %s", conn->include_tablespaces);
		fl.include_tablespaces = xlf_find_tablespace_oids(conn);
	} else {
		fl.include_tablespaces = NULL;
	}


	startReceivingFrom = cmd->startpoint;
again:
	xlf_startstreaming(mc, startReceivingFrom, cmd->timeline);

	SendCopyBothResponse(conn);

	while (!endofwal)
	{
		int len;
		XfProcessRepliesIfAny(conn);
		len = xlf_receive(mc, NAPTIME, &buf);
		if (len != 0)
		{
			for (;;)
			{
				if (len > 0)
				{
					xlf_process_message(mc, buf, len, msg);
					if (msg->type == MSG_WAL_DATA)
					{
						XLogRecPtr restartPos;
						if (!ProcessWalDataBlock(msg, &fl, &restartPos))
						{
							TimeLineID tli;
							xlf_endstreaming(mc, &tli);
							Assert(tli == 0);
							startReceivingFrom = restartPos;
							goto again;
						}
						XfSendWalBlock(conn, msg, &fl);
					}
				} else if (len == 0)
					break;
				else if (len < 0)
				{
					printf("End of WAL\n");
					endofwal = true;
					break;
				}
				len = xlf_receive(mc, 0, &buf);
			}
		}
		else
		{
		}
	}
	{
		TimeLineID tli;
		xlf_endstreaming(mc, &tli);
	}

	// query master server for data
	// send CopyBoth to client
	//     'W' b0 h0
	// flush
	// enter loop
	//     fetch data from master
	//		handle master keepalives
	//     process replies from client
	//			copy done
	//				send 'c'
	//			data
	//				replication status
	//				hot standby feedback
	//			exit
	//	   filter data
	//		check for timeout
	//	   push data out
	//			need to ensure aligned on record or page boundary
	//			'd' 'w' l(dataStart) l(walEnd) l(sendTime) s[WALdata]
	//			send keepalive if necessary
	//				'd' 'k' l(sentPtr) l(sentTime) b(RequestReply)
	// sendTimelineIsHistoric
	//	'T' h(2)
	//		s("next_tli") i(0) h(0) i(INT8OID) h(-1) i(0) h(2)
	//		s("next_tli_startpos") i(0) h(0) i(TEXTOID) h(-1) i(0) h(2)
	//	'D' h(2)
	//		i(strlen(tli_str)) p(tli_str)
	//		i(strlen(startpos_str)) p(startpos_str)
	// pq_puttextmessage('C', "START_STREAMING")
	xffree(msg);
}

ExecCommand(XfConn conn, PGconn *mc, char *query_string)
{
	int parse_rc;
	ReplicationCommand *cmd;

	replication_scanner_init(query_string);
	parse_rc = replication_yyparse();

	if (parse_rc != 0)
		error("Parse failed");

	cmd = replication_parse_result;

	xf_info("Query: %s\n", query_string);

	switch (cmd->command)
	{
		case REPL_IDENTIFY_SYSTEM:
			ExecIdentifySystem(conn, mc);
			break;
		case REPL_BASE_BACKUP:
		case REPL_CREATE_SLOT:
		case REPL_DROP_SLOT:
			error("Command not supported");
			break;
		case REPL_START_PHYSICAL:
			ExecStartPhysical2(conn, mc, cmd);
			break;
		case REPL_START_LOGICAL:
			error("Command not supported");
			break;
		case REPL_TIMELINE:
			ExecTimeline();
			//TODO
			break;
	}


	ConnBeginMessage(conn, 'C');
	ConnSendString(conn, "SELECT");
	ConnEndMessage(conn);
	xffree(cmd);
}

void
forbidden_in_wal_sender()
{
	error("Invalid command for walsender");
}

void XfPerformAuthentication(XfConn conn)
{
	int status = STATUS_ERROR;

	status = STATUS_OK;

	if (status == STATUS_OK)
	{
		xf_info("Send auth packet");
		ConnBeginMessage(conn, 'R');
		ConnSendInt(conn, (int32) AUTH_REQ_OK, sizeof(int32));
		ConnEndMessage(conn);
		ConnFlush(conn);
	}
	else
	{
		error("auth failed");
	}
}

static void
ReportGuc(XfConn conn, PGconn* mc, char *name)
{
	const char *value = PQparameterStatus(mc, name);
	if (!value)
		return;
	ConnBeginMessage(conn, 'S');
	ConnSendString(conn, name);
	ConnSendString(conn, value);
	ConnEndMessage(conn);
}

void
BeginReportingGUCOptions(XfConn conn, PGconn* mc)
{
	//conn->sendBufMsgLenPtr
	 ReportGuc(conn, mc, "server_version");
	 ReportGuc(conn, mc, "server_encoding");
	 ReportGuc(conn, mc, "client_encoding");
	 ReportGuc(conn, mc, "application_name");
	 ReportGuc(conn, mc, "is_superuser");
	 ReportGuc(conn, mc, "session_authorization");
	 ReportGuc(conn, mc, "DateStyle");
	 ReportGuc(conn, mc, "IntervalStyle");
	 ReportGuc(conn, mc, "TimeZone");
	 ReportGuc(conn, mc, "integer_datetimes");
	 ReportGuc(conn, mc, "standard_conforming_strings");
}

PGconn*
OpenConnectionToMaster(XfConn conn)
{
	PGconn* masterConn;
	char conninfo[MAX_CONNINFO_LEN+1];
	char *buf = conninfo;
	char *buf_end = &(conninfo[MAX_CONNINFO_LEN]);

	memset(conninfo, 0, sizeof(conninfo));

	if (conn->master_host) {
		buf += snprintf(buf, buf_end - buf, "host=%s ", conn->master_host);
	}

	if (conn->master_port)
		buf +=  snprintf(buf, buf_end - buf, "port=%s ", conn->master_port);

	if (conn->user_name)
		buf += snprintf(buf, buf_end - buf, "user=%s ", conn->user_name);

	buf += snprintf(buf, buf_end - buf, "dbname=replication replication=true application_name=walbouncer");

	xf_info("Start connecting to %s\n", conninfo);
	masterConn = PQconnectdb(conninfo);
	if (PQstatus(masterConn) != CONNECTION_OK)
		error(PQerrorMessage(masterConn));
	xf_info("Connected to master\n");
	return masterConn;
}


void
XfCommandLoop(XfConn conn)
{
	int firstchar;
	bool send_ready_for_query = true;
	PGconn* mc = OpenConnectionToMaster(conn);

	BeginReportingGUCOptions(conn, mc);

	// Cancel message
	ConnBeginMessage(conn, 'K');
	ConnSendInt(conn, 0, 4); // PID
	ConnSendInt(conn, 0, 4); // Cancel key
	ConnEndMessage(conn);

	// set up error handling

	for (;;)
	{
		char *msg;
		XfCommand cmd;
		cmd.msg = NULL;
		cmd.qtype = 0;
		if (send_ready_for_query)
		{
			ReadyForQuery(conn);
			send_ready_for_query = false;
		}

		firstchar = ReadCommand(conn, &cmd);
		xf_info("after read command\n");
		switch (firstchar)
		{
			case 'Q':
				{
					ExecCommand(conn, mc, cmd.msg->data);
					send_ready_for_query = true;

					/*char *query_string = pq_getmsgstgring(msg);
					pq_getmsgend(msg);

					ExecCommand(cmd->);

					send_ready_for_query = true;*/
				}
				break;
			case 'P':
			case 'B':
			case 'E':
			case 'F':
			case 'C':
			case 'D':
				forbidden_in_wal_sender();
				break;
			case 'H':
				ConnFlush(conn);
				break;
			case 'S':
				send_ready_for_query = true;
				break;
			case 'X':
			case EOF:
				// TODO: do something here?
				if (cmd.msg)
					ConnFreeMessage(cmd.msg);
				return;
			case 'd':
			case 'c':
			case 'f':
				break;
			default:
				error("invalid frontend message type");
		}
		if (cmd.msg)
			ConnFreeMessage(cmd.msg);
	}
}

char* listen_port = "5433";
char* master_host = "localhost";
char* master_port = "5432";

void XlogFilterMain()
{
	// set up signals for child reaper, etc.
	// open socket for listening
	XfSocket server = OpenServerSocket(listen_port);
	XfConn conn;

	conn = ConnCreate(server);
	conn->master_host = master_host;
	conn->master_port = master_port;

	XfInitConnection(conn);

	XfPerformAuthentication(conn);

	XfCommandLoop(conn);

	CloseConn(conn);

	CloseSocket(server);
}

const char* progname;

static void usage()
{
	printf("%s proxys PostgreSQL streaming replication connections and optionally does filtering\n\n", progname);
	printf("Options:\n");
	printf("  -?, --help                Print this message\n");
	printf("  -h, --host=HOST           Connect to master on this host. Default localhost\n");
	printf("  -p, --port=PORT           Run proxy on this port. Default 5433\n");

}

int
main(int argc, char **argv)
{
	int c;
	progname = "xlogfilter";

	while (1)
	{
		static struct option long_options[] =
		{
				{"port", required_argument, 0, 'p'},
				{"host", required_argument, 0, 'h'},
				{"masterport", required_argument, 0, 'P'},
				{"help", no_argument, 0, '?'},
				{0,0,0,0}
		};
		int option_index = 0;

		c = getopt_long(argc, argv, "p:h?",
				long_options, &option_index);

		if (c == -1)
			break;

		switch (c)
		{
		case 'p':
			listen_port = strdup(optarg);
			break;
		case 'h':
			master_host = strdup(optarg);
			break;
		case 'P':
			master_port = strdup(optarg);
			break;
		case '?':
			usage();
			exit(0);
			break;
		default:
			fprintf(stderr, "Invalid arguments\n");
			exit(1);
		}
	}


	XlogFilterMain();
	return 0;
}
