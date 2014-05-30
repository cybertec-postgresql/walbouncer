#include "wbmasterconn.h"

#include<errno.h>
#include<poll.h>
#include<string.h>

#include "xfutils.h"
#include "xf_pg_config.h"

#include "libpq-fe.h"

static bool libpq_select(MasterConn *master, int timeout_ms);
static void WbMcProcessWalsenderMessage(MasterConn *master, ReplMessage *msg);
static void WbMcSend(MasterConn *master, const char *buffer, int nbytes);
static void WbMcSendReply(MasterConn *master, bool force, bool requestReply);

struct MasterConn {
	PGconn* conn;
	char* recvBuf;
	XLogRecPtr latestWalEnd;
	TimestampTz latestSendTime;
};

MasterConn*
WbMcOpenConnection(const char *conninfo)
{
	MasterConn* master = xfalloc0(sizeof(MasterConn));
	master->conn = PQconnectdb(conninfo);
	if (PQstatus(master->conn) != CONNECTION_OK)
		error(PQerrorMessage(master->conn));

	return master;
}

void
WbMcCloseConnection(MasterConn *master)
{
	PQfinish(master->conn);
	xffree(master);
}

bool
WbMcStartStreaming(MasterConn *master, XLogRecPtr pos, TimeLineID tli)
{
	PGconn *mc = master->conn;
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
WbMcEndStreaming(MasterConn *master, TimeLineID *next_tli)
{
	PGconn *mc = master->conn;
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

/*
 * Wait until we can read WAL stream, or timeout.
 *
 * Returns true if data has become available for reading, false if timed out
 * or interrupted by signal.
 *
 * This is based on pqSocketCheck.
 */
static bool
libpq_select(MasterConn *master, int timeout_ms)
{
	PGconn *mc = master->conn;
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
WbMcReceiveWal(MasterConn *master, int timeout, char **buffer)
{
	PGconn *mc = master->conn;
	int			rawlen;

	if (master->recvBuf != NULL)
		PQfreemem(master->recvBuf);
	master->recvBuf = NULL;

	/* Try to receive a CopyData message */
	rawlen = PQgetCopyData(mc, &(master->recvBuf), 1);
	if (rawlen == 0)
	{
		/*
		 * No data available yet. If the caller requested to block, wait for
		 * more data to arrive.
		 */
		if (timeout > 0)
		{
			if (!libpq_select(master, timeout))
				return 0;
		}

		if (PQconsumeInput(mc) == 0)
			showPQerror(mc, "could not receive data from WAL stream");

		/* Now that we've consumed some input, try again */
		rawlen = PQgetCopyData(mc, &(master->recvBuf), 1);
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
	*buffer = master->recvBuf;
	return rawlen;
}

static void
WbMcProcessWalsenderMessage(MasterConn *master, ReplMessage *msg)
{
	master->latestWalEnd = msg->walEnd;
	master->latestSendTime = msg->sendTime;
}

/*
 * Send a message to XLOG stream.
 *
 * ereports on error.
 */
static void
WbMcSend(MasterConn *master, const char *buffer, int nbytes)
{
	PGconn *mc = master->conn;
	if (PQputCopyData(mc, buffer, nbytes) <= 0 ||
		PQflush(mc))
		showPQerror(mc, "could not send data to WAL stream");
}

static void
WbMcSendReply(MasterConn *master, bool force, bool requestReply)
{
	PGconn *mc = master->conn;
	XLogRecPtr writePtr = master->latestWalEnd;
	XLogRecPtr flushPtr = master->latestWalEnd;
	XLogRecPtr	applyPtr = master->latestWalEnd;
	TimestampTz sendTime = master->latestSendTime;
	TimestampTz now;
	char reply_message[1+4*8+1+1];

	/*
	 * If the user doesn't want status to be reported to the master, be sure
	 * to exit before doing anything at all.

	if (!force && wal_receiver_status_interval <= 0)
		return;*/

	/* Get current timestamp. */
	now = master->latestSendTime;//GetCurrentTimestamp();

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
	WbMcSend(master, reply_message, 34);
}

void
WbMcProcessMessage(MasterConn *master, char *buf, size_t len,
		ReplMessage *msg)
{
	PGconn *mc = master->conn;
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
				msg->nextPageBoundary = (XLOG_BLCKSZ - msg->dataStart) & (XLOG_BLCKSZ-1);

				xf_info("Received %lu byte WAL block\n", len-25);
				xf_info("   dataStart: %X/%X\n", FormatRecPtr(msg->dataStart));
				xf_info("   walEnd: %lu\n", msg->walEnd);
				xf_info("   sendTime: %lu\n", msg->sendTime);

				WbMcProcessWalsenderMessage(master, msg);
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
				WbMcProcessWalsenderMessage(master, msg);

				if (msg->replyRequested)
					WbMcSendReply(master, true, false);
				break;
			}
	}
}

bool
WbMcIdentifySystem(MasterConn* master,
		char** primary_sysid, char** primary_tli, char** primary_xpos)
{
	PGconn *mc = master->conn;

	PGresult *result = PQexec(mc, "IDENTIFY_SYSTEM");
	if (PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		PQclear(result);
		error(PQerrorMessage(mc));
	}
	if (PQnfields(result) < 3 || PQntuples(result) != 1)
	{
		error("Invalid response");
	}

	if (primary_sysid)
		*primary_sysid = xfstrdup(PQgetvalue(result, 0, 0));
	if (primary_tli)
		*primary_tli = xfstrdup(PQgetvalue(result, 0, 1));
	if (primary_xpos)
		*primary_xpos = xfstrdup(PQgetvalue(result, 0, 2));

	PQclear(result);
	return true;
}

Oid *
WbMcResolveTablespaceOids(const char *conninfo, const char* tablespace_names)
{
	MasterConn* master = WbMcOpenConnection(conninfo);
	Oid *oids;
	PGresult *res;
	int oidcount;
	int i;

	const char *paramValues[1] = {tablespace_names};
	res = PQexecParams(master->conn,
		"SELECT oid FROM pg_tablespace WHERE spcname = "
		"ANY (string_to_array($1, ',')) "
		"OR spcname IN ('pg_default', 'pg_global')",
		1, NULL, paramValues,
		NULL, NULL, 0);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		error("Could not retrieve tablespaces: %s", PQerrorMessage(master->conn));

	oidcount = PQntuples(res);
	oids = xfalloc0(sizeof(Oid)*(oidcount+1));

	for (i = 0; i < oidcount; i++)
	{
		char *oid = PQgetvalue(res, i, 0);
		xf_info("Found tablespace oid %s %d", oid, atoi(oid));
		oids[i] = atoi(oid);
	}
	PQclear(res);

	WbMcCloseConnection(master);

	return oids;
}

const char *
WbMcParameterStatus(MasterConn *master, char *name)
{
	return PQparameterStatus(master->conn, name);
}
