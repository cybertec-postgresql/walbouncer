#include "wbmasterconn.h"

#include<errno.h>
#include<poll.h>
#include<string.h>

#include "wbutils.h"
#include "wb_pg_config.h"

#include "libpq-fe.h"

static void WbMcProcessWalsenderMessage(MasterConn *master, ReplMessage *msg);
static void WbMcSend(MasterConn *master, const char *buffer, int nbytes);
static int WbMcReceiveWal(MasterConn *master, char **buffer);

struct MasterConn {
	PGconn* conn;
	char* recvBuf;
	XLogRecPtr latestWalEnd;
	TimestampTz latestSendTime;
};

MasterConn*
WbMcOpenConnection(const char *conninfo)
{
	MasterConn* master = wballoc0(sizeof(MasterConn));
	master->conn = PQconnectdb(conninfo);
	if (PQstatus(master->conn) != CONNECTION_OK)
		error(PQerrorMessage(master->conn));

	return master;
}

void
WbMcCloseConnection(MasterConn *master)
{
	if (master->recvBuf)
		PQfreemem(master->recvBuf);
	PQfinish(master->conn);
	wbfree(master);
}

int
WbMcGetSocket(MasterConn *master)
{
	return PQsocket(master->conn);
}

bool
WbMcStartStreaming(MasterConn *master, XLogRecPtr pos, TimeLineID tli)
{
	PGconn *mc = master->conn;
	char cmd[256];
	PGresult *res;

	log_info("Start streaming from master at %X/%X", FormatRecPtr(pos));

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
WbMcEndStreaming(MasterConn *master, TimeLineID *nextTli, char** nextTliStart)
{
	PGconn *mc = master->conn;
	PGresult   *res;

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

	if (PQresultStatus(res) == PGRES_TUPLES_OK && nextTli && nextTliStart)
	{
		/*
		 * Read the next timeline's ID. The server also sends the timeline's
		 * starting point, but it is ignored.
		 */
		if (PQnfields(res) < 2 || PQntuples(res) != 1)
			error("unexpected result set after end-of-streaming");
		if (nextTli)
			*nextTli = ensure_atoi(PQgetvalue(res, 0, 0));
		if (nextTliStart)
			*nextTliStart = wbstrdup(PQgetvalue(res, 0, 1));
		log_info("Ended streaming with master, received next TLI %u, start pos %s", *nextTli, *nextTliStart);

		PQclear(res);

		/* the result set should be followed by CommandComplete */
		res = PQgetResult(mc);
	}
	else
	{
		log_info("Ended streaming with master, no historic TLI information received");
		if (nextTli)
			*nextTli = 0;
		if (nextTliStart)
			*nextTliStart = NULL;
	}

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		error(PQerrorMessage(mc));

	/* Verify that there are no more results */
	res = PQgetResult(mc);
	while (res!=NULL) {
		log_debug1("Status while ending streaming: %d", PQresultStatus(res));
		res = PQgetResult(mc);
	}

	if (res != NULL)
		error("unexpected result after CommandComplete: %s", PQerrorMessage(mc));

}

bool
WbMcReceiveWalMessage(MasterConn *master, ReplMessage *msg)
{
	int len;
	char *buf;
	len = WbMcReceiveWal(master, &buf);
	if (len > 0)
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
					msg->nextPageBoundary = (XLOG_BLCKSZ - msg->dataStart) & (XLOG_BLCKSZ-1);

					log_debug1("Received %u byte WAL block. dataStart: %X/%X walEnd: %X/%X sendTime: %s",
							len-25,
							FormatRecPtr(msg->dataStart),
							FormatRecPtr(msg->walEnd),
							timestamptz_to_str(msg->sendTime));

					WbMcProcessWalsenderMessage(master, msg);
					break;
				}
			case 'k':
				{
					msg->type = MSG_KEEPALIVE;
					msg->walEnd = fromnetwork64(buf+1);
					msg->sendTime = fromnetwork64(buf+9);
					msg->replyRequested = *(buf+17);

					log_debug1("Received keepalive message. walEnd: %X/%X sendTime: %s",
												FormatRecPtr(msg->walEnd),
												timestamptz_to_str(msg->sendTime));

					WbMcProcessWalsenderMessage(master, msg);
					break;
				}
		}
	}
	else
		msg->type = len < 0 ? MSG_END_OF_WAL : MSG_NOTHING;

	return msg->type != MSG_NOTHING;
}


static int
WbMcReceiveWal(MasterConn *master, char **buffer)
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
	// TODO: this is not currently needed, revisit when featurecomplete.
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

void
WbMcSendReply(MasterConn *master, StandbyReplyMessage *reply, bool force, bool requestReply)
{
	XLogRecPtr writePtr = reply->writePtr;
	XLogRecPtr flushPtr = reply->flushPtr;
	XLogRecPtr	applyPtr = reply->applyPtr;
	TimestampTz sendTime = reply->sendTime;
	char reply_message[1+4*8+1];

	memset(reply_message, 0, sizeof(reply_message));
	reply_message[0] = 'r';
	write64(&(reply_message[1]), writePtr);
	write64(&(reply_message[9]), flushPtr);
	write64(&(reply_message[17]), applyPtr);
	write64(&(reply_message[25]), sendTime);
	reply_message[33] = requestReply ? 1 : 0;

	log_debug1("Send reply to master: write %X/%X flush %X/%X apply %X/%X sendtime %s%s",
			FormatRecPtr(writePtr),
			FormatRecPtr(flushPtr),
			FormatRecPtr(applyPtr),
			timestamptz_to_str(sendTime),
			requestReply ? " (reply requested" : "");
	WbMcSend(master, reply_message, sizeof(reply_message));
}

void
WbMcSendFeedback(MasterConn *master, HSFeedbackMessage *feedback)
{
	char feedback_message[1+8+4+4
	#if PG_VERSION >= 100000
		+4+4
	#endif
	];

	memset(feedback_message, 0, sizeof(feedback_message));

	feedback_message[0] = 'h';
	write64(&(feedback_message[1]), feedback->sendTime);
	write32(&(feedback_message[9]), feedback->xmin);
	write32(&(feedback_message[13]), feedback->xmin_epoch);
	write32(&(feedback_message[17]), feedback->catalog_xmin);
	write32(&(feedback_message[21]), feedback->catalog_xmin_epoch);

	log_debug1("Send HS feedback to master: xmin %d epoch %d sendtime %s",
			feedback->xmin,
			feedback->xmin_epoch,
			timestamptz_to_str(feedback->sendTime));

	WbMcSend(master, feedback_message, sizeof(feedback_message));
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
		*primary_sysid = wbstrdup(PQgetvalue(result, 0, 0));
	if (primary_tli)
		*primary_tli = wbstrdup(PQgetvalue(result, 0, 1));
	if (primary_xpos)
		*primary_xpos = wbstrdup(PQgetvalue(result, 0, 2));

	PQclear(result);
	return true;
}

bool
WbMcGetTimelineHistory(MasterConn* master, TimeLineID timeline,
		TimelineHistory *history)
{
	PGconn *mc = master->conn;
	char query[16+1+10+1];
	PGresult *result;

	sprintf(query, "TIMELINE_HISTORY %d", timeline);

	result = PQexec(mc, query);

	if (PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		// TODO: handle missing timeline case
		log_debug1("Timeline query returned %s", PQresStatus(PQresultStatus(result)));
		PQclear(result);
		error("Getting timeline history from master failed with: %s", PQerrorMessage(mc));
	}
	if (PQnfields(result) < 2 || PQntuples(result) != 1)
	{
		error("Invalid response for timeline history");
	}

	history->filename = wbstrdup(PQgetvalue(result, 0, 0));
	history->contentLen = PQgetlength(result, 0, 1);
	history->content = wballoc(history->contentLen);
    memcpy(history->content, PQgetvalue(result, 0, 1), history->contentLen);

    PQclear(result);
	return true;
}

char *
WbMcShowVariable(MasterConn* master, char *varname)
{
	PGconn *mc = master->conn;
	char query[1024];
	PGresult *result;
	char	*value;

	snprintf(query, 1024, "SHOW %s", varname);

	result = PQexec(mc, query);

	if (PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		log_debug1("Variable returned %s", PQresStatus(PQresultStatus(result)));
		PQclear(result);
		error("Getting variable from master failed with: %s", PQerrorMessage(mc));
	}
	if (PQntuples(result) != 1)
	{
		error("Invalid response for SHOW command: %d tuples", PQnfields(result));
	}

	value = wbstrdup(PQgetvalue(result, 0, 0));
    PQclear(result);
	return value;
}

Oid *
WbMcResolveOids(MasterConn *master, OidResolveKind kind, bool include, char** names, int n_items)
{
	Oid *oids;
	PGresult *res;
	int oidcount;
	int i;
	char sql[1000];
	int sqlpos = 0;
	const char * const* paramValues = (const char * const*) names;
	char *itemkind = "";

	switch (kind)
	{
		case OID_RESOLVE_TABLESPACES:
			sqlpos = snprintf(sql, sizeof(sql),
					"SELECT oid, spcname FROM pg_tablespace WHERE spcname IN (");
			itemkind = "tablespaces";
			if (include)
				sqlpos += snprintf(sql+sqlpos, (sizeof(sql) - sqlpos),
									"'pg_default', 'pg_global', ");
			break;
		case OID_RESOLVE_DATABASES:
			sqlpos = snprintf(sql, sizeof(sql),
					"SELECT oid, datname FROM pg_database WHERE datname IN (");
			itemkind = "databases";
			if (include)
				sqlpos += snprintf(sql+sqlpos, (sizeof(sql) - sqlpos),
									"'template0', 'template1', ");
			break;
	}

	for (i = 0; i < n_items; i++)
	{
		sqlpos += snprintf(sql + sqlpos, (sizeof(sql) - sqlpos), i ? ", $%d" : "$%d", i+1);
	}
	sqlpos += snprintf(sql + sqlpos, (sizeof(sql) - sqlpos), ");");
	if (sqlpos >= sizeof(sql))
		error("Too many %s specified", itemkind);

	res = PQexecParams(master->conn, sql,
		n_items, NULL, paramValues,
		NULL, NULL, 0);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		error("Could not retrieve %s: %s", itemkind, PQerrorMessage(master->conn));

	oidcount = PQntuples(res);
	oids = wballoc0(sizeof(Oid)*(oidcount+1));

	for (i = 0; i < oidcount; i++)
	{
		char *oid = PQgetvalue(res, i, 0);
		oids[i] = atoi(oid);
		log_debug1("Found %s oid for %s: %d", itemkind, PQgetvalue(res, i, 1), oids[i]);
	}
	PQclear(res);

	return oids;
}

const char *
WbMcParameterStatus(MasterConn *master, char *name)
{
	return PQparameterStatus(master->conn, name);
}
