#include "wbclientconn.h"

#include <arpa/inet.h>
#include <errno.h>
#include <poll.h>
#include <string.h>

#include "wbsocket.h"
#include "wbutils.h"
#include "wbfilter.h"
#include "wbmasterconn.h"

#include "parser/parser.h"

#define MAX_CONNINFO_LEN 4000
#define NAPTIME 60000

typedef struct {
	int qtype;
	WbMessage *msg;
} XfCommand;

typedef struct {
	char *name;
	Oid	type;
	void *value;
	int valueLen;
} ResultCol;


static int WbCCProcessStartupPacket(WbConn conn, bool SSLdone);
static int WbCCReadCommand(WbConn conn, XfCommand *cmd);
static void WbCCSendReadyForQuery(WbConn conn);
static MasterConn* WbCCOpenConnectionToMaster(WbConn conn);
static void ForbiddenInWalBouncer();
static void WbCCBeginReportingGUCOptions(WbConn conn, MasterConn* master);
static void WbCCReportGuc(WbConn conn, MasterConn* master, char *name);
static void WbCCExecCommand(WbConn conn, MasterConn *master, char *query_string);
static void WbCCExecIdentifySystem(WbConn conn, MasterConn *master);
static bool WbCCWaitForData(WbConn conn, MasterConn *master);
static void WbCCExecStartPhysical(WbConn conn, MasterConn *master, ReplicationCommand *cmd);
static void WbCCExecTimeline(WbConn conn, MasterConn *master, ReplicationCommand *cmd);
static void WbCCExecShow(WbConn conn, MasterConn *master, ReplicationCommand *cmd);
static void WbCCLookupFilteringOids(WbConn conn, FilterData *fl);
//static void WbCCSendWALRecord(XfConn conn, char *data, int len, XLogRecPtr sentPtr, TimestampTz lastSend);
//static void WbCCSendEndOfWal(XfConn conn);
static void WbCCProcessRepliesIfAny(WbConn conn);
static void WbCCProcessReplyMessage(WbConn conn);
static void WbCCProcessStandbyReplyMessage(WbConn conn, WbMessage *msg);
static void WbCCSendKeepalive(WbConn conn, bool request_reply);
static void WbCCProcessStandbyHSFeedbackMessage(WbConn conn, WbMessage *msg);
static void WbCCForwardPendingReplies(WbConn conn, MasterConn* master);
static void WbCCSendCopyBothResponse(WbConn conn);
static void WbCCSendWalBlock(WbConn conn, ReplMessage *msg, FilterData *fl);
static void WbCCSendResultset(WbConn conn, int ncols, ResultCol *cols);
static void WbCCSendErrorReport(WbConn conn, LogLevel level, char *message, char* detail);



void
WbCCInitConnection(WbConn conn)
{
	log_info("Received conn from %08X:%d", conn->client.addr, conn->client.port);

	//FIXME: need to timeout here
	// setup error log destination
	// copy socket info out here
	if (WbCCProcessStartupPacket(conn, false) != STATUS_OK)
		error("Error while processing startup packet");
}

void
WbCCPerformAuthentication(WbConn conn)
{
	int status = STATUS_ERROR;

	status = STATUS_OK;

	if (status == STATUS_OK)
	{
		log_debug1("Sending authentication packet");
		ConnBeginMessage(conn, 'R');
		ConnSendInt(conn, (int32) AUTH_REQ_OK, sizeof(int32));
		ConnEndMessage(conn);
		ConnFlush(conn, FLUSH_IMMEDIATE);
	}
	else
	{
		error("auth failed");
	}
}

static bool
WbCCMatchConfigEntry(WbConn conn)
{
	wb_config_list_entry *listitem;
	for (listitem = CurrentConfig->configurations;
		 listitem;
		 listitem = listitem->next)
	{
		wb_config_entry *entry = &listitem->entry;
		log_debug2("Matching config entry %s", entry->name);

		if (entry->match.application_name)
		{
			if (!conn->application_name ||
					strcmp(conn->application_name,
							entry->match.application_name) != 0)
				continue;
			log_debug2("application_name matches");
		}

		if (entry->match.source_ip.mask > 0)
		{
			if (!match_hostmask(&entry->match.source_ip, conn->client.addr))
				continue;
			log_debug2("Source IP mask matches");
		}
		log_debug2("Matched config entry %s", entry->name);
		conn->configEntry = entry;
		return true;
	}
	return false;
}

static int
WbCCProcessStartupPacket(WbConn conn, bool SSLdone)
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
		buf = wballoc0(sizeof(StartupPacket) + 1);
	else
		buf = wballoc0(len + 1);

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
		return WbCCProcessStartupPacket(conn, true);
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

	conn->guc_options = wballoc0(len);
	conn->gucs_len = 0;
	conn->database_name = NULL;
	conn->user_name = NULL;
	conn->cmdline_options = NULL;
	conn->application_name = NULL;

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
				conn->database_name = wbstrdup(valptr);
			else if (strcmp(nameptr, "user") == 0)
				conn->user_name = wbstrdup(valptr);
			else if (strcmp(nameptr, "options") == 0)
				conn->cmdline_options = wbstrdup(valptr);
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
				conn->application_name = wbstrdup(valptr);
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

	/* Match the config entry */
	if (!WbCCMatchConfigEntry(conn))
		error("No configuration entry matches the connection");

	return STATUS_OK;
}


void
WbCCCommandLoop(WbConn conn)
{
	int firstchar;
	bool send_ready_for_query = true;
	MasterConn* master = WbCCOpenConnectionToMaster(conn);

	WbCCBeginReportingGUCOptions(conn, master);

	// Cancel message
	ConnBeginMessage(conn, 'K');
	ConnSendInt(conn, 0, 4); // PID
	ConnSendInt(conn, 0, 4); // Cancel key
	ConnEndMessage(conn);

	// set up error handling

	for (;;)
	{
		XfCommand cmd;
		cmd.msg = NULL;
		cmd.qtype = 0;
		if (send_ready_for_query)
		{
			WbCCSendReadyForQuery(conn);
			send_ready_for_query = false;
		}

		firstchar = WbCCReadCommand(conn, &cmd);

		switch (firstchar)
		{
			case 'Q':
				{
					WbCCExecCommand(conn, master, cmd.msg->data);
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
				ForbiddenInWalBouncer();
				break;
			case 'H':
				ConnFlush(conn, FLUSH_IMMEDIATE);
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

static void
ForbiddenInWalBouncer()
{
	error("Invalid command for walsender");
}



static int
WbCCReadCommand(WbConn conn, XfCommand *cmd)
{
	int qtype;
	cmd->qtype = qtype = ConnGetByte(conn);

	if (qtype == EOF)
		return EOF;

	if (ConnGetMessage(conn, &(cmd->msg)))
		return EOF;

	log_debug1("Command %c payload %d", qtype, cmd->msg->len);

	return qtype;
}

static void
WbCCSendReadyForQuery(WbConn conn)
{
	ConnBeginMessage(conn, 'Z');
	ConnSendInt(conn, 'I', 1);
	ConnEndMessage(conn);
	ConnFlush(conn, FLUSH_IMMEDIATE);
}

static MasterConn*
WbCCOpenConnectionToMaster(WbConn conn)
{
	MasterConn* master;
	char conninfo[MAX_CONNINFO_LEN+1];
	char *buf = conninfo;
	char *buf_end = &(conninfo[MAX_CONNINFO_LEN]);

	memset(conninfo, 0, sizeof(conninfo));

	if (conn->master_host) {
		buf += snprintf(buf, buf_end - buf, "host=%s ", conn->master_host);
	}

	if (conn->master_port)
		buf +=  snprintf(buf, buf_end - buf, "port=%d ", conn->master_port);

	if (conn->user_name)
		buf += snprintf(buf, buf_end - buf, "user=%s ", conn->user_name);

	buf += snprintf(buf, buf_end - buf, "dbname=replication replication=true application_name=walbouncer");

	log_info("Start connecting to %s", conninfo);
	master = WbMcOpenConnection(conninfo);
	log_info("Connected to master");
	return master;
}

static void
WbCCBeginReportingGUCOptions(WbConn conn, MasterConn* master)
{
	//conn->sendBufMsgLenPtr
	 WbCCReportGuc(conn, master, "server_version");
	 WbCCReportGuc(conn, master, "server_encoding");
	 WbCCReportGuc(conn, master, "client_encoding");
	 WbCCReportGuc(conn, master, "application_name");
	 WbCCReportGuc(conn, master, "is_superuser");
	 WbCCReportGuc(conn, master, "session_authorization");
	 WbCCReportGuc(conn, master, "DateStyle");
	 WbCCReportGuc(conn, master, "IntervalStyle");
	 WbCCReportGuc(conn, master, "TimeZone");
	 WbCCReportGuc(conn, master, "integer_datetimes");
	 WbCCReportGuc(conn, master, "standard_conforming_strings");
}

static void
WbCCReportGuc(WbConn conn, MasterConn* master, char *name)
{
	const char *value = WbMcParameterStatus(master, name);
	if (!value)
		return;
	ConnBeginMessage(conn, 'S');
	ConnSendString(conn, name);
	ConnSendString(conn, value);
	ConnEndMessage(conn);
}

static void
WbCCExecCommand(WbConn conn, MasterConn *master, char *query_string)
{
	int parse_rc;
	ReplicationCommand *cmd;

	replication_scanner_init(query_string);
	parse_rc = replication_yyparse();

	if (parse_rc != 0)
		error("Parse failed");

	cmd = replication_parse_result;

	log_info("Received query from client: %s", query_string);

	switch (cmd->command)
	{
		case REPL_IDENTIFY_SYSTEM:
			WbCCExecIdentifySystem(conn, master);
			break;
		case REPL_BASE_BACKUP:
		case REPL_CREATE_SLOT:
		case REPL_DROP_SLOT:
			error("Command not supported");
			break;
		case REPL_START_PHYSICAL:
			WbCCExecStartPhysical(conn, master, cmd);
			break;
		case REPL_START_LOGICAL:
			error("Command not supported");
			break;
		case REPL_TIMELINE:
			WbCCExecTimeline(conn, master, cmd);
			break;
		case REPL_SHOW_VAR:
			WbCCExecShow(conn, master, cmd);
			break;
	}


	ConnBeginMessage(conn, 'C');
	ConnSendString(conn, "SELECT");
	ConnEndMessage(conn);
	wbfree(cmd);
}

/* TODO: move these to PG version specific config file */
#define TEXTOID 25
#define INT8OID 20
#define INT4OID 23
#define BYTEAOID 17

static void
WbCCExecIdentifySystem(WbConn conn, MasterConn *master)
{
	// query master server, pass through data
	char *primary_sysid;
	char *primary_tli;
	char *primary_xpos;
	char *dbname = NULL;

	if (!WbMcIdentifySystem(master,
			&primary_sysid,
			&primary_tli,
			&primary_xpos))
		error("Identify system failed.");

	log_info("Received system information from master:\n"
			"    System identifier: %s\n"
			"    TLI: %s\n"
			"    Xpos: %s\n"
			"    dbname: %s",
			primary_sysid, primary_tli, primary_xpos, dbname);

	//TODO: parse out tli and xpos for our use

	{
		ResultCol cols[4] = {
				{"systemid", TEXTOID, primary_sysid, 0},
				{"timeline", INT4OID, primary_tli, 0},
				{"xlogpos", TEXTOID, primary_xpos, 0},
				{"dbname", TEXTOID, dbname, 0}
		};
		WbCCSendResultset(conn, 4, cols);
	}

	wbfree(primary_sysid);
	wbfree(primary_tli);
	wbfree(primary_xpos);
}

/*
 * Wait for new data on master or slave connections depending on state.
 * Returns true if anything interesting happened.
 */
static bool
WbCCWaitForData(WbConn conn, MasterConn *master)
{
	struct pollfd fds[2];
	int ret;
	int numfds = 0;

	fds[numfds].fd = ConnGetSocket(conn);
	fds[numfds].events = POLLIN | POLLERR;
	fds[numfds].revents = 0;
	numfds++;

	if (ConnHasDataToFlush(conn))
	{
		/*
		 * If we are in process of flushing out a message to slave we only
		 * care if we can resume sending or the slave has sent us a reply
		 * message we need to relay back to the master.
		 *
		 * We don't care about failed master connections at this point as we
		 * want to finish sending processed WAL out.
		 **/
		 fds[0].events |= POLLOUT;
	} else {
		/*
		 * If we are finished forwarding data to the the slave we want to get
		 * a new message from the master.
		 */
		fds[numfds].fd = WbMcGetSocket(master);
		if (fds[numfds].fd == -1)
			error("Master socket has been closed");
		fds[numfds].events = POLLIN | POLLERR;
		fds[numfds].revents = 0;
		numfds++;
	}

	log_debug2("Waiting up to %dms on %d file descriptors", NAPTIME, numfds);
	ret = poll(fds, numfds, NAPTIME);

	if (ret == 0 || (ret < 0 && errno == EINTR))
		return false;

	return true;
}


static void
WbCCSendResultset(WbConn conn, int ncols, ResultCol *cols)
{
	int i;

	ConnBeginMessage(conn, 'T');

	ConnSendInt(conn, ncols, 2);

	for (i = 0; i < ncols; i++)
	{
		ConnSendString(conn, cols[i].name);
		ConnSendInt(conn, 0, 4); /* table oid */
		ConnSendInt(conn, 0, 2); /* attnum */
		ConnSendInt(conn, cols[i].type, 4); /* type oid */
		ConnSendInt(conn, -1, 2);
		ConnSendInt(conn, 0, 4);
		ConnSendInt(conn, 0, 2);
	}
	ConnEndMessage(conn);

	ConnBeginMessage(conn, 'D');
	ConnSendInt(conn, ncols, 2);

	for (i = 0; i < ncols; i++)
	{
		if (!cols[i].value)
		{
			ConnSendInt(conn, -1, 4);
			continue;
		}
		switch (cols[i].type)
		{
		case INT4OID: // We only textual version of int4oid
		case TEXTOID:
		{
			char *value = (char*) cols[i].value;
			size_t len = strlen(value);
			ConnSendInt(conn, len, 4);
			ConnSendBytes(conn, value, len);
			break;
		}
		case BYTEAOID:
			ConnSendInt(conn, cols[i].valueLen, 4);
			ConnSendBytes(conn, cols[i].value, cols[i].valueLen);
			break;
		case INT8OID:
		{
			// This is a bit of a hack, but as long as we only need this for the
			// next TLI value it works.
			uint32 value = *(uint32*) cols[i].value;
			char buf[11];
			size_t len;
			len = snprintf(buf, 11, "%u", (uint32) value);
			ConnSendInt(conn, len, 4);
			ConnSendBytes(conn, buf, len);
			break;
		}
		default:
			error("Unexpected OID for resultset");
		}
	}

	ConnEndMessage(conn);
}


static void
WbCCExecStartPhysical(WbConn conn, MasterConn *master, ReplicationCommand *cmd)
{
	bool endofwal = false;
	XLogRecPtr startReceivingFrom;
	ReplMessage *msg = wballoc(sizeof(ReplMessage));
	FilterData *fl = WbFCreateProcessingState(cmd->startpoint);
	int server_version, xlog_page_magic;

	/*
	 * Each page of XLOG file has a header like this:
	 */
	server_version = atoi(WbMcShowVariable(master, "server_version_num"));
	if (server_version >= 180000)
		xlog_page_magic = 0xD118;
	else if (server_version >= 170000)
		xlog_page_magic = 0xD116;
	else if (server_version >= 160000)
		xlog_page_magic = 0xD113;
	else if (server_version >= 150000)
		xlog_page_magic = 0xD110;
	else if (server_version >= 140000)
		xlog_page_magic = 0xD10D;
	else if (server_version >= 130000)
		xlog_page_magic = 0xD106;
	else
		error("Unsupported master version %d", server_version);

	WbCCLookupFilteringOids(conn, fl);

	WbCCSendCopyBothResponse(conn);

	startReceivingFrom = cmd->startpoint;
again:
	WbMcStartStreaming(master, startReceivingFrom, cmd->timeline);

	while (!endofwal)
	{
		if (!DaemonIsAlive())
			error("Master died, exiting!");

		if (!WbCCWaitForData(conn, master))
			continue;

		WbCCProcessRepliesIfAny(conn);
		WbCCForwardPendingReplies(conn, master);

		if (ConnHasDataToFlush(conn))
		{
			ConnFlush(conn, FLUSH_ASYNC);
			continue;
		}

		if (conn->copyDoneSent && conn->copyDoneReceived)
			break;

		if (WbMcReceiveWalMessage(master, msg))
		{
			switch (msg->type)
			{
				case MSG_END_OF_WAL:
					log_info("End of WAL");
					log_debug1("Sending CopyDone to client");
					ConnBeginMessage(conn, 'c');
					ConnEndMessage(conn);
					// TODO handle waiting for client CopyDone reply.
					endofwal = true;
					break;
				case MSG_WAL_DATA:
				{
					XLogRecPtr restartPos;
					if (!WbFProcessWalDataBlock(msg, fl, &restartPos, xlog_page_magic))
					{
						WbMcEndStreaming(master, NULL, NULL);
						startReceivingFrom = restartPos;
						goto again;
					}
					WbCCSendWalBlock(conn, msg, fl);
					break;
				}
				case MSG_KEEPALIVE:
					conn->lastSend = msg->sendTime;
					WbCCSendKeepalive(conn, msg->replyRequested);
					break;
				case MSG_NOTHING:
					// Nothing received, we loop back around and wait for data.
					break;
			}
		}
	}
	{
		TimeLineID nextTli;
		char *nextTliStart;
		WbMcEndStreaming(master, &nextTli, &nextTliStart);

		if (nextTli && nextTliStart)
		{
			ResultCol cols[2] = {
					{ "next_tli", INT8OID, &nextTli, 0},
					{ "next_tli_startpos", TEXTOID, nextTliStart, 0}
			};
			WbCCSendResultset(conn, 2, cols);
			wbfree(nextTliStart);
		}
		ConnBeginMessage(conn, 'C');
		ConnSendString(conn, "START_STREAMING");
		ConnEndMessage(conn);
	}

	WbFFreeProcessingState(fl);
	wbfree(msg);
}

static void
WbCCExecTimeline(WbConn conn, MasterConn *master, ReplicationCommand *cmd)
{
	TimelineHistory history;

	log_info("Received request for timeline %d", cmd->timeline);

	WbMcGetTimelineHistory(master, cmd->timeline, &history);

	{
		ResultCol cols[2] = {
				{ "filename", TEXTOID, history.filename, 0},
				{ "content", BYTEAOID, history.content, history.contentLen}
		};
		WbCCSendResultset(conn, 2, cols);
	}

	log_info("Sending out timeline history file %s", history.filename);

	wbfree(history.filename);
	wbfree(history.content);
}

static void
WbCCExecShow(WbConn conn, MasterConn *master, ReplicationCommand *cmd)
{
	char	*value;

	log_info("Received request for variable %s", cmd->varname);

	value = WbMcShowVariable(master, cmd->varname);

	{
		ResultCol cols[1] = {
				{ cmd->varname, TEXTOID, value, 0}
		};
		WbCCSendResultset(conn, 1, cols);
	}

	log_info("Sent out variable value %s", value);

	wbfree(value);
}

static void
WbCCLookupFilteringOids(WbConn conn, FilterData *fl)
{
	// TODO: take in other options
	char conninfo[MAX_CONNINFO_LEN+1];
	char *buf = conninfo;
	char *buf_end = &(conninfo[MAX_CONNINFO_LEN]);
	MasterConn* master;

	if (!conn->configEntry)
		return;

	if ((conn->configEntry->filter.n_include_tablespaces +
		 conn->configEntry->filter.n_include_databases +
		 conn->configEntry->filter.n_exclude_tablespaces +
		 conn->configEntry->filter.n_exclude_databases) == 0)
		return;

	memset(conninfo, 0, sizeof(conninfo));

	if (conn->master_host) {
		buf += snprintf(buf, buf_end - buf, "host=%s ", conn->master_host);
	}

	if (conn->master_port)
		buf +=  snprintf(buf, buf_end - buf, "port=%d ", conn->master_port);

	if (conn->user_name)
		buf += snprintf(buf, buf_end - buf, "user=%s ", conn->user_name);

	buf += snprintf(buf, buf_end - buf, "dbname=postgres application_name=walbouncer");

	master = WbMcOpenConnection(conninfo);

	if (conn->configEntry->filter.n_include_tablespaces)
		fl->include_tablespaces = WbMcResolveOids(master,
				OID_RESOLVE_TABLESPACES, true,
				conn->configEntry->filter.include_tablespaces,
				conn->configEntry->filter.n_include_tablespaces);
	if (conn->configEntry->filter.n_include_databases)
		fl->include_databases = WbMcResolveOids(master,
				OID_RESOLVE_DATABASES, true,
				conn->configEntry->filter.include_databases,
				conn->configEntry->filter.n_include_databases);
	if (conn->configEntry->filter.n_exclude_tablespaces)
		fl->exclude_tablespaces = WbMcResolveOids(master,
				OID_RESOLVE_TABLESPACES, false,
				conn->configEntry->filter.exclude_tablespaces,
				conn->configEntry->filter.n_exclude_tablespaces);
	if (conn->configEntry->filter.n_exclude_databases)
		fl->exclude_databases = WbMcResolveOids(master,
				OID_RESOLVE_DATABASES, false,
				conn->configEntry->filter.exclude_databases,
				conn->configEntry->filter.n_exclude_databases);

	{
		char buf[32000];
		int i;
		int pos = 0;

		if (conn->configEntry->filter.n_include_tablespaces)
		{
			if (pos)
				pos += snprintf(buf+pos, sizeof(buf) - pos, " ");
			pos += snprintf(buf+pos, sizeof(buf) - pos, "Tablespaces included: ");
			for (i = 0; i < conn->configEntry->filter.n_include_tablespaces; i++)
				pos += snprintf(buf+pos, sizeof(buf) - pos, i ? ", %s" : "%s",
						conn->configEntry->filter.include_tablespaces[i]);
		}

		if (conn->configEntry->filter.n_exclude_tablespaces)
		{
			if (pos)
				pos += snprintf(buf+pos, sizeof(buf) - pos, " ");
			pos += snprintf(buf+pos, sizeof(buf) - pos, "Tablespaces excluded: ");
			for (i = 0; i < conn->configEntry->filter.n_exclude_tablespaces; i++)
				pos += snprintf(buf+pos, sizeof(buf) - pos, i ? ", %s" : "%s",
						conn->configEntry->filter.exclude_tablespaces[i]);

		}
		if (conn->configEntry->filter.n_include_databases)
		{
			if (pos)
				pos += snprintf(buf+pos, sizeof(buf) - pos, " ");
			pos += snprintf(buf+pos, sizeof(buf) - pos, "Databases included: ");
			for (i = 0; i < conn->configEntry->filter.n_include_databases; i++)
				pos += snprintf(buf+pos, sizeof(buf) - pos, i ? ", %s" : "%s",
						conn->configEntry->filter.include_databases[i]);
		}
		if (conn->configEntry->filter.n_exclude_databases)
		{
			if (pos)
				pos += snprintf(buf+pos, sizeof(buf) - pos, " ");
			pos += snprintf(buf+pos, sizeof(buf) - pos, "Databases excluded: ");
			for (i = 0; i < conn->configEntry->filter.n_exclude_databases; i++)
				pos += snprintf(buf+pos, sizeof(buf) - pos, i ? ", %s" : "%s",
						conn->configEntry->filter.exclude_databases[i]);
		}
		WbCCSendErrorReport(conn, LOG_INFO, "WAL stream is being filtered", buf);
	}

	WbMcCloseConnection(master);
}
/* TODO: Probably not necessary
static void
WbCCSendWALRecord(XfConn conn, char *data, int len, XLogRecPtr sentPtr, TimestampTz lastSend)
{
	log_info("Sending out %d bytes of WAL\n", len);
	ConnBeginMessage(conn, 'd');
	ConnSendBytes(conn, data, len);
	ConnEndMessage(conn);
	conn->sentPtr = sentPtr;
	conn->lastSend = lastSend;
	ConnFlush(conn);
}*/

static void
WbCCSendEndOfWal(WbConn conn)
{
	ConnBeginMessage(conn, 'c');
	ConnEndMessage(conn);
	//ConnFlush(conn);
	conn->copyDoneSent = true;
}

static void
WbCCProcessRepliesIfAny(WbConn conn)
{
	char firstchar;
	int r;

	// TODO: record last receive timestamp here

	for (;;)
	{
		r = ConnGetByteIfAvailable(conn, &firstchar);
		if (r < 0)
		{
			error("Unexpected EOF from receiver");
		}
		if (r == 0)
			break;

		if (conn->copyDoneReceived && firstchar != 'X')
			error("Unexpected standby message type \"%c\", after receiving CopyDone",
					firstchar);

		switch (firstchar)
		{
			case 'd':
				WbCCProcessReplyMessage(conn);
				break;
			case 'c':
				if (!conn->copyDoneSent)
					WbCCSendEndOfWal(conn);
				// consume the CopyData message
				{
					WbMessage *msg;
					if (ConnGetMessage(conn, &msg))
						error("Unexpected EOF on standby connection");
					ConnFreeMessage(msg);
				}
				conn->copyDoneReceived = true;
				break;
			case 'X':
				error("Standby is closing the socket");
			default:
				error("Invalid standby message");
		}
	}
}

static void
WbCCProcessReplyMessage(WbConn conn)
{
	WbMessage *msg;
	if (ConnGetMessage(conn, &msg))
		error("unexpected EOF from receiver");

	switch (msg->data[0])
	{
		case 'r':
			WbCCProcessStandbyReplyMessage(conn, msg);
			break;
		case 'h':
			WbCCProcessStandbyHSFeedbackMessage(conn, msg);
			break;
		default:
			error("Unexpected message type");
	}

	ConnFreeMessage(msg);
}

static void
WbCCProcessStandbyReplyMessage(WbConn conn, WbMessage *msg)
{
	StandbyReplyMessage *reply = &(conn->lastReply);

	/* the caller already consumed the msgtype byte */
	reply->writePtr = fromnetwork64(msg->data + 1);
	reply->flushPtr = fromnetwork64(msg->data + 9);
	reply->applyPtr = fromnetwork64(msg->data + 17);
	reply->sendTime = fromnetwork64(msg->data + 25);		/* sendTime; not used ATM */
	reply->replyRequested = msg->data[33];

	log_debug1("Standby reply msg: write %X/%X flush %X/%X apply %X/%X sendTime %s%s",
		 FormatRecPtr(reply->writePtr),
		 FormatRecPtr(reply->flushPtr),
		 FormatRecPtr(reply->applyPtr),
		 timestamptz_to_str(reply->sendTime),
		 reply->replyRequested ? " (reply requested)" : "");

	/* Send a reply if the standby requested one. */
	if (reply->replyRequested)
		WbCCSendKeepalive(conn, false);

	conn->replyForwarded = false;
}

static void
WbCCSendKeepalive(WbConn conn, bool request_reply)
{
	log_debug1("sending keepalive message %X/%X%s",
			(uint32) (conn->sentPtr>>32),
			(uint32) conn->sentPtr,
			request_reply ? " (reply requested)" : "");

	ConnBeginMessage(conn, 'd');
	ConnSendInt(conn, 'k', 1);
	ConnSendInt64(conn, conn->sentPtr);
	ConnSendInt64(conn, conn->lastSend);
	ConnSendInt(conn, request_reply ? 1 : 0, 1);
	ConnEndMessage(conn);
}

static void
WbCCProcessStandbyHSFeedbackMessage(WbConn conn, WbMessage *msg)
{
	HSFeedbackMessage *feedback = &(conn->lastFeedback);
	/*
	 * Decipher the reply message. The caller already consumed the msgtype
	 * byte.
	 */
	feedback->sendTime = fromnetwork64(msg->data + 1);		/* sendTime; not used ATM */
	feedback->xmin = fromnetwork32(msg->data + 9);
	feedback->xmin_epoch = fromnetwork32(msg->data + 13);
	#if PG_VERSION >= 100000
	feedback->catalog_xmin = fromnetwork32(msg->data + 17);
	feedback->catalog_xmin_epoch = fromnetwork32(msg->data + 21);
	#endif

	log_debug1("hot standby feedback xmin %u epoch %u sendTime %s",
		 feedback->xmin,
		 feedback->xmin_epoch,
		 timestamptz_to_str(feedback->sendTime));

	conn->feedbackForwarded = false;
}

static void
WbCCForwardPendingReplies(WbConn conn, MasterConn* master)
{
	if (!conn->replyForwarded)
	{
		WbMcSendReply(master, &(conn->lastReply), false, false);
		conn->replyForwarded = true;
	}
	if (!conn->feedbackForwarded)
	{
		WbMcSendFeedback(master, &(conn->lastFeedback));
		conn->feedbackForwarded = true;
	}
}

static void
WbCCSendCopyBothResponse(WbConn conn)
{
	/* Send a CopyBothResponse message, and start streaming */
	ConnBeginMessage(conn, 'W');
	ConnSendInt(conn, 0, 1);
	ConnSendInt(conn, 0, 2);
	ConnEndMessage(conn);
	ConnFlush(conn, FLUSH_IMMEDIATE);
}

static void
WbCCSendWalBlock(WbConn conn, ReplMessage *msg, FilterData *fl)
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
		log_debug2("Sending %d bytes of unbuffered data", unsentLen);
	}

	//'d' 'w' l(dataStart) l(walEnd) l(sendTime) s[WALdata]
	if (fl->state & FS_BUFFERING_STATE)
	{
		// Chomp the buffered data off of what we send
		buffered = fl->bufferLen;
		// Stash it away into fl state, we will send it with the next block
		fl->unsentBufferLen = fl->bufferLen;
		memcpy(fl->unsentBuffer, fl->buffer, fl->bufferLen);
		// Make note that record starts in the unsent buffer for rewriting
		fl->recordStart = -1;
		log_debug2("Buffering %d bytes of data", buffered);

	} else {
		// Clear out unsent buffer
		fl->unsentBufferLen = 0;
	}

	// Don't send anything if we are not synchronized, we will see this data again after replication restart
	if (!fl->synchronized)
	{
		log_debug2("Skipping sending data.");
		return;
	}

	// Include the previously unsent data
	dataStart = msg->dataStart - unsentLen;

	if (fl->requestedStartPos > dataStart) {
		if (fl->requestedStartPos > (msg->dataStart + msg->dataLen))
		{
			log_info("Skipping whole WAL message as not requested");
			return;
		}
		msgOffset = fl->requestedStartPos - dataStart;
		dataStart = fl->requestedStartPos;
		Assert(msgOffset < (msg->dataLen + unsentLen));
		log_debug2("Chomping WAL message down to size at %d", msgOffset);
	}

	log_debug2("Sending data start %X/%X", FormatRecPtr(dataStart));

	ConnBeginMessage(conn, 'd');
	ConnSendInt(conn, 'w', 1);
	ConnSendInt64(conn, dataStart);
	ConnSendInt64(conn, msg->walEnd - buffered);
	ConnSendInt64(conn, msg->sendTime);
	log_debug1("Sending out %d bytes of WAL at %X/%X",
			msg->dataLen + unsentLen - msgOffset - buffered,
			FormatRecPtr(dataStart));

	if (unsentLen && msgOffset < unsentLen) {
		log_debug2("Sending unsent data at offset %d, %d bytes", msgOffset, unsentLen-msgOffset);
		ConnSendBytes(conn, unsentBuf + msgOffset, unsentLen-msgOffset);
		msgOffset = msgOffset < unsentLen ? 0 : msgOffset - unsentLen;
	}

	ConnSendBytes(conn, msg->data + msgOffset, msg->dataLen - msgOffset - buffered);
	ConnEndMessage(conn);

	conn->sentPtr = msg->dataStart + msg->dataLen - buffered;
	conn->lastSend = msg->sendTime;
	ConnFlush(conn, FLUSH_ASYNC);
}

static char*
ErrorSeverity(LogLevel level)
{
	switch (level)
	{
	case LOG_ERROR:
		return "ERROR";
	case LOG_WARNING:
		return "WARNING";
	case LOG_INFO:
		return "INFO";
	case LOG_DEBUG1:
	case LOG_DEBUG2:
	case LOG_DEBUG3:
		return "DEBUG";
	default:
		Assert(false);
		return "";
	}
}

static void
WbCCSendErrorReport(WbConn conn, LogLevel level, char *message, char* detail)
{
	ConnBeginMessage(conn, level >= LOG_ERROR ? 'E' : 'N');

	// Severity
	ConnSendInt(conn, 'S', 1);
	ConnSendString(conn, ErrorSeverity(level));

	// SQLState
	ConnSendInt(conn, 'C', 1);
	ConnSendString(conn, "XX000");

	// Message primary
	ConnSendInt(conn, 'M', 1);
	ConnSendString(conn, message);

	// Message detail
	if (detail)
	{
		ConnSendInt(conn, 'D', 1);
		ConnSendString(conn, detail);
	}
	// hint H
	// filename F
	// lineno L
	// function R

	ConnSendInt(conn, '\0', 1);
	ConnEndMessage(conn);
	ConnFlush(conn, FLUSH_IMMEDIATE);
}
