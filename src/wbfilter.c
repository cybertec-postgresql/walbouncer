#include "wbfilter.h"

#include <string.h>

#include "wbpgtypes.h"
#include "wbutils.h"



static bool IsAtWalPageBoundary(ReplMessage *msg);
static char *ReplMessageConsume(ReplMessage *msg, size_t amount);
static void ReplMessageBuffer(FilterData *fl, ReplMessage *msg, int amount);
static void ReplMessageCopy(FilterData *fl, ReplMessage *msg, int amount);
static void ReplMessageZero(FilterData *fl, ReplMessage *msg, int amount);
static void ReplMessageAlign(ReplMessage *msg);
static int ReplDataRemainingInSegment(ReplMessage *msg);
static pg_crc32 CalculateCRC32(char *buffer, int len, int total_len);
static void WriteNoopRecord(FilterData *fl, ReplMessage *msg);
static void FilterClearBuffer(FilterData *fl);
static bool NeedToFilter(FilterData *fl, RelFileNode *node);
static void FilterBufferRecordHeader(FilterData* fl, ReplMessage* msg);

FilterData*
WbFCreateProcessingState(XLogRecPtr startPoint)
{
	FilterData *fl;
	fl = wballoc0(sizeof(FilterData));

	fl->state = FS_SYNCHRONIZING;
	fl->dataNeeded = 0;
	fl->recordRemaining = 0;
	fl->synchronized = false;
	fl->requestedStartPos = startPoint;
	fl->recordStart = 0;
	fl->headerPos = -1;
	fl->headerLen = 0;
	fl->bufferLen = 0;
	fl->unsentBufferLen = 0;

	return fl;
}
void WbFFreeProcessingState(FilterData* fl)
{
	wbfree(fl);
}

/*#define parse_debug(...) do{\
	fprintf (stderr, __VA_ARGS__);\
	fprintf (stderr, "\n");\
}while(0)*/

#define parse_debug(...)

bool
WbFProcessWalDataBlock(ReplMessage* msg, FilterData* fl, XLogRecPtr *retryPos)
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

			msg->nextPageBoundary += XLOG_BLCKSZ;

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
				/* no break */
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
				break;
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
				/* no break */
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
									FilterClearBuffer(fl);
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
									FilterClearBuffer(fl);
									parse_debug(" - Other XLOG record, copying %d bytes ", fl->dataNeeded);
								}
							}
							break;
						default:
							// No need for filtering
							fl->state = FS_COPY_NORMAL;
							fl->dataNeeded = fl->recordRemaining;
							FilterClearBuffer(fl);
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
						FilterClearBuffer(fl);
						parse_debug(" - Filter record");
					}
					else
					{
						fl->state = FS_COPY_NORMAL;
						FilterClearBuffer(fl);
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
	wb_info("Curpos %X/%X segEnd %X/%X", FormatRecPtr(curPos), FormatRecPtr(segEnd));
	return segEnd - curPos;
}

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
		targetpos += amount + fl->headerLen;

		// Copy the rest if any
		amount = toCopy;
		memcpy(msg->data + targetpos, fl->buffer + copied, amount);
	}
}

static void
FilterClearBuffer(FilterData *fl)
{
	fl->bufferLen = 0;
}

static bool
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
	wb_info("Filtering data in tablespace %d", node->spcNode);
	return true;
}

static void
FilterBufferRecordHeader(FilterData* fl, ReplMessage* msg)
{
	fl->state = FS_BUFFER_RECORD;
	fl->recordStart = msg->dataPtr;
	fl->dataNeeded = REC_HEADER_LEN;
	fl->headerPos = -1;
	fl->headerLen = 0;
	fl->bufferLen = 0;
}
