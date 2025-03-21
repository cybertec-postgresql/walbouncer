#include "wbfilter.h"

#include <string.h>

#include "wbpgtypes.h"
#include "wbutils.h"
#include "wbcrc32c.h"



static bool IsAtWalPageBoundary(ReplMessage *msg);
static char *ReplMessageConsume(ReplMessage *msg, size_t amount);
static void ReplMessageBuffer(FilterData *fl, ReplMessage *msg, int amount);
static void ReplMessageCopy(FilterData *fl, ReplMessage *msg, int amount);
static void ReplMessageZero(FilterData *fl, ReplMessage *msg, int amount);
static void ReplMessageAlign(ReplMessage *msg);
static int ReplDataRemainingInSegment(ReplMessage *msg);
static void WriteNoopRecord(FilterData *fl, ReplMessage *msg);
static void FilterClearBuffer(FilterData *fl);
static bool NeedToFilter(FilterData *fl, RelFileNode *node);
static void FilterBufferRecordHeader(FilterData* fl, ReplMessage* msg);
static pg_crc32c CalculateCRC32(char *buffer, int len, int total_len);
static void InjectDummyDataHeaderLongAfterRecordHeader(XLogRecord *rec);

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
WbFProcessWalDataBlock(ReplMessage* msg, FilterData* fl, XLogRecPtr *retryPos, int xlog_page_magic)
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

			msg->nextPageBoundary += XLOG_BLCKSZ;

			/*
			 * If we're copying the rest of the segment following XLOG switch,
			 * we cannot examine the header because the switch record should
			 * be followed only by zeroes. And in fact we do not need any
			 * header info.
			 */
			if (fl->state == FS_COPY_SWITCH)
			{
				/* Subtract the amount already "consumed" above. */
				fl->dataNeeded -= SizeOfXLogShortPHD;
				/* Normal processing should continue now. */
				continue;
			}

			if (header->xlp_magic != xlog_page_magic)
				error("Received page with invalid page magic 0x%x", header->xlp_magic);

			if (header->xlp_info & XLP_LONG_HEADER)
				ReplMessageConsume(msg, SizeOfXLogLongPHD - SizeOfXLogShortPHD);

			/*
			 * Adjust recordStart so it does not point to the beginning of the
			 * page.
			 */
			if (fl->recordStart == headerPos)
				fl->recordStart = msg->dataPtr;

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
			case FS_BUFFER_BLOCK_ID:
			case FS_BUFFER_BLOCK_HEADER:
			case FS_BUFFER_IMAGE_HEADER:
			case FS_BUFFER_COMPRESSION_HEADER:
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

					if (rec->xl_rmid == RM_XLOG_ID && (rec->xl_info & 0xF0) == XLOG_SWITCH)
					{
						// Stream out data until end of buffer
						fl->state = FS_COPY_SWITCH;
						fl->dataNeeded = ReplDataRemainingInSegment(msg);
						FilterClearBuffer(fl);
						parse_debug(" - Xlog switch, copying %d bytes ", fl->dataNeeded);
						break;
					}
					else if (fl->recordRemaining == 0)
					{
						fl->state = FS_COPY_NORMAL;
						fl->dataNeeded = fl->recordRemaining;
						FilterClearBuffer(fl);
						parse_debug(" - Other record, copying %d bytes ", fl->dataNeeded);
					}
					else
					{
						fl->state = FS_BUFFER_BLOCK_ID;
						fl->dataNeeded = 1;
						parse_debug(" - Buffer block ID");
					}
				}
				break;
			case FS_BUFFER_BLOCK_ID:
				if (fl->dataNeeded <= amountAvailable)
					ReplMessageBuffer(fl, msg, fl->dataNeeded);
				else
					ReplMessageBuffer(fl, msg, amountAvailable);
				if (!fl->dataNeeded)
				{
					uint8 block_id = *((uint8*) (fl->buffer + REC_HEADER_LEN));

					fl->recordRemaining -= 1;

					if (block_id > XLR_MAX_BLOCK_ID)
					{
						XLogRecord *rec = (XLogRecord*) fl->buffer;

						if (rec->xl_rmid == RM_SMGR_ID &&
							(rec->xl_info & 0xF0) == XLOG_SMGR_CREATE)
						{
							/* SMGR record should not be longer than 255 bytes. */
							Assert(block_id == XLR_BLOCK_ID_DATA_SHORT);

							fl->state = FS_BUFFER_FILENODE;
							/* Include the data length info. */
							fl->dataNeeded = sizeof(uint8) + sizeof(RelFileNode);
							fl->recordRemaining -= sizeof(uint8);
							parse_debug(" - SMGR record, buffering %d bytes for filenode",
										fl->dataNeeded);
							break;
						}
						else if (rec->xl_rmid == RM_SMGR_ID &&
								 (rec->xl_info & 0xF0) == XLOG_SMGR_TRUNCATE)
						{
							/* SMGR record should not be longer than 255 bytes. */
							Assert(block_id == XLR_BLOCK_ID_DATA_SHORT);

							fl->state = FS_BUFFER_FILENODE;
							// Here we rely on the fact that FS_BUFFER_FILENODE will ignore any extra data
							/* Include the data length info. */
							fl->dataNeeded = sizeof(uint8) + sizeof(BlockNumber) +
								sizeof(RelFileNode);
							fl->recordRemaining -= sizeof(uint8) + sizeof(BlockNumber);
							parse_debug(" - SMGR record, buffering %d bytes for filenode",
										fl->dataNeeded);
							break;
						}
						/*
						 * XXX The following seems to be dead code since PG
						 * calls XLogRegisterBuffer() before it inserts the
						 * XLOG_SEQ_LOG record. Thus the RelFileNode is
						 * located after XLogRecordBlockHeader, so we can do
						 * our filtering without checking the actual record.
						 */
						else if (rec->xl_rmid == RM_SEQ_ID &&
								 (rec->xl_info & 0xF0) == XLOG_SEQ_LOG)
						{
							fl->state = FS_BUFFER_FILENODE;
							fl->dataNeeded = sizeof(RelFileNode);

							/*
							 * Make sure we skip the whole "block id" and the
							 * size info.
							 */
							if (block_id == XLR_BLOCK_ID_DATA_SHORT)
							{
								fl->dataNeeded += sizeof(uint8);
								fl->recordRemaining -= sizeof(uint8);
							}
							else if (block_id == XLR_BLOCK_ID_DATA_LONG)
							{
								fl->dataNeeded += sizeof(uint32);
								fl->recordRemaining -= sizeof(uint32);
							}
							else
								Assert(false);

							parse_debug(" - SMGR record, buffering %d bytes for filenode",
										fl->dataNeeded);
							break;
						}
						else
						{
							fl->state = FS_COPY_NORMAL;
							fl->dataNeeded = fl->recordRemaining;
							FilterClearBuffer(fl);
							parse_debug(" - No block references in record, copying %d bytes ", fl->dataNeeded);
						}
					}
					else
					{
						fl->state = FS_BUFFER_BLOCK_HEADER;
						fl->dataNeeded = SizeOfXLogRecordBlockHeader - 1;
						parse_debug(" - Buffer block reference header");
					}
				}
				break;
			case FS_BUFFER_BLOCK_HEADER:
				if (fl->dataNeeded <= amountAvailable)
					ReplMessageBuffer(fl, msg, fl->dataNeeded);
				else
					ReplMessageBuffer(fl, msg, amountAvailable);
				if (!fl->dataNeeded)
				{
					XLogRecordBlockHeader *block = (XLogRecordBlockHeader*) (fl->buffer + REC_HEADER_LEN);

					fl->recordRemaining -= SizeOfXLogRecordBlockHeader - 1;

					if (block->fork_flags & BKPBLOCK_SAME_REL)
					{
						log_error("First block reference has SAME_REL set");
						exit(1);
					}

					if (block->fork_flags & BKPBLOCK_HAS_IMAGE)
					{
						fl->state = FS_BUFFER_IMAGE_HEADER;
						fl->dataNeeded = SizeOfXLogRecordBlockImageHeader;
						parse_debug(" - Block header has image header, buffering %d", fl->dataNeeded);
					}
					else
					{
						fl->state = FS_BUFFER_FILENODE;
						fl->dataNeeded = sizeof(RelFileNode);
						parse_debug(" - Block reference, buffering %d bytes for filenode", fl->dataNeeded);
					}

				}
				break;
			case FS_BUFFER_IMAGE_HEADER:
				if (fl->dataNeeded <= amountAvailable)
					ReplMessageBuffer(fl, msg, fl->dataNeeded);
				else
					ReplMessageBuffer(fl, msg, amountAvailable);
				if (!fl->dataNeeded)
				{
					XLogRecordBlockImageHeader *imghdr = (XLogRecordBlockImageHeader*) (fl->buffer + fl->bufferLen - SizeOfXLogRecordBlockImageHeader);
					bool has_compr_header = (imghdr->bimg_info & BKPIMAGE_HAS_HOLE) &&
						BKPIMAGE_COMPRESSED(imghdr->bimg_info);

					fl->recordRemaining -= SizeOfXLogRecordBlockImageHeader;

					if (has_compr_header)
					{
						fl->state = FS_BUFFER_COMPRESSION_HEADER;
						fl->dataNeeded = SizeOfXLogRecordBlockCompressHeader;
						parse_debug(" - FPI, buffering %d bytes for filenode", fl->dataNeeded);
					}
					else
					{
						fl->state = FS_BUFFER_FILENODE;
						fl->dataNeeded = sizeof(RelFileNode);
						parse_debug(" - Block reference, buffering %d bytes for filenode", fl->dataNeeded);
					}
				}
				break;
			case FS_BUFFER_COMPRESSION_HEADER:
				if (fl->dataNeeded <= amountAvailable)
					ReplMessageBuffer(fl, msg, fl->dataNeeded);
				else
					ReplMessageBuffer(fl, msg, amountAvailable);
				if (!fl->dataNeeded)
				{
					fl->recordRemaining -= SizeOfXLogRecordBlockCompressHeader;
					fl->state = FS_BUFFER_FILENODE;
					fl->dataNeeded = sizeof(RelFileNode);
					parse_debug(" - Block reference, buffering %d bytes for filenode", fl->dataNeeded);
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
							(RelFileNode*) (fl->buffer + fl->bufferLen - sizeof(RelFileNode))))
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
	return segEnd - curPos;
}

static pg_crc32c
CalculateCRC32(char *buffer, int len, int total_len)
{
    	pg_crc32c crc;
    	INIT_CRC32C(crc);
    	COMP_CRC32C(crc, buffer + SizeOfXLogRecord, len - SizeOfXLogRecord);
    	COMP_CRC32C_ZERO(crc, buffer + len, total_len - len);
    	COMP_CRC32C(crc, buffer, offsetof(XLogRecord, xl_crc));
    	FIN_CRC32C(crc);
    	return crc;
}

static void
WriteNoopRecord(FilterData *fl, ReplMessage *msg)
{
	XLogRecord *rec = (XLogRecord*) fl->buffer;

	Assert(fl->bufferLen >= REC_HEADER_LEN + sizeof(RelFileNode));

	// tot_len, xid stay the same
	rec->xl_info = XLOG_NOOP;
	rec->xl_rmid = RM_XLOG_ID;


    // zero everything between XLogRecord + DataHeaderLong and bufferLen as we don't need it anymore
    for (int i = REC_HEADER_LEN + SizeOfXLogRecordDataHeaderLong; i < fl->bufferLen; i++)
		*(fl->buffer + i) = 0;

	InjectDummyDataHeaderLongAfterRecordHeader(rec);

	rec->xl_crc = CalculateCRC32(fl->buffer, REC_HEADER_LEN + SizeOfXLogRecordDataHeaderLong, rec->xl_tot_len);

    parse_debug(" - Writing NOOP record with %d bytes at %X/%X, xl_crc = 0x%X",
                rec->xl_tot_len, (uint32) ((msg->dataStart + fl->recordStart) >> 32), (uint32) (msg->dataStart + fl->recordStart), rec->xl_crc);

	{
		// We scribble over data in the message buffer
		// some of the data may be in the filter unsent buffer.


		int targetpos = fl->recordStart;
		int copied = 0;
		int amount;
		int toCopy = fl->bufferLen;
		bool	skip_header = false;

		if (targetpos == -1)
		{
			// Rewrite first part in the unsentbuffer
			amount = fl->unsentBufferLen;
			memcpy(fl->unsentBuffer, fl->buffer + copied, amount);
			copied += amount;
			toCopy -= amount;

			/*
			 * In the corner case of msg->data starting with page header make
			 * sure that we do not overwrite the header.
			 */
			if (fl->headerPos == 0)
				targetpos = fl->headerLen;
			else
				targetpos = 0;
		}
		/*
		 * Copy up to next page header or to end of buffer.
		 *
		 * The last page header seen can actually be in front of the current
		 * record, if the current message starts exactly at page boundary or
		 * in padding bytes that terminate the previous page so that the page
		 * header does not have XLP_FIRST_IS_CONTRECORD set. In such a case we
		 * don't care about the page header.
		 */
		if (fl->headerPos >= 0 && fl->headerPos > targetpos)
			skip_header = true;

		if (skip_header)
			amount = fl->headerPos - targetpos;
		else
			amount = toCopy;
		memcpy(msg->data + targetpos, fl->buffer + copied, amount);

		// Skip over page header in the target buffer
		toCopy -= amount;
		copied += amount;

		if (!toCopy)
			return;

		targetpos += amount;
		if (skip_header)
			targetpos += fl->headerLen;

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
OidInZeroTermOidList(Oid search, Oid *list)
{
	Oid *cur = list;
	for (; *cur; cur++)
		if (search == *cur)
			return true;
	return false;
}

static bool
NeedToFilter(FilterData *fl, RelFileNode *node)
{
    log_debug2("Checking relfilnode [relNode, dbNode, spcNode] = [%u, %u, %u]",
			   node->relNode, node->dbNode, node->spcNode);
	if (fl->include_tablespaces)
		if (!OidInZeroTermOidList(node->spcNode, fl->include_tablespaces))
		{
			log_debug2("Data in tablespace %d is not included", node->spcNode);
			return true;
		}

	if (fl->exclude_tablespaces)
		if (OidInZeroTermOidList(node->spcNode, fl->exclude_tablespaces))
		{
			log_debug2("Data in tablespace %d is excluded", node->spcNode);
			return true;
		}

	if (fl->include_databases)
		if (node->dbNode != 0 &&
			!OidInZeroTermOidList(node->dbNode, fl->include_databases))
		{
			log_debug2("Data in database %d is not included", node->dbNode);
			return true;
		}

	if (fl->exclude_databases)
		if (OidInZeroTermOidList(node->dbNode, fl->exclude_databases))
		{
			log_debug2("Data in database %d is excluded", node->dbNode);
			return true;
		}

	/* If configuration doesn't say otherwise we allow it */
	return false;
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

static void
InjectDummyDataHeaderLongAfterRecordHeader(XLogRecord* rec)
{
    char *buffer = (char*)rec;
    *((uint8*)(buffer + REC_HEADER_LEN)) = (uint8)XLR_BLOCK_ID_DATA_LONG;
    *((uint32*)(buffer + REC_HEADER_LEN + 1)) = (uint32)(rec->xl_tot_len - REC_HEADER_LEN - SizeOfXLogRecordDataHeaderLong);
}
