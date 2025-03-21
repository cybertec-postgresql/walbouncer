#ifndef	_WB_FILTER_H
#define _WB_FILTER_H 1

#include "wbglobals.h"
#include "wbmasterconn.h"

#define FS_BUFFERING_STATE (1 << 8)
typedef enum {
	FS_SYNCHRONIZING = 0,
	FS_COPY_SWITCH = 1,
	FS_COPY_NORMAL = 2,
	FS_COPY_ZERO = 3,
	FS_BUFFER_RECORD = (4 | FS_BUFFERING_STATE),
	FS_BUFFER_BLOCK_ID = (5 | FS_BUFFERING_STATE),
	FS_BUFFER_BLOCK_HEADER = (6 | FS_BUFFERING_STATE),
	FS_BUFFER_IMAGE_HEADER = (7 | FS_BUFFERING_STATE),
	FS_BUFFER_COMPRESSION_HEADER = (8 | FS_BUFFERING_STATE),
	FS_BUFFER_FILENODE = (9 | FS_BUFFERING_STATE)
} FilterState;

#define FL_BUFFER_LEN 128

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
	Oid *include_databases;
	Oid *exclude_tablespaces;
	Oid *exclude_databases;
} FilterData;

FilterData* WbFCreateProcessingState(XLogRecPtr startPos);
void WbFFreeProcessingState(FilterData* fl);
bool WbFProcessWalDataBlock(ReplMessage* msg, FilterData* fl, XLogRecPtr *retryPos, int xlog_page_magic);

#endif
