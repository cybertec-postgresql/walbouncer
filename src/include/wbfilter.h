#ifndef	_WB_FILTER_H
#define _WB_FILTER_H 1

#include "wbglobals.h"
#include "wbmasterconn.h"

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

FilterData* WbFCreateProcessingState(XLogRecPtr startPos);
void WbFFreeProcessingState(FilterData* fl);
bool WbFProcessWalDataBlock(ReplMessage* msg, FilterData* fl, XLogRecPtr *retryPos);

#endif
