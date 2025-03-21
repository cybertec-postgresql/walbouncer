#ifndef _WB_PGTYPES_H
#define _WB_PGTYPES_H

#include "pg_config.h"

#include "wbglobals.h"
#include "wb_pg_config.h"

typedef unsigned long int	uintptr_t;

#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define MAXALIGN(LEN)			TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))

typedef unsigned int Oid;

typedef struct RelFileNode
{
	Oid			spcNode;		/* tablespace */
	Oid			dbNode;			/* database */
	Oid			relNode;		/* relation */
} RelFileNode;

typedef enum ForkNumber
{
	InvalidForkNumber = -1,
	MAIN_FORKNUM = 0,
	FSM_FORKNUM,
	VISIBILITYMAP_FORKNUM,
	INIT_FORKNUM

	/*
	 * NOTE: if you add a new fork, change MAX_FORKNUM and possibly
	 * FORKNAMECHARS below, and update the forkNames array in
	 * src/common/relpath.c
	 */
} ForkNumber;

#define MAX_FORKNUM		INIT_FORKNUM

typedef uint32 BlockNumber;

/*
 * Header info for a backup block appended to an XLOG record.
 *
 * As a trivial form of data compression, the XLOG code is aware that
 * PG data pages usually contain an unused "hole" in the middle, which
 * contains only zero bytes.  If hole_length > 0 then we have removed
 * such a "hole" from the stored data (and it's not counted in the
 * XLOG record's CRC, either).  Hence, the amount of block data actually
 * present following the BkpBlock struct is BLCKSZ - hole_length bytes.
 *
 * Note that we don't attempt to align either the BkpBlock struct or the
 * block's data.  So, the struct must be copied to aligned local storage
 * before use.
 */
typedef struct BkpBlock
{
	RelFileNode node;			/* relation containing block */
	ForkNumber	fork;			/* fork within the relation */
	BlockNumber block;			/* block number */
	uint16		hole_offset;	/* number of bytes before "hole" */
	uint16		hole_length;	/* number of bytes in "hole" */

	/* ACTUAL BLOCK DATA FOLLOWS AT END OF STRUCT */
} BkpBlock;

/*
 * Each page of XLOG file has a header like this:
 */
#if PG_VERSION_NUM >= 170000
#define XLOG_PAGE_MAGIC 0xD116
#elif PG_VERSION_NUM >= 160000
#define XLOG_PAGE_MAGIC 0xD113
#elif PG_VERSION_NUM >= 150000
#define XLOG_PAGE_MAGIC 0xD110
#elif PG_VERSION_NUM >= 140000
#define XLOG_PAGE_MAGIC 0xD10D
#elif PG_VERSION_NUM >= 130000
#define XLOG_PAGE_MAGIC 0xD106
#else
#error "unsupported PG version"
#endif

typedef struct XLogPageHeaderData
{
	uint16		xlp_magic;		/* magic value for correctness checks */
	uint16		xlp_info;		/* flag bits, see below */
	TimeLineID	xlp_tli;		/* TimeLineID of first record on page */
	XLogRecPtr	xlp_pageaddr;	/* XLOG address of this page */

	/*
	 * When there is not enough space on current page for whole record, we
	 * continue on the next page.  xlp_rem_len is the number of bytes
	 * remaining from a previous page.
	 *
	 * Note that xl_rem_len includes backup-block data; that is, it tracks
	 * xl_tot_len not xl_len in the initial header.  Also note that the
	 * continuation data isn't necessarily aligned.
	 */
	uint32		xlp_rem_len;	/* total len of remaining data for record */
} XLogPageHeaderData;

#define SizeOfXLogShortPHD	MAXALIGN(sizeof(XLogPageHeaderData))

typedef XLogPageHeaderData *XLogPageHeader;

/*
 * When the XLP_LONG_HEADER flag is set, we store additional fields in the
 * page header.  (This is ordinarily done just in the first page of an
 * XLOG file.)	The additional fields serve to identify the file accurately.
 */
typedef struct XLogLongPageHeaderData
{
	XLogPageHeaderData std;		/* standard header fields */
	uint64		xlp_sysid;		/* system identifier from pg_control */
	uint32		xlp_seg_size;	/* just as a cross-check */
	uint32		xlp_xlog_blcksz;	/* just as a cross-check */
} XLogLongPageHeaderData;

#define SizeOfXLogLongPHD	MAXALIGN(sizeof(XLogLongPageHeaderData))

typedef XLogLongPageHeaderData *XLogLongPageHeader;

/* When record crosses page boundary, set this flag in new page's header */
#define XLP_FIRST_IS_CONTRECORD		0x0001
/* This flag indicates a "long" page header */
#define XLP_LONG_HEADER				0x0002
/* This flag indicates backup blocks starting in this page are optional */
#define XLP_BKP_REMOVABLE			0x0004
/* All defined flag bits in xlp_info (used for validity checking of header) */
#define XLP_ALL_FLAGS				0x0007

#define XLogPageHeaderSize(hdr)		\
	(((hdr)->xlp_info & XLP_LONG_HEADER) ? SizeOfXLogLongPHD : SizeOfXLogShortPHD)

/*
 * The XLOG is split into WAL segments (physical files) of the size indicated
 * by XLOG_SEG_SIZE.
 */
#define XLogSegSize		((uint32) XLOG_SEG_SIZE)
#define XLogSegmentsPerXLogId	(UINT64CONST(0x100000000) / XLOG_SEG_SIZE)

#define XLogSegNoOffsetToRecPtr(segno, offset, dest) \
		(dest) = (segno) * XLOG_SEG_SIZE + (offset)

/*
 * Compute ID and segment from an XLogRecPtr.
 *
 * For XLByteToSeg, do the computation at face value.  For XLByteToPrevSeg,
 * a boundary byte is taken to be in the previous segment.  This is suitable
 * for deciding which segment to write given a pointer to a record end,
 * for example.
 */
#define XLByteToSeg(xlrp, logSegNo) \
	logSegNo = (xlrp) / XLogSegSize

#define XLByteToPrevSeg(xlrp, logSegNo) \
	logSegNo = ((xlrp) - 1) / XLogSegSize

/*
 * Is an XLogRecPtr within a particular XLOG segment?
 *
 * For XLByteInSeg, do the computation at face value.  For XLByteInPrevSeg,
 * a boundary byte is taken to be in the previous segment.
 */
#define XLByteInSeg(xlrp, logSegNo) \
	(((xlrp) / XLogSegSize) == (logSegNo))

#define XLByteInPrevSeg(xlrp, logSegNo) \
	((((xlrp) - 1) / XLogSegSize) == (logSegNo))

/* Check if an XLogRecPtr value is in a plausible range */
#define XRecOffIsValid(xlrp) \
		((xlrp) % XLOG_BLCKSZ >= SizeOfXLogShortPHD)

/*
 * These macros encapsulate knowledge about the exact layout of XLog file
 * names, timeline history file names, and archive-status file names.
 */
#define MAXFNAMELEN		64

#define XLogFileName(fname, tli, logSegNo)	\
	snprintf(fname, MAXFNAMELEN, "%08X%08X%08X", tli,		\
			 (uint32) ((logSegNo) / XLogSegmentsPerXLogId), \
			 (uint32) ((logSegNo) % XLogSegmentsPerXLogId))

#define XLogFromFileName(fname, tli, logSegNo)	\
	do {												\
		uint32 log;										\
		uint32 seg;										\
		sscanf(fname, "%08X%08X%08X", tli, &log, &seg); \
		*logSegNo = (uint64) log * XLogSegmentsPerXLogId + seg; \
	} while (0)

#define XLogFilePath(path, tli, logSegNo)	\
	snprintf(path, MAXPGPATH, XLOGDIR "/%08X%08X%08X", tli,				\
			 (uint32) ((logSegNo) / XLogSegmentsPerXLogId),				\
			 (uint32) ((logSegNo) % XLogSegmentsPerXLogId))

#define TLHistoryFileName(fname, tli)	\
	snprintf(fname, MAXFNAMELEN, "%08X.history", tli)

#define TLHistoryFilePath(path, tli)	\
	snprintf(path, MAXPGPATH, XLOGDIR "/%08X.history", tli)

#define StatusFilePath(path, xlog, suffix)	\
	snprintf(path, MAXPGPATH, XLOGDIR "/archive_status/%s%s", xlog, suffix)

#define BackupHistoryFileName(fname, tli, logSegNo, offset) \
	snprintf(fname, MAXFNAMELEN, "%08X%08X%08X.%08X.backup", tli, \
			 (uint32) ((logSegNo) / XLogSegmentsPerXLogId),		  \
			 (uint32) ((logSegNo) % XLogSegmentsPerXLogId), offset)

#define BackupHistoryFilePath(path, tli, logSegNo, offset)	\
	snprintf(path, MAXPGPATH, XLOGDIR "/%08X%08X%08X.%08X.backup", tli, \
			 (uint32) ((logSegNo) / XLogSegmentsPerXLogId), \
			 (uint32) ((logSegNo) % XLogSegmentsPerXLogId), offset)

typedef uint32 pg_crc32c;
typedef uint8 RmgrId;

typedef struct XLogRecord
{
	uint32		xl_tot_len;		/* total len of entire record */
	TransactionId xl_xid;		/* xact id */
	XLogRecPtr	xl_prev;		/* ptr to previous record in log */
	uint8		xl_info;		/* flag bits, see below */
	RmgrId		xl_rmid;		/* resource manager for this record */
	/* 2 bytes of padding here, initialize to zero */
	pg_crc32c	xl_crc;			/* CRC for this record */

	/* XLogRecordBlockHeaders and XLogRecordDataHeader follow, no padding */

} XLogRecord;

#define SizeOfXLogRecord	(offsetof(XLogRecord, xl_crc) + sizeof(pg_crc32c))

#define XLogRecGetData(record)	((char*) (record) + SizeOfXLogRecord)

#define XLR_BKP_BLOCK_MASK		0x0F	/* all info bits used for bkp blocks */
#define XLR_MAX_BKP_BLOCKS		4
#define XLR_BKP_BLOCK(iblk)		(0x08 >> (iblk))		/* iblk in 0..3 */




#define RM_XLOG_ID 0
#define RM_XACT_ID 1
#define RM_SMGR_ID 2
#define RM_CLOG_ID 3
#define RM_DBASE_ID 4
#define RM_TBLSPC_ID 5
#define RM_MULTIXACT_ID 6
#define RM_RELMAP_ID 7
#define RM_STANDBY_ID 8
#define RM_HEAP2_ID 9
#define RM_HEAP_ID 10
#define RM_BTREE_ID 11
#define RM_HASH_ID 12
#define RM_GIN_ID 13
#define RM_GIST_ID 14
#define RM_SEQ_ID 15
#define RM_SPGIST_ID 16

#define XLOG_NOOP 0x20
#define XLOG_SWITCH 0x40
#define XLOG_FPI 0xA0

#define XLOG_SMGR_CREATE	0x10
#define XLOG_SMGR_TRUNCATE	0x20

#define XLOG_SEQ_LOG			0x00

#define REC_HEADER_LEN 24

#define XLR_MAX_BLOCK_ID			32

typedef struct XLogRecordBlockHeader
{
	uint8		id;				/* block reference ID */
	uint8		fork_flags;		/* fork within the relation, and flags */
	uint16		data_length;	/* number of payload bytes (not including page
								 * image) */

	/* If BKPBLOCK_HAS_IMAGE, an XLogRecordBlockImageHeader struct follows */
	/* If BKPBLOCK_SAME_REL is not set, a RelFileNode follows */
	/* BlockNumber follows */
} XLogRecordBlockHeader;

#define SizeOfXLogRecordBlockHeader (offsetof(XLogRecordBlockHeader, data_length) + sizeof(uint16))

#define BKPBLOCK_FORK_MASK	0x0F
#define BKPBLOCK_FLAG_MASK	0xF0
#define BKPBLOCK_HAS_IMAGE	0x10	/* block data is an XLogRecordBlockImage */
#define BKPBLOCK_HAS_DATA	0x20
#define BKPBLOCK_WILL_INIT	0x40	/* redo will re-init the page */
#define BKPBLOCK_SAME_REL	0x80	/* RelFileNode omitted, same as previous */

#define SizeOfXLogRecordDataHeaderLong (sizeof(uint8) + sizeof(uint32))

typedef struct XLogRecordBlockImageHeader
{
	uint16		length;			/* number of page image bytes */
	uint16		hole_offset;	/* number of bytes before "hole" */
	uint8		bimg_info;		/* flag bits, see below */

	/*
	 * If BKPIMAGE_HAS_HOLE and BKPIMAGE_IS_COMPRESSED, an
	 * XLogRecordBlockCompressHeader struct follows.
	 */
} XLogRecordBlockImageHeader;

#define SizeOfXLogRecordBlockImageHeader	\
	(offsetof(XLogRecordBlockImageHeader, bimg_info) + sizeof(uint8))

/* Information stored in bimg_info */
#define BKPIMAGE_HAS_HOLE		0x01	/* page image has "hole" */
#define BKPIMAGE_APPLY			0x02	/* page image should be restored
						 * during replay */
/* compression methods supported */
#define BKPIMAGE_COMPRESS_PGLZ	0x04
#define BKPIMAGE_COMPRESS_LZ4	0x08
#define BKPIMAGE_COMPRESS_ZSTD	0x10

#define	BKPIMAGE_COMPRESSED(info) \
	((info & (BKPIMAGE_COMPRESS_PGLZ | BKPIMAGE_COMPRESS_LZ4 | \
			  BKPIMAGE_COMPRESS_ZSTD)) != 0)

/*
 * Extra header information used when page image has "hole" and
 * is compressed.
 */
typedef struct XLogRecordBlockCompressHeader
{
	uint16		hole_length;	/* number of bytes in "hole" */
} XLogRecordBlockCompressHeader;

#define SizeOfXLogRecordBlockCompressHeader \
	sizeof(XLogRecordBlockCompressHeader)

#define XLR_BLOCK_ID_DATA_SHORT		255
#define XLR_BLOCK_ID_DATA_LONG		254
#define XLR_BLOCK_ID_ORIGIN			253

#endif   /* XF_PGTYPES_H */
