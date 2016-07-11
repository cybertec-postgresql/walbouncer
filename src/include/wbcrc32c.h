/*-------------------------------------------------------------------------
 *
 * pg_crc32c.h
 *	  Routines for computing CRC-32C checksums.
 *
 * The speed of CRC-32C calculation has a big impact on performance, so we
 * jump through some hoops to get the best implementation for each
 * platform. Some CPU architectures have special instructions for speeding
 * up CRC calculations (e.g. Intel SSE 4.2), on other platforms we use the
 * Slicing-by-8 algorithm which uses lookup tables.
 *
 * The public interface consists of four macros:
 *
 * INIT_CRC32C(crc)
 *		Initialize a CRC accumulator
 *
 * COMP_CRC32C(crc, data, len)
 *		Accumulate some (more) bytes into a CRC
 *
 * FIN_CRC32C(crc)
 *		Finish a CRC calculation
 *
 * EQ_CRC32C(c1, c2)
 *		Check for equality of two CRCs.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port/pg_crc32c.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _WB_CRC32C_H
#define _WB_CRC32C_H

#include "wbglobals.h"
#include "wbpgtypes.h"


/* The INIT and EQ macros are the same for all implementations. */
#define INIT_CRC32C(crc) ((crc) = 0xFFFFFFFF)
#define EQ_CRC32C(c1, c2) ((c1) == (c2))

/*
 * Use slicing-by-8 algorithm.
 *
 * On big-endian systems, the intermediate value is kept in reverse byte
 * order, to avoid byte-swapping during the calculation. FIN_CRC32C reverses
 * the bytes to the final order.
 */
#define COMP_CRC32C(crc, data, len) \
    ((crc) = pg_comp_crc32c_sb8((crc), (data), (len)))
#define COMP_CRC32C_ZERO(crc, data, len) \
    ((crc) = pg_comp_crc32c_sb8_zero((crc), (data), (len)))

#define FIN_CRC32C(crc) ((crc) ^= 0xFFFFFFFF)


extern pg_crc32c pg_comp_crc32c_sb8(pg_crc32c crc, const void *data, size_t len);
extern pg_crc32c pg_comp_crc32c_sb8_zero(pg_crc32c crc, const void *data, size_t len);

#endif   /* WB_CRC32C_H */
