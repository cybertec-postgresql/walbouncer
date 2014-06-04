#ifndef	_WB_GLOBALS_H
#define _WB_GLOBALS_H 1

#include<stdlib.h>
#include<stdio.h>
#include<stdint.h>
#include<stddef.h>

#include "libpq-fe.h"

typedef unsigned char bool;
#define false 0
#define true 1

typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;

/* Compatibility stuff */
typedef uint64 XLogRecPtr;
typedef uint32 TimeLineID;
typedef uint64 TimestampTz;
typedef uint32 TransactionId;

#define DEBUG 1

#ifndef EOF
#define EOF (-1)
#endif

#define STATUS_OK 0
#define STATUS_ERROR -1

#endif
