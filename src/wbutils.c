#include <arpa/inet.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include "wbutils.h"

LogLevel loggingLevel = LOG_INFO;

/* Error reporting functions */

void error(const char *message, ...)
{
	va_list args;
	va_start(args, message);
	vfprintf(stderr, message, args);
	fprintf(stderr, "\n");
	va_end(args);
	exit(1);
}

void do_wb_log(LogLevel logLevel, const char* logLevelStr, const char *file, const char *message, ...)
{
	time_t now;
	char nowStr[20];
	va_list args;
	va_start(args, message);

	time(&now);

	strftime(nowStr, 20, "%Y-%m-%d %H:%M:%S", localtime(&now));

	fprintf (stderr, "[%s] %s %s: ", nowStr, file, logLevelStr);
	vfprintf (stderr, message, args);
	fprintf (stderr, "\n");
	va_end(args);
}

void showPQerror(PGconn *mc, char *message)
{
	log_error("%s: %s", message, PQerrorMessage(mc));
	exit(1);
}

/* Memory allocation functions */

void *wballoc(size_t amount)
{
	void *result = malloc(amount);
	if (!result)
		error("Out of memory!");
	return result;
}

void *wballoc0(size_t amount)
{
	char *result = wballoc(amount);
	memset(result, 0, amount);
	return result;
}
char *wbstrdup(char *s)
{
	char *result = strdup(s);
	if (!result)
		error("Out of memory!");
	return result;
}

void *rewballoc(void *ptr, size_t amount)
{
	void *result = realloc(ptr, amount);
	if (!result)
		error("Out of memory!");
	return result;
}

void wbfree(void *ptr)
{
	free(ptr);
}

/* Miscellaneous utility functions */

int
ensure_atoi(char *s)
{
	char *endptr;
	int result;
	result = strtol(s, &endptr, 0);

	//FIXME: check for error here

	return result;
}

uint64
fromnetwork64(char *buf)
{
	// XXX: unaligned reads
	uint32 h = *((uint32*) buf);
	uint32 l = *(((uint32*) buf)+1);
	return ((uint64)ntohl(h) << 32) | ntohl(l);
}

uint32
fromnetwork32(char *buf)
{
	// XXX: unaligned read
	return ntohl(*((uint32*) buf));
}

void
write64(char *buf, uint64 v)
{
	int i;
	for (i = 7; i>= 0; i--)
		*buf++ = (char)(v >> i*8);
}

#define INT64CONST(x)  ((int64) x)
#define MAXDATELEN 30
#define USECS_PER_SEC	INT64CONST(1000000)
/*
 * Produce a C-string representation of a TimestampTz.
 *
 * This is mostly for use in emitting messages.  The primary difference
 * from timestamptz_out is that we force the output format to ISO.  Note
 * also that the result is in a static buffer, not pstrdup'd.
 */
const char *
timestamptz_to_str(TimestampTz t)
{
	static char buf[MAXDATELEN];
	struct tm tm;
	time_t time;
	int offset;

	time = (int32) (t / USECS_PER_SEC) + 946684800;
	gmtime_r(&time, &tm);
	offset = strftime(buf, MAXDATELEN, "%Y-%m-%d %H:%M:%S", &tm);
	Assert(offset > 0);
	sprintf(buf+offset, ".%06d+00", (int32) (t % USECS_PER_SEC));
	return buf;
}
