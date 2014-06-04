#include <arpa/inet.h>
#include <stdarg.h>
#include <string.h>

#include "wbutils.h"

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

void showPQerror(PGconn *mc, char *message)
{
	printf("%s: %s\n", message, PQerrorMessage(mc));
	exit(1);
}

/* Memory allocation functions */

void *xfalloc(size_t amount)
{
	void *result = malloc(amount);
	if (!result)
		error("Out of memory!");
	return result;
}

void *xfalloc0(size_t amount)
{
	char *result = xfalloc(amount);
	memset(result, 0, amount);
	return result;
}
char *xfstrdup(char *s)
{
	char *result = strdup(s);
	if (!result)
		error("Out of memory!");
	return result;
}

void *rexfalloc(void *ptr, size_t amount)
{
	void *result = realloc(ptr, amount);
	if (!result)
		error("Out of memory!");
	return result;
}

void xffree(void *ptr)
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
