#include "xfutils.h"
#include <string.h>
#include <stdarg.h>

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
