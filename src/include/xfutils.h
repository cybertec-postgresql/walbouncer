#ifndef	_XF_UTILS_H
#define _XF_UTILS_H 1

#include "xfglobals.h"

#define xf_info(...) do{\
	fprintf (stderr, __VA_ARGS__);\
	fprintf (stderr, "\n");\
}while(0)
#define log_error(x) xf_info(x)


void __attribute__((noreturn)) error(const char *message, ...);
void showPQerror(PGconn *mc, char *message);


void *xfalloc(size_t amount);
void *xfalloc0(size_t amount);
void *rexfalloc(void *ptr, size_t amount);
char *xfstrdup(char *s);
void xffree(void *ptr);

#define Assert(x) do {\
		if (!(x)) {\
			xf_info("Assert failure at %s:%d", __FILE__, __LINE__);\
		}\
} while(0)

#define FormatRecPtr(x) (uint32)((x) >> 32), (uint32) (x)

#endif
