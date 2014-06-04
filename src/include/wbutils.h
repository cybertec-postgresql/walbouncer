#ifndef	_WB_UTILS_H
#define _WB_UTILS_H 1

#include "wbglobals.h"

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


int ensure_atoi(char *s);
uint64 fromnetwork64(char *buf);
uint32 fromnetwork32(char *buf);
void write64(char *buf, uint64 v);

#endif
