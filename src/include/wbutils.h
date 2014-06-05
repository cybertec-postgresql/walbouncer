#ifndef	_WB_UTILS_H
#define _WB_UTILS_H 1

#include "wbglobals.h"

typedef enum LogLevel {
	LOG_DEBUG3,
	LOG_DEBUG2,
	LOG_DEBUG1,
	LOG_INFO,
	LOG_WARNING,
	LOG_ERROR
} LogLevel;
#define LOG_LOWEST_LEVEL LOG_DEBUG3

extern LogLevel loggingLevel;

#define wb_log(level, levelStr, ...) if (loggingLevel <= level)\
{\
	do_wb_log(level, levelStr, __FILE__, __VA_ARGS__);\
}

#define log_debug3(...) wb_log(LOG_DEBUG3, "DEBUG3", __VA_ARGS__)
#define log_debug2(...) wb_log(LOG_DEBUG2, "DEBUG2", __VA_ARGS__)
#define log_debug1(...) wb_log(LOG_DEBUG1, "DEBUG1", __VA_ARGS__)
#define log_info(...) wb_log(LOG_INFO, "INFO", __VA_ARGS__)
#define log_warning(...) wb_log(LOG_WARNING, "WARNING", __VA_ARGS__)
#define log_error(...) wb_log(LOG_ERROR, "ERROR", __VA_ARGS__)

void do_wb_log(LogLevel logLevel, const char* logLevelStr, const char* file, const char* message, ...);
void __attribute__((noreturn)) error(const char *message, ...);
void showPQerror(PGconn *mc, char *message);


void *wballoc(size_t amount);
void *wballoc0(size_t amount);
void *rewballoc(void *ptr, size_t amount);
char *wbstrdup(char *s);
void wbfree(void *ptr);

#define Assert(x) do {\
		if (!(x)) {\
			log_info("Assert failure at %s:%d", __FILE__, __LINE__);\
		}\
} while(0)

#define FormatRecPtr(x) (uint32)((x) >> 32), (uint32) (x)


int ensure_atoi(char *s);
uint64 fromnetwork64(char *buf);
uint32 fromnetwork32(char *buf);
void write64(char *buf, uint64 v);

#endif
