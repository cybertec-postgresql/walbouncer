
#ifndef PARSER_H
#define PARSER_H

#include "xfglobals.h"

typedef enum {
	REPL_IDENTIFY_SYSTEM,
	REPL_BASE_BACKUP,
	REPL_CREATE_SLOT,
	REPL_DROP_SLOT,
	REPL_START_PHYSICAL,
	REPL_START_LOGICAL,
	REPL_TIMELINE
} ReplCommandType;

typedef struct ReplicationCommand {
	ReplCommandType command;
	char *slotname;
	TimeLineID timeline;
	XLogRecPtr startpoint;
} ReplicationCommand;

// implemented by gram_support.c

ReplicationCommand*
MakeReplCommand(ReplCommandType cmd);

// implemented by repl_gram.c

extern int	replication_yyparse(void);
extern int	replication_yylex(void);
extern void replication_yyerror(const char *str);
extern void replication_scanner_init(const char *query_string);
extern void replication_scanner_finish(void);

extern ReplicationCommand *replication_parse_result;



#endif   /* PARSER_H */
