#include "parser/parser.h"
#include "wbutils.h"

ReplicationCommand*
MakeReplCommand(ReplCommandType type)
{
	ReplicationCommand *cmd = xfalloc0(sizeof(ReplicationCommand));
	cmd->command = type;
	return cmd;
}
