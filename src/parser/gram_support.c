#include "parser/parser.h"
#include "wbutils.h"

ReplicationCommand*
MakeReplCommand(ReplCommandType type)
{
	ReplicationCommand *cmd = wballoc0(sizeof(ReplicationCommand));
	cmd->command = type;
	return cmd;
}
