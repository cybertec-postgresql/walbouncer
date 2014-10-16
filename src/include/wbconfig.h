#ifndef	_WB_CONFIG_H
#define _WB_CONFIG_H 1

#include "wbutils.h"

typedef struct {
	char *name;
	struct {
		hostmask source_ip;
		char *application_name;
	} match;
	struct {
		char **include_tablespaces;
		int n_include_tablespaces;
		char **exclude_tablespaces;
		int n_exclude_tablespaces;
		char **include_databases;
		int n_include_databases;
		char **exclude_databases;
		int n_exclude_databases;
	} filter;
} wb_config_entry;

typedef struct wb_config_list_entry {
	struct wb_config_list_entry *next;
	wb_config_entry entry;
} wb_config_list_entry;

typedef struct {
	int listen_port;
	struct {
		char *host;
		int port;
	} master;
	wb_config_list_entry *configurations;
} wb_configuration;

extern wb_configuration *CurrentConfig;

wb_configuration* wb_new_config();
wb_configuration* wb_read_config(wb_configuration* config, char *filename);
void wb_delete_config(wb_configuration* config);

#endif
