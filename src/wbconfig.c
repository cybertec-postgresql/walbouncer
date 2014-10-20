#include <stdio.h>

#include <yaml.h>
#include "wbconfig.h"
#include "wbutils.h"

typedef struct {
	yaml_parser_t parser;
	yaml_event_t event;
	bool done;
} wb_config_parser_state;

wb_configuration *CurrentConfig = NULL;

static int wb_read_main_config(wb_config_parser_state *state, wb_configuration* config);
static int wb_read_master_config(wb_config_parser_state *state, wb_configuration* config);
static int wb_read_configurations(wb_config_parser_state *state, wb_configuration* config);
static int wb_read_configuration_entry(wb_config_parser_state *state, wb_config_entry *entry);


static wb_config_list_entry*
wb_new_config_entry()
{
	wb_config_list_entry *item = wballoc(sizeof(wb_config_list_entry));
	item->next = NULL;
	memset(&(item->entry), 0, sizeof(wb_config_entry));
	return item;
}

static void
wb_config_parser_init(wb_config_parser_state *state)
{
	if (!yaml_parser_initialize(&(state->parser)))
		error("Configuration parser initialization failed.");
	state->done = false;
}

static void
wb_config_parser_delete(wb_config_parser_state *state)
{
	yaml_parser_delete(&(state->parser));
}

static bool
wb_expect_mapping(wb_config_parser_state *state)
{
	bool result = true;
	if (!yaml_parser_parse(&(state->parser), &(state->event)))
	{
		state->done = true;
		return true;
	}
	if (state->event.type != YAML_MAPPING_START_EVENT)
		result = false;
	yaml_event_delete(&(state->event));

	return result;
}

static bool
wb_expect_mapping_file(wb_config_parser_state *state)
{
	bool keep_scanning = true;
	bool result = true;
	while (keep_scanning)
	{
		if (!yaml_parser_parse(&(state->parser), &(state->event)))
		{
			state->done = true;
			return true;
		}
		keep_scanning = (state->event.type == YAML_STREAM_START_EVENT ||
						 state->event.type == YAML_DOCUMENT_START_EVENT);
		result = (state->event.type == YAML_MAPPING_START_EVENT);
		yaml_event_delete(&(state->event));
	}
	return result;
}

static bool
wb_expect_sequence(wb_config_parser_state *state)
{
	bool result = true;
	if (!yaml_parser_parse(&(state->parser), &(state->event)))
	{
		state->done = true;
		return true;
	}
	if (state->event.type != YAML_SEQUENCE_START_EVENT)
		result = false;
	yaml_event_delete(&(state->event));

	return result;
}

static char*
wb_str_value(wb_config_parser_state *state)
{
	return strndup((char*) state->event.data.scalar.value, state->event.data.scalar.length);
}

static bool
wb_int_value(wb_config_parser_state *state, int *result)
{
	char format[32];
	char *buf = (char*) state->event.data.scalar.value;
	size_t len = state->event.data.scalar.length;

	snprintf(format, 32, "%%%ldi", len);
	if (sscanf(buf, format, result))
		return true;
	return false;
}

static char*
wb_read_key(wb_config_parser_state *state)
{
	char *result;
	if (!yaml_parser_parse(&(state->parser), &(state->event)))
	{
		state->done = true;
		return NULL;
	}
	if (state->event.type == YAML_MAPPING_END_EVENT)
		result = NULL;
	else if (state->event.type != YAML_SCALAR_EVENT)
		error("Unexpected event type %d while parsing YAML", state->event.type);
	else
		result = wb_str_value(state);

	yaml_event_delete(&(state->event));
	return result;
}




static int
wb_read_int(wb_config_parser_state *state)
{
	int result;
	if (!yaml_parser_parse(&(state->parser), &(state->event)))
	{
		state->done = true;
		return 0;
	}
	if (state->event.type != YAML_SCALAR_EVENT)
		error("Unexpected event type while parsing YAML");
	else
		if (!wb_int_value(state, &result))
			error("Invalid format for integer: '%s'", wb_str_value(state));

	yaml_event_delete(&(state->event));
	return result;
}

static char*
wb_read_string(wb_config_parser_state *state)
{
	char *result;
	if (!yaml_parser_parse(&(state->parser), &(state->event)))
	{
		state->done = true;
		return 0;
	}
	if (state->event.type != YAML_SCALAR_EVENT)
		error("Unexpected event type while parsing YAML");
	else
		result = wb_str_value(state);

	yaml_event_delete(&(state->event));
	return result;
}

static void
wb_read_list_of_string(wb_config_parser_state *state, char ***list, int *n)
{
	bool done = false;

	if (!wb_expect_sequence(state))
		error("Expecting a sequence of strings");

	*n = 0;

	while (!done)
	{
		if (!yaml_parser_parse(&(state->parser), &(state->event)))
		{
			state->done = true;
			return;
		}
		if (state->event.type == YAML_SCALAR_EVENT)
		{
			if (*n)
			{
				*n += 1;
				*list = rewballoc(*list, sizeof(char**)*(*n));
			}
			else
			{
				*n = 1;
				*list = wballoc(sizeof(char**));
			}
			(*list)[*n-1] = wb_str_value(state);
		}
		else if (state->event.type == YAML_SEQUENCE_END_EVENT)
		{
			done = true;
		}
		else
			error("Invalid value for a list of strings");
		yaml_event_delete(&(state->event));
	}
}

static bool
wb_sequence_of_mappings(wb_config_parser_state *state)
{
	bool result;
	if (!yaml_parser_parse(&(state->parser), &(state->event)))
	{
		state->done = true;
		return true;
	}

	if (state->event.type == YAML_MAPPING_START_EVENT)
		result = true;
	else if (state->event.type == YAML_SEQUENCE_END_EVENT)
		result = false;
	else
		error("Items of sequence must be mappings, %d", state->event.type );
	//log_debug2("Sequence of mappings, with %d, result %d", state->event.type, result);
	yaml_event_delete(&(state->event));
	return result;
}

wb_configuration*
wb_new_config()
{
	wb_configuration *config = wballoc(sizeof(wb_configuration));

	config->listen_port = 5433;
	config->master.host = "localhost";
	config->master.port = 5432;
	config->configurations = NULL;

	return config;
}

#define FreeIfNotNull(x) if (x) { wbfree(x); }

static void
wb_delete_config_entry(wb_config_entry *entry)
{
	FreeIfNotNull(entry->filter.include_databases);
	FreeIfNotNull(entry->filter.include_tablespaces);
	FreeIfNotNull(entry->filter.exclude_databases);
	FreeIfNotNull(entry->filter.exclude_tablespaces);

	FreeIfNotNull(entry->match.application_name);

	wbfree(entry);
}

void
wb_delete_config(wb_configuration* config)
{
	{
		wb_config_list_entry *entry = config->configurations;
		while (entry)
		{
			wb_config_list_entry *next = entry->next;
			wb_delete_config_entry(&(entry->entry));
			wbfree(entry);
			entry = next;
		}
	}
}

#define CHECK_FOR_FAILURE(state) if (state->done) { \
	return -1;\
}

wb_configuration*
wb_read_config(wb_configuration *config, char *filename)
{
	wb_config_parser_state state_static_alloc;
	wb_config_parser_state *state = &state_static_alloc;

	FILE *input = fopen(filename, "rb");

	if (!input)
		error("Reading configuration file %s failed", filename);

	wb_config_parser_init(state);

	yaml_parser_set_input_file(&(state->parser), input);

	wb_read_main_config(state, config);

	wb_config_parser_delete(state);
	return config;
}

static int
wb_read_main_config(wb_config_parser_state *state, wb_configuration *config)
{
	char *key;
	if (!wb_expect_mapping_file(state))
		error("Configuration file must be a YAML mapping");

	CHECK_FOR_FAILURE(state);

	while ((key = wb_read_key(state)))
	{
		if (strcmp(key, "listen_port") == 0)
			config->listen_port = wb_read_int(state);
		else if (strcmp(key, "master") == 0)
			wb_read_master_config(state, config);
		else if (strcmp(key, "configurations") == 0)
			wb_read_configurations(state, config);
		else
			log_warning("Unknown configuration entry with key %s", key);
		free(key);
		CHECK_FOR_FAILURE(state);
	}

	return 0;
}

static int
wb_read_master_config(wb_config_parser_state *state, wb_configuration *config)
{
	char *key;
	if (!wb_expect_mapping(state))
		error("Master config must be a YAML mapping");

	CHECK_FOR_FAILURE(state);
	while ((key = wb_read_key(state)))
	{
		if (strcmp(key, "host") == 0)
			config->master.host = wb_read_string(state);
		else if (strcmp(key, "port") == 0)
			config->master.port = wb_read_int(state);
		else
			log_warning("Unknown configuration entry with key %s", key);
		free(key);
		CHECK_FOR_FAILURE(state);
	}

	return 0;
}

static int
wb_read_configurations(wb_config_parser_state *state, wb_configuration *config)
{
	char *key;
	wb_config_list_entry **next_ptr;
	if (!wb_expect_sequence(state))
		error("Configuration file must be a YAML sequence");

	CHECK_FOR_FAILURE(state);

	next_ptr = &(config->configurations);
	while (wb_sequence_of_mappings(state))
	{
		wb_config_list_entry *item;
		//log_debug2("Read name of conf key");
		key = wb_read_key(state);
		if (!key)
			error("Configuration mappings must contain a key");

		item = wb_new_config_entry();
		item->entry.name = key;
		wb_read_configuration_entry(state, &(item->entry));

		//log_debug2("Read end of mapping key");
		key = wb_read_key(state);
		if (key)
			error("Configuration entries must be maps with a single key");

		*next_ptr = item;
		next_ptr = &(item->next);
	}

	return 0;
}
static int
wb_read_configuration_entry(wb_config_parser_state *state, wb_config_entry *entry)
{
	char *key;
	if (!wb_expect_mapping(state))
		error("Configuration must be a mapping");
	//log_debug2("Read config entry keys");

	while ((key = wb_read_key(state)))
	{
		if (strcmp(key, "match") == 0)
		{
			if (!wb_expect_mapping(state))
				error("Match must be a mapping");
			while ((key = wb_read_key(state)))
			{
				if (strcmp(key, "source") == 0)
				{
					char *mask = wb_read_string(state);
					if (!parse_hostmask(mask, &(entry->match.source_ip)))
						error("Invalid hostmask %s", mask);
					wbfree(mask);
				}
				else if (strcmp(key, "application_name") == 0)
					entry->match.application_name = wb_read_string(state);
				else
					error("Unexpected key %s for match", key);
				free(key);
			}
		}
		else if (strcmp(key, "filter") == 0)
		{
			if (!wb_expect_mapping(state))
				error("Match must be a mapping");
			while ((key = wb_read_key(state)))
			{
				if (strcmp(key, "include_tablespaces") == 0)
					wb_read_list_of_string(state,
							&(entry->filter.include_tablespaces),
							&(entry->filter.n_include_tablespaces));
				else if (strcmp(key, "include_databases") == 0)
					wb_read_list_of_string(state,
							&(entry->filter.include_databases),
							&(entry->filter.n_include_databases));
				else if (strcmp(key, "exclude_tablespaces") == 0)
					wb_read_list_of_string(state,
							&(entry->filter.exclude_tablespaces),
							&(entry->filter.n_exclude_tablespaces));
				else if (strcmp(key, "exclude_databases") == 0)
					wb_read_list_of_string(state,
							&(entry->filter.exclude_databases),
							&(entry->filter.n_exclude_databases));
				else
					error("Unexpected key %s for match", key);
				free(key);
			}
		}
		else
		{
			error("Unknown config entry %s", key);
		}
	}
	return 0;
}

