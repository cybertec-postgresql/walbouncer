#include<stdlib.h>
#include<stdio.h>
#include<errno.h>
#include<string.h>


#include <getopt.h>

#include "wbutils.h"
#include "wbsocket.h"

#include "wbclientconn.h"

char* listen_port = "5433";
char* master_host = "localhost";
char* master_port = "5432";

void WalBouncerMain()
{
	// set up signals for child reaper, etc.
	// open socket for listening
	XfSocket server = OpenServerSocket(listen_port);
	XfConn conn;

	conn = ConnCreate(server);
	conn->master_host = master_host;
	conn->master_port = master_port;

	WbCCInitConnection(conn);

	WbCCPerformAuthentication(conn);

	WbCCCommandLoop(conn);

	CloseConn(conn);

	CloseSocket(server);
}

const char* progname;

static void usage()
{
	printf("%s proxys PostgreSQL streaming replication connections and optionally does filtering\n\n", progname);
	printf("Options:\n");
	printf("  -?, --help                Print this message\n");
	printf("  -h, --host=HOST           Connect to master on this host. Default localhost\n");
	printf("  -P, --masterport=PORT     Connect to master on this port. Default 5432\n");
	printf("  -p, --port=PORT           Run proxy on this port. Default 5433\n");
	printf("  -v, --verbose             Output additional debugging information\n");

}

int
main(int argc, char **argv)
{
	int c;
	progname = "walbouncer";

	while (1)
	{
		static struct option long_options[] =
		{
				{"port", required_argument, 0, 'p'},
				{"host", required_argument, 0, 'h'},
				{"masterport", required_argument, 0, 'P'},
				{"verbose", no_argument, 0, 'v'},
				{"help", no_argument, 0, '?'},
				{0,0,0,0}
		};
		int option_index = 0;

		c = getopt_long(argc, argv, "p:h:P:v?",
				long_options, &option_index);

		if (c == -1)
			break;

		switch (c)
		{
		case 'p':
			listen_port = wbstrdup(optarg);
			break;
		case 'h':
			master_host = wbstrdup(optarg);
			break;
		case 'P':
			master_port = wbstrdup(optarg);
			break;
		case 'v':
			if (loggingLevel > LOG_LOWEST_LEVEL)
				loggingLevel--;
			break;
		case '?':
			usage();
			exit(0);
			break;
		default:
			fprintf(stderr, "Invalid arguments\n");
			exit(1);
		}
	}


	WalBouncerMain();
	return 0;
}
