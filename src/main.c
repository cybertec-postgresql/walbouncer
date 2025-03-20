#include<stdlib.h>
#include<stdio.h>
#include<errno.h>
#include<string.h>


#include <getopt.h>
#include <unistd.h>
#include <sys/wait.h>

#include "wbconfig.h"
#include "wbutils.h"
#include "wbsocket.h"
#include "wbsignals.h"
#include "wbclientconn.h"

typedef enum {
	SLOT_UNUSED,
	SLOT_ACTIVE
} BouncerSlotState;

typedef struct {
	pid_t pid;
	BouncerSlotState state;
} BouncerSlot;
typedef struct {
	BouncerSlot* slots;
	int numSlots;
} BouncerArrayStruct;

char* config_filename = NULL;
BouncerArrayStruct BouncerArray;

static pid_t fork_process();
static void InitializeBouncerArray();
static void ResizeBouncerArray(int newSize);

static pid_t fork_process()
{
	pid_t result;

	fflush(NULL);

	result = fork();
	return result;
}

static void
InitializeBouncerArray()
{
	BouncerArray.numSlots = 0;
	ResizeBouncerArray(8);
}
static void
ResizeBouncerArray(int newSize)
{
	int i;
	int oldSize = BouncerArray.numSlots;

	log_debug2("Resizing bouncer array from %d to %d", oldSize, newSize);
	sleep(1);

	if (oldSize == 0)
		BouncerArray.slots = wballoc0(sizeof(BouncerSlot)*newSize);
	else
		BouncerArray.slots = rewballoc(BouncerArray.slots, sizeof(BouncerSlot)*newSize);

	for (i = oldSize; i < newSize; i++)
	{
		BouncerArray.slots[i].pid = 0;
		BouncerArray.slots[i].state = SLOT_UNUSED;
	}
	BouncerArray.numSlots = newSize;
}

static BouncerSlot*
FindBouncerSlot()
{
	do
	{
		int i;
		for (i = 0; i < BouncerArray.numSlots; i++)
		{
			if (BouncerArray.slots[i].state == SLOT_UNUSED)
				return &(BouncerArray.slots[i]);
		}
		ResizeBouncerArray(BouncerArray.numSlots * 2);
	} while (true);
}

static void
CleanupBackend(int pid, int exitstatus)
{
	int i;
	BouncerSlot *slot = NULL;

	for (i = 0; i < BouncerArray.numSlots; i++)
		if (BouncerArray.slots[i].pid == pid)
			slot = &(BouncerArray.slots[i]);

	if (!slot)
	{
		log_warning("Trying to clean non-existant backend with PID %d", pid);
		return;
	}

	log_debug2("Backend %d exited in state %d", pid, slot->state);

	if (((exitstatus & 0xff00) >> 8) != 1 && exitstatus != 0)
	{
		log_warning("Backend with PID %d crashed with exit code %d", pid, exitstatus);
	}
	
	/* Mark the slot as empty */
	slot->pid = 0;
	slot->state = SLOT_UNUSED;
}

static void
BlockSignals()
{}

static void
UnblockSignals()
{}

static void
reaper(int signum)
{
	int save_errno = errno;
	int pid;
	int exitstatus;

	BlockSignals();

	log_debug2("Reaping dead child process");

	while ((pid = waitpid(-1, &exitstatus, WNOHANG)) > 0)
		CleanupBackend(pid, exitstatus);

	UnblockSignals();
	errno = save_errno;
}

static int
InitMasks(fd_set *rmask, WbSocket server)
{
	int maxsock = - 1;
	int fd = server->fd;
	FD_ZERO(rmask);

	FD_SET(fd, rmask);
	if (fd > maxsock)
		maxsock = fd;

	return maxsock + 1;
}

void WalBouncerMain()
{
	// set up signals for child reaper, etc.
	WbInitializeSignals();
	signal(SIGCHLD, reaper);

	// open socket for listening
	WbSocket server = OpenServerSocket(CurrentConfig->listen_port);
	WbConn conn;
	fd_set readmask;
	int nSock;

	nSock = InitMasks(&readmask, server);


	while (!stopRequested)
	{
		pid_t pid;
		{
			fd_set rmask;
			int selres;
			struct timeval timeout;
			timeout.tv_sec = 60;
			timeout.tv_usec = 0;

			memcpy((char*) &rmask, (char*)&readmask, sizeof(fd_set));
			selres = select(nSock, &rmask, NULL, NULL, &timeout);
			log_debug2("select returned %d", selres)
			/* Now check the select() result */
			if (selres < 0)
				if (errno != EINTR && errno != EWOULDBLOCK)
					error("select failed");
			if (selres <= 0)
				continue;
		}

		conn = ConnCreate(server);
		conn->master_host = CurrentConfig->master.host;
		conn->master_port = CurrentConfig->master.port;

		log_debug2("Received new connection");

		pid = fork_process();
		if (pid == 0) /* child */
		{
			CloseSocket(server);
			CloseDeathwatchPort();

			WbCCInitConnection(conn);

			WbCCPerformAuthentication(conn);

			WbCCCommandLoop(conn);

			CloseConn(conn);

			return;
		}

		if (pid < 0)
		{
			/* failed fork */
		}
		else if (pid > 0)
		{
			BouncerSlot* slot = FindBouncerSlot();
			slot->pid = pid;
			slot->state = SLOT_ACTIVE;
			CloseConn(conn);
		}
	}
	log_info("Stopping server.");
	CloseSocket(server);
}

const char* progname;

static void usage()
{
	printf("%s proxys PostgreSQL streaming replication connections and optionally does filtering\n\n", progname);
	printf("Options:\n");
	printf("  -?, --help                Print this message\n");
	printf("  -c, --config=FILE         Read configuration from this file.\n");
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

	CurrentConfig = wb_new_config();

	while (1)
	{
		static struct option long_options[] =
		{
				{"config", required_argument, 0, 'c'},
				{"port", required_argument, 0, 'p'},
				{"host", required_argument, 0, 'h'},
				{"masterport", required_argument, 0, 'P'},
				{"verbose", no_argument, 0, 'v'},
				{"help", no_argument, 0, '?'},
				{0,0,0,0}
		};
		int option_index = 0;

		c = getopt_long(argc, argv, "c:p:h:P:v?",
				long_options, &option_index);

		if (c == -1)
			break;

		switch (c)
		{
		case 'c':
			config_filename = wbstrdup(optarg);
			wb_read_config(CurrentConfig, config_filename);
			break;
		case 'p':
			CurrentConfig->listen_port = ensure_atoi(optarg);
			break;
		case 'h':
			CurrentConfig->master.host = wbstrdup(optarg);
			break;
		case 'P':
			CurrentConfig->master.port = ensure_atoi(optarg);
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

	InitializeBouncerArray();
	InitDeathWatchHandle();

	WalBouncerMain();
	return 0;
}
