// For asprintf. TODO: make POSIX compatible
#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include "wbsocket.h"
#include "wbutils.h"

#define BACKLOG 10
#define SEND_BUFFER_INIT_SIZE (256*1024)
#define RECV_BUFFER_SIZE 8192

WbSocket
OpenServerSocket(char *port)
{
	int status;
	struct addrinfo hints;
	struct addrinfo *res;
	int yes=1;
	WbSocket sock = wballoc(sizeof(WbSocketStruct));

	log_info("Starting socket on port %s", port);

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	if ((status = getaddrinfo(NULL, port, &hints, &res)) != 0) {
		char *errmsg;
	    if (asprintf(&errmsg, "getaddrinfo error: %s", gai_strerror(status)) < 0)
	    	error("Out of memory while reporting error");
	    error(errmsg);
	}

	sock->fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);

	if (setsockopt(sock->fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1)
	    error("setsockopt");

	if (bind(sock->fd, res->ai_addr, res->ai_addrlen))
		error("Bind failed");
	if (listen(sock->fd, BACKLOG))
		error("Listen failed");

	freeaddrinfo(res);

	return sock;
}

WbConn
ConnCreate(WbSocket server)
{
	struct sockaddr_storage their_addr;
	socklen_t addr_size;
	WbConn conn = wballoc0(sizeof(WbPortStruct));

	conn->fd = accept(server->fd, (struct sockaddr *) &their_addr, &addr_size);

	conn->recvBuffer = wballoc(RECV_BUFFER_SIZE);
	conn->recvPointer = 0;
	conn->recvLength = 0;

	conn->sendBuffer = wballoc(SEND_BUFFER_INIT_SIZE);
	conn->sendBufSize = SEND_BUFFER_INIT_SIZE;
	conn->sendBufLen = 0;
	conn->sendBufMsgLenPtr = -1;
	conn->sendBufFlushPtr = 0;

	conn->sentPtr = 0;
	conn->lastSend = 0;
	conn->copyDoneSent = false;
	conn->copyDoneReceived = false;

	conn->replyForwarded = true;
	conn->feedbackForwarded = true;

	log_info("Waiting for connections.");

	return conn;
}

bool
ConnHasDataToFlush(WbConn conn)
{
	return conn->sendBufFlushPtr < conn->sendBufLen;
}

int
ConnFlush(WbConn conn, ConnFlushMode mode)
{
	int sent = conn->sendBufFlushPtr;
	int remaining = conn->sendBufLen - conn->sendBufFlushPtr;
	int flags = 0;

	// We should not be in the middle of constructing a message while we are
	// flushing WAL.
	Assert(conn->sendBufMsgLenPtr == -1);

	if (mode == FLUSH_ASYNC)
		flags |= MSG_DONTWAIT;

	while (remaining > 0)
	{
		int r;
		log_debug1("Conn: Sending to client %d bytes of data", remaining);

		r = send(conn->fd, conn->sendBuffer + sent, remaining, flags);
		if (r <= 0)
		{
			if (errno == EINTR)
				continue;

			if (errno == EAGAIN || errno == EWOULDBLOCK)
			{
				if (mode != FLUSH_ASYNC)
					error("Socket returned %d on a blocking send call", errno);
				log_debug1("Sending out data to client would have blocked.");
				conn->sendBufFlushPtr = sent;
				return 0;
			}

			error("Could not send data to client");
			return EOF;
		}
		sent += r;
		if (r < remaining)
			log_debug1("Sent out %d/%d bytes", sent, remaining);
		remaining -= r;
	}
	conn->sendBufFlushPtr = 0;
	conn->sendBufLen = 0;
	conn->sendBufMsgLenPtr = -1;

#if DEBUG
	memset(conn->sendBuffer, '~', conn->sendBufSize);
#endif

	return 0;
}

bool
ConnSetNonBlocking(WbConn conn, bool nonblocking)
{
	if (nonblocking)
		return (fcntl(conn->fd, F_SETFL, O_NONBLOCK) != -1);
	else
	{
		int			flags;

		flags = fcntl(conn->fd, F_GETFL);
		if (flags < 0 || fcntl(conn->fd, F_SETFL, (long) (flags & ~O_NONBLOCK)))
			return false;
		return true;
	}
}

static int
ConnRecvBuf(WbConn conn)
{
	if (conn->recvPointer > 0)
	{
		if (conn->recvLength > conn->recvPointer)
		{
			memmove(conn->recvBuffer, conn->recvBuffer + conn->recvPointer,
					conn->recvLength - conn->recvPointer);
			conn->recvLength -= conn->recvPointer;
			conn->recvPointer = 0;
		}
		else
			conn->recvLength = conn->recvPointer = 0;
	}

	ConnSetNonBlocking(conn, false);

	for (;;)
	{
		int r;
		r = recv(conn->fd, conn->recvBuffer + conn->recvLength,
				RECV_BUFFER_SIZE - conn->recvLength, 0);
		if (r < 0)
		{
			if (errno == EINTR)
				continue;

			log_error("Could not read from socket");
			return EOF;
		}
		if (r == 0)
		{
			return EOF;
		}
		conn->recvLength += r;
		return 0;
	}
	// silence the compiler
	Assert(false);
	return EOF;
}

int
ConnGetBytes(WbConn conn, char *s, size_t len)
{
	size_t amount;

	while (len > 0)
	{
		while (conn->recvPointer >= conn->recvLength)
		{
			if (ConnRecvBuf(conn))
				return EOF;
		}
		amount = conn->recvLength - conn->recvPointer;
		if (amount > len)
			amount = len;
		memcpy(s, conn->recvBuffer + conn->recvPointer, amount);
		conn->recvPointer += amount;
		s += amount;
		len -= amount;
	}
	return 0;
}

int
ConnGetByteIfAvailable(WbConn conn, char *c)
{
	int			r;

	if (conn->recvPointer < conn->recvLength)
	{
		*c = conn->recvBuffer[conn->recvPointer++];
		return 1;
	}

	/* Put the socket into non-blocking mode */
	ConnSetNonBlocking(conn, true);

	r = recv(conn->fd, c, 1, 0);

	ConnSetNonBlocking(conn, false);

	if (r < 0)
	{
		/*
		 * Ok if no data available without blocking or interrupted (though
		 * EINTR really shouldn't happen with a non-blocking socket). Report
		 * other errors.
		 */
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
			r = 0;
		else
		{
			/*
			 * Careful: an ereport() that tries to write to the client would
			 * cause recursion to here, leading to stack overflow and core
			 * dump!  This message must go *only* to the postmaster log.
			 */
			error("could not receive nb data from client");
			r = EOF;
		}
	}
	else if (r == 0)
	{
		/* EOF detected */
		r = EOF;
	}

	return r;
}

void
CloseConn(WbConn conn)
{
	close(conn->fd);
	free(conn);
}



void
CloseSocket(WbSocket sock)
{
	close(sock->fd);
	free(sock);
}

static
void
ConnEnsureFreeSpace(WbConn conn, int amount)
{
	if (conn->sendBufSize - conn->sendBufLen < amount)
	{
		int new_size = conn->sendBufSize*2;
		conn->sendBuffer = rewballoc(conn->sendBuffer, new_size);
		conn->sendBufSize = new_size;
	}
}

int
ConnGetSocket(WbConn conn)
{
	return conn->fd;
}

void
ConnBeginMessage(WbConn conn, char type)
{
	Assert(conn->sendBufMsgLenPtr == -1);

	ConnEnsureFreeSpace(conn, 5);
	conn->sendBufMsgLenPtr = conn->sendBufLen + 1;
	*(conn->sendBuffer + conn->sendBufLen) = type;
	conn->sendBufLen += 5;

}

void
ConnSendInt(WbConn conn, int i, int b)
{
	char *target = conn->sendBuffer + conn->sendBufLen;
	ConnEnsureFreeSpace(conn, b);

	switch (b)
	{
		case 1:
			*((unsigned char *) target) = (unsigned char) i;
			break;
		case 2:
			*((uint16*) target) = htons((uint16)i);
			break;
		case 4:
			*((uint32*) target) = htonl((uint32) i);
			break;
		default:
			error("Invalid int size");
	}
	conn->sendBufLen += b;
}

void
ConnSendInt64(WbConn conn, int64 i)
{
	uint32 hi = htonl((uint32) (i >> 32));
	uint32 lo = htonl((uint32) i);
	ConnEnsureFreeSpace(conn, 8);

	*( (int32*) (conn->sendBuffer + conn->sendBufLen)) = hi;
	conn->sendBufLen += 4;
	*( (int32*) (conn->sendBuffer + conn->sendBufLen)) = lo;
	conn->sendBufLen += 4;
}

void
ConnSendString(WbConn conn, const char *str)
{
	int slen = strlen(str);

	ConnEnsureFreeSpace(conn, slen+1);

	//TODO: pg_server_to_client encoding changehere
	memcpy(conn->sendBuffer + conn->sendBufLen, str, slen+1);

	conn->sendBufLen += slen + 1;
}

void
ConnSendBytes(WbConn conn, const char *str, int n)
{
	ConnEnsureFreeSpace(conn, n);

	memcpy(conn->sendBuffer + conn->sendBufLen, str, n);
	conn->sendBufLen += n;
}

void
ConnEndMessage(WbConn conn)
{
	uint32 n32;

	Assert(conn->sendBufMsgLenPtr > 0);

	n32 = htonl((uint32) (conn->sendBufLen - conn->sendBufMsgLenPtr));
	log_debug3("Buffering message %c with length %d", conn->sendBuffer[conn->sendBufMsgLenPtr-1], (conn->sendBufLen - conn->sendBufMsgLenPtr));

	// TODO: handle unaligned access
	*((uint32*)(conn->sendBuffer + conn->sendBufMsgLenPtr)) = n32;

	conn->sendBufMsgLenPtr = -1;
}

int
ConnGetByte(WbConn conn)
{
	unsigned char value;
	if (ConnGetBytes(conn, (char*) &value, 1) == EOF)
		return EOF;

	return value;
}

int
ConnGetMessage(WbConn conn, WbMessage **msg)
{
	int32 len;
	WbMessage *buf;

	if (ConnGetBytes(conn, (char*) &len, 4) == EOF)
	{
		log_error("Unexpeted EOF within message length word");
		return EOF;
	}

	len = ntohl(len);

	if (len < 4)
	{
		log_error("Invalid message length");
		return EOF;
	}

	len -= 4;
	*msg = buf = wballoc(offsetof(WbMessage, data) + len + 1);
	buf->len = len;
	if (len > 0)
	{
		if (ConnGetBytes(conn, buf->data, len) == EOF)
		{
			log_error("Incomplete message from client");
			return EOF;
		}
		buf->data[len] = '\0';
	}
	return 0;
}

void
ConnFreeMessage(WbMessage *msg)
{
	wbfree(msg);
}
