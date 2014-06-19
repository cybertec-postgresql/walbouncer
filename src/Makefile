all: walbouncer

CFLAGS=-O2 -Wall -Werror -g -fno-omit-frame-pointer

pglibdir = $(shell pg_config --libdir)
pgincludedir = $(shell pg_config --includedir)

objects = main.o wbsocket.o wbutils.o parser/repl_gram.o parser/scansup.o parser/stringinfo.o parser/gram_support.o pg_crc.o wbmasterconn.o wbfilter.o wbclientconn.o wbsignals.o  

walbouncer: $(objects)
	gcc $(CFLAGS) -o walbouncer $(objects) -L$(pglibdir) -lpq

 $(objects): %.o: %.c include/*.h
	gcc $(CFLAGS) -I$(pgincludedir) -Iinclude -c $< -o $@

parser/repl_scanner.c : parser/repl_scanner.l
	flex -o $@ $<

parser/repl_gram.c : parser/repl_gram.y parser/repl_scanner.c
	bison -o $@ $<

test: all
	./walbouncer
	echo "Test success"
