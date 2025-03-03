all: walbouncer

# Check for PostgrSQL 16
ifeq ($(shell pg_config --version | grep -E "PostgreSQL 16"),)
    $(error PostgreSQL version 9.5 through 16 required, pg_config provides $(shell pg_config --version))
endif

CFLAGS=-O2 -Wall -Werror -g -std=gnu99

pglibdir = $(shell pg_config --libdir)
pgincludedir = $(shell pg_config --includedir)
pgbindir = $(shell pg_config --bindir)

objects = main.o wbsocket.o wbutils.o parser/repl_gram.o parser/scansup.o parser/stringinfo.o parser/gram_support.o wbcrc32c.o wbmasterconn.o wbfilter.o wbclientconn.o wbsignals.o wbconfig.o

walbouncer: $(objects)
	gcc $(CFLAGS) -o walbouncer $(objects) -L$(pglibdir)/ -lpq -lyaml

 $(objects): %.o: %.c include/*.h
	gcc $(CFLAGS) -I$(pgincludedir) -Iinclude -c $< -o $@

clean:
	rm walbouncer parser/repl_scanner.c parser/repl_gram.c $(objects)

parser/repl_scanner.c : parser/repl_scanner.l
	flex -o $@ $<

parser/repl_gram.c : parser/repl_gram.y parser/repl_scanner.c
	bison -Wno-deprecated -o $@ $<

test: all
	cd ../tests; ./run_demo.sh

unittests/test: unittests/test.c wbutils.o
	gcc $(CFLAGS) -o $@ $^ -I$(pgincludedir) -Iinclude -L$(pglibdir) -lpq -lyaml

run-unit: walbouncer unittests/test
	unittests/test

install: walbouncer
	cp walbouncer $(pgbindir)/walbouncer

uninstall:
	rm $(pgbindir)/walbouncer
