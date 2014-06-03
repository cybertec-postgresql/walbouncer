all: xlogfilter

pglibdir = $(shell pg_config --libdir)
pgincludedir = $(shell pg_config --includedir)

objects = main.o xfsocket.o xfutils.o parser/repl_gram.o parser/scansup.o parser/stringinfo.o parser/gram_support.o pg_crc.o wbmasterconn.o wbfilter.o  

xlogfilter: $(objects)
	gcc -o xlogfilter $(objects) -L$(pglibdir) -lpq

 $(objects): %.o: %.c include/*.h
	gcc -I$(pgincludedir) -Iinclude -c $< -o $@

parser/repl_scanner.c : parser/repl_scanner.l
	flex -o $@ $<

parser/repl_gram.c : parser/repl_gram.y parser/repl_scanner.c
	bison -o $@ $<

test: all
	./xlogfilter
	echo "Test success"
