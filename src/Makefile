all: xlogfilter

objects = main.o xfsocket.o xfutils.o parser/repl_gram.o parser/scansup.o parser/stringinfo.o parser/gram_support.o pg_crc.o  

xlogfilter: $(objects)
	gcc -o xlogfilter $(objects) -L/home/ants/code/compiles/pg-master/lib -lpq

 $(objects): %.o: %.c include/*.h
	gcc -I/home/ants/code/compiles/pg-master/include -Iinclude -c $< -o $@

parser/repl_scanner.c : parser/repl_scanner.l
	flex -o $@ $<

parser/repl_gram.c : parser/repl_gram.y parser/repl_scanner.c
	bison -o $@ $<

test: all
	./xlogfilter
	echo "Test success"
