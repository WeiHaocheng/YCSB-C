CC=g++
CFLAGS=-std=c++11 -g -Wall -pthread -I./ -L ./ -L/usr/local/lib
#LDFLAGS= -lpthread -ltbb -lhiredis -lrocksdb_debug -lzstd -llz4 -lbz2 -lz -lsnappy -lleveldb -lbcc
LDFLAGS= -lpthread -ltbb -lhiredis -lrocksdb_debug -lzstd -llz4 -lbz2 -lz -lsnappy -lleveldb
#LDFLAGS= -lpthread -ltbb -lhiredis -lrocksdb -lzstd -llz4 -lbz2 -lz -lsnappy -lpmemkv
SUBDIRS=core db redis
SUBSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc)
OBJECTS=$(SUBSRCS:.cc=.o)
EXEC=ycsbc

all: $(SUBDIRS) $(EXEC)

$(SUBDIRS):
	$(MAKE) -C $@

$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
	done
	$(RM) $(EXEC)

.PHONY: $(SUBDIRS) $(EXEC)

