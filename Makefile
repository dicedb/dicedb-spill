CC = gcc
CFLAGS = -fPIC -std=gnu99 -Wall -Wextra -O2
LDFLAGS = -shared -lrocksdb -lstdc++ -lpthread -ldl
INCLUDES = -I/usr/local/include

MODULE = dicedb-infcache.so
SOURCE = main.c

all: $(MODULE)

$(MODULE): $(SOURCE)
	$(CC) $(CFLAGS) $(INCLUDES) -I/home/arpit/workspace/dicedb/dice2/src -o $@ $< $(LDFLAGS)

clean:
	rm -f $(MODULE)

install: $(MODULE)
	cp $(MODULE) /usr/local/lib/

.PHONY: all clean install
