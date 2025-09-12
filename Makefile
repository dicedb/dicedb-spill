CC = gcc
CFLAGS = -fPIC -std=gnu99 -Wall -Wextra -O2
LDFLAGS = -shared

MODULE = dicedb-infcache.so
SOURCE = main.c

all: $(MODULE)

$(MODULE): $(SOURCE)
	$(CC) $(CFLAGS) $(LDFLAGS) -I/home/arpit/workspace/dicedb/dice2/src -o $@ $<

clean:
	rm -f $(MODULE)

install: $(MODULE)
	cp $(MODULE) /usr/local/lib/

.PHONY: all clean install
