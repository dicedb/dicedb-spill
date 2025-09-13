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
	rm -f $(MODULE) test_unit
	rm -rf .test_venv __pycache__ *.pyc

install: $(MODULE)
	cp $(MODULE) /usr/local/lib/

test: $(MODULE)
	./run_tests.sh

.PHONY: all clean install test
