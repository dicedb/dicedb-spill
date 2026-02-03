SHOBJ_CFLAGS ?= -W -Wall -fno-common -g -ggdb3 -std=gnu99 -O2
SHOBJ_LDFLAGS ?= -shared

MODULE_NAME = lib-infcache
MODULE_SO = $(MODULE_NAME).so
MODULE_H_DIR = ../../src

# Compression support: set ENABLE_COMPRESSION=1 to link compression libraries
ENABLE_COMPRESSION ?= 0

INCLUDES = -I/usr/local/include
LIBS_BASE = -L/usr/local/lib -lrocksdb -lstdc++ -lpthread -ldl
LIBS_COMPRESSION = -lsnappy -lz -lbz2 -llz4 -lzstd

ifeq ($(ENABLE_COMPRESSION),1)
LIBS = $(LIBS_BASE) $(LIBS_COMPRESSION)
else
LIBS = $(LIBS_BASE)
endif

.SUFFIXES:
.PHONY: all clean test

all: $(MODULE_SO)

SOURCES = infcache.c
OBJECTS = $(SOURCES:.c=.o)

$(MODULE_SO): $(OBJECTS)
	$(CC) -I. $(CFLAGS) $(SHOBJ_CFLAGS) -I$(MODULE_H_DIR) $(INCLUDES) -fPIC -o $@ $^ $(SHOBJ_LDFLAGS) $(LIBS) -lc

%.o: %.c
	$(CC) -I. $(CFLAGS) $(SHOBJ_CFLAGS) -I$(MODULE_H_DIR) $(INCLUDES) -fPIC -c $< -o $@

clean:
	rm -f $(MODULE_SO) $(OBJECTS) test_unit
	rm -rf .test_venv __pycache__ *.pyc

test:
	cd tests && ./run_tests.sh
