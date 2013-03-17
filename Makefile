LUACPATH = /usr/lib/lua/5.1

SRCNAME = src/lua-mongoc.c
LIBNAME = mongoc.so

LUAPKG = lua

CFLAGS = `pkg-config $(LUAPKG) --cflags` -O2 -fpic
LFLAGS = -shared -L/usr/lib -lmongoc

CC = gcc

all: $(LIBNAME)

$(LIBNAME):
	$(CC) --std=c99 -o $(LIBNAME) $(SRCNAME) $(CFLAGS) $(LFLAGS)

install: $(LIBNAME)
	cp $(LIBNAME) $(LUACPATH)

uninstall:
	rm $(LUACPATH)/$(LIBNAME)

clean: 
	rm -f $(LIBNAME)
