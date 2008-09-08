
# Standard compiler parameters
CFLAGS += -I../libmseed -I../libdali

LDFLAGS = -L../libmseed -L../libdali
LDLIBS  = -lmseed -ldali

# For SunOS/Solaris uncomment the following line
#LDLIBS = -lmseed -ldali -lresolv -lsocket -lnsl -lrt

BIN  = ../miniseed2dmc

OBJS = rbtree.o stack.o seedutil.o edir.o miniseed2dmc.o

all: $(BIN)

$(BIN): $(OBJS)
	$(CC) $(CFLAGS) -o $(BIN) $(OBJS) $(LDFLAGS) $(LDLIBS)

static: $(OBJS)
	$(CC) $(CFLAGS) -static -o $(BIN) $(OBJS) $(LDFLAGS) $(LDLIBS)

cc:
	@$(MAKE) "CC=$(CC)" "CFLAGS=$(CFLAGS)"

gcc:
	@$(MAKE) "CC=$(GCC)" "CFLAGS=$(GCCFLAGS)"

debug:
	$(MAKE) "CC=$(CC)" "CFLAGS=-g $(CFLAGS)"

gccdebug:
	$(MAKE) "CC=$(GCC)" "CFLAGS=-g $(GCCFLAGS)"

clean:
	rm -f $(OBJS) $(BIN)

install:
	@echo
	@echo "No install method, copy the binary and man page yourself"
	@echo
