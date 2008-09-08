
DIRS = libmseed libdali src

# Test for Makefile/makefile and run make, run configure if it exists
# and no Makefile does.

all clean static install gcc gcc32 gcc64 debug gccdebug gcc32debug gcc64debug ::
	@for d in $(DIRS) ; do \
	  echo "Running $(MAKE) $@ in $$d" ; \
	  if [ -f $$d/Makefile -o -f $$d/makefile ] ; then \
	    ( cd $$d && $(MAKE) $@ ) ; \
	  elif [ -d $$d ] ; \
	    then ( echo "ERROR: no Makefile/makefile in $$d for $(CC)" ) ; \
	  fi ; \
	done
