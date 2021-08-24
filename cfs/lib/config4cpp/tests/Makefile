SUBDIRS= schema-types

.PHONY: all clean clobber

all:
	for dir in $(SUBDIRS); do $(MAKE) all -C $$dir $@; done

clean:
	for dir in $(SUBDIRS); do $(MAKE) clean -C $$dir $@; done

clobber: clean

