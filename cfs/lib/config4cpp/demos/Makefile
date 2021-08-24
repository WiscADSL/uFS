SUBDIRS= encapsulate-lookup-api extended-schema-validator log-level \
	 recipes simple-encapsulation

.PHONY: all clean clobber

all:
	for dir in $(SUBDIRS); do $(MAKE) -C $$dir $@; done

clean:
	for dir in $(SUBDIRS); do $(MAKE) clean -C $$dir $@; done

clobber: clean

