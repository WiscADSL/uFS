SUBDIRS = src tests demos

.PHONY: all clean clobber

all:
	for dir in $(SUBDIRS); do $(MAKE) all -C $$dir $@; done

clean:
	for dir in $(SUBDIRS); do $(MAKE) clean -C $$dir $@; done

clobber:
	for dir in $(SUBDIRS); do $(MAKE) clobber -C $$dir $@; done

