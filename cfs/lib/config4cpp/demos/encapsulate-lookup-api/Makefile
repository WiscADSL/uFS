PROJECT_ROOT=../..
include $(PROJECT_ROOT)/Makefile.inc

OBJ_FILES= FooConfiguration.o FallbackConfiguration.o main.o

.PHONY: all clean clobber

all:	demo$(EXE_EXT)

FallbackConfiguration.cpp FallbackConfiguration.h: FallbackConfiguration.cfg
	$(BIN_DIR)/config2cpp -cfg FallbackConfiguration.cfg \
			-class FallbackConfiguration -singleton

FallbackConfiguration.o: FallbackConfiguration.cpp FallbackConfiguration.h

FooConfiguration.o: FooConfiguration.h FooConfiguration.cpp \
				FallbackConfiguration.h

demo$(EXE_EXT):	$(OBJ_FILES)
	$(CXX) -o demo$(EXE_EXT) $(OBJ_FILES) $(LINK_WITH_CONFIG4CPP)

clean:
	rm -f *.o demo$(EXE_EXT)
	rm -f FallbackConfiguration.h FallbackConfiguration.cpp

clobber: clean

