PROJECT_ROOT=../..
include $(PROJECT_ROOT)/Makefile.inc

OBJ_FILES= FooConfiguration.o SchemaTypeHex.o main.o

.PHONY: all clean clobber

all:		demo$(EXE_EXT)

FooConfiguration.o: FooConfiguration.h FooConfiguration.cpp

demo$(EXE_EXT):	$(OBJ_FILES)
	$(CXX) -o demo$(EXE_EXT) $(OBJ_FILES) $(LINK_WITH_CONFIG4CPP)

clean:
	rm -f *.o demo$(EXE_EXT)
	rm -f FallbackConfiguration.h FallbackConfiguration.cpp

clobber: clean

