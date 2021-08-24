PROJECT_ROOT=../..
include $(PROJECT_ROOT)/Makefile.inc

OBJ_FILES= FooConfiguration.o Logger.o A.o B.o main.o

.PHONY: all clean clobber

all:		demo$(EXE_EXT)

FooConfiguration.o: FooConfiguration.h FooConfiguration.cpp

demo$(EXE_EXT):	$(OBJ_FILES)
	$(CXX) -o demo$(EXE_EXT) $(OBJ_FILES) $(LINK_WITH_CONFIG4CPP)

clean:
	rm -f *.o demo$(EXE_EXT)

clobber: clean

