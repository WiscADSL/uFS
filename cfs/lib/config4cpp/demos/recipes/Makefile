PROJECT_ROOT=../..
include $(PROJECT_ROOT)/Makefile.inc

OBJ_FILES= RecipeFileParser.o main.o

.PHONY: all clean clobber

all:		demo$(EXE_EXT)

DefaultFooConfigString.cpp DefaultFooConfigString.h: DefaultFooConfigString.cfg
		$(BIN_DIR)/config2cpp -cfg DefaultFooConfigString.cfg \
					-class DefaultFooConfigString

FooConfiguration.o: FooConfiguration.h FooConfiguration.cpp \
					DefaultFooConfigString.h

demo$(EXE_EXT):	$(OBJ_FILES)
	$(CXX) -o demo$(EXE_EXT) $(OBJ_FILES) $(LINK_WITH_CONFIG4CPP)

clean:
	rm -f *.o demo$(EXE_EXT)

clobber: clean

