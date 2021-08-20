#----
# TO DO: check the output of "uname -o" on HPUX and AIX
#----
OS= $(shell uname -o)

ifeq ($(OS),Gnu/Linux)
OS=Linux
endif
ifeq ($(OS),GNU/Linux)
OS=Linux
endif


LIB_DIR=$(PROJECT_ROOT)/lib
BIN_DIR=$(PROJECT_ROOT)/bin
INC_DIR=$(PROJECT_ROOT)/include

ifeq ($(BUILD_TYPE),debug)
LIB_CONFIG4CPP=libconfig4cppd.a
LINK_WITH_CONFIG4CPP=-L$(LIB_DIR) -lconfig4cppd
else
LIB_CONFIG4CPP=libconfig4cpp.a
LINK_WITH_CONFIG4CPP=-L$(LIB_DIR) -lconfig4cpp
endif


#--------
# EXE_EXT is ".exe" on Windows, and "" on UNIX-like operating systems.
#--------
ifeq ($(OS),Cygwin)
EXE_EXT=.exe
else
EXE_EXT=
endif


#--------
# Update CXX_FLAGS based on BUILD_TYPE (debug or release)
#--------
ifeq ($(BUILD_TYPE),release)
override CXX_FLAGS+= -I$(PROJECT_ROOT)/include -fPIC -O -DNDEBUG
else
BUILD_TYPE=debug
override CXX_FLAGS+= -I$(PROJECT_ROOT)/include -fPIC -g
endif


LINK_FLAGS=	


#-----
# Determine:
#	- The commands used to build a library
#	- The C++ compiler
#-----
ifeq ($(OS),Linux)
#-----
# Linux
#-----
AR		= ar
AR_FLAGS	= -r
RANLIB		= /bin/true
RANLIB_FLAGS	= 
CXX		= g++
else ifeq ($(OS),Cygwin)
#-----
# Cygwin: same as Linux
#-----
AR		= ar
AR_FLAGS	= -r
RANLIB		= /bin/true
RANLIB_FLAGS	= 
CXX		= g++
else ifeq ($(OS),SunOS)
#-----
# Solaris
#-----
AR		= ar
AR_FLAGS	= cr
RANLIB		= /bin/true
RANLIB_FLAGS	= 
CXX		= g++
else ifeq ($(OS),HPUX)
#-----
# HPUX
#-----
AR		= ar
AR_FLAGS	= cr
RANLIB		= /bin/true
RANLIB_FLAGS	= 
CXX		= g++
else ifeq ($(OS),Aix)
#-----
# AIX
#-----
AR		= ar
AR_FLAGS	= -r
RANLIB		= ranlib
RANLIB_FLAGS	= 
CXX		= g++
endif





#--------
# Generic build rules
#--------
.SUFFIXES:		.cpp

.cpp.o:
	$(CXX) -c $(CXX_FLAGS) $<
