#----
# Makefile settings for using the Visual C++ compiler.
#----
LIB_DIR=	$(PROJECT_ROOT)\lib
BIN_DIR=	$(PROJECT_ROOT)\bin
INC_DIR=	$(PROJECT_ROOT)\include

CXX=			CL
LIB_CONFIG4CPP=		config4cpp.lib
LINK_WITH_CONFIG4CPP=	$(LIB_DIR)\$(LIB_CONFIG4CPP)


#--------
# Append either DEBUG_OPTIONS or OPTIMISE_OPTIONS to CXX_FLAGS
#--------
DEBUG_OPTIONS=		/RTC1 /DDEBUG
OPTIMISE_OPTIONS=	/O2 /DNDEBUG
CXX_FLAGS=		/I$(INC_DIR) /DWIN32 /EHsc /Zi /nologo $(DEBUG_OPTIONS)


LINK_FLAGS=	




#--------
# Generic build rules
#--------
.SUFFIXES:		.cpp

.cpp.obj:
	$(CXX) /c $(CXX_FLAGS) $<
