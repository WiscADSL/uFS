PROJECT_ROOT=..
include $(PROJECT_ROOT)/Makefile.inc

PUB_INC_FILES= \
		$(INC_DIR)/config4cpp/Configuration.h \
		$(INC_DIR)/config4cpp/ConfigurationException.h \
		$(INC_DIR)/config4cpp/StringBuffer.h \
		$(INC_DIR)/config4cpp/StringVector.h \
		$(INC_DIR)/config4cpp/SchemaValidator.h \
		$(INC_DIR)/config4cpp/namespace.h

LIB_HDR_FILES	= \
		$(PUB_INC_FILES) \
		SchemaLex.h \
		SchemaParser.h \
		SchemaRuleInfo.h \
		MBChar.h \
		platform.h \
		ConfigurationImpl.h \
		UidIdentifierProcessor.h \
		UidIdentifierDummyProcessor.h \
		util.h \
		ConfigParser.h \
		ConfigScope.h \
		ConfigScopeEntry.h \
		ConfigItem.h \
		LexToken.h \
		LexBase.h \
		ConfigLex.h \
		SchemaTypeBoolean.h \
		SchemaTypeDummy.h \
		SchemaTypeDurationMicroseconds.h \
		SchemaTypeDurationMilliseconds.h \
		SchemaTypeDurationSeconds.h \
		SchemaTypeEnum.h \
		SchemaTypeFloat.h \
		SchemaTypeFloatWithUnits.h \
		SchemaTypeInt.h \
		SchemaTypeIntWithUnits.h \
		SchemaTypeList.h \
		SchemaTypeMemorySizeBytes.h \
		SchemaTypeMemorySizeKB.h \
		SchemaTypeMemorySizeMB.h \
		SchemaTypeScope.h \
		SchemaTypeString.h \
		SchemaTypeTable.h \
		SchemaTypeTuple.h \
		SchemaTypeTypedef.h \
		SchemaTypeUnitsWithFloat.h \
		SchemaTypeUnitsWithInt.h


LIB_OBJ_FILES	= \
		SchemaLex.o \
		SchemaParser.o \
		MBChar.o \
		SchemaValidator.o \
		platform.o \
		util.o \
		Configuration.o \
		DefaultSecurity.o \
		DefaultSecurityConfiguration.o \
		ConfigurationException.o \
		ConfigurationImpl.o \
		ConfigParser.o \
		UidIdentifierProcessor.o \
		ConfigScope.o \
		ConfigScopeEntry.o \
		ConfigItem.o \
		LexToken.o \
		LexBase.o \
		ConfigLex.o \
		StringBuffer.o \
		StringVector.o \
		SchemaType.o \
		SchemaTypeBoolean.o \
		SchemaTypeDurationMicroseconds.o \
		SchemaTypeDurationMilliseconds.o \
		SchemaTypeDurationSeconds.o \
		SchemaTypeEnum.o \
		SchemaTypeFloat.o \
		SchemaTypeFloatWithUnits.o \
		SchemaTypeInt.o \
		SchemaTypeIntWithUnits.o \
		SchemaTypeMemorySizeBytes.o \
		SchemaTypeMemorySizeKB.o \
		SchemaTypeMemorySizeMB.o \
		SchemaTypeScope.o \
		SchemaTypeString.o \
		SchemaTypeUnitsWithFloat.o \
		SchemaTypeUnitsWithInt.o \
		SchemaTypeList.o \
		SchemaTypeTable.o \
		SchemaTypeTuple.o \
		SchemaTypeTypedef.o

.PHONY: all clean clobber

all:	config4cpp$(EXE_EXT) config2cpp$(EXE_EXT) config2cpp-nocheck$(EXE_EXT)

$(LIB_CONFIG4CPP):	$(LIB_OBJ_FILES)
	$(AR) $(AR_FLAGS) $(LIB_CONFIG4CPP) $(LIB_OBJ_FILES)
	$(RANLIB) $(RANLIB_FLAGS) $(LIB_CONFIG4CPP)
	cp $(LIB_CONFIG4CPP) $(LIB_DIR)

DefaultSecurity.cpp DefaultSecurity.h: DefaultSecurity.cfg config2cpp-nocheck$(EXE_EXT)
	./config2cpp-nocheck -cfg DefaultSecurity.cfg -class DefaultSecurity \
			-namespace CONFIG4CPP_NAMESPACE

DefaultSecurityConfiguration.o: DefaultSecurityConfiguration.cpp \
					DefaultSecurityConfiguration.h \
					DefaultSecurity.h

config2cpp$(EXE_EXT):	config2cpp-main.o Config2Cpp.o $(LIB_CONFIG4CPP)
		$(CXX) -o config2cpp$(EXE_EXT) config2cpp-main.o Config2Cpp.o \
			$(LIB_CONFIG4CPP)
		cp config2cpp$(EXE_EXT) $(BIN_DIR)

config2cpp-nocheck$(EXE_EXT):	config2cpp-nocheck-main.o Config2Cpp.o
		$(CXX) -o config2cpp-nocheck$(EXE_EXT) \
			config2cpp-nocheck-main.o Config2Cpp.o

config4cpp$(EXE_EXT):	config4cpp.o $(LIB_CONFIG4CPP)
		$(CXX) -o config4cpp$(EXE_EXT) config4cpp.o $(LIB_CONFIG4CPP)
		cp config4cpp$(EXE_EXT) $(BIN_DIR)

config4cpp.o:	config4cpp.cpp 	$(LIB_HDR_FILES)

$(LIB_OBJ_FILES):	$(LIB_HDR_FILES)

clean:
	rm -f *.o config2cpp$(EXE_EXT) config4cpp$(EXE_EXT) core
	rm -f config2cpp-nocheck$(EXE_EXT)
	rm -f libconfig4cpp*.a

clobber: clean
	rm -f $(BIN_DIR)/config2cpp$(EXE_EXT)
	rm -f $(BIN_DIR)/config4cpp$(EXE_EXT)
	rm -f $(LIB_DIR)/libconfig4cpp*.a

