//-----------------------------------------------------------------------
// Copyright 2011 Ciaran McHale.
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions.
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.  
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
// BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
// ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//----------------------------------------------------------------------


//--------
// #include's
//--------
#include "ConfigLex.h"


namespace CONFIG4CPP_NAMESPACE {

//--------
// This array must be kept sorted becaue we do a binary search on it.
//--------
static LexBase::KeywordInfo	keywordInfoArray[] = {
//----------------------------------------------------------------------
// spelling      symbol
//----------------------------------------------------------------------
  {"@copyFrom",  ConfigLex::LEX_COPY_FROM_SYM},
  {"@else",      ConfigLex::LEX_ELSE_SYM},
  {"@elseIf",    ConfigLex::LEX_ELSE_IF_SYM},
  {"@error",     ConfigLex::LEX_ERROR_SYM},
  {"@if",        ConfigLex::LEX_IF_SYM},
  {"@ifExists",  ConfigLex::LEX_IF_EXISTS_SYM},
  {"@in",        ConfigLex::LEX_IN_SYM},
  {"@include",   ConfigLex::LEX_INCLUDE_SYM},
  {"@matches",   ConfigLex::LEX_MATCHES_SYM},
  {"@remove",    ConfigLex::LEX_REMOVE_SYM},
};

const static int keywordInfoArraySize
		= sizeof(keywordInfoArray) / sizeof(keywordInfoArray[0]);

//--------
// This array must be kept sorted becaue we do a binary search on it.
//--------
static LexBase::FuncInfo	funcInfoArray[] = {
//----------------------------------------------------------------------
// spelling            type             symbol
//----------------------------------------------------------------------
  {"configFile(",      LexBase::STRING_FUNC, ConfigLex::LEX_FUNC_CONFIG_FILE_SYM},
  {"configType(",      LexBase::STRING_FUNC, ConfigLex::LEX_FUNC_CONFIG_TYPE_SYM},
  {"exec(",            LexBase::STRING_FUNC, ConfigLex::LEX_FUNC_EXEC_SYM},
  {"fileToDir(",       LexBase::STRING_FUNC, ConfigLex::LEX_FUNC_FILE_TO_DIR_SYM},
  {"getenv(",          LexBase::STRING_FUNC, ConfigLex::LEX_FUNC_GETENV_SYM},
  {"isFileReadable(",  LexBase::BOOL_FUNC,   ConfigLex::LEX_FUNC_IS_FILE_READABLE_SYM},
  {"join(",            LexBase::STRING_FUNC, ConfigLex::LEX_FUNC_JOIN_SYM},
  {"osDirSeparator(",  LexBase::STRING_FUNC, ConfigLex::LEX_FUNC_OS_DIR_SEP_SYM},
  {"osPathSeparator(", LexBase::STRING_FUNC, ConfigLex::LEX_FUNC_OS_PATH_SEP_SYM},
  {"osType(",          LexBase::STRING_FUNC, ConfigLex::LEX_FUNC_OS_TYPE_SYM},
  {"readFile(",        LexBase::STRING_FUNC, ConfigLex::LEX_FUNC_READ_FILE_SYM},
  {"replace(",         LexBase::STRING_FUNC, ConfigLex::LEX_FUNC_REPLACE_SYM},
  {"siblingScope(",    LexBase::STRING_FUNC, ConfigLex::LEX_FUNC_SIBLING_SCOPE_SYM},
  {"split(",           LexBase::LIST_FUNC,   ConfigLex::LEX_FUNC_SPLIT_SYM},
};

const static int funcInfoArraySize = sizeof(funcInfoArray) / sizeof(funcInfoArray[0]);



ConfigLex::ConfigLex(
	Configuration::SourceType	sourceType,
	const char *				source,
	UidIdentifierProcessor *	uidIdentifierProcessor)
												throw(ConfigurationException)
	: LexBase(sourceType, source, uidIdentifierProcessor)
{
	m_keywordInfoArray     = keywordInfoArray;
	m_keywordInfoArraySize = keywordInfoArraySize;
	m_funcInfoArray        = funcInfoArray;
	m_funcInfoArraySize    = funcInfoArraySize ;
}



ConfigLex::~ConfigLex()
{
	// Nothing to do
}

}; // namespace CONFIG4CPP_NAMESPACE
