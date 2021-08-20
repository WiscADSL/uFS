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

#ifndef	CONFIG4CPP_CONFIG_LEX_H_
#define	CONFIG4CPP_CONFIG_LEX_H_


//--------
// #include's and #define's
//--------
#include "LexBase.h"


namespace CONFIG4CPP_NAMESPACE {

class ConfigLex : public LexBase
{
public:
	enum ConfigLexSymbols {
			//--------
			// Constants for lexical symbols.
			// First, keywords
			//--------
			LEX_COPY_FROM_SYM             = 101,
			LEX_ELSE_SYM                  = 102,
			LEX_ELSE_IF_SYM               = 103,
			LEX_ERROR_SYM                 = 104,
			LEX_IF_SYM                    = 105,
			LEX_IF_EXISTS_SYM             = 106,
			LEX_IN_SYM                    = 107,
			LEX_INCLUDE_SYM               = 108,
			LEX_MATCHES_SYM               = 109,
			LEX_REMOVE_SYM                = 110,
			//--------
			// Now, functions
			//--------
			LEX_FUNC_CONFIG_FILE_SYM      = 201,
			LEX_FUNC_CONFIG_TYPE_SYM      = 202,
			LEX_FUNC_EXEC_SYM             = 203,
			LEX_FUNC_FILE_TO_DIR_SYM      = 204,
			LEX_FUNC_GETENV_SYM           = 205,
			LEX_FUNC_IS_FILE_READABLE_SYM = 206,
			LEX_FUNC_JOIN_SYM             = 207,
			LEX_FUNC_OS_DIR_SEP_SYM       = 208,
			LEX_FUNC_OS_PATH_SEP_SYM      = 209,
			LEX_FUNC_OS_TYPE_SYM          = 210,
			LEX_FUNC_READ_FILE_SYM        = 211,
			LEX_FUNC_REPLACE_SYM          = 212,
			LEX_FUNC_SIBLING_SCOPE_SYM    = 213,
			LEX_FUNC_SPLIT_SYM            = 214
	};

	//--------
	// Constructors and destructor
	//--------
	ConfigLex(
			Configuration::SourceType	sourceType,
			const char *				source,
			UidIdentifierProcessor *	uidIdentifierProcessor)
												throw(ConfigurationException);
	virtual ~ConfigLex();

private:
	//--------
	// Not implemented.
	//--------
	ConfigLex();
	ConfigLex(const ConfigLex &);
	ConfigLex & operator=(const ConfigLex &);
};


}; // namespace CONFIG4CPP_NAMESPACE
#endif
