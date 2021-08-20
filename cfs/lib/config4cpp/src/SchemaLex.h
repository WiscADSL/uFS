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

#ifndef	CONFIG4CPP_SCHEMA_LEX_H_
#define	CONFIG4CPP_SCHEMA_LEX_H_


//--------
// #include's and #define's
//--------
#include "LexBase.h"


namespace CONFIG4CPP_NAMESPACE {

class SchemaLex : public LexBase
{
public:
	enum SchemaLexSymbols {
		//--------
		// Constants for lexical symbols.
		// Keywords
		//--------
		LEX_IGNORE_EVERYTHING_IN_SYM  = 101,
		LEX_IGNORE_SCOPES_IN_SYM      = 102,
		LEX_IGNORE_VARIABLES_IN_SYM   = 103,
		LEX_TYPEDEF_SYM               = 104,
		LEX_OPTIONAL_SYM              = 105,
		LEX_REQUIRED_SYM              = 106
		//--------
		// There are no functions in the schema language
		//--------
	};

	//--------
	// Constructors and destructor
	//--------
	SchemaLex(const char * str) throw(ConfigurationException);
	virtual ~SchemaLex();

private:
	//--------
	// Unsupported constructors and assignment operators
	//--------
	SchemaLex();
	SchemaLex(const SchemaLex &);
	SchemaLex & operator=(const SchemaLex &);
};


}; // namespace CONFIG4CPP_NAMESPACE
#endif
