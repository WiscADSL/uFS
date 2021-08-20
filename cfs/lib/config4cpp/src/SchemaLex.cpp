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

#include "SchemaLex.h"



namespace CONFIG4CPP_NAMESPACE {

//--------
// This array must be kept sorted becaue we do a binary search on it.
//--------
static LexBase::KeywordInfo	keywordInfoArray[] = {
//----------------------------------------------------------------------
// spelling               symbol
//----------------------------------------------------------------------
  {"@ignoreEverythingIn", SchemaLex::LEX_IGNORE_EVERYTHING_IN_SYM},
  {"@ignoreScopesIn",     SchemaLex::LEX_IGNORE_SCOPES_IN_SYM},
  {"@ignoreVariablesIn",  SchemaLex::LEX_IGNORE_VARIABLES_IN_SYM},
  {"@optional",           SchemaLex::LEX_OPTIONAL_SYM},
  {"@required",           SchemaLex::LEX_REQUIRED_SYM},
  {"@typedef",            SchemaLex::LEX_TYPEDEF_SYM},
};

const static int keywordInfoArraySize
		= sizeof(keywordInfoArray) / sizeof(keywordInfoArray[0]);

SchemaLex::SchemaLex(const char * str) throw(ConfigurationException)
	: LexBase(str)
{
	m_keywordInfoArray     = keywordInfoArray;
	m_keywordInfoArraySize = keywordInfoArraySize;
}



SchemaLex::~SchemaLex()
{
}



}; // namespace CONFIG4CPP_NAMESPACE
