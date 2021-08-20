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

#ifndef CONFIG4CPP_SCHEMA_RULE_INFO_H_
#define CONFIG4CPP_SCHEMA_RULE_INFO_H_

//--------
// #include's
//--------
#include <config4cpp/StringBuffer.h>
#include <config4cpp/StringVector.h>


namespace CONFIG4CPP_NAMESPACE {

class SchemaIdRuleInfo {
public:
	StringBuffer		m_locallyScopedName;
	StringBuffer		m_typeName;
	StringVector		m_args;
	bool				m_isOptional;
};


class SchemaIgnoreRuleInfo {
public:
	short				m_symbol;
	StringBuffer		m_locallyScopedName;
};


}; // namespace CONFIG4CPP_NAMESPACE
#endif
