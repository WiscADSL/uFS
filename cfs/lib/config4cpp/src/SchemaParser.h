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

#ifndef CONFIG4CPP_SCHEMA_PARSER_H_
#define CONFIG4CPP_SCHEMA_PARSER_H_


//--------
// #include's
//--------
#include <config4cpp/SchemaValidator.h>
#include <config4cpp/Configuration.h>
#include "SchemaRuleInfo.h"
#include "SchemaLex.h"


namespace CONFIG4CPP_NAMESPACE {

class SchemaValidator;

class SchemaParser
{
public:

	//--------
	// Constructors and destructor
	//--------
	SchemaParser(SchemaValidator * sv);
	~SchemaParser();

	//--------
	// Public API
	//--------
	void parse(const char ** schema, int schemaSize)
												throw(ConfigurationException);

private:
	//--------
	// Helper operations.
	//--------
	void parseIdRule(
			const char *			rule,
			SchemaIdRuleInfo *		SchemaIdRuleInfo)
												throw(ConfigurationException);
	void parseIgnoreRule(
			const char *			rule,
			SchemaIgnoreRuleInfo *	SchemaIgnoreRuleInfo)
												throw(ConfigurationException);

	void parseUserTypeDef(const char * str) throw(ConfigurationException);

	void accept(
			short					sym,
			const char *			rule,
			const char *			msg) throw(ConfigurationException);

	//--------
	// Instance variables
	//--------
	SchemaLex *			m_lex;
	LexToken			m_token;
	SchemaValidator *	m_sv;
	Configuration *		m_cfg;

	//--------
	// Not implemented.
	//--------
	SchemaParser(const SchemaParser &);
	SchemaParser & operator=(const SchemaParser &);
};


}; // namespace CONFIG4CPP_NAMESPACE
#endif
