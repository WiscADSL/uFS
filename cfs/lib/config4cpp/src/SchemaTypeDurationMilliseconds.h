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

#ifndef CONFIG4CPP_SCHEMA_TYPE_DURATION_MILLISECONDS_H_
#define CONFIG4CPP_SCHEMA_TYPE_DURATION_MILLISECONDS_H_

#include <config4cpp/SchemaType.h>


namespace CONFIG4CPP_NAMESPACE {

class SchemaTypeDurationMilliseconds
	: public SchemaType
{
public:
	SchemaTypeDurationMilliseconds()
	: SchemaType("durationMilliseconds",
	             CONFIG4CPP_NAMESPACE_STR "::SchemaTypeDurationMilliseconds",
	             Configuration::CFG_STRING)
	{ }
	virtual ~SchemaTypeDurationMilliseconds() { }

protected:
	virtual void checkRule(
		const SchemaValidator *	sv,
		const Configuration *	cfg,
		const char *			typeName,
		const StringVector &	typeArgs,
		const char *			rule) const throw(ConfigurationException);

	virtual bool isA(
		const SchemaValidator *	sv,
		const Configuration *	cfg,
		const char *			value,
		const char *			typeName,
		const StringVector &	typeArgs,
		int						indentLevel,
		StringBuffer &			errSuffix) const;
};


}; // namespace CONFIG4CPP_NAMESPACE
#endif
