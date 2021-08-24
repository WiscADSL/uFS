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

#ifndef SCHEMA_TYPE_HEX_H_
#define SCHEMA_TYPE_HEX_H_

#include <config4cpp/SchemaValidator.h>
using CONFIG4CPP_NAMESPACE::Configuration;
using CONFIG4CPP_NAMESPACE::ConfigurationException;
using CONFIG4CPP_NAMESPACE::SchemaValidator;
using CONFIG4CPP_NAMESPACE::SchemaType;
using CONFIG4CPP_NAMESPACE::StringBuffer;
using CONFIG4CPP_NAMESPACE::StringVector;


class SchemaTypeHex : public SchemaType
{
public:
	SchemaTypeHex()
		: SchemaType("hex", "SchemaTypeHex", Configuration::CFG_STRING)
	{ }
	virtual ~SchemaTypeHex() { }

	static int lookupHex(
		const Configuration *	cfg,
		const char *			scope,
		const char *			localName) throw(ConfigurationException);

	static int lookupHex(
		const Configuration *	cfg,
		const char *			scope,
		const char *			localName,
		int						defaultVal) throw(ConfigurationException);
	static int stringToHex(
		const Configuration *	cfg,
		const char *			scope,
		const char *			localName,
		const char *			str,
		const char *			typeName = "hex") throw(ConfigurationException);

	static bool isHex(const char * str);

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
		int						indentlevel,
		StringBuffer &			errSuffix) const;
};

#endif
