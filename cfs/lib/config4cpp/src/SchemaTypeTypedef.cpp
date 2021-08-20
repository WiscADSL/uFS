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

#include "SchemaTypeTypedef.h"


namespace CONFIG4CPP_NAMESPACE {

void
SchemaTypeTypedef::checkRule(
	const SchemaValidator *		sv,
	const Configuration *		cfg,
	const char *				typeName,
	const StringVector &		typeArgs,
	const char *				rule) const throw(ConfigurationException)
{
	StringBuffer				msg;

	if (typeArgs.length() != 0) {
		msg << "you cannot specify arguments when using user-defined type '"
			<< typeName << "' in '" << rule << "'";
		throw ConfigurationException(msg.c_str());
	}
}



void
SchemaTypeTypedef::validate(
	const SchemaValidator *		sv,
	const Configuration *		cfg,
	const char *				scope,
	const char *				name,
	const char *				typeName,
	const char *				origTypeName,
	const StringVector &		typeArgs,
	int							indentLevel) const
											throw(ConfigurationException)
{
	StringBuffer				msg;
	SchemaType *				baseTypeDef;
	const char *				baseTypeName;

	assert(typeArgs.length() == 0);
	baseTypeName = m_baseTypeName.c_str();
	baseTypeDef = findType(sv, baseTypeName);
	callValidate(baseTypeDef, sv, cfg, scope, name, baseTypeName, origTypeName,
				 m_baseTypeArgs, indentLevel + 1);
}



bool
SchemaTypeTypedef::isA(
	const SchemaValidator *		sv,
	const Configuration *		cfg,
	const char *				value,
	const char *				typeName,
	const StringVector &		typeArgs,
	int							indentLevel,
	StringBuffer &				errSuffix) const
{
	StringBuffer				msg;
	SchemaType *				baseTypeDef;
	const char *				baseTypeName;
	bool						result;

	assert(typeArgs.length() == 0);
	baseTypeName = m_baseTypeName.c_str();
	baseTypeDef = findType(sv, baseTypeName);
	assert(baseTypeDef != 0);
	result = callIsA(baseTypeDef, sv, cfg, value, baseTypeName, m_baseTypeArgs,
					 indentLevel + 1, errSuffix);
	return result;
}

}; // namespace CONFIG4CPP_NAMESPACE
