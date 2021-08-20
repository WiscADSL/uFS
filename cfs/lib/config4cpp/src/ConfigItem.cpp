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
#include <config4cpp/Configuration.h>
#include "ConfigItem.h"
#include "ConfigScope.h"
#include "UidIdentifierProcessor.h"
#include <string.h>
#include <assert.h>


namespace CONFIG4CPP_NAMESPACE {

static char *
copyString(const char * str)
{
	char *		result;

	result = new char[strlen(str) + 1];
	strcpy(result, str);
	return result;
}



static char *
escapeString(const char * str)
{
	int					i;
	int					len;
	StringBuffer		buf;
	char *				result;

	len = strlen(str);
	for (i = 0; i < len; i++) {
		switch (str[i]) {
		case '\t':
			buf.append("%t");
			break;
		case '\n':
			buf.append("%n");
			break;
		case '%':
			buf.append("%%");
			break;
		case '"':
			buf.append("%\"");
			break;
		default:
			buf.append(str[i]);
			break;
		}
	}
	result = new char[buf.length() + 1];
	strcpy(result, buf.c_str());
	return result;
}



static void
printIndent(StringBuffer & buf, int indentLevel)
{
	int				i;

	for (i = 0; i < indentLevel; i++) {
		buf.append("\t");
	}
}



//----------------------------------------------------------------------
// Function:	Constructor (overloaded)
//
// Description:
//----------------------------------------------------------------------

ConfigItem::ConfigItem(const char * name, const char * str)
{
	m_type      = Configuration::CFG_STRING;
	m_name      = copyString(name);
	m_stringVal = copyString(str);
	m_listVal   = 0;
	m_scope     = 0;
}



ConfigItem::ConfigItem(const char * name, const StringVector & list)
{
	m_type      = Configuration::CFG_LIST;
	m_name      = copyString(name);
	m_listVal   = new StringVector(list);
	m_scope     = 0;
	m_stringVal = 0;
}



ConfigItem::ConfigItem(const char * name, const char ** array, int size)
{
	int				i;

	m_type      = Configuration::CFG_LIST;
	m_name      = copyString(name);
	m_scope     = 0;
	m_stringVal = 0;
	m_listVal   = new StringVector(size);
	for (i = 0; i < size; i++) {
		m_listVal->add(array[i]);
	}
}



ConfigItem::ConfigItem(const char * name, ConfigScope * scope)
{
	m_type      = Configuration::CFG_SCOPE;
	m_name      = copyString(name);
	m_scope     = scope;
	m_listVal   = 0;
	m_stringVal = 0;
}



//----------------------------------------------------------------------
// Function:	Destructor
//
// Description:
//----------------------------------------------------------------------

ConfigItem::~ConfigItem()
{
	delete m_listVal;
	delete m_scope;
	delete [] m_stringVal;
	delete [] m_name;
}



//----------------------------------------------------------------------
// Function:	dump()
//
// Description:	Dump out the ConfigItem's contents.
//----------------------------------------------------------------------

void
ConfigItem::dump(
	StringBuffer &				buf,
	const char *				name,
	bool						wantExpandedUidNames,
	int							indentLevel) const
{
	int							i;
	int							len;
	char *						escStr;
	StringBuffer				nameBuf;
	UidIdentifierProcessor		uidIdProc;

	if (!wantExpandedUidNames) {
		name = uidIdProc.unexpand(name, nameBuf);
	}
	printIndent(buf, indentLevel);
	switch (m_type) {
	case Configuration::CFG_STRING:
		escStr = escapeString(m_stringVal);
		buf << name << " = \"" << escStr << "\";\n";
		delete [] escStr;
		break;
	case Configuration::CFG_LIST:
		buf << name << " = [";
		len = m_listVal->length();
		for (i = 0; i < len; i++) {
			escStr = escapeString((*m_listVal)[i]);
			buf << "\"" << escStr << "\"";
			delete [] escStr;
			if (i < len-1) {
				buf << ", ";
			}
		}
		buf << "];\n";
		break;
	case Configuration::CFG_SCOPE:
		buf << name << " {\n";
		m_scope->dump(buf, wantExpandedUidNames, indentLevel + 1);
		printIndent(buf, indentLevel);
		buf << "}\n";
		break;
	default:
		assert(0);	// Bug
		break;
	};
}

}; // namespace CONFIG4CPP_NAMESPACE
