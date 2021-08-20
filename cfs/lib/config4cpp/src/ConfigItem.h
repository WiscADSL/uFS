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

#ifndef CONFIG4CPP_CONFIG_ITEM_H_
#define CONFIG4CPP_CONFIG_ITEM_H_


//--------
// #include's
//--------
#include <config4cpp/Configuration.h>
#include <stdio.h>
#include <assert.h>


namespace CONFIG4CPP_NAMESPACE {

class ConfigScope;

//--------------------------------------------------------------
// Class:	ConfigItem
//
// Description:
//		A config file contains "name = <value>" statements and
//		"name <scope>" statements. This class is used to store
//		name plus the the <value> part, (which can be a string
//		or a sequence of string) or a <scope>.
//--------------------------------------------------------------

class ConfigItem
{
public:

	//--------
	// Ctor and dtor
	//--------
	ConfigItem(const char * name, const char *         str);
	ConfigItem(const char * name, const StringVector & list);
	ConfigItem(const char * name, const char ** array, int size);
	ConfigItem(const char * name, ConfigScope *        scope);
	virtual ~ConfigItem();

	//--------
	// Public operations
	//--------
	inline Configuration::Type type();
	inline const char * name() const;
	inline const char * stringVal() const;
	inline StringVector & listVal() const;
	inline ConfigScope * scopeVal() const;

	//--------
	// Debugging aid
	//--------
	void dump(
				StringBuffer &	buf,
				 const char *	name,
				 bool			wantExpandedUidNames,
				 int			indentLevel = 0) const;

protected:
	//--------
	// Instance variables
	//--------
	Configuration::Type		m_type;
	char *					m_name;
	char *					m_stringVal;
	StringVector *			m_listVal;
	ConfigScope *			m_scope;

private:
	//--------
	// Unsupported constructors and operator=
	//--------
	ConfigItem();
	ConfigItem(const ConfigItem &);
	ConfigItem & operator=(const ConfigItem &);
};


//--------
// Inline implementation of operations
//--------

inline Configuration::Type
ConfigItem::type()
{
	return m_type;
}


inline const char *
ConfigItem::name() const
{
	return m_name;
}


inline const char *
ConfigItem::stringVal() const
{
	assert(m_type == Configuration::CFG_STRING);
	return m_stringVal;
}


inline StringVector &
ConfigItem::listVal() const
{
	assert(m_type == Configuration::CFG_LIST);
	assert(m_listVal != 0);
	return *m_listVal;
}


inline ConfigScope *
ConfigItem::scopeVal() const
{
	assert(m_type == Configuration::CFG_SCOPE);
	assert(m_scope != 0);
	return m_scope;
}


}; // namespace CONFIG4CPP_NAMESPACE
#endif
