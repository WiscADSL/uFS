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

#ifndef CONFIG4CPP_CONFIG_SCOPE_ENTRY_H_
#define CONFIG4CPP_CONFIG_SCOPE_ENTRY_H_

//--------
// #include's
//--------
#include <config4cpp/namespace.h>
#include "ConfigItem.h"
#include <assert.h>


namespace CONFIG4CPP_NAMESPACE {

class ConfigScope;

class ConfigScopeEntry
{
public:
	//--------
	// Ctors & dtor
	//--------
	ConfigScopeEntry();
	ConfigScopeEntry(
			const char *		name,
			ConfigItem *		item,
			ConfigScopeEntry *	next);
	~ConfigScopeEntry ();

	inline const char * name();
	inline const ConfigItem * item();
	inline const Configuration::Type type();
	void setItem(ConfigItem * item);

protected:
	friend class ConfigScope;

	//--------
	// Instance variables
	//--------
	ConfigItem *			m_item;
	ConfigScopeEntry *		m_next;

private:
	//--------
	// Constructors and operators that are not suported
	//--------
	ConfigScopeEntry & operator=(const ConfigScopeEntry &);

};


//--------
// Inline implementation of operations
//--------

inline const char *
ConfigScopeEntry::name()
{
	return m_item->name();
}


inline const ConfigItem *
ConfigScopeEntry::item()
{
	return m_item;
}


inline const Configuration::Type
ConfigScopeEntry::type()
{
	return m_item->type();
}


}; // namespace CONFIG4CPP_NAMESPACE
#endif
