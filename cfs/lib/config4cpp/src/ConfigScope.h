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

#ifndef CONFIG4CPP_CONFIG_SCOPE_H_
#define CONFIG4CPP_CONFIG_SCOPE_H_


//--------
// #include's
//--------
#include <config4cpp/ConfigurationException.h>
#include "ConfigScopeEntry.h"


namespace CONFIG4CPP_NAMESPACE {

//----------------------------------------------------------------------
// Class:	ConfigScope
//
// Description:	A hash table for storing (name, item) pairs.
//----------------------------------------------------------------------

class ConfigScope
{

public:
	//--------
	// Ctors & dtor
	//--------
	ConfigScope(ConfigScope * parentScope, const char * name);
	~ConfigScope();

	//--------
	// Operations.
	//--------
	inline const char *	scopedName() const;

	bool addOrReplaceString(
					const char *			name,
					const char *			str);

	bool addOrReplaceList(
					const char *			name,
					const char**			array,
					int						size);
	bool addOrReplaceList(
					const char *			name,
					const StringVector &	list);

	bool ensureScopeExists(
					const char *			name,
					ConfigScope *&			scope);

	bool removeItem(const char * name);

	ConfigItem * findItem(const char * name) const;
	ConfigScopeEntry * findEntry(const char * name, int & index) const;

	bool is_in_table(const char * name) const;

	inline void listFullyScopedNames(
					Configuration::Type		typeMask,
					bool					recursive,
					StringVector &			vec) const;

	inline void listFullyScopedNames(
					Configuration::Type		typeMask,
					bool					recursive,
					const StringVector &	filterPatterns,
					StringVector &			vec) const;

	inline void listLocallyScopedNames(
					Configuration::Type		typeMask,
					bool					recursive,
					const StringVector &	filterPatterns,
					StringVector &			vec) const;

	inline ConfigScope * parentScope() const;
	ConfigScope * rootScope() const;

	//--------
	// Debugging aids
	//--------
	void dump(
				StringBuffer &		buf,
				bool				wantExpandedUidNames,
				int					indentLevel = 0) const;

protected:
	//--------
	// Helper operations
	//--------
	int hash(const char *name) const;

	void growIfTooFull();

	void listLocalNames(
					Configuration::Type		typeMask,
					StringVector &			vec) const;

	void listScopedNamesHelper(
					const char *			prefix,
					Configuration::Type		typeMask,
					bool					recursive,
					const StringVector &	filterPatterns,
					StringVector &			vec) const;

	bool listFilter(
					const char *			name,
					const StringVector &	filterPatterns) const;

protected:
	//--------
	// Instance variables
	//--------
	ConfigScope *		m_parentScope;
	StringBuffer		m_scopedName;
	StringBuffer		m_localName;
	ConfigScopeEntry *	m_table;
	int					m_tableSize;
	int					m_numEntries;

	//--------
	// Not implemented.
	//--------
	ConfigScope();
	ConfigScope & operator=(const ConfigScope &);
};


inline ConfigScope *
ConfigScope::parentScope() const
{
	return m_parentScope;
}


inline const char *
ConfigScope::scopedName() const
{
	return m_scopedName.c_str();
}


inline void
ConfigScope::listFullyScopedNames(
	Configuration::Type			typeMask,
	bool						recursive,
	StringVector &				vec) const
{
	StringVector				filterPatterns;

	listScopedNamesHelper(m_scopedName.c_str(), typeMask, recursive,
				filterPatterns, vec);
}


inline void
ConfigScope::listFullyScopedNames(
	Configuration::Type			typeMask,
	bool						recursive,
	const StringVector &		filterPatterns,
	StringVector &				vec) const
{
	vec.empty();
	listScopedNamesHelper(m_scopedName.c_str(), typeMask, recursive,
				filterPatterns, vec);
}


inline void
ConfigScope::listLocallyScopedNames(
	Configuration::Type			typeMask,
	bool						recursive,
	const StringVector &		filterPatterns,
	StringVector &				vec) const
{
	vec.empty();
	listScopedNamesHelper("", typeMask, recursive, filterPatterns, vec);
}


}; // namespace CONFIG4CPP_NAMESPACE
#endif
