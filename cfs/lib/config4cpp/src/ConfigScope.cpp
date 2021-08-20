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
#include "ConfigScope.h"
#include "UidIdentifierProcessor.h"
#include <string.h>
#include <assert.h>
#include <stdlib.h>


namespace CONFIG4CPP_NAMESPACE {

//----------------------------------------------------------------------
// Function:	Constructor
//
// Description:	
//----------------------------------------------------------------------

ConfigScope::ConfigScope(ConfigScope * parentScope, const char * name)
{
	m_parentScope = parentScope;
	m_tableSize   = 16;
	m_table       = new ConfigScopeEntry[m_tableSize];
	m_numEntries  = 0;

	if (m_parentScope == 0) {
		assert(name[0] == '\0');
		m_scopedName = "";
	} else {
		m_scopedName = m_parentScope->m_scopedName;
		if (m_parentScope->m_parentScope != 0) {
			m_scopedName.append(".");
		}
		m_scopedName.append(name);
	}
	m_localName = name;
}



//----------------------------------------------------------------------
// Function:	Destructor
//
// Description:	
//----------------------------------------------------------------------

ConfigScope::~ConfigScope()
{
	delete [] m_table;
}



//----------------------------------------------------------------------
// Function:	rootScope
//
// Description:	
//----------------------------------------------------------------------

ConfigScope *
ConfigScope::rootScope() const
{
	const ConfigScope * scope;

	scope = this;
	while (scope->m_parentScope != 0) {
		scope = scope->m_parentScope;
	}
	return (ConfigScope *)scope;
}



//----------------------------------------------------------------------
// Function:	addOrReplaceString()
//
// Description:	Add an entry to the hash table.
//
// Notes:	Replaces the previous entry with the same name, if any.
//----------------------------------------------------------------------

bool
ConfigScope::addOrReplaceString(
	const char *			name,
	const char *			str)
{
	int						index;
	ConfigScopeEntry *		entry;
	ConfigScopeEntry *		nextEntry;
	ConfigScopeEntry *		newEntry;

	entry = findEntry(name, index);

	if (entry != 0 && entry->type() == Configuration::CFG_SCOPE) {
		//--------
		// Fail because there is a scope with the same name.
		//--------
		return false;
	} else if (entry != 0) {
		//--------
		// It already exists.
		// Replace the existing item
		//--------
		delete entry->m_item;
		entry->m_item = new ConfigItem(name, str);

	} else {
		//--------
		// It doesn't already exist.
		// Add a new entry into the list.
		//--------
		m_numEntries ++;
		growIfTooFull();
		index = hash(name);
		entry = &m_table[index];
		nextEntry = entry->m_next;
		newEntry = new ConfigScopeEntry(name, new ConfigItem(name, str),
										nextEntry);
		entry->m_next = newEntry;
	}
	return true;
}



//----------------------------------------------------------------------
// Function:	addOrReplaceList()
//
// Description:	Add an entry to the hash table.
//
// Notes:	Replaces the previous entry with the same name, if any.
//----------------------------------------------------------------------

bool
ConfigScope::addOrReplaceList(
	const char *			name,
	const StringVector &	list)
{
	int						index;
	ConfigScopeEntry *		entry;
	ConfigScopeEntry *		nextEntry;
	ConfigScopeEntry *		newEntry;

	entry = findEntry(name, index);
	if (entry && entry->type() == Configuration::CFG_SCOPE) {
		//--------
		// Fail because there is a scope with the same name.
		//--------
		return false;
	} else if (entry) {
		//--------
		// It already exists. Replace the existing item
		//--------
		delete entry->m_item;
		entry->m_item = new ConfigItem(name, list);

	} else {
		//--------
		// It doesn't already exist. Add a new entry into the list.
		//--------
		m_numEntries ++;
		growIfTooFull();
		index = hash(name);
		entry = &m_table[index];
		nextEntry = entry->m_next;
		newEntry = new ConfigScopeEntry(name, new ConfigItem(name, list),
		                                 nextEntry);
		entry->m_next = newEntry;
	}
	return true;
}



//----------------------------------------------------------------------
// Function:	addOrReplaceList()
//
// Description:	Add an entry to the hash table.
//
// Notes:	Replaces the previous entry with the same name, if any.
//----------------------------------------------------------------------

bool
ConfigScope::addOrReplaceList(
	const char *			name,
	const char**			array,
	int						size)
{
	int						index;
	ConfigScopeEntry *		entry;
	ConfigScopeEntry *		nextEntry;
	ConfigScopeEntry *		newEntry;

	entry = findEntry(name, index);
	if (entry && entry->type() == Configuration::CFG_SCOPE) {
		//--------
		// Fail because there is a scope with the same name.
		//--------
		return false;
	} else if (entry) {
		//--------
		// It already exists. Replace the existing item
		//--------
		delete entry->m_item;
		entry->m_item = new ConfigItem(name, array, size);

	} else {
		//--------
		// It doesn't already exist. Add a new entry into the list.
		//--------
		m_numEntries ++;
		growIfTooFull();
		index = hash(name);
		entry = &m_table[index];
		nextEntry = entry->m_next;
		newEntry = new ConfigScopeEntry(name, new ConfigItem(name, array,size),
										nextEntry);
		entry->m_next = newEntry;
	}
	return true;
}



//----------------------------------------------------------------------
// Function:	ensureScopeExists()
//
// Description:	
//----------------------------------------------------------------------

bool
ConfigScope::ensureScopeExists(
	const char *			name,
	ConfigScope *&			scope)
{
	int						index;
	ConfigScopeEntry *		entry;
	ConfigScopeEntry *		nextEntry;
	ConfigScopeEntry *		newEntry;

	entry = findEntry(name, index);
	if (entry && entry->type() != Configuration::CFG_SCOPE) {
		//--------
		// Fail because it already exists, but not as a scope
		//--------
		scope = 0;
		return false;
	} else if (entry) {
		//--------
		// It already exists.
		//--------
		scope = entry->item()->scopeVal();
		return true;
	} else {
		//--------
		// It doesn't already exist. Add a new entry into the list.
		//--------
		m_numEntries ++;
		growIfTooFull();
		index = hash(name);
		entry = &m_table[index];
		nextEntry = entry->m_next;
		scope = new ConfigScope(this, name);
		newEntry = new ConfigScopeEntry(name, new ConfigItem(name, scope),
										nextEntry);
		entry->m_next = newEntry;
	}
	return true;
}



//----------------------------------------------------------------------
// Function:	findItem()
//
// Description:	Returns the named item if it exists.
//
// Notes:	Returns a nil pointer on failure.
//----------------------------------------------------------------------

ConfigItem *
ConfigScope::findItem(const char * name) const
{
	int					index;
	ConfigScopeEntry *	entry;
	ConfigItem *		result;

	result = 0;
	entry = findEntry(name, index);
	if (entry != 0) {
		result = entry->m_item;
	}
	return result;
}



//----------------------------------------------------------------------
// Function:	findEntry()
//
// Description:	Returns the named entry if it exists.
//
// Notes:	Returns a nil pointer on failure.
//		Always returns the index (both on success and failure).
//----------------------------------------------------------------------

ConfigScopeEntry *
ConfigScope::findEntry(const char * name, int & index) const
{
	ConfigScopeEntry *		entry;

	index = hash(name);
	entry = m_table[index].m_next;

	//--------
	// Iterate over singly linked list,
	// searching for the named entry.
	//--------
	while (entry) {
		if (!strcmp(name, entry->name())) {
			//--------
			// Found it!
			//--------
			return entry;
		}
		entry = entry->m_next;
	}
	//--------
	// Not found.
	//--------
	return 0;
}



bool
ConfigScope::removeItem(const char * name)
{
	ConfigScopeEntry *		entry;
	ConfigScopeEntry *		victim;
	int						index;

	index = hash(name);
	entry = &m_table[index];
	//--------
	// Iterate over singly linked list,
	// searching for the named entry.
	//--------
	while (entry->m_next != 0) {
		if (!strcmp(name, entry->m_next->name())) {
			//--------
			// Found it!
			//--------
			victim = entry->m_next;
			entry->m_next = victim->m_next;
			victim->m_next = 0;
			delete victim;
			m_numEntries --;
			return true;
		}
		entry = entry->m_next;
	}
	//--------
	// Not found.
	//--------
	return false;
}



//----------------------------------------------------------------------
// Function:	is_in_table()
//
// Description:	Returns true if the specified entry exists in the table.
//		Returns false otherwise.
//----------------------------------------------------------------------

bool
ConfigScope::is_in_table(const char *name) const
{
	int	index;

	return (findEntry(name, index) != 0);
}



//----------------------------------------------------------------------
// Function:	listLocalNames()
//
// Description:	
//----------------------------------------------------------------------

void
ConfigScope::listLocalNames(
	Configuration::Type		typeMask,
	StringVector &			vec) const
{
	int						i;
	int						countWanted;
	int						countUnwanted;
	ConfigScopeEntry *		entry;

	//--------
	// Iterate over all the entries in the hash table and copy
	// their names into the StringVector
	//--------
	vec.empty();
	vec.ensureCapacity(m_numEntries);
	countWanted = 0;
	countUnwanted = 0;
	for (i = 0; i < m_tableSize; i++) {
		entry = m_table[i].m_next;
		while (entry) {
			if (entry->type() & typeMask) {
				vec.add(entry->name());
				countWanted++;
			} else {
				countUnwanted++;
			}
			entry = entry->m_next;
		}
	}
	assert(countWanted + countUnwanted == m_numEntries);
}



//----------------------------------------------------------------------
// Function:	listScopedNamesHelper()
//
// Description:	
//----------------------------------------------------------------------

void
ConfigScope::listScopedNamesHelper(
	const char *				prefix,
	Configuration::Type			typeMask,
	bool						recursive,
	const StringVector &		filterPatterns,
	StringVector &				vec) const
{
	int							i;
	ConfigScopeEntry *			entry;
	StringBuffer				scopedName;

	//--------
	// Iterate over all the entries in the hash table and copy
	// their locally-scoped names into the StringVector
	//--------
	vec.ensureCapacity(vec.length() + m_numEntries);
	for (i = 0; i < m_tableSize; i++) {
		entry = m_table[i].m_next;
		while (entry) {
			scopedName = prefix;
			if (prefix[0] != '\0') {
				scopedName.append(".");
			}
			scopedName.append(entry->name());
			if ((entry->type() & typeMask)
			    && listFilter(scopedName.c_str(), filterPatterns))
			{
				vec.add(scopedName);
			}
			if (recursive && entry->type() == Configuration::CFG_SCOPE) {
				entry->item()->scopeVal()->listScopedNamesHelper(
												scopedName.c_str(), typeMask,
												true, filterPatterns, vec);
			}
			entry = entry->m_next;
		}
	}
}



//----------------------------------------------------------------------
// Function:	listFilter()
//
// Description:	
//----------------------------------------------------------------------

bool
ConfigScope::listFilter(
	const char *				name,
	const StringVector &		filterPatterns) const
{
	int							i;
	int							len;
	const char *				unexpandedName;
	const char *				pattern;
	StringBuffer				buf;
	UidIdentifierProcessor		uidProc;

	len = filterPatterns.length();
	if (len == 0) {
		return true;
	}

	unexpandedName = uidProc.unexpand(name, buf);
	for (i = 0; i < len; i++) {
		pattern = filterPatterns[i];
		if (Configuration::patternMatch(unexpandedName, pattern)) {
			return true;
		}
	}
	return false;
}



//----------------------------------------------------------------------
// Function:	dump()
//
// Description:	Dump the contents of the entire hash table to a file.
//
// Notes:	This is intended for debugging purposes.
//----------------------------------------------------------------------

void
ConfigScope::dump(
	StringBuffer &			buf,
	bool					wantExpandedUidNames,
	int						indentLevel) const
{
	int						i;
	int						len;
	StringVector			nameVec;
	ConfigItem *			item;
	
	//--------
	// First pass. Dump the variables
	//--------
	listLocalNames(Configuration::CFG_VARIABLES, nameVec);
	nameVec.sort();
	len = nameVec.length();
	for (i = 0; i < len; i++) {
		item = findItem(nameVec[i]);
		assert(item->type() & Configuration::CFG_VARIABLES);
		item->dump(buf, item->name(), wantExpandedUidNames, indentLevel);
	}

	//--------
	// Second pass. Dump the nested scopes
	//--------
	listLocalNames(Configuration::CFG_SCOPE, nameVec);
	nameVec.sort();
	len = nameVec.length();
	for (i = 0; i < len; i++) {
		item = findItem(nameVec[i]);
		assert(item->type() == Configuration::CFG_SCOPE);
		item->dump(buf, item->name(), wantExpandedUidNames, indentLevel);
	}
}



//----------------------------------------------------------------------
// Function:	hash()
//
// Description:	Hashes the name to provide an integer value.
//----------------------------------------------------------------------

int
ConfigScope::hash(const char *name) const
{
	int				result;
	const char *	p;

	result = 0;
	for (p = name; *p != '\0'; p++) {
		result += (*p);
	}
	result = result % m_tableSize;

	return result;
}



//----------------------------------------------------------------------
// Function:	growIfTooFull()
//
// Description:	
//----------------------------------------------------------------------

void
ConfigScope::growIfTooFull()
{
	int						origTableSize;
	int						i;
	int						index;
	ConfigScopeEntry *		origTable;
	ConfigScopeEntry *		entry;
	ConfigScopeEntry *		nextEntry;

	if (m_numEntries * 2 < m_tableSize) {
		return;
	}

	origTable     = m_table;
	origTableSize = m_tableSize;
	m_tableSize   = m_tableSize * 2;
	m_table       = new ConfigScopeEntry[m_tableSize];

	for (i = 0; i < origTableSize; i++) {
		entry = origTable[i].m_next;
		while (entry) {
			index = hash(entry->name());
			nextEntry = entry->m_next;
			entry->m_next = m_table[index].m_next;
			m_table[index].m_next = entry;
			entry = nextEntry;
		}
		origTable[i].m_next = 0;
	}
	delete [] origTable;
}

}; // namespace CONFIG4CPP_NAMESPACE
