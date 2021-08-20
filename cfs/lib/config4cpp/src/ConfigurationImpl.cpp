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
#include "ConfigurationImpl.h"
#include "util.h"
#include "platform.h"
#include "DefaultSecurityConfiguration.h"
#include "ConfigParser.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>


namespace CONFIG4CPP_NAMESPACE {

//----------------------------------------------------------------------
// Function:	Constructor
//
// Description:
//----------------------------------------------------------------------

ConfigurationImpl::ConfigurationImpl()
{
	m_fileName             = "<no file>";
	m_rootScope            = new ConfigScope(0, "");
	m_currScope            = m_rootScope;
	m_fallbackCfg          = 0;
	m_amOwnerOfFallbackCfg = false;
	m_amOwnerOfSecurityCfg = false;
	m_securityCfg          = &DefaultSecurityConfiguration::singleton;
}



//----------------------------------------------------------------------
// Function:	Destructor
//
// Description:
//----------------------------------------------------------------------

ConfigurationImpl::~ConfigurationImpl()
{
	delete m_rootScope;
	if (m_amOwnerOfSecurityCfg) {
		m_securityCfg->destroy();
	}
	if (m_amOwnerOfFallbackCfg) {
		m_fallbackCfg->destroy();
	}
}



void
ConfigurationImpl::setFallbackConfiguration(Configuration * cfg)
{
	if (m_amOwnerOfFallbackCfg) {
		m_fallbackCfg->destroy();
	}
	m_fallbackCfg = static_cast<ConfigurationImpl *>(cfg);
	m_amOwnerOfFallbackCfg = false;
}



void
ConfigurationImpl::setFallbackConfiguration(
	Configuration::SourceType	sourceType,
	const char *				source,
	const char *				sourceDescription)
												throw(ConfigurationException)
{
	Configuration *				cfg;
	StringBuffer				msg;

	cfg = Configuration::create();
	try {
		cfg->parse(sourceType, source, sourceDescription);
	} catch(const ConfigurationException & ex) {
		cfg->destroy();
		msg << "cannot set default configuration: " << ex.c_str();
		throw ConfigurationException(msg.c_str());
	}

	if (m_amOwnerOfFallbackCfg) {
		m_fallbackCfg->destroy();
	}
	m_fallbackCfg = static_cast<ConfigurationImpl *>(cfg);
	m_amOwnerOfFallbackCfg = true;
}



const Configuration *
ConfigurationImpl::getFallbackConfiguration()
{
	return m_fallbackCfg;
}



void
ConfigurationImpl::setSecurityConfiguration(
	Configuration *			cfg,
	bool					takeOwnership,
	const char *			scope) throw (ConfigurationException)
{
	StringVector			dummyList;
	StringBuffer			msg;

	try {
		cfg->lookupList(scope, "allow_patterns", dummyList);
		cfg->lookupList(scope, "deny_patterns", dummyList);
		cfg->lookupList(scope, "trusted_directories", dummyList);
	} catch(const ConfigurationException & ex) {
		msg << "cannot set security configuration: "
		    << ex.c_str();
		throw ConfigurationException(msg.c_str());
	}

	m_securityCfg = cfg;
	m_securityCfgScope = scope;
	m_amOwnerOfSecurityCfg = takeOwnership;
}



void
ConfigurationImpl::setSecurityConfiguration(
	const char *			cfgInput,
	const char *			scope) throw (ConfigurationException)
{
	Configuration *			cfg;
	StringVector			dummyList;
	StringBuffer			msg;

	cfg = Configuration::create();
	try {
		cfg->parse(cfgInput);
		cfg->lookupList(scope, "allow_patterns", dummyList);
		cfg->lookupList(scope, "deny_patterns", dummyList);
		cfg->lookupList(scope, "trusted_directories", dummyList);
	} catch(const ConfigurationException & ex) {
		cfg->destroy();
		msg << "cannot set security configuration: "
		    << ex.c_str();
		throw ConfigurationException(msg.c_str());
	}

	if (m_amOwnerOfSecurityCfg) {
		m_securityCfg->destroy();
	}
	m_securityCfg = cfg;
	m_securityCfgScope = scope;
	m_amOwnerOfSecurityCfg = true;
}



void
ConfigurationImpl::getSecurityConfiguration(
	const Configuration *&	cfg,
	const char *&			scope)
{
	cfg = m_securityCfg;
	scope = m_securityCfgScope.c_str();
}



//----------------------------------------------------------------------
// Function:	parse()
//
// Description:	
//----------------------------------------------------------------------

void
ConfigurationImpl::parse(
	Configuration::SourceType	sourceType,
	const char *				source,
	const char *				sourceDescription) throw(ConfigurationException)
{
	StringBuffer				trustedCmdLine;
	StringBuffer				msg;

	switch (sourceType) {
	case Configuration::INPUT_FILE:
		m_fileName = source;
		break;
	case Configuration::INPUT_STRING:
		if (strcmp(sourceDescription, "") == 0) {
			m_fileName = "<string-based configuration>";
		} else {
			m_fileName = sourceDescription;
		}
		break;
	case Configuration::INPUT_EXEC:
		if (strcmp(sourceDescription, "") == 0) {
			m_fileName.empty();
			m_fileName << "exec#" << source;
		} else {
			m_fileName = sourceDescription;
		}
		if (!isExecAllowed(source, trustedCmdLine)) {
			msg << "cannot parse output of executing \"" << source << "\" "
				<< "due to security restrictions";
			throw ConfigurationException(msg.c_str());
		}
		break;
	default:
		assert(0); // Bug!
		break;
	}
	ConfigParser parser(sourceType, source, trustedCmdLine.c_str(),
	                    m_fileName.c_str(), this);
}



//----------------------------------------------------------------------
// Function:	type()
//
// Description:	Return the type of the named entry.
//----------------------------------------------------------------------

Configuration::Type
ConfigurationImpl::type(
	const char *			scope,
	const char *			localName) const
{
	ConfigItem *			item;
	Configuration::Type		result;
	StringBuffer			fullyScopedName;
	
	mergeNames(scope, localName, fullyScopedName);
	item = lookup(fullyScopedName.c_str(), localName);
	if (item == 0) {
		result = Configuration::CFG_NO_VALUE;
	} else {
		result = item->type();
	}
	return result;
}



//----------------------------------------------------------------------
// Function:	stringValue()
//
// Description:	Return the string, if any, associated with the named
//		entry. Indicates success/failure via the "status" parameter.
//----------------------------------------------------------------------

void
ConfigurationImpl::stringValue(
		const char *			fullyScopedName,
		const char *			localName,
		const char *&			str,
		Configuration::Type &	type) const
{
	ConfigItem *				item;

	item = lookup(fullyScopedName, localName);
	if (item == 0) {
		type = Configuration::CFG_NO_VALUE;
		str = (const char*)0;
	} else {
		type = item->type();
		if (type == Configuration::CFG_STRING) {
			str = item->stringVal();
		} else {
			str = (const char *)0;
		}
	}
}



//----------------------------------------------------------------------
// Function:	listValue()
//
// Description:	Return the list, if any, associated with the named
//		entry.
//----------------------------------------------------------------------

void
ConfigurationImpl::listValue(
		const char *			fullyScopedName,
		const char *			localName,
		StringVector &			list,
		Configuration::Type &	type) const
{
	ConfigItem *				item;
	
	item = lookup(fullyScopedName, localName);
	if (item == 0) {
		type = Configuration::CFG_NO_VALUE;
		list.empty();
	} else {
		type = item->type();
		if (type == Configuration::CFG_LIST) {
			list = item->listVal();
		} else {
			list.empty();
		}
	}
}



//----------------------------------------------------------------------
// Function:	listValue()
//
// Description:	Return the list, if any, associated with the named entry.
//----------------------------------------------------------------------

void
ConfigurationImpl::listValue(
	const char *			fullyScopedName,
	const char *			localName,
	const char **&			array,
	int &					arraySize,
	Configuration::Type &	type) const
{
	ConfigItem *			item;
	StringVector *			list;
	
	item = lookup(fullyScopedName, localName);
	if (item == 0) {
		type = Configuration::CFG_NO_VALUE;
		array = 0;
		arraySize  = 0;
	} else {
		type = item->type();
		if (type == Configuration::CFG_LIST) {
			list = &item->listVal();
			list->c_array(array, arraySize);
		} else {
			array = 0;
			arraySize  = 0;
		}
	}
}



//----------------------------------------------------------------------
// Function:	insertString()
//
// Description:	Insert a named string into the symbol table.
//
// Notes:	Overwrites an existing entry of the same name.
//----------------------------------------------------------------------

void
ConfigurationImpl::insertString(
	const char *			scope,
	const char *			localName,
	const char *			str) throw(ConfigurationException)
{
	StringVector			vec;
	int						len;
	ConfigScope *			scopeObj;
	StringBuffer			msg;
	StringBuffer			fullyScopedName;
	
	mergeNames(scope, localName, fullyScopedName);
	splitScopedNameIntoVector(fullyScopedName.c_str(), vec);
	len = vec.length();
	ensureScopeExists(vec, 0, len-2, scopeObj);
	if (!scopeObj->addOrReplaceString(vec[len-1], str)) {
		msg << fileName() << ": "
		    << "variable '"
		    << fullyScopedName
		    << "' was previously used as a scope";
		throw ConfigurationException(msg.c_str());
	}
}



//----------------------------------------------------------------------
// Function:	ensureScopeExists()
//
// Description:	
//----------------------------------------------------------------------

void
ConfigurationImpl::ensureScopeExists(
	const char *			scope,
	const char *			localName) throw(ConfigurationException)
{
	StringBuffer			fullyScopedName;
	ConfigScope *			dummyScope;
	
	mergeNames(scope, localName, fullyScopedName);
	ensureScopeExists(fullyScopedName.c_str(), dummyScope);
}



//----------------------------------------------------------------------
// Function:	insertList()
//
// Description:	Insert a named list into the symbol table.
//
// Notes:	Overwrites an existing entry of the same name.
//----------------------------------------------------------------------

void
ConfigurationImpl::insertList(
	const char *			scope,
	const char *			localName,
	const char **			array,
	int						arraySize) throw(ConfigurationException)
{
	StringVector			vec;
	int						len;
	ConfigScope *			scopeObj;
	StringBuffer			msg;
	StringBuffer			fullyScopedName;
	
	mergeNames(scope, localName, fullyScopedName);
	splitScopedNameIntoVector(fullyScopedName.c_str(), vec);
	len = vec.length();
	ensureScopeExists(vec, 0, len-2, scopeObj);
	if (!scopeObj->addOrReplaceList(vec[len-1], array, arraySize)) {
		msg << fileName() << ": " << "variable '" << fullyScopedName
			<< "' was previously used as a scope";
		throw ConfigurationException(msg.c_str());
	}
}



//----------------------------------------------------------------------
// Function:	insertList()
//
// Description:	Insert a named list into the symbol table.
//
// Notes:	Overwrites an existing entry of the same name.
//----------------------------------------------------------------------

void
ConfigurationImpl::insertList(
	const char *		scope,
	const char *		localName,
	const char **		nullTerminatedArray) throw(ConfigurationException)
{
	int					size;
	
	size = 0;
	while (nullTerminatedArray[size] != 0) {
		size++;
	}
	insertList(scope, localName, nullTerminatedArray, size);
}



//----------------------------------------------------------------------
// Function:	insertList()
//
// Description:	Insert a named list into the symbol table.
//
// Notes:	Overwrites an existing entry of the same name.
//----------------------------------------------------------------------

void	
ConfigurationImpl::insertList(
	const char *			scope,
	const char *			localName,
	const StringVector &	vec) throw(ConfigurationException)
{
	const char **			array;
	int						size;

	vec.c_array(array, size);
	insertList(scope, localName, array, size);
}



//----------------------------------------------------------------------
// Function:	insertList()
//
// Description:	Insert a named list into the symbol table.
//
// Notes:	Overwrites an existing entry of the same name.
//----------------------------------------------------------------------

void
ConfigurationImpl::insertList(
	const char *				name,
	const StringVector &		list) throw(ConfigurationException)
{
	StringVector				vec;
	int							len;
	ConfigScope *				scope;
	StringBuffer				msg;
	
	splitScopedNameIntoVector(name, vec);
	len = vec.length();
	ensureScopeExists(vec, 0, len-2, scope);
	if (!scope->addOrReplaceList(vec[len-1], list)) {
		msg << fileName() << ": " << "variable '" << name
			<< "' was previously used as a scope";
		throw ConfigurationException(msg.c_str());
	}
}



//----------------------------------------------------------------------
// Function:	remove()
//
// Description:	Remove the specified item.
//----------------------------------------------------------------------

void
ConfigurationImpl::remove(const char * scope, const char * localName)
					throw(ConfigurationException)
{
	StringBuffer			fullyScopedName;
	StringBuffer			msg;
	ConfigScope *			scopeObj;
	ConfigItem *			item;
	StringVector			vec;
	int						i;
	int						len;
	
	scopeObj = m_currScope;
	mergeNames(scope, localName, fullyScopedName);
	splitScopedNameIntoVector(fullyScopedName.c_str(), vec);
	len = vec.length();
	for (i = 0; i < len - 1; i++) {
		item = scopeObj->findItem(vec[i]);
		if (item == 0 || item->type() != Configuration::CFG_SCOPE) {
			msg << fileName() << ": '" << fullyScopedName
				<< "' does not exist'";
			throw ConfigurationException(msg.c_str());
		}
		scopeObj = item->scopeVal();
		assert(scopeObj != 0);
	}
	assert(i == len - 1);
	assert(scopeObj != 0);
	if (!scopeObj->removeItem(vec[i])) {
		msg << fileName() << ": '" << fullyScopedName << "' does not exist'";
		throw ConfigurationException(msg.c_str());
	}
}



//----------------------------------------------------------------------
// Function:	empty()
//
// Description:	Re-initialize the configuration object
//----------------------------------------------------------------------

void
ConfigurationImpl::empty()
{
	delete m_rootScope;
	m_fileName  = "<no file>";
	m_rootScope = new ConfigScope(0, "");
	m_currScope = m_rootScope;
}



//----------------------------------------------------------------------
// Function:	lookup()
//
// Description:	
//----------------------------------------------------------------------

ConfigItem *
ConfigurationImpl::lookup(
	const char *			fullyScopedName,
	const char *			localName,
	bool					startInRoot) const
{
	StringVector			vec;
	ConfigScope *			scope;
	ConfigItem *			item;

	if (fullyScopedName[0] == '\0') {
		return 0;
	}
	if (fullyScopedName[0] == '.') {
		//--------
		// Search only in the root scope and skip over '.'
		//--------
		splitScopedNameIntoVector(&fullyScopedName[1], vec);
		scope = m_rootScope;
	} else if (startInRoot) {
		//--------
		// Search only in the root scope
		//--------
		splitScopedNameIntoVector(fullyScopedName, vec);
		scope = m_rootScope;
	} else {
		//--------
		// Start search from the current scope
		//--------
		splitScopedNameIntoVector(fullyScopedName, vec);
		scope = m_currScope;
	}
	item = 0;
	while (scope != 0) {
		item = lookupHelper(scope, vec);
		if (item != 0) {
			break;
		}
		scope = scope->parentScope();
	}
	if (item == 0 && m_fallbackCfg != 0) {
		item = m_fallbackCfg->lookup(localName, localName, true);
	}
	return item;
}



ConfigItem *
ConfigurationImpl::lookupHelper(
	ConfigScope *			scope,
	const StringVector &	vec) const
{
	int						len;
	int						i;
	ConfigItem *			item;

	len = vec.length();
	for (i = 0; i < len - 1; i++) {
		item = scope->findItem(vec[i]);
		if (item == 0 || item->type() != Configuration::CFG_SCOPE) {
			return 0;
		}
		scope = item->scopeVal();
		assert(scope != 0);
	}
	assert(i == len - 1);
	assert(scope != 0);
	item = scope->findItem(vec[i]);
	return item;
}



//----------------------------------------------------------------------
// Function:	dump()
//
// Description:	Generate a "printout" of the entire contents.
//----------------------------------------------------------------------

void
ConfigurationImpl::dump(StringBuffer & buf, bool wantExpandedUidNames) const
{
	buf = "";
	m_rootScope->dump(buf, wantExpandedUidNames);
}



//----------------------------------------------------------------------
// Function:	dump()
//
// Description:	Generate a "printout" of the specified entry.
//----------------------------------------------------------------------

void
ConfigurationImpl::dump(
	StringBuffer &			buf,
	bool					wantExpandedUidNames,
	const char *			scope,
	const char *			localName) const throw(ConfigurationException)
{
	ConfigItem *			item;
	StringBuffer			msg;
	StringBuffer			fullyScopedName;

	buf = "";
	mergeNames(scope, localName, fullyScopedName);
	if (strcmp(fullyScopedName.c_str(), "") == 0) {
		m_rootScope->dump(buf, wantExpandedUidNames);
	} else {
		item = lookup(fullyScopedName.c_str(), localName, true);
		if (item == 0) {
			msg << fileName() << ": " << "'" << fullyScopedName
				<< "' is not an entry";
			throw ConfigurationException(msg.c_str());
		}
		item->dump(buf, fullyScopedName.c_str(), wantExpandedUidNames);
	}
}



//----------------------------------------------------------------------
// Function:	listFullyScopedNames()
//
// Description:
//----------------------------------------------------------------------

void
ConfigurationImpl::listFullyScopedNames(
	const char *			scope,
	const char *			localName,
	Type					typeMask,
	bool					recursive,
	StringVector &			names) const throw(ConfigurationException)
{
	StringVector			filterPatterns;

	listFullyScopedNames(scope, localName, typeMask, recursive, filterPatterns,
	                     names);
}


void
ConfigurationImpl::listFullyScopedNames(
	const char *			scope,
	const char *			localName,
	Type					typeMask,
	bool					recursive,
	const char *			filterPattern,
	StringVector &			names) const throw(ConfigurationException)
{
	StringVector			filterPatterns;

	filterPatterns.add(filterPattern);
	listFullyScopedNames(scope, localName, typeMask, recursive, filterPatterns,
	                     names);

}


void
ConfigurationImpl::listFullyScopedNames(
	const char *			scope,
	const char *			localName,
	Type					typeMask,
	bool					recursive,
	const StringVector &	filterPatterns,
	StringVector &			names) const throw(ConfigurationException)
{
	StringBuffer			fullyScopedName;
	StringBuffer			msg;
	ConfigItem *			item;
	ConfigScope *			scopeObj;

	mergeNames(scope, localName, fullyScopedName);
	if (strcmp(fullyScopedName.c_str(), "") == 0) {
		scopeObj = m_rootScope;
	} else {
		item = lookup(fullyScopedName.c_str(), localName, true);
		if (item == 0 || item->type() != Configuration::CFG_SCOPE) {
			msg << fileName() << ": " << "'" << fullyScopedName
				<< "' is not a scope";
			throw ConfigurationException(msg.c_str());
		}
		scopeObj = item->scopeVal();
	}
	scopeObj->listFullyScopedNames(typeMask, recursive, filterPatterns, names);
	names.sort();
}



//----------------------------------------------------------------------
// Function:	listLocallyScopedNames()
//
// Description:
//----------------------------------------------------------------------

void
ConfigurationImpl::listLocallyScopedNames(
	const char *			scope,
	const char *			localName,
	Type					typeMask,
	bool					recursive,
	const char *			filterPattern,
	StringVector &			names) const throw(ConfigurationException)
{
	StringVector			filterPatterns;

	filterPatterns.add(filterPattern);
	listLocallyScopedNames(scope, localName, typeMask, recursive,
	                       filterPatterns, names);
}


void
ConfigurationImpl::listLocallyScopedNames(
	const char *			scope,
	const char *			localName,
	Type					typeMask,
	bool					recursive,
	StringVector &			names) const throw(ConfigurationException)
{
	StringVector			filterPatterns;

	listLocallyScopedNames(scope, localName, typeMask, recursive,
	                       filterPatterns, names);
}


void
ConfigurationImpl::listLocallyScopedNames(
	const char *			scope,
	const char *			localName,
	Type					typeMask,
	bool					recursive,
	const StringVector &	filterPatterns,
	StringVector &			names) const throw(ConfigurationException)
{
	StringBuffer			fullyScopedName;
	StringBuffer			msg;
	ConfigItem *			item;
	ConfigScope *			scopeObj;

	mergeNames(scope, localName, fullyScopedName);
	if (strcmp(fullyScopedName.c_str(), "") == 0) {
		scopeObj = m_rootScope;
	} else {
		item = lookup(fullyScopedName.c_str(), localName, true);
		if (item == 0 || item->type() != Configuration::CFG_SCOPE) {
			msg << fileName() << ": " << "'" << fullyScopedName
				<< "' is not a scope";
			throw ConfigurationException(msg.c_str());
		}
		scopeObj = item->scopeVal();
	}
	scopeObj->listLocallyScopedNames(typeMask, recursive, filterPatterns,names);
	names.sort();
}


const char *
ConfigurationImpl::lookupString(
	const char *			scope,
	const char *			localName,
	const char *			defaultVal) const throw(ConfigurationException)
{
	Configuration::Type	 	type;
	StringBuffer			msg;
	const char *			str;
	StringBuffer			fullyScopedName;
	
	mergeNames(scope, localName, fullyScopedName);
	stringValue(fullyScopedName.c_str(), localName, str, type);
	switch (type) {
	case Configuration::CFG_STRING:
		break;
	case Configuration::CFG_NO_VALUE:
		str = defaultVal;
		break;
	case Configuration::CFG_SCOPE:
		msg << fileName() << ": '" << fullyScopedName
			<< "' is a scope instead of a string";
		throw ConfigurationException(msg.c_str());
	case Configuration::CFG_LIST:
		msg << fileName() << ": '" << fullyScopedName
			<< "' is a list instead of a string";
		throw ConfigurationException(msg.c_str());
	default:
		assert(0);	// Bug
	}
	return str;
}



const char *
ConfigurationImpl::lookupString(
	const char *			scope,
	const char *			localName) const throw(ConfigurationException)
{
	Configuration::Type	 	type;
	StringBuffer			msg;
	const char *			str;
	StringBuffer			fullyScopedName;
	
	mergeNames(scope, localName, fullyScopedName);
	stringValue(fullyScopedName.c_str(), localName, str, type);
	switch (type) {
	case Configuration::CFG_STRING:
		break;
	case Configuration::CFG_NO_VALUE:
		msg << fileName() << ": no value specified for '" << fullyScopedName
			<< "'";
		throw ConfigurationException(msg.c_str());
	case Configuration::CFG_SCOPE:
		msg << fileName() << ": '" << fullyScopedName
			<< "' is a scope instead of a string";
		throw ConfigurationException(msg.c_str());
	case Configuration::CFG_LIST:
		msg << fileName() << ": " << "'" << fullyScopedName
			<< "' is a list instead of a string";
		throw ConfigurationException(msg.c_str());
	default:
		assert(0);	// Bug
	}
	return str;
}



void
ConfigurationImpl::lookupList(
	const char *			scope,
	const char *			localName,
	const char **&			array,
	int &					arraySize,
	const char **			defaultArray,
	int						defaultArraySize) const
												throw(ConfigurationException)
{
	Configuration::Type	 	type;
	StringBuffer			msg;
	int						i;
	StringBuffer			fullyScopedName;
	
	mergeNames(scope, localName, fullyScopedName);
	listValue(fullyScopedName.c_str(), localName, array, arraySize, type);
	switch (type) {
	case Configuration::CFG_LIST:
		break;
	case Configuration::CFG_NO_VALUE:
		arraySize = defaultArraySize;
		array = new const char *[arraySize];
		for (i = 0; i < arraySize; i++) {
			array[i] = defaultArray[i];
		}
		break;
	case Configuration::CFG_SCOPE:
		msg << fileName() << ": " << "'" << fullyScopedName
			<< "' is a scope instead of a list";
		throw ConfigurationException(msg.c_str());
	case Configuration::CFG_STRING:
		msg << fileName() << ": " << "'" << fullyScopedName
			<< "' is a string instead of a list";
		throw ConfigurationException(msg.c_str());
	default:
		assert(0);	// Bug
	}
}



void
ConfigurationImpl::lookupList(
	const char *			scope,
	const char *			localName,
	const char **&			array,
	int &					arraySize) const throw(ConfigurationException)
{
	Configuration::Type	 	type;
	StringBuffer			msg;
	StringBuffer			fullyScopedName;
	
	mergeNames(scope, localName, fullyScopedName);
	listValue(fullyScopedName.c_str(), localName, array, arraySize, type);
	switch (type) {
	case Configuration::CFG_LIST:
		break;
	case Configuration::CFG_NO_VALUE:
		msg << fileName() << ": no value specified for '" << fullyScopedName
			<< "'";
		throw ConfigurationException(msg.c_str());
	case Configuration::CFG_SCOPE:
		msg << fileName() << ": '" << fullyScopedName
			<< "' is a scope instead of a list";
		throw ConfigurationException(msg.c_str());
	case Configuration::CFG_STRING:
		msg << fileName() << ": " << "'" << fullyScopedName
			<< "' is a string instead of a list";
		throw ConfigurationException(msg.c_str());
	default:
		assert(0);	// Bug
	}
}



void
ConfigurationImpl::lookupList(
	const char *			scope,
	const char *			localName,
	StringVector &			list,
	const StringVector &	defaultList) const throw(ConfigurationException)
{
	Configuration::Type	 	type;
	StringBuffer			msg;
	int						i;
	StringBuffer			fullyScopedName;
	const char **			array;
	int						arraySize;
	
	mergeNames(scope, localName, fullyScopedName);
	listValue(fullyScopedName.c_str(), localName, array, arraySize, type);
	switch (type) {
	case Configuration::CFG_LIST:
		list.empty();
		for (i = 0; i < arraySize; i++) {
			list.add(array[i]);
		}
		break;
	case Configuration::CFG_NO_VALUE:
		list = defaultList;
		break;
	case Configuration::CFG_SCOPE:
		msg << fileName() << ": '" << fullyScopedName
			<< "' is a scope instead of a list";
		throw ConfigurationException(msg.c_str());
	case Configuration::CFG_STRING:
		msg << fileName() << ": '" << fullyScopedName
			<< "' is a string instead of a list";
		throw ConfigurationException(msg.c_str());
	default:
		assert(0);	// Bug
	}
}



void
ConfigurationImpl::lookupList(
	const char *			scope,
	const char *			localName,
	StringVector &			list) const throw(ConfigurationException)
{
	Configuration::Type	 	type;
	StringBuffer			msg;
	int						i;
	StringBuffer			fullyScopedName;
	const char **			array;
	int						arraySize;
	
	mergeNames(scope, localName, fullyScopedName);
	listValue(fullyScopedName.c_str(), localName, array, arraySize, type);
	switch (type) {
	case Configuration::CFG_LIST:
		list.empty();
		for (i = 0; i < arraySize; i++) {
			list.add(array[i]);
		}
		break;
	case Configuration::CFG_NO_VALUE:
		msg << fileName() << ": no value specified for '"
			<< fullyScopedName << "'";
		throw ConfigurationException(msg.c_str());
		break;
	case Configuration::CFG_SCOPE:
		msg << fileName() << ": '" << fullyScopedName
			<< "' is a scope instead of a list";
		throw ConfigurationException(msg.c_str());
	case Configuration::CFG_STRING:
		msg << fileName() << ": " << "'" << fullyScopedName
			<< "' is a string instead of a list";
		throw ConfigurationException(msg.c_str());
	default:
		assert(0);	// Bug
	}
}



int
ConfigurationImpl::lookupEnum(
	const char *				scope,
	const char *				localName,
	const char *				typeName,
	const EnumNameAndValue *	enumInfo,
	int 						numEnums,
	const char *				defaultVal) const throw(ConfigurationException)
{
	const char *				strValue;
	StringBuffer				msg;
	int							result;
	StringBuffer				fullyScopedName;
	int							i;
	
	strValue = lookupString(scope, localName, defaultVal);

	//--------
	// Check if the value matches anything in the enumInfo list.
	//--------
	if (!enumVal(strValue, enumInfo, numEnums, result)) {
		mergeNames(scope, localName, fullyScopedName);
		msg << fileName() << ": bad " << typeName << " value ('" << strValue
			<< "') specified for '" << fullyScopedName
			<< "'; should be one of:";
		for (i = 0; i < numEnums; i++) {
			if (i < numEnums-1) {
				msg << " '" << enumInfo[i].name << "',";
			} else {
				msg << " '" << enumInfo[i].name << "'";
			}
		}
		throw ConfigurationException(msg.c_str());
	}
	return result;
}



int
ConfigurationImpl::lookupEnum(
	const char *				scope,
	const char *				localName,
	const char *				typeName,
	const EnumNameAndValue *	enumInfo,
	int 						numEnums,
	int							defaultVal) const throw(ConfigurationException)
{
	const char *				strValue;
	StringBuffer				msg;
	int							result;
	int							i;
	StringBuffer				fullyScopedName;
	
	if (type(scope, localName) == Configuration::CFG_NO_VALUE) {
		return defaultVal;
	}

	strValue = lookupString(scope, localName);

	//--------
	// Check if the value matches anything in the enumInfo list.
	//--------
	if (!enumVal(strValue, enumInfo, numEnums, result)) {
		mergeNames(scope, localName, fullyScopedName);
		msg << fileName() << ": bad " << typeName << " value ('" << strValue
			<< "') specified for '" << fullyScopedName
			<< "'; should be one of:";
		for (i = 0; i < numEnums; i++) {
			if (i < numEnums-1) {
				msg << " '" << enumInfo[i].name << "',";
			} else {
				msg << " '" << enumInfo[i].name << "'";
			}
		}
		throw ConfigurationException(msg.c_str());
	}
	return result;
}



int
ConfigurationImpl::lookupEnum(
	const char *				scope,
	const char *				localName,
	const char *				typeName,
	const EnumNameAndValue *	enumInfo,
	int 						numEnums) const throw(ConfigurationException)
{
	const char *				strValue;
	StringBuffer				msg;
	int							result;
	StringBuffer				fullyScopedName;
	int							i;
	
	strValue = lookupString(scope, localName);

	//--------
	// Check if the value matches anything in the enumInfo list.
	//--------
	if (!enumVal(strValue, enumInfo, numEnums, result)) {
		mergeNames(scope, localName, fullyScopedName);
		msg << fileName() << ": bad " << typeName << " value ('" << strValue
			<< "') specified for '" << fullyScopedName
			<< "'; should be one of:";
		for (i = 0; i < numEnums; i++) {
			if (i < numEnums-1) {
				msg << " '" << enumInfo[i].name << "',";
			} else {
				msg << " '" << enumInfo[i].name << "'";
			}
		}
		throw ConfigurationException(msg.c_str());
	}
	return result;
}



//----------------------------------------------------------------------
// Function:	EnumVal()
//
// Description:	Compare "name"'s spelling against the list
//		of supplied enumerated (spelling, value) pairs. If
//		present in the list, return the correct value. In any
//		event, return success/failure.
//----------------------------------------------------------------------

bool
ConfigurationImpl::enumVal(
	const char *				name,
	const EnumNameAndValue *	enumInfo,
	int							numEnums,
	int &						val) const
{
	int							i;

	for (i = 0; i < numEnums; i++) {
		if (!strcmp(name, enumInfo[i].name)) {
			//--------
			// Found it.
			//--------
			val = enumInfo[i].value;
			return true;
		}
	}

	//--------
	// Failure.
	//--------
	return false;
}



static EnumNameAndValue boolInfo[] = {
	{ "false",	0 },
	{ "true",	1 }
};

int	countBoolInfo = sizeof(boolInfo)/sizeof(boolInfo[0]);


bool
ConfigurationImpl::lookupBoolean(
	const char *			scope,
	const char *			localName,
	bool					defaultVal) const throw(ConfigurationException)
{
	int						intVal;
	const char *			defaultStrVal;
	
	if (defaultVal) {
		defaultStrVal = "true";
	} else {
		defaultStrVal = "false";
	}
	intVal = lookupEnum(scope, localName, "boolean", boolInfo, countBoolInfo,
	                    defaultStrVal);
	return intVal != 0;
}



bool
ConfigurationImpl::lookupBoolean(
	const char *			scope,
	const char *			localName) const throw(ConfigurationException)
{
	int	intVal;

	intVal= lookupEnum(scope, localName, "boolean", boolInfo, countBoolInfo);
	return intVal != 0;
}



int
ConfigurationImpl::lookupInt(
	const char *		scope,
	const char *		localName,
	int					defaultVal) const throw(ConfigurationException)
{
	const char *		strValue;
	int					result;
	char				defaultStrVal[64]; // Big enough

	sprintf(defaultStrVal, "%d", defaultVal);
	strValue = lookupString(scope, localName, defaultStrVal);
	result = stringToInt(scope, localName, strValue);
	return result;
}



int
ConfigurationImpl::lookupInt(
	const char *			scope,
	const char *			localName) const throw(ConfigurationException)
{
	const char *			strValue;
	int						result;

	strValue = lookupString(scope, localName);
	result = stringToInt(scope, localName, strValue);
	return result;
}



bool
ConfigurationImpl::isInt(const char * str) const
{
	int				i;
	int				intValue;
	char			dummy;
	
	i = sscanf(str, "%d%c", &intValue, &dummy);
	return i == 1;
}



int
ConfigurationImpl::stringToInt(
	const char *		scope,
	const char *		localName,
	const char *		str) const throw(ConfigurationException)
{
	int					result;
	char				dummy;
	int					i;
	StringBuffer		msg;
	StringBuffer		fullyScopedName;
	
	//--------
	// Convert the string value into an int value.
	//--------
	i = sscanf(str, "%d%c", &result, &dummy);
	if (i != 1) {
		//--------
		// The number is badly formatted. Report an error.
		//--------
		mergeNames(scope, localName, fullyScopedName);
		msg << fileName() << ": non-integer value for '" << fullyScopedName
			<< "'";
		throw ConfigurationException(msg.c_str());
	}
	return result;
}



bool
ConfigurationImpl::isFloat(const char * str) const
{
	int				i;
	float			floatValue;
	char			dummy;
	
	i = sscanf(str, "%f%c", &floatValue, &dummy);
	return i == 1;
}



float
ConfigurationImpl::stringToFloat(
	const char *		scope,
	const char *		localName,
	const char *		str) const throw(ConfigurationException)
{
	float				result;
	char				dummy;
	int					i;
	StringBuffer		msg;
	StringBuffer		fullyScopedName;
	
	//--------
	// Convert the string value into a float value.
	//--------
	i = sscanf(str, "%f%c", &result, &dummy);
	if (i != 1) {
		//--------
		// The number is badly formatted. Report an error.
		//--------
		mergeNames(scope, localName, fullyScopedName);
		msg << fileName() << ": non-numeric value for '" << fullyScopedName
			<< "'";
		throw ConfigurationException(msg.c_str());
	}
	return result;
}



bool
ConfigurationImpl::isEnum(
	const char *				str,
	const EnumNameAndValue *	enumInfo,
	int 						numEnums) const
{
	int							dummyValue;
	bool						result;

	result = enumVal(str, enumInfo, numEnums, dummyValue);
	return result;
}



bool
ConfigurationImpl::isBoolean(const char * str) const
{
	int				dummyValue;
	bool			result;

	result = enumVal(str, boolInfo, countBoolInfo, dummyValue);
	return result;
}



int
ConfigurationImpl::stringToEnum(
	const char *				scope,
	const char *				localName,
	const char *				typeName,
	const char *				str,
	const EnumNameAndValue *	enumInfo,
	int 						numEnums) const throw(ConfigurationException)
{
	StringBuffer				msg;
	StringBuffer				fullyScopedName;
	int							result;
	int							i;
	
	//--------
	// Check if the value matches anything in the enumInfo list.
	//--------
	if (!enumVal(str, enumInfo, numEnums, result)) {
		mergeNames(scope, localName, fullyScopedName);
		msg << fileName() << ": bad " << typeName << " value specified for '"
			<< fullyScopedName << "'; should be one of:";
		for (i = 0; i < numEnums; i++) {
			if (i < numEnums-1) {
				msg << " '" << enumInfo[i].name << "',";
			} else {
				msg << " '" << enumInfo[i].name << "'";
			}
		}
		throw ConfigurationException(msg.c_str());
	}
	return result;
}



bool
ConfigurationImpl::stringToBoolean(
	const char *			scope,
	const char *			localName,
	const char *			str) const throw(ConfigurationException)
{
	int				result;

	result = stringToEnum(scope, localName, "boolean", str, boolInfo,
	                      countBoolInfo);
	return result != 0;
}



void
ConfigurationImpl::lookupFloatWithUnits(
	const char *		scope,
	const char *		localName,
	const char *		typeName,
	const char **		allowedUnits,
	int					allowedUnitsSize,
	float &				floatResult,
	const char *&		unitsResult) const throw(ConfigurationException)
{
	const char *		str;

	str = lookupString(scope, localName);
	stringToFloatWithUnits(scope, localName, typeName, str, allowedUnits,
			allowedUnitsSize, floatResult, unitsResult);
}



void
ConfigurationImpl::lookupFloatWithUnits(
	const char *		scope,
	const char *		localName,
	const char *		typeName,
	const char **		allowedUnits,
	int					allowedUnitsSize,
	float &				floatResult,
	const char *&		unitsResult,
	float				defaultFloat,
	const char *		defaultUnits) const throw(ConfigurationException)
{
	if (type(scope, localName) == CFG_NO_VALUE) {
		floatResult = defaultFloat;
		unitsResult = defaultUnits;
	} else {
		lookupFloatWithUnits(scope, localName, typeName, allowedUnits,
		                     allowedUnitsSize, floatResult, unitsResult,
		                     defaultFloat, defaultUnits);
	}
}



bool
ConfigurationImpl::isFloatWithUnits(
	const char *		str,
	const char **		allowedUnits,
	int					allowedUnitsSize) const
{
	char *				unitSpelling;
	int					i;
	float				fVal;

	//--------
	// See if it is in the form "<float> <units>"
	//--------
	unitSpelling = new char[strlen(str)+1]; // big enough
	i = sscanf(str, "%f %s", &fVal, unitSpelling);
	if (i != 2) {
		delete [] unitSpelling;
		return false;
	}

	//--------
	// The entry appears to be in the correct format. Find out
	// what the specified units are.
	//--------
	for (i = 0; i < allowedUnitsSize; i++) {
		if (strcmp(unitSpelling, allowedUnits[i]) == 0) {
			delete [] unitSpelling;
			return true;
		}
	}

	//--------
	// An unknown unit was specified.
	//--------
	delete [] unitSpelling;
	return false;
}



void
ConfigurationImpl::lookupUnitsWithFloat(
	const char *		scope,
	const char *		localName,
	const char *		typeName,
	const char **		allowedUnits,
	int					allowedUnitsSize,
	float &				floatResult,
	const char *&		unitsResult) const throw(ConfigurationException)
{
	const char *		str;

	str = lookupString(scope, localName);
	stringToUnitsWithFloat(scope, localName, typeName, str, allowedUnits,
			allowedUnitsSize, floatResult, unitsResult);
}



void
ConfigurationImpl::lookupUnitsWithFloat(
	const char *		scope,
	const char *		localName,
	const char *		typeName,
	const char **		allowedUnits,
	int					allowedUnitsSize,
	float &				floatResult,
	const char *&		unitsResult,
	float				defaultFloat,
	const char *		defaultUnits) const throw(ConfigurationException)
{
	if (type(scope, localName) == CFG_NO_VALUE) {
		floatResult = defaultFloat;
		unitsResult = defaultUnits;
	} else {
		lookupFloatWithUnits(scope, localName, typeName, allowedUnits,
		                     allowedUnitsSize, floatResult, unitsResult,
		                     defaultFloat, defaultUnits);
	}
}



bool
ConfigurationImpl::isUnitsWithFloat(
	const char *		str,
	const char **		allowedUnits,
	int					allowedUnitsSize) const
{
	char *				formatStr;
	char *				unitSpelling;
	char				dummyCh;
	int					i;
	int					index;
	int					maxUnitsLen;
	int					len;
	float				fVal;

	maxUnitsLen = 0;
	for (index = 0; index < allowedUnitsSize; index++) {
		len = strlen(allowedUnits[index]);
		if (len > maxUnitsLen) {
			maxUnitsLen = len;
		}
	}
	formatStr = new char[maxUnitsLen + 7]; // big enough
	unitSpelling = new char[strlen(str)+1]; // big enough

	//--------
	// See if the string is in the form "allowedUnits[index] <float>"
	//--------
	for (index = 0; index < allowedUnitsSize; index++) {
		sprintf(formatStr, "%s %%f%%c", allowedUnits[index]);
		i = sscanf(str, formatStr, &fVal, &dummyCh);
		if (i == 1) {
			delete [] formatStr;
			delete [] unitSpelling;
			return true;
		}
	}

	delete [] formatStr;
	delete [] unitSpelling;
	return false;
}



void
ConfigurationImpl::stringToIntWithUnits(
	const char *		scope,
	const char *		localName,
	const char *		typeName,
	const char *		str,
	const char **		allowedUnits,
	int					allowedUnitsSize,
	int &				intResult,
	const char *&		unitsResult) const throw(ConfigurationException)
{
	char *				unitSpelling;
	int					i;
	int					intVal;
	StringBuffer		msg;
	StringBuffer		fullyScopedName;

	//--------
	// See if the string is in the form "<int> <units>"
	//--------
	unitSpelling = new char[strlen(str)+1]; // big enough
	i = sscanf(str, "%d %s", &intVal, unitSpelling);
	if (i != 2) {
		delete [] unitSpelling;
		mergeNames(scope, localName, fullyScopedName);
		msg << fileName() << ": invalid " << typeName << " ('" << str
			<< "') specified for '" << fullyScopedName << "': should be"
			<< " '<int> <units>' where <units> are";
		for (i = 0; i < allowedUnitsSize; i++) {
			msg << " '" << allowedUnits[i] << "'";
			if (i < allowedUnitsSize-1) {
				msg << ",";
			}
		}
		throw ConfigurationException(msg.c_str());
	}

	//--------
	// The entry appears to be in the correct format. Find out
	// what the specified units are.
	//--------
	for (i = 0; i < allowedUnitsSize; i++) {
		if (strcmp(unitSpelling, allowedUnits[i]) == 0) {
			//--------
			// Success!
			//--------
			delete [] unitSpelling;
			intResult = intVal;
			unitsResult = allowedUnits[i];
			return;
		}
	}

	//--------
	// Error: an unknown unit was specified.
	//--------
	delete [] unitSpelling;
	mergeNames(scope, localName, fullyScopedName);
	msg << fileName() << ": invalid " << typeName << " ('" << str
	    << "') specified for '" << fullyScopedName << "': should be"
	    << " '<int> <units>' where <units> are";
	for (i = 0; i < allowedUnitsSize; i++) {
		msg << " '" << allowedUnits[i] << "'";
		if (i < allowedUnitsSize-1) {
			msg << ",";
		}
	}
	throw ConfigurationException(msg.c_str());
}



void
ConfigurationImpl::lookupIntWithUnits(
	const char *		scope,
	const char *		localName,
	const char *		typeName,
	const char **		allowedUnits,
	int					allowedUnitsSize,
	int &				intResult,
	const char *&		unitsResult) const throw(ConfigurationException)
{
	const char *		str;

	str = lookupString(scope, localName);
	stringToIntWithUnits(scope, localName, typeName, str, allowedUnits,
			allowedUnitsSize, intResult, unitsResult);
}



void
ConfigurationImpl::lookupIntWithUnits(
	const char *		scope,
	const char *		localName,
	const char *		typeName,
	const char **		allowedUnits,
	int					allowedUnitsSize,
	int &				intResult,
	const char *&		unitsResult,
	int					defaultInt,
	const char *		defaultUnits) const throw(ConfigurationException)
{
	if (type(scope, localName) == CFG_NO_VALUE) {
		intResult = defaultInt;
		unitsResult = defaultUnits;
	} else {
		lookupIntWithUnits(scope, localName, typeName, allowedUnits,
		                   allowedUnitsSize, intResult, unitsResult,
		                   defaultInt, defaultUnits);
	}
}



bool
ConfigurationImpl::isIntWithUnits(
	const char *		str,
	const char **		allowedUnits,
	int					allowedUnitsSize) const
{
	char *				unitSpelling;
	int					i;
	int					intVal;

	//--------
	// See if it is in the form "<int> <units>"
	//--------
	unitSpelling = new char[strlen(str)+1]; // big enough
	i = sscanf(str, "%d %s", &intVal, unitSpelling);
	if (i != 2) {
		delete [] unitSpelling;
		return false;
	}

	//--------
	// The entry appears to be in the correct format. Find out
	// what the specified units are.
	//--------
	for (i = 0; i < allowedUnitsSize; i++) {
		if (strcmp(unitSpelling, allowedUnits[i]) == 0) {
			delete [] unitSpelling;
			return true;
		}
	}

	//--------
	// An unknown unit was specified.
	//--------
	delete [] unitSpelling;
	return false;
}



void
ConfigurationImpl::stringToUnitsWithInt(
	const char *		scope,
	const char *		localName,
	const char *		typeName,
	const char *		str,
	const char **		allowedUnits,
	int					allowedUnitsSize,
	int &				intResult,
	const char *&		unitsResult) const throw(ConfigurationException)
{
	char *				formatStr;
	char *				unitSpelling;
	char				dummyCh;
	int					i;
	int					index;
	int					maxUnitsLen;
	int					len;
	int					intVal;
	StringBuffer		msg;
	StringBuffer		fullyScopedName;

	maxUnitsLen = 0;
	for (index = 0; index < allowedUnitsSize; index++) {
		len = strlen(allowedUnits[index]);
		if (len > maxUnitsLen) {
			maxUnitsLen = len;
		}
	}
	formatStr = new char[maxUnitsLen + 7]; // big enough
	unitSpelling = new char[strlen(str)+1]; // big enough

	//--------
	// See if the string is in the form "allowedUnits[index] <int>"
	//--------
	for (index = 0; index < allowedUnitsSize; index++) {
		sprintf(formatStr, "%s %%d%%c", allowedUnits[index]);
		i = sscanf(str, formatStr, &intVal, &dummyCh);
		if (i == 1) {
			unitsResult = allowedUnits[index];
			intResult = intVal;
			delete [] unitSpelling;
			delete [] formatStr;
			return;
		}
	}

	//--------
	// Incorrect format. Report an error.
	//--------
	delete [] unitSpelling;
	delete [] formatStr;
	mergeNames(scope, localName, fullyScopedName);
	msg << fileName() << ": invalid " << typeName << " ('" << str
		<< "') specified for '" << fullyScopedName << "': should be"
		<< " '<units> <int>' where <units> are";
	for (i = 0; i < allowedUnitsSize; i++) {
		msg << " '" << allowedUnits[i] << "'";
		if (i < allowedUnitsSize-1) {
			msg << ",";
		}
	}
	throw ConfigurationException(msg.c_str());
}



void
ConfigurationImpl::lookupUnitsWithInt(
	const char *		scope,
	const char *		localName,
	const char *		typeName,
	const char **		allowedUnits,
	int					allowedUnitsSize,
	int &				intResult,
	const char *&		unitsResult) const throw(ConfigurationException)
{
	const char *		str;

	str = lookupString(scope, localName);
	stringToUnitsWithInt(scope, localName, typeName, str, allowedUnits,
			allowedUnitsSize, intResult, unitsResult);
}



void
ConfigurationImpl::lookupUnitsWithInt(
	const char *		scope,
	const char *		localName,
	const char *		typeName,
	const char **		allowedUnits,
	int					allowedUnitsSize,
	int &				intResult,
	const char *&		unitsResult,
	int					defaultInt,
	const char *		defaultUnits) const throw(ConfigurationException)
{
	if (type(scope, localName) == CFG_NO_VALUE) {
		intResult = defaultInt;
		unitsResult = defaultUnits;
	} else {
		lookupIntWithUnits(scope, localName, typeName, allowedUnits,
		                   allowedUnitsSize, intResult, unitsResult,
		                   defaultInt, defaultUnits);
	}
}



bool
ConfigurationImpl::isUnitsWithInt(
	const char *		str,
	const char **		allowedUnits,
	int					allowedUnitsSize) const
{
	char *				formatStr;
	char *				unitSpelling;
	char				dummyCh;
	int					i;
	int					index;
	int					maxUnitsLen;
	int					len;
	int					intVal;

	maxUnitsLen = 0;
	for (index = 0; index < allowedUnitsSize; index++) {
		len = strlen(allowedUnits[index]);
		if (len > maxUnitsLen) {
			maxUnitsLen = len;
		}
	}
	formatStr = new char[maxUnitsLen + 7]; // big enough
	unitSpelling = new char[strlen(str)+1]; // big enough

	//--------
	// See if the string is in the form "allowedUnits[index] <int>"
	//--------
	for (index = 0; index < allowedUnitsSize; index++) {
		sprintf(formatStr, "%s %%d%%c", allowedUnits[index]);
		i = sscanf(str, formatStr, &intVal, &dummyCh);
		if (i == 1) {
			delete [] formatStr;
			delete [] unitSpelling;
			return true;
		}
	}

	delete [] formatStr;
	delete [] unitSpelling;
	return false;
}



static SpellingAndValue durationMicrosecondsUnitsInfo[] =
{
	{ "microsecond",	1 },
	{ "microseconds",	1 },
	{ "millisecond",	1000 },
	{ "milliseconds",	1000 },
	{ "second",			1000 * 1000 },
	{ "seconds",		1000 * 1000 },
	{ "minute",			1000 * 1000 * 60 },
	{ "minutes",		1000 * 1000 * 60 },
};
static const int countDurationMicrosecondsInfo =
		sizeof(durationMicrosecondsUnitsInfo)
		/ sizeof(durationMicrosecondsUnitsInfo[0]);

static const char * allowedDurationMicrosecondsUnits[] = {
	"microsecond",
	"microseconds",
	"millisecond",
	"milliseconds",
	"second",
	"seconds",
	"minute",
	"minutes",
};
static const int countAllowedDurationMicrosecondsUnits =
			sizeof(allowedDurationMicrosecondsUnits)
			/ sizeof(allowedDurationMicrosecondsUnits[0]);



static SpellingAndValue durationMillisecondsUnitsInfo[] =
{
	{ "millisecond",	1 },
	{ "milliseconds",	1 },
	{ "second",			1000 },
	{ "seconds",		1000 },
	{ "minute",			1000 * 60 },
	{ "minutes",		1000 * 60 },
	{ "hour",			1000 * 60 * 60 },
	{ "hours",			1000 * 60 * 60 },
	{ "day",			1000 * 60 * 60 * 24 },
	{ "days",			1000 * 60 * 60 * 24 },
	{ "week",			1000 * 60 * 60 * 24 * 7},
	{ "weeks",			1000 * 60 * 60 * 24 * 7},
};
static const int countDurationMillisecondsInfo =
		sizeof(durationMillisecondsUnitsInfo)
		/ sizeof(durationMillisecondsUnitsInfo[0]);

static const char * allowedDurationMillisecondsUnits[] = {
	"millisecond",
	"milliseconds",
	"second",
	"seconds",
	"minute",
	"minutes",
	"hour",
	"hours",
	"day",
	"days",
	"week",
	"weeks"
};
static const int countAllowedDurationMillisecondsUnits =
			sizeof(allowedDurationMillisecondsUnits)
			/ sizeof(allowedDurationMillisecondsUnits[0]);



static SpellingAndValue durationSecondsUnitsInfo[] =
{
	{ "second",		1 },
	{ "seconds",	1 },
	{ "minute",		60 },
	{ "minutes",	60 },
	{ "hour",		60 * 60 },
	{ "hours",		60 * 60 },
	{ "day",		60 * 60 * 24 },
	{ "days",		60 * 60 * 24 },
	{ "week",		60 * 60 * 24 * 7},
	{ "weeks",		60 * 60 * 24 * 7},
};
static const int countDurationSecondsInfo =
		sizeof(durationSecondsUnitsInfo)
		/ sizeof(durationSecondsUnitsInfo[0]);

static const char * allowedDurationSecondsUnits[] = {
	"second",
	"seconds",
	"minute",
	"minutes",
	"hour",
	"hours",
	"day",
	"days",
	"week",
	"weeks"
};
static const int countAllowedDurationSecondsUnits =
			sizeof(allowedDurationSecondsUnits)
			/ sizeof(allowedDurationSecondsUnits[0]);


bool
ConfigurationImpl::isDurationMicroseconds(const char * str) const
{
	if (!strcmp(str, "infinite"))  {
		return true;
	}
	return isFloatWithUnits(str, allowedDurationMicrosecondsUnits,
				countAllowedDurationMicrosecondsUnits);
}


bool
ConfigurationImpl::isDurationMilliseconds(const char * str) const
{
	if (!strcmp(str, "infinite"))  {
		return true;
	}
	return isFloatWithUnits(str, allowedDurationMillisecondsUnits,
				countAllowedDurationMillisecondsUnits);
}



bool
ConfigurationImpl::isDurationSeconds(const char * str) const
{
	if (!strcmp(str, "infinite"))  {
		return true;
	}
	return isFloatWithUnits(str, allowedDurationSecondsUnits,
				countAllowedDurationSecondsUnits);
}



static SpellingAndValue MemorySizeBytesUnitsInfo[] =
{
	{ "byte",		1 },
	{ "bytes",		1 },
	{ "KB",			1024 },
	{ "MB",			1024 * 1024 },
	{ "GB",			1024 * 1024 * 1024 },
};



static SpellingAndValue MemorySizeKBUnitsInfo[] =
{
	{ "KB",			1 },
	{ "MB",			1024 },
	{ "GB",			1024 * 1024 },
	{ "TB",			1024 * 1024 * 1024 },
};



static SpellingAndValue MemorySizeMBUnitsInfo[] =
{
	{ "MB",			1 },
	{ "GB",			1024 },
	{ "TB",			1024 * 1024 },
	{ "PB",			1024 * 1024 * 1024 },
};



bool
ConfigurationImpl::isMemorySizeBytes(const char * str) const
{
	static const char * allowedUnits[]= {"byte", "bytes", "KB", "MB", "GB"};
	return isFloatWithUnits(str, allowedUnits, 5);
}



bool
ConfigurationImpl::isMemorySizeKB(const char * str) const
{
	static const char * allowedUnits[] = {"KB", "MB", "GB", "TB"};
	return isFloatWithUnits(str, allowedUnits, 4);
}



bool
ConfigurationImpl::isMemorySizeMB(const char * str) const
{
	static const char * allowedUnits[] = {"MB", "GB", "TB", "PB"};
	return isFloatWithUnits(str, allowedUnits, 4);
}



void
ConfigurationImpl::stringToUnitsWithFloat(
	const char *		scope,
	const char *		localName,
	const char *		typeName,
	const char *		str,
	const char **		allowedUnits,
	int					allowedUnitsSize,
	float &				floatResult,
	const char *&		unitsResult) const throw(ConfigurationException)
{
	char *				formatStr;
	char *				unitSpelling;
	char				dummyCh;
	int					i;
	int					index;
	int					maxUnitsLen;
	int					len;
	float				fVal;
	StringBuffer		msg;
	StringBuffer		fullyScopedName;

	maxUnitsLen = 0;
	for (index = 0; index < allowedUnitsSize; index++) {
		len = strlen(allowedUnits[index]);
		if (len > maxUnitsLen) {
			maxUnitsLen = len;
		}
	}
	formatStr = new char[maxUnitsLen + 7]; // big enough
	unitSpelling = new char[strlen(str)+1]; // big enough

	//--------
	// See if the string is in the form "allowedUnits[index] <float>"
	//--------
	for (index = 0; index < allowedUnitsSize; index++) {
		sprintf(formatStr, "%s %%f%%c", allowedUnits[index]);
		i = sscanf(str, formatStr, &fVal, &dummyCh);
		if (i == 1) {
			unitsResult = allowedUnits[index];
			floatResult = fVal;
			delete [] formatStr;
			delete [] unitSpelling;
			return;
		}
	}

	//--------
	// Incorrect format. Report an error.
	//--------
	delete [] unitSpelling;
	delete [] formatStr;
	mergeNames(scope, localName, fullyScopedName);
	msg << fileName() << ": invalid " << typeName << " ('" << str
		<< "') specified for '" << fullyScopedName << "': should be"
		<< " '<units> <float>' where <units> are";
	for (i = 0; i < allowedUnitsSize; i++) {
		msg << " '" << allowedUnits[i] << "'";
		if (i < allowedUnitsSize-1) {
			msg << ",";
		}
	}
	throw ConfigurationException(msg.c_str());
}



void
ConfigurationImpl::stringToFloatWithUnits(
	const char *		scope,
	const char *		localName,
	const char *		typeName,
	const char *		str,
	const char **		allowedUnits,
	int					allowedUnitsSize,
	float &				floatResult,
	const char *&		unitsResult) const throw(ConfigurationException)
{
	char *				unitSpelling;
	int					i;
	float				fVal;
	StringBuffer		msg;
	StringBuffer		fullyScopedName;

	//--------
	// See if the string is in the form "<float> <units>"
	//--------
	unitSpelling = new char[strlen(str)+1]; // big enough
	i = sscanf(str, "%f %s", &fVal, unitSpelling);
	if (i != 2) {
		delete [] unitSpelling;
		mergeNames(scope, localName, fullyScopedName);
		msg << fileName() << ": invalid " << typeName << " ('" << str
			<< "') specified for '" << fullyScopedName << "': should be"
			<< " '<float> <units>' where <units> are";
		for (i = 0; i < allowedUnitsSize; i++) {
			msg << " '" << allowedUnits[i] << "'";
			if (i < allowedUnitsSize-1) {
				msg << ",";
			}
		}
		throw ConfigurationException(msg.c_str());
	}

	//--------
	// The entry appears to be in the correct format. Find out
	// what the specified units are.
	//--------
	for (i = 0; i < allowedUnitsSize; i++) {
		if (strcmp(unitSpelling, allowedUnits[i]) == 0) {
			//--------
			// Success!
			//--------
			delete [] unitSpelling;
			floatResult = fVal;
			unitsResult = allowedUnits[i];
			return;
		}
	}

	//--------
	// Error: an unknown unit was specified.
	//--------
	delete [] unitSpelling;
	mergeNames(scope, localName, fullyScopedName);
	msg << fileName() << ": invalid " << typeName << " ('" << str
		<< "') specified for '" << fullyScopedName << "': should be"
		<< " '<float> <units>' where <units> are";
	for (i = 0; i < allowedUnitsSize; i++) {
		msg << " '" << allowedUnits[i] << "'";
		if (i < allowedUnitsSize-1) {
			msg << ",";
		}
	}
	throw ConfigurationException(msg.c_str());
}


int
ConfigurationImpl::stringToDurationMicroseconds(
	const char *		scope,
	const char *		localName,
	const char *		str) const throw(ConfigurationException)
{
	float				floatVal;
	const char *		units;
	StringBuffer		msg;
	int					i;
	int					result;
	int					unitsVal;

	//--------
	// Is the duration "infinite"?
	//--------
	if (!strcmp(str, "infinite"))  {
		return -1;
	}

	//--------
	// Use stringToFloatWithUnits()
	//--------
	try {
		stringToFloatWithUnits(scope, localName, "durationMicroseconds", str,
							   allowedDurationMicrosecondsUnits,
							   countAllowedDurationMicrosecondsUnits,
							   floatVal, units);
	} catch (const ConfigurationException & ex) {
		msg = ex.c_str();
		msg << "; alternatively, you can use 'infinite'";
		throw ConfigurationException(msg.c_str());
	}
	assert(countDurationMicrosecondsInfo
			== countAllowedDurationMicrosecondsUnits);
	result = -1; // avoid compiler warning about an unitialized variable
	for (i = 0; i < countDurationMicrosecondsInfo; i++) {
		if (strcmp(durationMicrosecondsUnitsInfo[i].spelling, units)==0) {
			unitsVal = durationMicrosecondsUnitsInfo[i].val;
			result = (int)(floatVal * unitsVal);
			break;
		}
	}
	assert(i < countDurationMicrosecondsInfo);
	return result;
}


int
ConfigurationImpl::stringToDurationMilliseconds(
	const char *		scope,
	const char *		localName,
	const char *		str) const throw(ConfigurationException)
{
	float				floatVal;
	const char *		units;
	StringBuffer		msg;
	int					i;
	int					result;
	int					unitsVal;

	//--------
	// Is the duration "infinite"?
	//--------
	if (!strcmp(str, "infinite"))  {
		return -1;
	}

	//--------
	// Use stringToFloatWithUnits()
	//--------
	try {
		stringToFloatWithUnits(scope, localName, "durationMilliseconds", str,
							   allowedDurationMillisecondsUnits,
							   countAllowedDurationMillisecondsUnits,
							   floatVal, units);
	} catch (const ConfigurationException & ex) {
		msg = ex.c_str();
		msg << "; alternatively, you can use 'infinite'";
		throw ConfigurationException(msg.c_str());
	}
	assert(countDurationMillisecondsInfo
			== countAllowedDurationMillisecondsUnits);
	result = -1; // avoid compiler warning about an unitialized variable
	for (i = 0; i < countDurationMillisecondsInfo; i++) {
		if (strcmp(durationMillisecondsUnitsInfo[i].spelling, units)==0) {
			unitsVal = durationMillisecondsUnitsInfo[i].val;
			result = (int)(floatVal * unitsVal);
			break;
		}
	}
	assert(i < countDurationMillisecondsInfo);
	return result;
}



int
ConfigurationImpl::stringToDurationSeconds(
	const char *		scope,
	const char *		localName,
	const char *		str) const throw(ConfigurationException)
{
	float				floatVal;
	const char *		units;
	StringBuffer		msg;
	int					i;
	int					result;
	int					unitsVal;

	//--------
	// Is the duration "infinite"?
	//--------
	if (!strcmp(str, "infinite"))  {
		return -1;
	}

	//--------
	// Use stringToFloatWithUnits()
	//--------
	try {
		stringToFloatWithUnits(scope, localName, "durationSeconds", str,
							   allowedDurationSecondsUnits,
							   countAllowedDurationSecondsUnits, floatVal,
							   units);
	} catch (const ConfigurationException & ex) {
		msg = ex.c_str();
		msg << "; alternatively, you can use 'infinite'";
		throw ConfigurationException(msg.c_str());
	}
	assert(countDurationSecondsInfo == countAllowedDurationSecondsUnits);
	result = -1; // avoid compiler warning about an unitialized variable
	for (i = 0; i < countDurationSecondsInfo; i++) {
		if (strcmp(durationSecondsUnitsInfo[i].spelling, units) == 0) {
			unitsVal = durationSecondsUnitsInfo[i].val;
			result = (int)(floatVal * unitsVal);
			break;
		}
	}
	assert(i < countDurationSecondsInfo);
	return result;
}



int
ConfigurationImpl::lookupDurationMicroseconds(
	const char *		scope,
	const char *		localName,
	int					defaultVal) const throw(ConfigurationException)
{
	char				defaultStrValue[128]; // big enough
	const char *		strValue;
	int					result;

	if (defaultVal == -1) {
		sprintf(defaultStrValue, "infinite");
	} else {
		sprintf(defaultStrValue, "%d microseconds", defaultVal);
	}
	strValue = lookupString(scope, localName, defaultStrValue);
	result = stringToDurationMicroseconds(scope, localName, strValue);
	return result;
}



int
ConfigurationImpl::lookupDurationMicroseconds(
	const char *		scope,
	const char *		localName) const throw(ConfigurationException)
{
	const char *		strValue;
	int					result;

	strValue = lookupString(scope, localName);
	result = stringToDurationMicroseconds(scope, localName, strValue);
	return result;
}



int
ConfigurationImpl::lookupDurationMilliseconds(
	const char *		scope,
	const char *		localName,
	int					defaultVal) const throw(ConfigurationException)
{
	char				defaultStrValue[128]; // big enough
	const char *		strValue;
	int					result;

	if (defaultVal == -1) {
		sprintf(defaultStrValue, "infinite");
	} else {
		sprintf(defaultStrValue, "%d milliseconds", defaultVal);
	}
	strValue = lookupString(scope, localName, defaultStrValue);
	result = stringToDurationMilliseconds(scope, localName, strValue);
	return result;
}



int
ConfigurationImpl::lookupDurationMilliseconds(
	const char *		scope,
	const char *		localName) const throw(ConfigurationException)
{
	const char *		strValue;
	int					result;

	strValue = lookupString(scope, localName);
	result = stringToDurationMilliseconds(scope, localName, strValue);
	return result;
}



int
ConfigurationImpl::lookupDurationSeconds(
	const char *		scope,
	const char *		localName,
	int					defaultVal) const throw(ConfigurationException)
{
	char				defaultStrValue[128]; // big enough
	const char *		strValue;
	int					result;

	if (defaultVal == -1) {
		sprintf(defaultStrValue, "infinite");
	} else {
		sprintf(defaultStrValue, "%d seconds", defaultVal);
	}
	strValue = lookupString(scope, localName, defaultStrValue);
	result = stringToDurationSeconds(scope, localName, strValue);
	return result;
}



int
ConfigurationImpl::lookupDurationSeconds(
	const char *		scope,
	const char *		localName) const throw(ConfigurationException)
{
	const char *		strValue;
	int					result;

	strValue = lookupString(scope, localName);
	result = stringToDurationSeconds(scope, localName, strValue);
	return result;
}



int
ConfigurationImpl::stringToMemorySizeGeneric(
	const char *			typeName,
	SpellingAndValue		unitsInfo[],
	int						unitsInfoSize,
	const char *			allowedUnits[],
	const char *			scope,
	const char *			localName,
	const char *			str) const throw(ConfigurationException)
{
	float					floatVal;
	const char *			units;
	StringBuffer			msg;
	int						i;
	int						result;
	int						unitsVal;

	stringToFloatWithUnits(scope, localName, typeName, str, allowedUnits,
						   unitsInfoSize, floatVal, units);
	result = -1; // avoid compiler warning about an unitialized variable
	for (i = 0; i < unitsInfoSize; i++) {
		if (strcmp(unitsInfo[i].spelling, units) == 0) {
			unitsVal = unitsInfo[i].val;
			result = (int)(floatVal * unitsVal);
			break;
		}
	}
	assert(i < unitsInfoSize);
	return result;
}



int
ConfigurationImpl::stringToMemorySizeBytes(
	const char *		scope,
	const char *		localName,
	const char *		str) const throw(ConfigurationException)
{
	static const char * allowedUnits[]= {"byte", "bytes", "KB", "MB", "GB"};
	return stringToMemorySizeGeneric("memorySizeBytes",
									 MemorySizeBytesUnitsInfo, 5, allowedUnits,
									 scope, localName, str);
}



int
ConfigurationImpl::stringToMemorySizeKB(
	const char *		scope,
	const char *		localName,
	const char *		str) const throw(ConfigurationException)
{
	static const char * allowedUnits[]= {"KB", "MB", "GB", "TB"};
	return stringToMemorySizeGeneric("memorySizeKB", MemorySizeKBUnitsInfo, 4,
									 allowedUnits, scope, localName, str);
}



int
ConfigurationImpl::stringToMemorySizeMB(
	const char *		scope,
	const char *		localName,
	const char *		str) const throw(ConfigurationException)
{
	static const char * allowedUnits[]= {"MB", "GB", "TB", "PB"};
	return stringToMemorySizeGeneric("memorySizeMB", MemorySizeMBUnitsInfo, 4,
								     allowedUnits, scope, localName, str);
}



int
ConfigurationImpl::lookupMemorySizeBytes(
	const char *		scope,
	const char *		localName,
	int					defaultVal) const throw(ConfigurationException)
{
	char				defaultStrValue[64]; // big enough
	const char *		strValue;
	int					result;

	sprintf(defaultStrValue, "%d milliseconds", defaultVal);
	strValue = lookupString(scope, localName, defaultStrValue);
	result = stringToMemorySizeBytes(scope, localName, strValue);
	return result;
}



int
ConfigurationImpl::lookupMemorySizeBytes(
	const char *		scope,
	const char *		localName) const throw(ConfigurationException)
{
	const char *		strValue;
	int					result;

	strValue = lookupString(scope, localName);
	result = stringToMemorySizeBytes(scope, localName, strValue);
	return result;
}



int
ConfigurationImpl::lookupMemorySizeKB(
	const char *		scope,
	const char *		localName,
	int					defaultVal) const throw(ConfigurationException)
{
	char				defaultStrValue[64]; // big enough
	const char *		strValue;
	int					result;

	sprintf(defaultStrValue, "%d KB", defaultVal);
	strValue = lookupString(scope, localName, defaultStrValue);
	result = stringToMemorySizeKB(scope, localName, strValue);
	return result;
}



int
ConfigurationImpl::lookupMemorySizeKB(
	const char *		scope,
	const char *		localName) const throw(ConfigurationException)
{
	const char *		strValue;
	int					result;

	strValue = lookupString(scope, localName);
	result = stringToMemorySizeKB(scope, localName, strValue);
	return result;
}



int
ConfigurationImpl::lookupMemorySizeMB(
	const char *		scope,
	const char *		localName,
	int					defaultVal) const throw(ConfigurationException)
{
	char				defaultStrValue[64]; // big enough
	const char *		strValue;
	int					result;

	sprintf(defaultStrValue, "%d MB", defaultVal);
	strValue = lookupString(scope, localName, defaultStrValue);
	result = stringToMemorySizeMB(scope, localName, strValue);
	return result;
}



int
ConfigurationImpl::lookupMemorySizeMB(
	const char *		scope,
	const char *		localName) const throw(ConfigurationException)
{
	const char *		strValue;
	int					result;

	strValue = lookupString(scope, localName);
	result = stringToMemorySizeMB(scope, localName, strValue);
	return result;
}



float
ConfigurationImpl::lookupFloat(
	const char *		scope,
	const char *		localName,
	float				defaultVal) const throw(ConfigurationException)
{
	const char *		strValue;
	float				result;
	char				defaultStrVal[64]; // Big enough

	sprintf(defaultStrVal, "%f", defaultVal);
	strValue = lookupString(scope, localName, defaultStrVal);
	result = stringToFloat(scope, localName, strValue);
	return result;
}



float
ConfigurationImpl::lookupFloat(
	const char *		scope,
	const char *		localName) const throw(ConfigurationException)
{
	const char *		strValue;
	float				result;

	strValue = lookupString(scope, localName);
	result = stringToFloat(scope, localName, strValue);
	return result;
}



void
ConfigurationImpl::lookupScope(
	const char *		scope,
	const char *		localName) const throw(ConfigurationException)
{
	StringBuffer		msg;
	StringBuffer		fullyScopedName;
	
	mergeNames(scope, localName, fullyScopedName);
	switch (type(scope, localName)) {
	case Configuration::CFG_SCOPE:
		// Okay
		break;
	case Configuration::CFG_STRING:
		msg << fileName() << ": '" << fullyScopedName
			<< "' is a string instead of a scope";
		throw ConfigurationException(msg.c_str());
	case Configuration::CFG_LIST:
		msg << fileName() << ": '" << fullyScopedName
			<< "' is a list instead of a scope";
		throw ConfigurationException(msg.c_str());
	case Configuration::CFG_NO_VALUE:
		msg << fileName() << ": scope '" << fullyScopedName
			<< "' does not exist";
		throw ConfigurationException(msg.c_str());
	default:
		assert(0);	// Bug
	}
}



void
ConfigurationImpl::pushIncludedFilename(const char * fileName)
{
	m_fileNameStack.add(fileName);
}



void
ConfigurationImpl::popIncludedFilename(const char * fileName)
{
	int	size;

	size = m_fileNameStack.length();
	assert(size > 0);
	assert (strcmp(m_fileNameStack[size-1], fileName) == 0);
	m_fileNameStack.removeLast();
}



void
ConfigurationImpl::checkForCircularIncludes(
	const char *	file,
	int				includeLineNum) throw (ConfigurationException)
{
	int				size;
	int				i;
	StringBuffer	msg;

	size = m_fileNameStack.length();
	for (i = 0; i < size; i++) {
		if (strcmp(m_fileNameStack[i], file) == 0) {
			msg << fileName() << ": line " << includeLineNum
				<< ", circular include of '" << file << "'";
			throw ConfigurationException(msg.c_str());
		}
	}
}



bool
ConfigurationImpl::uidEquals(const char * s1, const char * s2) const
{
	const char *		us1;
	const char *		us2;
	StringBuffer		buf1;
	StringBuffer		buf2;
	
	us1 = m_uidIdentifierProcessor.unexpand(s1, buf1);
	us2 = m_uidIdentifierProcessor.unexpand(s2, buf2);
	return strcmp(us1, us2) == 0;
}



void
ConfigurationImpl::expandUid(StringBuffer & spelling)
												throw(ConfigurationException)
{
	m_uidIdentifierProcessor.expand(spelling);
}



const char *
ConfigurationImpl::unexpandUid(const char * spelling, StringBuffer & buf) const
{
	return m_uidIdentifierProcessor.unexpand(spelling, buf);
}



void
ConfigurationImpl::ensureScopeExists(
	const char *		name,
	ConfigScope *&		scope) throw(ConfigurationException)
{
	StringVector		vec;

	splitScopedNameIntoVector(name, vec);
	ensureScopeExists(vec, 0, vec.length()-1, scope);
}



void
ConfigurationImpl::ensureScopeExists(
	const StringVector &	vec,
	int						firstIndex,
	int						lastIndex,
	ConfigScope *&			scope) throw(ConfigurationException)
{
	int						i;
	int						j;
	StringBuffer			msg;

	scope = m_currScope;
	for (i = firstIndex; i <= lastIndex; i++) {
		if (!scope->ensureScopeExists(vec[i], scope)) {
			msg << fileName() << ": " << "scope '";
			for (j = firstIndex; j <= i; j++) {
				msg << vec[j];
				if (j < i) {
					msg << ".";
				}
			}
			msg << "' was previously used as a variable name";
			throw ConfigurationException(msg.c_str());
		}
	}
}



bool
ConfigurationImpl::isExecAllowed(
	const char *			cmdLine,
	StringBuffer &			trustedCmdLine)
{
	StringVector			allowPatterns;
	StringVector			denyPatterns;
	StringVector			trustedDirs;
	StringBuffer			cmd;
	const char *			ptr;
	const char *			scope;
	int						i;
	int						j;
	int						len;

	if (this == &DefaultSecurityConfiguration::singleton || m_securityCfg== 0) {
		return false;
	}
	scope = m_securityCfgScope.c_str();

	m_securityCfg->lookupList(scope, "allow_patterns", allowPatterns);
	m_securityCfg->lookupList(scope, "deny_patterns", denyPatterns);
	m_securityCfg->lookupList(scope, "trusted_directories", trustedDirs);

	//--------
	// Check if there is any rule to deny execution.
	//--------
	len = denyPatterns.length();
	for (i = 0; i < len; i++) {
		if (patternMatch(cmdLine, denyPatterns[i])) {
			return false;
		}
	}

	//--------
	// Check if there is any rule to allow execution *and* the
	// command can be found in trusted_directories.
	//--------
	len = allowPatterns.length();
	for (i = 0; i < len; i++) {
		if (!patternMatch(cmdLine, allowPatterns[i])) {
			continue;
		}
		//--------
		// Found cmdLine in allow_patterns. Now extract the
		// first word from cmdLine to get the actual command.
		//--------
		cmd = "";
		ptr = cmdLine;
		while (*ptr != '\0' && !isspace(*ptr)) {
			cmd.append(*ptr);
			ptr ++;
		}

		//--------
		// Check if cmd resides in a directory in
		// trusted_directories.
		//--------
		for (j = 0; j < trustedDirs.length(); j++) {
			if (isCmdInDir(cmd.c_str(), trustedDirs[j])) {
				trustedCmdLine = "";
				trustedCmdLine << trustedDirs[j]
				               << CONFIG4CPP_DIR_SEP
				               << cmd
					       << &cmdLine[strlen(cmd.c_str())];
				return true;
			}
		}
	}
	return false;
}

}; // namespace CONFIG4CPP_NAMESPACE
