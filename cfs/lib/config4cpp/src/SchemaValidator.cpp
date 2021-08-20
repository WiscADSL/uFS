//----------------------------------------------------------------------
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
#include <config4cpp/SchemaValidator.h>
#include "SchemaParser.h"
#include "SchemaRuleInfo.h"
#include "SchemaTypeBoolean.h"
#include "SchemaTypeDurationMicroseconds.h"
#include "SchemaTypeDurationMilliseconds.h"
#include "SchemaTypeDurationSeconds.h"
#include "SchemaTypeDummy.h"
#include "SchemaTypeEnum.h"
#include "SchemaTypeFloat.h"
#include "SchemaTypeFloatWithUnits.h"
#include "SchemaTypeInt.h"
#include "SchemaTypeIntWithUnits.h"
#include "SchemaTypeList.h"
#include "SchemaTypeMemorySizeBytes.h"
#include "SchemaTypeMemorySizeKB.h"
#include "SchemaTypeMemorySizeMB.h"
#include "SchemaTypeScope.h"
#include "SchemaTypeString.h"
#include "SchemaTypeTable.h"
#include "SchemaTypeTuple.h"
#include "SchemaTypeTypedef.h"
#include "SchemaTypeUnitsWithFloat.h"
#include "SchemaTypeUnitsWithInt.h"
#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include <assert.h>


namespace CONFIG4CPP_NAMESPACE {

int
compareSchemaIdRuleInfo(const void * p1, const void * p2)
{
	SchemaIdRuleInfo **		r1;
	SchemaIdRuleInfo **		r2;
	int						result;

	r1 = (SchemaIdRuleInfo **)p1;
	r2 = (SchemaIdRuleInfo **)p2;
	result = strcmp((*r1)->m_locallyScopedName.c_str(),
					(*r2)->m_locallyScopedName.c_str());
	return result;
}



extern "C" int
CONFIG4CPP_C_PREFIX(compareSchemaIdRuleInfo_c)(
	const void *			p1,
	const void *			p2)
{
	return compareSchemaIdRuleInfo(p1, p2);
}



int
compareSchemaType(const void * p1, const void * p2)
{
	SchemaType **		r1;
	SchemaType **		r2;
	int					result;

	r1 = (SchemaType **)p1;
	r2 = (SchemaType **)p2;
	result = strcmp((*r1)->typeName(), (*r2)->typeName());
	return result;
}



extern "C" int
CONFIG4CPP_C_PREFIX(compareSchemaType_c)(const void * p1, const void * p2)
{
	return compareSchemaType(p1, p2);
}



SchemaIdRuleInfo *
SchemaValidator::findIdRule(const char * name) const
{
	SchemaIdRuleInfo		search;
	SchemaIdRuleInfo *		searchPtr;
	SchemaIdRuleInfo **		result;

	search.m_locallyScopedName = name;
	searchPtr = &search;
	result = (SchemaIdRuleInfo **)
		bsearch(&searchPtr, m_idRules, m_idRulesCurrSize,
				sizeof(SchemaIdRuleInfo*),
				CONFIG4CPP_C_PREFIX(compareSchemaIdRuleInfo_c));
	if (result == 0) {
		return 0;
	}
	return *result;
}



void
SchemaValidator::sortTypes()
{
	qsort(m_types, m_typesCurrSize, sizeof(SchemaType*),
		  CONFIG4CPP_C_PREFIX(compareSchemaType_c));
	m_areTypesSorted = true;
}



SchemaType *
SchemaValidator::findType(const char * name) const
{
	SchemaTypeDummy		search(name);
	SchemaType *		searchPtr;
	SchemaType **		result;
	int					i;
	SchemaType *		typeDef;

	if (m_areTypesSorted) {
		searchPtr = &search;
		result = (SchemaType **)
			bsearch(&searchPtr, m_types, m_typesCurrSize, sizeof(SchemaType*),
					CONFIG4CPP_C_PREFIX(compareSchemaType_c));
		if (result == 0) {
			return 0;
		}
		assert(*result != 0);
		return *result;
	} else {
		for (i = 0; i < m_typesCurrSize; i++) {
			typeDef = m_types[i];
			if (strcmp(typeDef->typeName(), name) == 0) {
				assert(typeDef != 0);
				return typeDef;
			}
		}
	}
	return 0;
}



//----------------------------------------------------------------------
// Function:	Constructor
//
// Description:
//----------------------------------------------------------------------

SchemaValidator::SchemaValidator()
{
	try {
		m_wantDiagnostics     = false;
		m_idRulesCurrSize     = 0;
		m_idRulesMaxSize      = 0;
		m_idRules             = 0;
		m_ignoreRulesCurrSize = 0;
		m_ignoreRulesMaxSize  = 0;
		m_ignoreRules         = 0;
		m_typesMaxSize        = 25; // can grow bigger, if necessary
		m_types               = new SchemaType*[m_typesMaxSize];
		m_typesCurrSize       = 0;
		m_areTypesSorted      = false;
		registerBuiltinTypes();
	} catch(const ConfigurationException &) {
		assert(0); // Bug!
	}
}



//----------------------------------------------------------------------
// Function:	Destructor
//
// Description:
//----------------------------------------------------------------------

SchemaValidator::~SchemaValidator()
{
	int				i;

	for (i = 0; i < m_idRulesCurrSize; i++) {
		delete m_idRules[i];
	}
	delete [] m_idRules;

	for (i = 0; i < m_ignoreRulesCurrSize; i++) {
		delete m_ignoreRules[i];
	}
	delete [] m_ignoreRules;

	for (i = 0; i < m_typesCurrSize; i++) {
		delete m_types[i];
	}

	delete [] m_types;
}



void
SchemaValidator::registerBuiltinTypes()
{
	registerType(new SchemaTypeScope());

	//--------
	// List-based types
	//--------
	registerType(new SchemaTypeList());
	registerType(new SchemaTypeTable());
	registerType(new SchemaTypeTuple());

	//--------
	// String-based types
	//--------
	registerType(new SchemaTypeString());
	registerType(new SchemaTypeBoolean());
	registerType(new SchemaTypeDurationMicroseconds());
	registerType(new SchemaTypeDurationMilliseconds());
	registerType(new SchemaTypeDurationSeconds());
	registerType(new SchemaTypeEnum());
	registerType(new SchemaTypeFloat());
	registerType(new SchemaTypeFloatWithUnits());
	registerType(new SchemaTypeInt());
	registerType(new SchemaTypeIntWithUnits());
	registerType(new SchemaTypeUnitsWithFloat());
	registerType(new SchemaTypeUnitsWithInt());
	registerType(new SchemaTypeMemorySizeBytes());
	registerType(new SchemaTypeMemorySizeKB());
	registerType(new SchemaTypeMemorySizeMB());
}



void
SchemaValidator::registerType(SchemaType * type) throw(ConfigurationException)
{
	checkTypeDoesNotExist(type->typeName());
	ensureSpaceInTypesArray();
	m_types[m_typesCurrSize] = type;
	m_typesCurrSize++;
	m_areTypesSorted = false;
}



void
SchemaValidator::registerTypedef(
	const char *				typeName,
	Configuration::Type			cfgType,
	const char *				baseTypeName,
	const StringVector &		baseTypeArgs) throw(ConfigurationException)
{
	checkTypeDoesNotExist(typeName);
	ensureSpaceInTypesArray();
	m_types[m_typesCurrSize]
		= new SchemaTypeTypedef(typeName, cfgType, baseTypeName, baseTypeArgs);
	m_typesCurrSize++;
	m_areTypesSorted = false;
}



void
SchemaValidator::checkTypeDoesNotExist(const char * typeName)
{
	StringBuffer		msg;
	int					i;

	for (i = 0; i < m_typesCurrSize; i++) {
		if (strcmp(m_types[i]->typeName(), typeName) ==0) {
			msg << "schema type '" << typeName << "' is already registed";
			throw ConfigurationException(msg.c_str());
		}
	}
}



void
SchemaValidator::ensureSpaceInTypesArray()
{
	SchemaType **		newArray;
	int					i;

	if (m_typesCurrSize == m_typesMaxSize) {
		m_typesMaxSize = m_typesMaxSize * 2;
		newArray = new SchemaType*[m_typesMaxSize];
		for (i = 0; i < m_typesCurrSize; i++) {
			newArray[i] = m_types[i];
		}
		delete [] m_types;
		m_types = newArray;
	}
}



void
SchemaValidator::parseSchema(const char ** nullTerminatedRulesArray)
												throw(ConfigurationException)
{
	int				size;

	for (size = 0; nullTerminatedRulesArray[size] != 0; size++) {
	}
	parseSchema(nullTerminatedRulesArray, size);
}



void
SchemaValidator::parseSchema(
	const char **		schema,
	int					schemaSize) throw(ConfigurationException)
{
	SchemaParser		schemaParser(this);
	const char *		prefix = "---- " CONFIG4CPP_NAMESPACE_STR
								 "::SchemaValidator::parseSchema()";

	if (m_wantDiagnostics) {
		printf("\n%s: start\n", prefix);
	}
	try {
		schemaParser.parse(schema, schemaSize);
	} catch(const ConfigurationException & ex) {
		if (m_wantDiagnostics) {
			printf("\n%s: error: %s\n\n", prefix, ex.c_str());
		}
		throw;
	}
	if (m_wantDiagnostics) {
		printf("\n%s: end\n\n", prefix);
	}
}



void
SchemaValidator::validate(
	const Configuration *		cfg,
	const char *				scope,
	const char *				localName,
	bool						recurseIntoSubscopes,
	Configuration::Type			typeMask,
	ForceMode					forceMode) const
												throw(ConfigurationException)
{
	StringBuffer				fullyScopedName;
	StringVector				itemNames;

	//--------
	// Get a list of the entries in the scope.
	//--------
	cfg->mergeNames(scope, localName, fullyScopedName);
	cfg->listLocallyScopedNames(scope, localName, typeMask,
								recurseIntoSubscopes, itemNames);

	//--------
	// Now validte those names
	//--------
	validate(cfg, scope, localName, itemNames, forceMode);
}



void
SchemaValidator::validate(
	const Configuration *	cfg,
	const char *			scope,
	const char *			localName,
	const StringVector &	itemNames,
	ForceMode				forceMode) const throw(ConfigurationException)
{
	StringBuffer			fullyScopedName;
	StringBuffer			unlistedName;
	StringBuffer			msg;
	StringBuffer			buf;
	const char *			unexpandedName;
	const char *			typeName;
	const char *			iName;
	int						i;
	int						len;
	SchemaIdRuleInfo *		idRule;
	SchemaType *			typeDef;
	const char *			prefix = "---- " CONFIG4CPP_NAMESPACE_STR
									 "::SchemaValidator::validate()";

	cfg->mergeNames(scope, localName, fullyScopedName);

	if (m_wantDiagnostics) {
		printf("\n%s: start\n", prefix);
	}
	//--------
	// Compare every name in itemNames with m_ignoreRules and m_idRules.
	//--------
	len = itemNames.length();
	for (i = 0; i < len; i++) {
		iName = itemNames[i];
		unexpandedName = cfg->unexpandUid(iName, buf);
		if (shouldIgnore(cfg, scope, iName, unexpandedName)) {
			if (m_wantDiagnostics) {
				printf("\n  ignoring '%s'\n", iName);
			}
			continue;
		}
		idRule = findIdRule(unexpandedName);
		if (idRule == 0) {
			//--------
			// Can't find an idRule for the entry
			//--------
			cfg->mergeNames(fullyScopedName.c_str(),
						iName, unlistedName);
			switch (cfg->type(unlistedName.c_str(), "")) {
			case Configuration::CFG_SCOPE:
				msg << cfg->fileName() << ": " << "the '" << unlistedName
					<< "' scope is unknown.";
				break;
			case Configuration::CFG_LIST:
			case Configuration::CFG_STRING:
				msg << cfg->fileName() << ": " << "the '" << unlistedName
					<< "' variable is unknown.";
				break;
			default:
				assert(0); // Bug!
			}
			if (m_wantDiagnostics) {
				printf("\n%s: error: %s\n",
					prefix, msg.c_str());
			}
			throw ConfigurationException(msg.c_str());
		}

		//--------
		// There is an idRule for the entry. Look up the idRule's
		// type, and invoke its validate() operation.
		//--------
		typeName = idRule->m_typeName.c_str();
		typeDef = findType(typeName);
		assert(typeDef != 0);
		try { 
			callValidate(typeDef, cfg, fullyScopedName.c_str(), iName,
						 typeName, typeName, idRule->m_args, 1);
		} catch (const ConfigurationException & ex) {
			if (m_wantDiagnostics) {
				printf("\n%s: end\n\n", prefix);
			}
			throw;
		}
	}

	validateForceMode(cfg, scope, localName, forceMode);

	if (m_wantDiagnostics) {
		printf("\n%s: end\n\n", prefix);
	}
}



void
SchemaValidator::validateForceMode(
	const Configuration *	cfg,
	const char *			scope,
	const char *			localName,
	ForceMode				forceMode) const throw(ConfigurationException)
{
	int						i;
	bool					isOptional;
	StringBuffer			fullyScopedName;
	StringBuffer			nameOfMissingEntry;
	StringBuffer			msg;
	const char *			typeName;
	const char *			nameInRule;
	SchemaIdRuleInfo *		idRule;
	

	if (forceMode == FORCE_OPTIONAL) {
		return;
	}
	cfg->mergeNames(scope, localName, fullyScopedName);
	for (i = 0; i < m_idRulesCurrSize; i++) {
		idRule = m_idRules[i];
		isOptional = idRule->m_isOptional;
		if (forceMode == DO_NOT_FORCE && isOptional) {
			continue;
		}
		nameInRule = idRule->m_locallyScopedName.c_str();
		if (strstr(nameInRule, "uid-") != 0) {
			validateRequiredUidEntry(cfg, fullyScopedName.c_str(),
						  idRule);
		} else {
			if (cfg->type(fullyScopedName.c_str(), nameInRule)
		           == Configuration::CFG_NO_VALUE)
			{
				cfg->mergeNames(fullyScopedName.c_str(),
						nameInRule, nameOfMissingEntry);
				typeName = idRule->m_typeName.c_str();
				msg << cfg->fileName() << ": the " << typeName << " '"
					<< nameOfMissingEntry << "' does not exist";
				throw ConfigurationException(msg.c_str());
			}
		}
	}
}



void
SchemaValidator::validateRequiredUidEntry(
	const Configuration *	cfg,
	const char *			fullScope,
	SchemaIdRuleInfo *		idRule) const throw(ConfigurationException)
{
	const char *			nameInRule;
	const char *			lastDot;
	StringBuffer			parentScopePattern;
	StringBuffer			nameOfMissingEntry;
	StringBuffer			msg;
	StringVector			parentScopes;
	const char *			typeName;
	const char *			ptr;
	int						i;
	int						len;

	nameInRule = idRule->m_locallyScopedName.c_str();
	assert(strstr(nameInRule, "uid-") != 0);
	lastDot = strrchr(nameInRule, '.');
	if (lastDot == 0 || strstr(lastDot+1, "uid-") != 0) {
		return;
	}
	parentScopePattern = fullScope;
	if (fullScope[0] != '\0') {
		parentScopePattern.append('.');
	}
	for (ptr = nameInRule; ptr != lastDot; ptr++) {
		parentScopePattern.append(*ptr);
	}
	cfg->listFullyScopedNames(fullScope, "", Configuration::CFG_SCOPE, true,
							  parentScopePattern.c_str(), parentScopes);
	len = parentScopes.length();
	for (i = 0; i < len; i++) {
		if (cfg->type(parentScopes[i], lastDot+1)
		   == Configuration::CFG_NO_VALUE)
		{
			cfg->mergeNames(parentScopes[i], lastDot+1, nameOfMissingEntry);
			typeName = idRule->m_typeName.c_str();
			msg << cfg->fileName() << ": the " << typeName << " '"
				<< nameOfMissingEntry << "' does not exist";
			throw ConfigurationException(msg.c_str());
		}
	}
}



bool
SchemaValidator::shouldIgnore(
	const Configuration *	cfg,
	const char *			scope,
	const char *			expandedName,
	const char *			unexpandedName) const
{
	int						i;
	int						len;
	short					symbol;
	const char *			name;
	const char *			nameAfterPrefix;
	bool					hasDotAfterPrefix;
	Configuration::Type		cfgType;

	for (i = 0; i < m_ignoreRulesCurrSize; i++) {
		//--------
		// Does unexpandedName start with rule.m_locallyScopedName
		// followed by "."?
		//--------
		name = m_ignoreRules[i]->m_locallyScopedName.c_str();
		len = strlen(name);
		if (strncmp(unexpandedName, name, len) != 0) {
			continue;
		}
		if (unexpandedName[len] != '.') {
			continue;
		}

		//--------
		// It does. Whether we ignore the item depends on the
		// "@ignore<something>" keyword used.
		//--------
		symbol = m_ignoreRules[i]->m_symbol;
		switch (symbol) {
		case SchemaLex::LEX_IGNORE_EVERYTHING_IN_SYM:
			return true;
		case SchemaLex::LEX_IGNORE_SCOPES_IN_SYM:
		case SchemaLex::LEX_IGNORE_VARIABLES_IN_SYM:
			break;
		default:
			assert(0); // Bug!
			break;
		}
		nameAfterPrefix = unexpandedName + len + 1;
		hasDotAfterPrefix = (strchr(nameAfterPrefix, '.') != 0);
		try {
			cfgType = cfg->type(scope, expandedName);
		} catch(const ConfigurationException & ex) {
			assert(0); // Bug!
		}
		if (symbol == SchemaLex::LEX_IGNORE_VARIABLES_IN_SYM) {
			if (hasDotAfterPrefix) {
				//--------
				// The item is a variable in a nested scope so
				// the "@ignoreVariablesIn" rule does not apply.
				//--------
				continue;
			}
			//--------
			// The item is directly in the scope, so the
			// "@ignoreVariablesIn" rule applies if the item
			// is a variable.
			//--------
			if ((cfgType & Configuration::CFG_VARIABLES) != 0) {
				return true;
			} else {
				continue;
			}
		}

		assert(symbol == SchemaLex::LEX_IGNORE_SCOPES_IN_SYM);
		if (hasDotAfterPrefix) {
			//--------
			// The item is in a *nested* scope, so we ignore it.
			//--------
			return true;
		}
		//--------
		// The item is directly in the ignore-able scope,
		// so we ignore it only if the item is a scope.
		//--------
		if (cfgType == Configuration::CFG_SCOPE) {
			return true;
		} else {
			continue;
		}
	}
	return false;
}



void
SchemaValidator::indent(int indentLevel) const
{
	int				i;

	for (i = 0; i < indentLevel; i++) {
		printf("  ");
	}
}



void
SchemaValidator::printTypeArgs(
	const StringVector &	typeArgs,
	int			indentLevel) const
{
	int				len;
	int				i;

	indent(indentLevel);
	printf("typeArgs = [");
	len = typeArgs.length();
	for (i = 0; i < len; i++) {
		printf("\"%s\"", typeArgs[i]);
		if (i < len-1) {
			printf(", ");
		}
	}
	printf("]\n");
}



void
SchemaValidator::printTypeNameAndArgs(
	const char *		typeName,
	const StringVector &	typeArgs,
	int			indentLevel) const
{
	int				len;
	int				i;

	indent(indentLevel);
	printf("typeName = \"%s\"; typeArgs = [", typeName);
	len = typeArgs.length();
	for (i = 0; i < len; i++) {
		printf("\"%s\"", typeArgs[i]);
		if (i < len-1) {
			printf(", ");
		}
	}
	printf("]\n");
}



void
SchemaValidator::callCheckRule(
	const SchemaType *		target,
	const Configuration *	cfg,
	const char *			typeName,
	const StringVector &	typeArgs,
	const char *			rule,
	int						indentLevel) const
{
	try {
		if (m_wantDiagnostics) {
			printf("\n");
			indent(indentLevel);
			printf("start %s::checkRule()\n", target->className());
			indent(indentLevel+1);
			printf("rule = \"%s\"\n", rule);
			printTypeNameAndArgs(typeName, typeArgs, indentLevel+1);
		}
		target->checkRule(this, cfg, typeName, typeArgs, rule);
		if (m_wantDiagnostics) {
			indent(indentLevel);
			printf("end %s::checkRule()\n", target->className());
		}
	} catch (const ConfigurationException & ex) {
		if (m_wantDiagnostics) {
			printf("\n");
			indent(indentLevel);
			printf("exception thrown from %s::checkRule(): %s\n",
				target->className(), ex.c_str());
		}
		throw;
	}
}



void
SchemaValidator::callValidate(
	const SchemaType *		target,
	const Configuration *	cfg,
	const char *			scope,
	const char *			name,
	const char *			typeName,
	const char *			origTypeName,
	const StringVector &	typeArgs,
	int						indentLevel) const
{
	try {
		if (m_wantDiagnostics) {
			printf("\n");
			indent(indentLevel);
			printf("start %s::validate()\n", target->className());
			indent(indentLevel+1);
			printf("scope = \"%s\"; name = \"%s\"\n", scope, name);
			indent(indentLevel+1);
			printf("typeName = \"%s\"; origTypeName = \"%s\"\n",
				typeName, origTypeName);
			printTypeArgs(typeArgs, indentLevel+1);
		}
		target->validate(this, cfg, scope, name, typeName, origTypeName,
				typeArgs, indentLevel+1);
		if (m_wantDiagnostics) {
			indent(indentLevel);
			printf("end %s::validate()\n", target->className());
		}
	} catch (const ConfigurationException & ex) {
		if (m_wantDiagnostics) {
			printf("\n");
			indent(indentLevel);
			printf("exception thrown from %s::validate(): %s\n",
				target->className(), ex.c_str());
		}
		throw;
	}
}



bool
SchemaValidator::callIsA(
	const SchemaType *		target,
	const Configuration *	cfg,
	const char *			value,
	const char *			typeName,
	const StringVector &	typeArgs,
	int						indentLevel,
	StringBuffer &			errSuffix) const
{
	bool					result;

	try {
		if (m_wantDiagnostics) {
			printf("\n");
			indent(indentLevel);
			printf("start %s::isA()\n", target->className());
			indent(indentLevel+1);
			printf("value = \"%s\"\n", value);
			printTypeNameAndArgs(typeName, typeArgs, indentLevel+1);
		}
		result = target->isA(this, cfg, value, typeName, typeArgs,
					indentLevel+1, errSuffix);
		if (m_wantDiagnostics) {
			indent(indentLevel);
			printf("end %s::isA()\n", target->className());
			indent(indentLevel+1);
			printf("result = %s; errSuffix = \"%s\"\n",
				(result? "true" : "false"), errSuffix.c_str());
		}
	} catch (const ConfigurationException & ex) {
		if (m_wantDiagnostics) {
			printf("\n");
			indent(indentLevel);
			printf("exception thrown from %s::isA(): %s\n",
				target->className(), ex.c_str());
		}
		throw;
	}
	return result;
}

}; // namespace CONFIG4CPP_NAMESPACE
