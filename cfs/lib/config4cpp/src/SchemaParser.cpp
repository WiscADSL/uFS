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
#include "SchemaParser.h"
#include "ConfigItem.h"
#include <string.h>
#include <ctype.h>
#include <stdlib.h>


namespace CONFIG4CPP_NAMESPACE {

extern "C" int CONFIG4CPP_C_PREFIX(compareSchemaIdRuleInfo_c)(
														const void * p1,
														const void * p2);


//----------------------------------------------------------------------
// Function:	Constructor
//
// Description:
//----------------------------------------------------------------------

SchemaParser::SchemaParser(SchemaValidator * sv)
{
	m_sv = sv;
	m_lex = 0;
	m_cfg = Configuration::create();
}


//----------------------------------------------------------------------
// Function:	Destructor
//
// Description:
//----------------------------------------------------------------------

SchemaParser::~SchemaParser()
{
	delete m_lex;
	m_cfg->destroy();
}



void
SchemaParser::parse(
	const char **		schema,
	int					schemaSize) throw(ConfigurationException)
{
	int					i;
	const char *		schemaItem;
	const char *		s1;
	const char *		s2;
	StringBuffer		name;
	StringBuffer		msg;

	//--------
	// Allocate more than enough space for idRules and ignoreRules
	// in the schema validator.
	//--------
	m_sv->m_idRulesMaxSize  = schemaSize;
	m_sv->m_idRulesCurrSize = 0;
	m_sv->m_idRules = new SchemaIdRuleInfo*[schemaSize];
	for (i = 0; i < m_sv->m_idRulesMaxSize; i++) {
		m_sv->m_idRules[i] = 0;
	}
	m_sv->m_ignoreRulesMaxSize  = schemaSize;
	m_sv->m_ignoreRulesCurrSize = 0;
	m_sv->m_ignoreRules = new SchemaIgnoreRuleInfo*[schemaSize];
	for (i = 0; i < m_sv->m_ignoreRulesMaxSize; i++) {
		m_sv->m_ignoreRules[i] = 0;
	}

	m_sv->sortTypes();
	for (i = 0; i < schemaSize; i++) {
		schemaItem = schema[i];
		delete m_lex;
		m_lex = new SchemaLex(schemaItem);
		m_lex->nextToken(m_token);
		switch (m_token.type()) {
		case SchemaLex::LEX_OPTIONAL_SYM:
		case SchemaLex::LEX_REQUIRED_SYM:
		case SchemaLex::LEX_IDENT_SYM:
			m_sv->m_idRules[m_sv->m_idRulesCurrSize] = new SchemaIdRuleInfo();
			m_sv->m_idRulesCurrSize++;
			parseIdRule(schemaItem, m_sv->m_idRules[m_sv->m_idRulesCurrSize-1]);
			break;
		case SchemaLex::LEX_IGNORE_EVERYTHING_IN_SYM:
		case SchemaLex::LEX_IGNORE_SCOPES_IN_SYM:
		case SchemaLex::LEX_IGNORE_VARIABLES_IN_SYM:
			m_sv->m_ignoreRules[m_sv->m_ignoreRulesCurrSize]
					= new SchemaIgnoreRuleInfo();
			m_sv->m_ignoreRulesCurrSize++;
			parseIgnoreRule(schemaItem,
						    m_sv->m_ignoreRules[m_sv->m_ignoreRulesCurrSize-1]);
			break;
		case SchemaLex::LEX_TYPEDEF_SYM:
			parseUserTypeDef(schemaItem);
			m_sv->sortTypes();
			break;
		default:
			accept(SchemaLex::LEX_IDENT_SYM, schemaItem,
			      "expecting an identifier or keyword");
			break;
		}
	}

	//--------
	// Sort the rules.
	//--------
	qsort(m_sv->m_idRules, m_sv->m_idRulesCurrSize,
		  sizeof(SchemaIdRuleInfo*),
		  CONFIG4CPP_C_PREFIX(compareSchemaIdRuleInfo_c));

	//--------
	// Check if multiple rules have the same name.
	//--------
	for (i = 0; i < m_sv->m_idRulesCurrSize-1; i++) {
		s1 = m_sv->m_idRules[i]->m_locallyScopedName.c_str();
		s2 = m_sv->m_idRules[i+1]->m_locallyScopedName.c_str();
		if (strcmp(s1, s2) == 0) {
			msg << "There are multiple rules for '" << s1 << "'";
			throw ConfigurationException(msg.c_str());
		}
	}

#if 0
	//--------
	// Diagnostics
	//--------
	printf("post qsort(): m_sv->m_idRulesCurrSize=%d; rules...\n",
		m_sv->m_idRulesCurrSize);
	for (i = 0; i < m_sv->m_idRulesCurrSize; i++) {
		printf("m_sv->rule[%d] = '%s' has %d args:", i,
			m_sv->m_idRules[i]->m_locallyScopedName.c_str(),
			m_sv->m_idRules[i]->m_args.length());
		for (int i2 = 0; i2 < m_sv->m_idRules[i]->m_args.length(); i2++)
		{
			printf(" '%s'", m_sv->m_idRules[i]->m_args[i2]);
		}
		printf("\n");
	}
#endif

}



//----------------------------------------------------------------------
// BNF for an identifier rule:
//  rule    =           OptOrRequired locallyScopedName '=' type
//                    | OptOrRequired locallyScopedName '=' type '[' args ']'
//  OptOrRequired =     '@optional'
//                    | '@required'
//                    | empty
//  args =              empty
//                    | arg { ',' arg }*
//  locallyScopedName = IDENT
//  type    =           IDENT
//  arg     =           IDENT
//                    | STRING
//----------------------------------------------------------------------

void
SchemaParser::parseIdRule(
	const char *			rule,
	SchemaIdRuleInfo *		ruleInfo) throw(ConfigurationException)
{
	StringBuffer			msg;
	SchemaType *			typeDef;
	bool					isOptional;
	const char *			name;
	const char *			ptr;

	switch (m_token.type()) {
	case SchemaLex::LEX_REQUIRED_SYM:
		isOptional = false;
		m_lex->nextToken(m_token);
		break;
	case SchemaLex::LEX_OPTIONAL_SYM:
		isOptional = true;
		m_lex->nextToken(m_token);
		break;
	default:
		isOptional = true;
		break;
	}

	ruleInfo->m_isOptional = isOptional;
	ruleInfo->m_locallyScopedName = m_token.spelling();
	accept(SchemaLex::LEX_IDENT_SYM, rule, "expecting an identifier");

	//--------
	// Complain if we have @required uid-<something>
	//--------
	if (!isOptional) {
		name = ruleInfo->m_locallyScopedName.c_str();
		ptr = strrchr(name, '.');
		if (ptr == 0) {
			ptr = name;
		} else {
			ptr ++; // skip over "."
		}
		if (strncmp(ptr, "uid-", 4) == 0) {
			msg << "Use of '@required' is incompatible with the uid- entry ('"
			    << ptr << "') in rule '" << rule << "'";
			throw ConfigurationException(msg.c_str());
		}
	}

	accept(SchemaLex::LEX_EQUALS_SYM, rule, "expecting '='");

	ruleInfo->m_typeName = m_token.spelling();
	accept(SchemaLex::LEX_IDENT_SYM, rule, "expecting an identifier");

	typeDef = m_sv->findType(ruleInfo->m_typeName.c_str());
	if (typeDef == 0) {
		msg << "Unknown type '" << ruleInfo->m_typeName << "' in rule '"
			<< rule << "'";
		throw ConfigurationException(msg.c_str());
	}
	if (m_token.type() == SchemaLex::LEX_EOF_SYM) {
		m_sv->callCheckRule(typeDef, m_cfg,
				ruleInfo->m_typeName.c_str(),
				ruleInfo->m_args, rule, 1);
		return;
	}

	accept(SchemaLex::LEX_OPEN_BRACKET_SYM, rule, "expecting '['");
	if (m_token.type() == SchemaLex::LEX_IDENT_SYM
	    || m_token.type() == SchemaLex::LEX_STRING_SYM)
	{
		ruleInfo->m_args.add(m_token.spelling());
		m_lex->nextToken(m_token);
	} else if (m_token.type() != SchemaLex::LEX_CLOSE_BRACKET_SYM) {
		accept(SchemaLex::LEX_IDENT_SYM, rule,
			   "expecting an identifier, string or ']'");
	}
	while (m_token.type() != SchemaLex::LEX_CLOSE_BRACKET_SYM) {
		accept(SchemaLex::LEX_COMMA_SYM, rule, "expecting ','");
		ruleInfo->m_args.add(m_token.spelling());
		if (m_token.type() == SchemaLex::LEX_IDENT_SYM
		    || m_token.type() == SchemaLex::LEX_STRING_SYM)
		{
			m_lex->nextToken(m_token);
		} else {
			accept(SchemaLex::LEX_IDENT_SYM, rule,
			      "expecting an identifier, string or ']'");
		}
	}
	accept(SchemaLex::LEX_CLOSE_BRACKET_SYM, rule, "expecting ']'");
	accept(SchemaLex::LEX_EOF_SYM, rule, "expecting <end of string>");
	m_sv->callCheckRule(typeDef, m_cfg, ruleInfo->m_typeName.c_str(),
					    ruleInfo->m_args, rule, 1);
}



//----------------------------------------------------------------------
// BNF for an ignore rule:
//  rule    =           '@ignoreEverythingIn' locallyScopedName
//                    | '@ignoreScopesIn'     locallyScopedName
//                    | '@ignoreVariablesIn'  locallyScopedName
//  locallyScopedName = IDENT
//----------------------------------------------------------------------

void
SchemaParser::parseIgnoreRule(
	const char *			rule,
	SchemaIgnoreRuleInfo *	ruleInfo) throw(ConfigurationException)
{
	ruleInfo->m_symbol = m_token.type();
	m_lex->nextToken(m_token); // consume the "@ignore<something>" keyword
	ruleInfo->m_locallyScopedName = m_token.spelling();
	accept(SchemaLex::LEX_IDENT_SYM, rule, "expecting an identifier");
	accept(SchemaLex::LEX_EOF_SYM,   rule, "expecting <end of string>");
}



//----------------------------------------------------------------------
// BNF for a user-defined type:
//  userType =          '@typedef' typeName '=' baseTypeName
//                    | '@typedef' typeName '=' baseTypeName '[' args ']'
//  args =              empty
//                    | arg { ',' arg }*
//  typeName =          IDENT
//  baseTypeName =      IDENT
//  arg     =           IDENT
//                    | STRING
//----------------------------------------------------------------------

void
SchemaParser::parseUserTypeDef(const char * str) throw(ConfigurationException)
{
	StringBuffer			msg;
	SchemaType *			baseTypeDef;
	StringBuffer			typeName;
	StringBuffer			baseTypeName;
	StringVector			baseTypeArgs;

	accept(SchemaLex::LEX_TYPEDEF_SYM, str, "expecting '@typedef'");
	typeName = m_token.spelling();
	accept(SchemaLex::LEX_IDENT_SYM, str, "expecting an identifier");
	accept(SchemaLex::LEX_EQUALS_SYM, str, "expecting '='");
	baseTypeName = m_token.spelling();
	accept(SchemaLex::LEX_IDENT_SYM, str, "expecting an identifier");

	baseTypeDef = m_sv->findType(baseTypeName.c_str());
	if (baseTypeDef == 0) {
		msg << "Unknown type '" << baseTypeName
			<< "' in user-type definition '" << str << "'";
		throw ConfigurationException(msg.c_str());
	}

	if (m_token.type() == SchemaLex::LEX_EOF_SYM) {
		//--------
		// Finished. Ask the base type to check its (empty) arguments
		// and then register the new command.
		//--------
		m_sv->callCheckRule(baseTypeDef, m_cfg, baseTypeName.c_str(),
						baseTypeArgs, str, 1);
		m_sv->registerTypedef(typeName.c_str(), baseTypeDef->cfgType(),
					baseTypeName.c_str(), baseTypeArgs);
		return;
	}

	accept(SchemaLex::LEX_OPEN_BRACKET_SYM, str, "expecting '['");
	if (m_token.type() == SchemaLex::LEX_IDENT_SYM
	    || m_token.type() == SchemaLex::LEX_STRING_SYM)
	{
		baseTypeArgs.add(m_token.spelling());
		m_lex->nextToken(m_token);
	} else if (m_token.type() != SchemaLex::LEX_CLOSE_BRACKET_SYM) {
		accept(SchemaLex::LEX_IDENT_SYM, str,
			   "expecting an identifier, string or ']'");
	}
	while (m_token.type() != SchemaLex::LEX_CLOSE_BRACKET_SYM) {
		accept(SchemaLex::LEX_COMMA_SYM, str, "expecting ','");
		baseTypeArgs.add(m_token.spelling());
		if (m_token.type() == SchemaLex::LEX_IDENT_SYM
		    || m_token.type() == SchemaLex::LEX_STRING_SYM)
		{
			m_lex->nextToken(m_token);
		} else {
			accept(SchemaLex::LEX_IDENT_SYM, str,
				   "expecting an identifier, string or ']'");
		}
	}
	accept(SchemaLex::LEX_CLOSE_BRACKET_SYM, str, "expecting ']'");
	accept(SchemaLex::LEX_EOF_SYM, str, "expecting <end of string>");

	//--------
	// Finished. Ask the base type to check its arguments
	// and then register the new command.
	//--------
	m_sv->callCheckRule(baseTypeDef, m_cfg, baseTypeName.c_str(),
				baseTypeArgs, str, 1);
	m_sv->registerTypedef(typeName.c_str(), baseTypeDef->cfgType(),
				baseTypeName.c_str(), baseTypeArgs);

}



void
SchemaParser::accept(
	short				sym,
	const char *		rule,
	const char *		msgPrefix) throw(ConfigurationException)
{
	StringBuffer		msg;

	if (m_token.type() == sym) {
		m_lex->nextToken(m_token);
	} else {
		msg << "error in validation rule '" << rule << "': " << msgPrefix
			<< " near '" << m_token.spelling() << "'";
		throw ConfigurationException(msg.c_str());
	}
}

}; // namespace CONFIG4CPP_NAMESPACE
