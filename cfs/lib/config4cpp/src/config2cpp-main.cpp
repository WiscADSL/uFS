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
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "Config2Cpp.h"
#include <config4cpp/Configuration.h>
#include <config4cpp/SchemaValidator.h>

using namespace CONFIG4CPP_NAMESPACE;



void
calculateRuleForName(
	const Configuration *		cfg,
	const char *				name,
	const char *				uName,
	const StringVector &		wildcardedNamesAndTypes,
	StringBuffer &				rule)
{
	int							i;
	int							len;
	const char *				str;
	const char *				keyword;
	const char *				wildcardedName;
	const char *				type;

	rule.empty();
	len = wildcardedNamesAndTypes.length();
	for (i = 0; i < len; i+=3) {
		keyword        = wildcardedNamesAndTypes[i+0]; // @optional or @required
		wildcardedName = wildcardedNamesAndTypes[i+1];
		type           = wildcardedNamesAndTypes[i+2];
		if (Configuration::patternMatch(uName, wildcardedName)) {
			rule << keyword << " " << uName << " = " << type;
			return;
		}
	}

	//--------
	// We couldn's determine the type from the wildcarded_names_and_types 
	// table. So we fall back to using heuristics to guess a good type.
	//--------
	if (cfg->type("", name) == Configuration::CFG_SCOPE) {
		rule << uName << " = scope";
	} else if (cfg->type("", name) == Configuration::CFG_LIST) {
		rule << uName << " = list[string]";
	} else {
		str = cfg->lookupString("", name);
		if (cfg->isBoolean(str)) {
			rule << uName << " = boolean";
		} else if (cfg->isInt(str)) {
			rule << uName << " = int";
		} else if (cfg->isFloat(str)) {
			rule << uName << " = float";
		} else if (cfg->isDurationSeconds(str)) {
			rule << uName << " = durationSeconds";
		} else if (cfg->isDurationMilliseconds(str)) {
			rule << uName << " = durationMilliseconds";
		} else if (cfg->isDurationMicroseconds(str)) {
			rule << uName << " = durationMicroseconds";
		} else if (cfg->isMemorySizeBytes(str)) {
			rule << uName << " = memorySizeBytes";
		} else if (cfg->isMemorySizeKB(str)) {
			rule << uName << " = memorySizeKB";
		} else if (cfg->isMemorySizeMB(str)) {
			rule << uName << " = memorySizeMB";
		} else {
			rule << uName << " = string";
		}
	}
}



bool
doesVectorcontainString(const StringVector & vec, const char * str)
{
	int				i;
	int				len;

	len = vec.length();
	for (i = 0; i < len; i++) {
		if (strcmp(vec[i], str) == 0) {
			return true;
		}
	}
	return false;
}



void
calculateSchema(
	const Configuration *		cfg,
	const StringVector &		namesList,
	const StringVector &		recipeUserTypes,
	const StringVector &		wildcardedNamesAndTypes,
	const StringVector &		recipeIgnoreRules,
	StringVector &				schema) throw(ConfigurationException)
{
	int							i;
	int							len;
	StringBuffer				rule;
	StringBuffer				buf;
	const char *				name;
	const char *				uName;
	StringVector				uidNames;

	schema.empty();
	schema.add(recipeIgnoreRules);
	schema.add(recipeUserTypes);
	len = namesList.length();
	for (i = 0; i < len; i++) {
		name = namesList[i];
		if (strstr(name, "uid-") == 0) {
			calculateRuleForName(cfg, name, name, wildcardedNamesAndTypes,rule);
			schema.add(rule);
		} else {
			uName = cfg->unexpandUid(name, buf);
			if (!doesVectorcontainString(uidNames, uName)) {
				uidNames.add(uName);
				calculateRuleForName(cfg, name, uName,
									 wildcardedNamesAndTypes, rule);
				schema.add(rule);
			}
		}
	}
}



bool
doesPatternMatchAnyUnexpandedNameInList(
	const Configuration *		cfg,
	const char *				pattern,
	const StringVector &		namesList)
{
	int							i;
	int							len;
	const char *				uName;
	StringBuffer				buf;

	len = namesList.length();
	for (i = 0; i < len; i++) {
		uName = cfg->unexpandUid(namesList[i], buf);
		if (Configuration::patternMatch(uName, pattern)) {
			return true;
		}
	}
	return false;
}



void
checkForUnmatchedPatterns(
	const Configuration *		cfg,
	const StringVector &		namesList,
	const StringVector &		wildcardedNamesAndTypes,
	StringVector &				unmatchedPatterns) throw(ConfigurationException)
{
	int							i;
	int							len;
	const char *				wildcardedName;

	unmatchedPatterns.empty();
	//--------
	// Check if there is a wildcarded name that does not match anything
	//--------
	len = wildcardedNamesAndTypes.length();
	for (i = 0; i < len; i += 3) {
		wildcardedName = wildcardedNamesAndTypes[i+1];
		if (!doesPatternMatchAnyUnexpandedNameInList(cfg, wildcardedName,
		                                             namesList))
		{
			unmatchedPatterns.add(wildcardedName);
		}
	}
}



//----------------------------------------------------------------------
// File: main()
//
// Description: Mainline of "config2cpp"
//----------------------------------------------------------------------

int
main(int argc, char ** argv)
{
	bool					ok;
	Configuration *			cfg;
	Configuration *			schemaCfg;
	Config2Cpp				util("config2cpp");
	StringVector			namesList;
	StringVector			recipeUserTypes;
	StringVector			wildcardedNamesAndTypes;
	StringVector			recipeIgnoreRules;
	StringVector			unmatchedPatterns;
	StringVector			schema;
	SchemaValidator			sv;
	const char *			scope;
	int						i;
	int						len;
	const char *			overrideSchema[] = {
					"@typedef keyword = enum[\"@optional\", \"@required\"]",
					"user_types = list[string]",
					"wildcarded_names_and_types = table[keyword,keyword, "
										"string,wildcarded-name, string,type]",
					"ignore_rules = list[string]",
					0 // null-terminated array
							};

	ok = util.parseCmdLineArgs(argc, argv);

	cfg       = Configuration::create();
	schemaCfg = Configuration::create();
	if (ok && util.wantSchema()) {
		try {
			cfg->parse(util.cfgFileName());
			cfg->listFullyScopedNames("", "", Configuration::CFG_SCOPE_AND_VARS,
									  true, namesList);
			if (util.schemaOverrideCfg() != 0) {
				schemaCfg->parse(util.schemaOverrideCfg());
				scope = util.schemaOverrideScope();
				sv.parseSchema(overrideSchema);
				sv.validate(schemaCfg, scope, "");
				schemaCfg->lookupList(scope, "user_types", recipeUserTypes);
				schemaCfg->lookupList(scope, "wildcarded_names_and_types",
									  wildcardedNamesAndTypes);
				schemaCfg->lookupList(scope, "ignore_rules", recipeIgnoreRules);
			}
			calculateSchema(cfg, namesList, recipeUserTypes,
							wildcardedNamesAndTypes, recipeIgnoreRules, schema);
			checkForUnmatchedPatterns(cfg, namesList, wildcardedNamesAndTypes,
									  unmatchedPatterns);
		} catch(const ConfigurationException & ex) {
			fprintf(stderr, "%s\n", ex.c_str());
			ok = false;
		}
		len = unmatchedPatterns.length();
		if (len != 0) {
			fprintf(stderr, "%s %s\n",
				"Error: the following patterns in the schema",
				"recipe did not match anything");
			for (i = 0; i < len; i++) {
				fprintf(stderr, "\t'%s'\n", unmatchedPatterns[i]);
			}
			ok = false;
		}
	}

	if (ok) {
		ok = util.generateFiles(schema.c_array(), schema.length());
	}

	cfg->destroy();
	if (ok) {
		return 0;
	} else {
		return 1;
	}
}

