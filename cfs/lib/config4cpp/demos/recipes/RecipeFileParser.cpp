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

#include "RecipeFileParser.h"
#include <config4cpp/SchemaValidator.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>

using CONFIG4CPP_NAMESPACE::Configuration;
using CONFIG4CPP_NAMESPACE::ConfigurationException;
using CONFIG4CPP_NAMESPACE::SchemaValidator;



RecipeFileParser::RecipeFileParser()
{
	m_cfg = 0;
	m_parseCalled = false; // set to 'true' after a successful parse().
}



RecipeFileParser::~RecipeFileParser()
{
	m_cfg->destroy();
}



void
RecipeFileParser::parse(
	const char *		recipeFilename,
	const char *		scope) throw (RecipeFileParserException)
{
	SchemaValidator		sv;
	StringBuffer		filter;
	const char *		schema[] = {
									"uid-recipe = scope",
									"uid-recipe.ingredients = list[string]",
									"uid-recipe.name = string",
									"uid-recipe.uid-step = string",
									0
						};

	assert(!m_parseCalled);
	m_cfg = Configuration::create();
	m_scope = scope;
	Configuration::mergeNames(scope, "uid-recipe", filter);
	try {
		m_cfg->parse(recipeFilename);
		sv.parseSchema(schema);
		sv.validate(m_cfg, m_scope.c_str(), "");
		m_cfg->listFullyScopedNames(m_scope.c_str(), "",
		                            Configuration::CFG_SCOPE, false,
		                            filter.c_str(), m_recipeScopeNames);
	} catch(const ConfigurationException & ex) {
		throw RecipeFileParserException(ex.c_str());
	}
	m_parseCalled = true;
}



void
RecipeFileParser::listRecipeScopes(StringVector & vec)
{
	assert(m_parseCalled);
	vec = m_recipeScopeNames;
}



const char *
RecipeFileParser::getRecipeName(const char * recipeScope)
											throw (RecipeFileParserException)
{
	assert(m_parseCalled);
	try {
		return m_cfg->lookupString(recipeScope, "name");
	} catch(const ConfigurationException & ex) {
		throw RecipeFileParserException(ex.c_str());
	}
}



void
RecipeFileParser::getRecipeIngredients(
	const char *			recipeScope,
	StringVector &			result) throw (RecipeFileParserException)
{
	assert(m_parseCalled);
	try {
		m_cfg->lookupList(recipeScope, "ingredients", result);
	} catch(const ConfigurationException & ex) {
		throw RecipeFileParserException(ex.c_str());
	}
}



void
RecipeFileParser::getRecipeSteps(
	const char *			recipeScope,
	StringVector &			result) throw (RecipeFileParserException)
{
	int						i;
	int						len;
	StringVector			namesVec;
	const char *			str;

	assert(m_parseCalled);
	try {
		m_cfg->listLocallyScopedNames(recipeScope, "",
		                              Configuration::CFG_STRING, false,
		                              "uid-step", namesVec);
	} catch(const ConfigurationException & ex) {
		throw RecipeFileParserException(ex.c_str());
	}
	result.empty();
	len = namesVec.length();
	try {
		for (i = 0; i < len; i++) {
			assert(m_cfg->uidEquals("uid-step", namesVec[i]));
			str = m_cfg->lookupString(recipeScope, namesVec[i]);
			result.add(str);
		}
	} catch(const ConfigurationException &) {
		abort(); // Bug!
	}
}

