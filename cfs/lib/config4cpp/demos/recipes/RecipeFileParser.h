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

#ifndef RECIPE_FILE_PARSER_H_
#define RECIPE_FILE_PARSER_H_

#include <config4cpp/Configuration.h>
#include "RecipeFileParserException.h"

using namespace CONFIG4CPP_NAMESPACE;


class RecipeFileParser
{
public:
	RecipeFileParser();
	~RecipeFileParser();

	//--------
	// Parse a recipies file in Config4* format and check
	// that all the recipes have their required details.
	//--------
	void parse(const char * recipeFilename, const char * scope)
											throw (RecipeFileParserException);

	//--------
	// Operations to query information about recipes
	//--------
	void		listRecipeScopes(StringVector & vec);

	const char * getRecipeName(const char * recipeScope)
										throw (RecipeFileParserException);

	void		getRecipeIngredients(
					const char *		recipeScope,
					StringVector &		vec) throw (RecipeFileParserException);

	void		getRecipeSteps(
					const char *		recipeScope,
					StringVector &		vec) throw (RecipeFileParserException);

private:
	//--------
	// Instance variables
	//--------
	CONFIG4CPP_NAMESPACE::Configuration *	m_cfg;
	StringBuffer							m_scope;
	bool									m_parseCalled;
	StringVector							m_recipeScopeNames;

	//--------
	// The following are not implemented
	//--------
	RecipeFileParser & operator=(const RecipeFileParser &);
	RecipeFileParser(const RecipeFileParser &);
};

#endif
