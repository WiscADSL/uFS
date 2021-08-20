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

#ifndef	CONFIG4CPP_LEX_BASE_H_
#define	CONFIG4CPP_LEX_BASE_H_


//--------
// #include's and #define's
//--------
#include <wchar.h>
#include <config4cpp/Configuration.h>
#include "LexToken.h"
#include "MBChar.h"
#include "UidIdentifierProcessor.h"
#include "platform.h"


namespace CONFIG4CPP_NAMESPACE {

class LexBase
{
public:
	//--------
	// Public operations
	//--------
	void nextToken(LexToken &token);

	//--------
	// Constants for the type of a function.
	//--------
	enum FunctionType {
			NOT_A_FUNC  = 1,
			STRING_FUNC = 2,
			LIST_FUNC   = 3,
			BOOL_FUNC   = 4
	};

	//--------
	// Constants for lexical symbols, for everything except
	// keywords and function names. Constants for those are
	// defined in a subclass.
	//--------
	enum LexBaseSymbols {
			LEX_IDENT_SYM                 =  1,
			LEX_SEMICOLON_SYM             =  2,
			LEX_PLUS_SYM                  =  3,
			LEX_QUESTION_EQUALS_SYM       =  4,
			LEX_EQUALS_SYM                =  5,
			LEX_EQUALS_EQUALS_SYM         =  6,
			LEX_NOT_EQUALS_SYM            =  7,
			LEX_STRING_SYM                =  8,
			LEX_COMMA_SYM                 =  9,
			LEX_AND_SYM                   = 10,
			LEX_OR_SYM                    = 11,
			LEX_NOT_SYM                   = 12,
			LEX_AT_SYM                    = 13,
			LEX_OPEN_BRACKET_SYM          = 14,
			LEX_CLOSE_BRACKET_SYM         = 15,
			LEX_OPEN_BRACE_SYM            = 16,
			LEX_CLOSE_BRACE_SYM           = 17,
			LEX_OPEN_PAREN_SYM            = 18,
			LEX_CLOSE_PAREN_SYM           = 19,
			LEX_EOF_SYM                   = 20,
			LEX_UNKNOWN_SYM               = 21,
			LEX_UNKNOWN_FUNC_SYM          = 22,
			LEX_IDENT_TOO_LONG_SYM        = 23,
			LEX_STRING_WITH_EOL_SYM       = 24,
			LEX_ILLEGAL_IDENT_SYM         = 25,
			LEX_TWO_DOTS_IDENT_SYM        = 26,
			LEX_BLOCK_STRING_WITH_EOF_SYM = 27,
			LEX_SOLE_DOT_IDENT_SYM        = 28,
	};

	struct KeywordInfo {
		const char *    m_spelling;
		short           m_symbol;
	};
	struct FuncInfo {
		const char *    m_spelling;
		short           m_funcType;
		short           m_symbol;
	};

protected:
	//--------
	// Constructors and destructor
	//--------
	LexBase(
		Configuration::SourceType     sourceType,
		const char *			      source,
		UidIdentifierProcessor *      uidIdentifierProcessor)
												throw(ConfigurationException);
	LexBase(const char * str) throw(ConfigurationException);
	virtual ~LexBase();

	//--------
	// The constructors of a subclass should initialize the
	// following variables.
	//--------
	KeywordInfo *		m_keywordInfoArray;
	int					m_keywordInfoArraySize;
	FuncInfo *			m_funcInfoArray;
	int					m_funcInfoArraySize;


private:
	//--------
	// Implementation-specific operations
	//--------
	void searchForKeyword(
			const char *	spelling,
			bool &			found,
			short &			symbol);

	void searchForFunction(
			const char *	spelling,
			bool &			found,
			short &			funcType,
			short &			symbol);

	void nextChar() throw(ConfigurationException);
	char nextByte();
	void consumeString(LexToken & token);
	void consumeBlockString(LexToken &token);
	bool isKeywordChar(const MBChar & ch);
	bool isIdentifierChar(const MBChar & ch);

	//--------
	// Instance variables
	//--------
	UidIdentifierProcessor *	m_uidIdentifierProcessor;
	bool						m_amOwnerOfUidIdentifierProcessor;
	int							m_lineNum; // Used for error reporting
	MBChar						m_ch; // Lookahead character
	Configuration::SourceType	m_sourceType;
	const char *				m_source;
	bool						m_atEOF;
	mbstate_t					m_mbtowcState;

	//--------
	// CFG_INPUT_FILE   uses m_file
	// CFG_INPUT_STRING uses m_ptr and m_source
	// CFG_INPUT_EXEC   uses m_ptr and m_execOutput
	//--------
	BufferedFileReader			m_file;
	const char *				m_ptr;
	StringBuffer				m_execOutput;

	//--------
	// Unsupported constructors and assignment operators
	//--------
	LexBase();
	LexBase(const LexBase &);
	LexBase & operator=(const LexBase &);
};


}; // namespace CONFIG4CPP_NAMESPACE
#endif
