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
#include "LexBase.h"
#include "UidIdentifierDummyProcessor.h"
#include <assert.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <ctype.h>
#include <wchar.h>
#include <stdio.h>


namespace CONFIG4CPP_NAMESPACE {

extern "C"
int
CONFIG4CPP_C_PREFIX(keywordInfoCmp)(const void * ptr1, const void * ptr2)
{
	const LexBase::KeywordInfo *	sas1;
	const LexBase::KeywordInfo *	sas2;

	sas1 = (const LexBase::KeywordInfo *)ptr1;
	sas2 = (const LexBase::KeywordInfo *)ptr2;
	return strcmp(sas1->m_spelling, sas2->m_spelling);
}



void
LexBase::searchForKeyword(
	const char *			spelling,
	bool &					found,
	short &					symbol)
{
	LexBase::KeywordInfo	searchItem;
	LexBase::KeywordInfo *	result = 0;

	searchItem.m_spelling = spelling;
	if (m_keywordInfoArraySize == 0) {
		found = false;
		return;
	}
	result = (LexBase::KeywordInfo *)
				bsearch(&searchItem, m_keywordInfoArray, m_keywordInfoArraySize,
				        sizeof(searchItem),CONFIG4CPP_C_PREFIX(keywordInfoCmp));
	if (result == 0) {
		found = false;
	} else {
		found  = true;
		symbol = result->m_symbol;
	}
}



extern "C"
int
CONFIG4CPP_C_PREFIX(funcInfoCmp_c)(const void * ptr1, const void * ptr2)
{
	const LexBase::FuncInfo *	sas1;
	const LexBase::FuncInfo *	sas2;

	sas1 = (const LexBase::FuncInfo *)ptr1;
	sas2 = (const LexBase::FuncInfo *)ptr2;
	return strcmp(sas1->m_spelling, sas2->m_spelling);
}



void
LexBase::searchForFunction(
	const char *		spelling,
	bool &				found,
	short &				funcType,
	short &				symbol)
{
	FuncInfo			searchItem;
	FuncInfo *			result = 0;

	if (m_funcInfoArraySize == 0) {
		found = false;
		return;
	}
	searchItem.m_spelling = spelling;
	result = (FuncInfo *)bsearch(&searchItem, m_funcInfoArray,
	                             m_funcInfoArraySize, sizeof(searchItem),
	                             CONFIG4CPP_C_PREFIX(funcInfoCmp_c));
	if (result == 0) {
		found = false;
	} else {
		found = true;
		funcType = result->m_funcType;
		symbol   = result->m_symbol;
	}
}



//----------------------------------------------------------------------
// Function:	Constructor
//
// Description:
//----------------------------------------------------------------------

LexBase::LexBase(
	Configuration::SourceType	sourceType,
	const char *				source,
	UidIdentifierProcessor *	uidIdentifierProcessor)
												throw(ConfigurationException)
{
	StringBuffer				msg;

	//--------
	// Initialize state for the multi-byte functions in the C library.
	//--------
	memset(&m_mbtowcState, 0, sizeof(mbstate_t));

	m_keywordInfoArray     = 0;
	m_keywordInfoArraySize = 0;
	m_funcInfoArray        = 0;
	m_funcInfoArraySize    = 0;

	m_uidIdentifierProcessor = uidIdentifierProcessor;
	m_amOwnerOfUidIdentifierProcessor = false;
	m_sourceType = sourceType;
	m_source = source;
	m_lineNum = 1;
	m_ptr = 0;
	m_atEOF = false;
	switch (sourceType) {
	case Configuration::INPUT_FILE:
		if (!m_file.open(source)) {
			msg << "cannot open " << source << ": " << strerror(errno);
			throw ConfigurationException(msg.c_str());
		}
		break;
	case Configuration::INPUT_STRING:
		m_ptr = m_source;
		break;
	case Configuration::INPUT_EXEC:
		if (!execCmd(source, m_execOutput)) {
			msg << "cannot parse 'exec#" << source << "': "
				<< m_execOutput.c_str();
			throw ConfigurationException(msg.c_str());
		}
		m_ptr = m_execOutput.c_str();
		break;
	default:
		assert(0); // Bug!
		break;
	}

	nextChar(); // initialize m_ch
}



//----------------------------------------------------------------------
// Function:	Constructor
//
// Description:
//----------------------------------------------------------------------

LexBase::LexBase(const char * str) throw(ConfigurationException)
{
	StringBuffer			msg;

	//--------
	// Initialize state for the multi-byte functions in the C library.
	//--------
	memset(&m_mbtowcState, 0, sizeof(mbstate_t));

	m_keywordInfoArray     = 0;
	m_keywordInfoArraySize = 0;
	m_funcInfoArray        = 0;
	m_funcInfoArraySize    = 0;

	m_uidIdentifierProcessor = new UidIdentifierDummyProcessor();
	m_amOwnerOfUidIdentifierProcessor = true;
	m_sourceType = Configuration::INPUT_STRING;
	m_source = str;
	m_lineNum = 1;
	m_ptr = m_source;
	m_atEOF = false;
	nextChar(); // initialize m_ch
}



//----------------------------------------------------------------------
// Function:	Destructor
//
// Description:
//----------------------------------------------------------------------

LexBase::~LexBase()
{
	if (m_amOwnerOfUidIdentifierProcessor) {
		delete m_uidIdentifierProcessor;
	}
}



//----------------------------------------------------------------------
// Function:	nextByte()
//
// Description:	Read the next byte from the input source
//----------------------------------------------------------------------

char
LexBase::nextByte()
{
	int				ch;

	if (m_sourceType == Configuration::INPUT_FILE) {
		do {
			ch = m_file.getChar();
		} while (ch == '\r');
	} else {
		do {
			ch = *m_ptr;
			if (ch == '\0') {
				ch = EOF;
			} else {
				m_ptr ++;
			}
		} while (ch == '\r');
	}
	m_atEOF = (ch == EOF);
	if (m_atEOF) {
		ch = 0;
	}
	return (char)ch;
}



//----------------------------------------------------------------------
// Function:	nextChar()
//
// Description:	Read the next char from the input file
//----------------------------------------------------------------------

void
LexBase::nextChar() throw(ConfigurationException)
{
	char				ch;
	int					status;
	wchar_t				wChar;

	m_ch.reset();
	status = -1;
	while (status == -1) {
		ch = nextByte();
		if (m_atEOF && !m_ch.isEmpty()) {
			StringBuffer			msg;
			msg << "Invalid multi-byte character on line " << m_lineNum;
			throw ConfigurationException(msg.c_str());
		}
		if (m_atEOF) {
			//--------
			// At EOF. Our work is done.
			//--------
			break;
		}
		if (!m_ch.add(ch)) {
			StringBuffer			msg;
			msg << "Invalid multi-byte character on line " << m_lineNum;
			throw ConfigurationException(msg.c_str());
		}

		status = mbrtowc(&wChar, m_ch.c_str(), m_ch.length(), &m_mbtowcState);
		if (status == -1 && m_ch.isFull()) {
			StringBuffer			msg;
			msg << "Invalid multi-byte character on line " << m_lineNum;
			throw ConfigurationException(msg.c_str());
		}
		m_ch.setWChar(wChar);
	}

	if (m_ch == '\n') {
		m_lineNum ++;
	}
}



//----------------------------------------------------------------------
// Function:	nextToken()
//
// Description:	Analyse the next token from the input file
//----------------------------------------------------------------------

void
LexBase::nextToken(LexToken &token)
{
	StringBuffer		spelling;
	bool				found;
	short				funcType;
	short				symbol;

	//--------
	// Skip leading white space
	//--------
	while (m_ch.isSpace()) {
		nextChar();
	}

	//--------
	// Check for EOF.
	//--------
	if (m_atEOF) {
		if (m_sourceType == Configuration::INPUT_STRING) {
			token.reset(LEX_EOF_SYM, m_lineNum, "<end of string>");
		} else {
			token.reset(LEX_EOF_SYM, m_lineNum, "<end of file>");
		}
		return;
	}

	//--------
	// Note the line number at the start of the token
	//--------
	const int	lineNum = m_lineNum;

	//--------
	// Miscellaneous kinds of tokens.
	//--------
	switch (m_ch.c_str()[0]) {
	case '?':
		nextChar();
		if (m_ch == '=') {
			nextChar();
			token.reset(LEX_QUESTION_EQUALS_SYM, lineNum, "?=");
		} else {
			token.reset(LEX_UNKNOWN_SYM, lineNum, spelling.c_str());
		}
		return;
	case '!':
		nextChar();
		if (m_ch == '=') {
			nextChar();
			token.reset(LEX_NOT_EQUALS_SYM, lineNum, "!=");
		} else {
			token.reset(LEX_NOT_SYM, lineNum, "!");
		}
		return;
	case '@':
		spelling.append(m_ch.c_str());
		nextChar();
		while (!m_atEOF && isKeywordChar(m_ch)) {
			spelling.append(m_ch.c_str());
			nextChar();
		}
		searchForKeyword(spelling.c_str(), found, symbol);
		if (found) {
			token.reset(symbol, lineNum, spelling.c_str());
		} else {
			token.reset(LEX_UNKNOWN_SYM, lineNum, spelling.c_str());
		}
		return;
	case '+':
		nextChar();
		token.reset(LEX_PLUS_SYM, lineNum, "+");
		return;
	case '&':
		nextChar();
		if (m_ch == '&') {
			nextChar();
			token.reset(LEX_AND_SYM, lineNum, "&&");
		} else {
			spelling << '&' << m_ch.c_str();
			token.reset(LEX_UNKNOWN_SYM, lineNum, spelling.c_str());
		}
		return;
	case '|':
		nextChar();
		if (m_ch == '|') {
			nextChar();
			token.reset(LEX_OR_SYM, lineNum, "||");
		} else {
			spelling << '|' << m_ch.c_str();
			token.reset(LEX_UNKNOWN_SYM, lineNum, spelling.c_str());
		}
		return;
	case '=':
		nextChar();
		if (m_ch == '=') {
			nextChar();
			token.reset(LEX_EQUALS_EQUALS_SYM, lineNum, "==");
		} else {
			token.reset(LEX_EQUALS_SYM, lineNum, "=");
		}
		return;
	case ';':
		nextChar();
		token.reset(LEX_SEMICOLON_SYM, lineNum, ";");
		return;
	case '[':
		nextChar();
		token.reset(LEX_OPEN_BRACKET_SYM, lineNum, "[");
		return;
	case ']':
		nextChar();
		token.reset(LEX_CLOSE_BRACKET_SYM, lineNum, "]");
		return;
	case '{':
		nextChar();
		token.reset(LEX_OPEN_BRACE_SYM, lineNum, "{");
		return;
	case '}':
		nextChar();
		token.reset(LEX_CLOSE_BRACE_SYM, lineNum, "}");
		return;
	case '(':
		nextChar();
		token.reset(LEX_OPEN_PAREN_SYM, lineNum, "(");
		return;
	case ')':
		nextChar();
		token.reset(LEX_CLOSE_PAREN_SYM, lineNum, ")");
		return;
	case ',':
		nextChar();
		token.reset(LEX_COMMA_SYM, lineNum, ",");
		return;
	case '"':
		consumeString(token);
		return;;
	case '<':
		nextChar();
		if (m_ch != '%') {
			token.reset(LEX_UNKNOWN_SYM, lineNum, "<");
			return;
		}
		nextChar(); // skip over '%'
		consumeBlockString(token);
		return;
	case '#':
		//--------
		// A comment. Consume it and immediately following
		// comments (without resorting to recursion).
		//--------
		while (m_ch == '#') {
			//--------
			// Skip to the end of line
			//--------
			while (!m_atEOF && m_ch != '\n') {
				nextChar();
			}
			if (m_ch == '\n') {
				nextChar();
			}
			//--------
			// Skip leading white space on the next line
			//--------
			while (m_ch.isSpace()) {
				nextChar();
			}
			//--------
			// Potentially loop around again to consume
			// more comment lines that follow immediately.
			//--------
		}
		//--------
		// Now use (a guaranteed single level of) recursion
		// to obtain the next (non-comment) token.
		//--------
		nextToken(token);
		return;
	}

	//--------
	// Is it a function or identifier?
	//--------
	if (isIdentifierChar(m_ch)) {
		//--------
		// Consume all the identifier characters
		// but not an immediately following "(", if any
		//--------
		spelling.append(m_ch.c_str());
		nextChar();
		while (!m_atEOF && isIdentifierChar(m_ch)) {
			spelling.append(m_ch.c_str());
			nextChar();
		}

		//--------
		// If "(" follows immediately then it is (supposed to be)
		// a function.
		//--------
		if (m_ch == '(') {
			spelling.append(m_ch.c_str());
			nextChar();
			searchForFunction(spelling.c_str(), found, funcType, symbol);
			if (found) {
				token.reset(symbol, lineNum, spelling.c_str(), funcType);
			} else {
				token.reset(LEX_UNKNOWN_FUNC_SYM, lineNum, spelling.c_str());
			}
			return;
		}

		//--------
		// It's not a function so it looks like an identifier.
		// Better check it's a legal identifier.
		//--------
		if (strcmp(spelling.c_str(), ".") == 0) {
			token.reset(LEX_SOLE_DOT_IDENT_SYM, lineNum, spelling.c_str());
		} else if (strstr(spelling.c_str(), "..") != 0) {
			token.reset(LEX_TWO_DOTS_IDENT_SYM, lineNum, spelling.c_str());
		} else {
			try {
				m_uidIdentifierProcessor->expand(spelling);
				token.resetWithOwnership(LEX_IDENT_SYM, lineNum, spelling);
			} catch (const ConfigurationException &) {
				token.resetWithOwnership(LEX_ILLEGAL_IDENT_SYM, lineNum,
				                         spelling);
			}
		}
		return;
	}

	//--------
	// None of the above
	//--------
	spelling << m_ch.c_str();
	nextChar();
	token.reset(LEX_UNKNOWN_SYM, lineNum, spelling.c_str());
}



//----------------------------------------------------------------------
// Function:	consumeBlockString()
//
// Description:	Consume a string from the input file and return the
//		relevant token.
//----------------------------------------------------------------------

void
LexBase::consumeBlockString(LexToken &token)
{
	StringBuffer		spelling;
	MBChar				prevCh;
	int					lineNum;

	//--------
	// Note the line number at the start of the string
	//--------
	lineNum = m_lineNum;

	//--------
	// Consume chars until we get to "%>"
	//--------
	prevCh = ' ';
	while (!(prevCh == '%' && m_ch == '>')) {
		if (m_atEOF) {
			token.reset(LEX_BLOCK_STRING_WITH_EOF_SYM, lineNum,
			            spelling.c_str());
			return;
		}
		spelling << m_ch.c_str();
		prevCh = m_ch;
		nextChar();
	}

	//--------
	// Spelling contains the string followed by '%'.
	// Remove that unwanted terminating character.
	//--------
	spelling.deleteLastChar();
	nextChar(); // consumer the '>'

	//--------
	// At the end of the string.
	//--------
	token.resetWithOwnership(LEX_STRING_SYM, lineNum, spelling);
	return;
}



//----------------------------------------------------------------------
// Function:	consumeString()
//
// Description:	Consume a string from the input file and return the
//		relevant token.
//----------------------------------------------------------------------

void
LexBase::consumeString(LexToken &token)
{
	StringBuffer			spelling;
	StringBuffer			msg;

	assert(m_ch == '"');

	//--------
	// Note the line number at the start of the string
	//--------
	const int	lineNum = m_lineNum;

	//--------
	// Consume chars until we get to the end of the sting
	//--------
	nextChar();
	while (m_ch != '"') {
		if (m_atEOF || m_ch.c_str()[0] == '\n') {
			token.reset(LEX_STRING_WITH_EOL_SYM, lineNum, spelling.c_str());
			return;
		}
		switch (m_ch.c_str()[0]) {
		case '%':
			//--------
			// Escape char in string
			//--------
			nextChar();
			if (m_atEOF || m_ch.c_str()[0] == '\n') {
				token.reset(LEX_STRING_WITH_EOL_SYM, lineNum, spelling.c_str());
				return;
			}
			switch (m_ch.c_str()[0]) {
			case 't':
				spelling << '\t';
				break;
			case 'n':
				spelling << '\n';
				break;
			case '%':
				spelling << '%';
				break;
			case '"':
				spelling << '"';
				break;
			default:
				msg << "Invalid escape sequence (%" << m_ch.c_str()[0]
					<< ") in string on line " << m_lineNum;
				throw ConfigurationException(msg.c_str());
			}
			break;
		default:
			//--------
			// Typical char in string
			//--------
			spelling << m_ch.c_str();
			break;
		}
		nextChar();
	}
	nextChar();	// consume the terminating double-quote char

	//--------
	// At the end of the string.
	//--------
	token.resetWithOwnership(LEX_STRING_SYM, lineNum, spelling);
	return;
}



//----------------------------------------------------------------------
// Function:	isKeywordChar()
//
// Description:	Determine if the parameter is a char that can appear
//		in a keyword (after the initial "@").
//----------------------------------------------------------------------

bool
LexBase::isKeywordChar(const MBChar & mbCh)
{
	wchar_t				wCh;

	wCh = mbCh.getWChar();
	if ('A' <= wCh  && wCh <= 'Z') { return true; }
	if ('a' <= wCh  && wCh <= 'z') { return true; }
	return false;
}



//----------------------------------------------------------------------
// Function:	isIdentifierChar()
//
// Description:	Determine if the parameter is a char that can appear
//		in an identifier.
//----------------------------------------------------------------------

bool
LexBase::isIdentifierChar(const MBChar & mbCh)
{
	wchar_t				wCh;
	bool				result;

	wCh = mbCh.getWChar();
	result =   (::iswalpha(wCh) != 0) // letter
		|| (::iswdigit(wCh) != 0) // digit
		|| mbCh == '-'   // dash
		|| mbCh == '_'   // underscore
		|| mbCh == '.'   // dot
		|| mbCh == ':'   // For C++ nested names, e.g., Foo::Bar
		|| mbCh == '$'   // For mangled names of Java nested classes
		|| mbCh == '?'   // For Ruby identifiers, e.g., found?
		|| mbCh == '/'   // For URLs, e.g., http://foo.com/bar/
		|| mbCh == '\\'  // For Windows directory names
		;
	return result;
}

}; // namespace CONFIG4CPP_NAMESPACE
