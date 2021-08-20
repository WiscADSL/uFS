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

#ifndef	CONFIG4CPP_LEX_TOKEN_H_
#define	CONFIG4CPP_LEX_TOKEN_H_


//--------
// #include's and #define's
//--------
#include <config4cpp/ConfigurationException.h>
#include <config4cpp/StringBuffer.h>


namespace CONFIG4CPP_NAMESPACE {

class LexToken
{
public:
	//--------
	// Ctor, dtor and assignment operator
	//--------
	LexToken();
	LexToken(short type, int lineNum, const char * spelling);
	virtual ~LexToken();

	//--------
	// Assignment operators
	//--------
	LexToken& operator=(const LexToken&);

	//--------
	// Accessor functions
	//--------
	inline const char *		spelling();
	inline int				lineNum();
	inline short			type();
	const char *			typeAsString();
	bool					isStringFunc();
	bool					isListFunc();
	bool					isBoolFunc();

	//--------
	// Modifier function
	//--------
	void reset(
			short			type,
			int				lineNum,
			const char *	spelling);

	void reset(
			short			type,
			int				lineNum,
			const char *	spelling,
			short			funcType);

	void resetWithOwnership(
			short			type,
			int				lineNum,
			StringBuffer &	str);

protected:
	//--------
	// Instance variables
	//--------
	short				m_type;
	StringBuffer			m_spelling;
	int				m_lineNum;
	short				m_funcType;
};


//--------
// Inline implementation of some operations
//--------

inline short        LexToken::type()     { return m_type; }
inline const char * LexToken::spelling() { return m_spelling.c_str(); }
inline int          LexToken::lineNum()  { return m_lineNum; }

}; // namespace CONFIG4CPP_NAMESPACE
#endif
