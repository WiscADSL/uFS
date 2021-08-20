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

#ifndef	CONFIG4CPP_MBCHAR_H_
#define	CONFIG4CPP_MBCHAR_H_


//--------
// #include's and #define's
//--------
#include <config4cpp/namespace.h>
#include <limits.h>
#include <stdlib.h>
#include <wchar.h>
#include <wctype.h>


namespace CONFIG4CPP_NAMESPACE {

class MBChar
{
public:

public:
	//--------
	// Constructors and destructor
	//--------
	MBChar();
	~MBChar();

	//--------
	// Public operations
	//--------
	inline bool			add(char ch);
	inline const char *	c_str() const;
	inline int			length()   const;

	inline void			setWChar(wchar_t wChar);
	inline wchar_t		getWChar() const;

	inline bool			isSpace()  const;
	inline bool			isEmpty()  const;
	inline bool			isFull()   const;

	inline void			reset();

	inline MBChar &		operator=(const MBChar &);
	inline MBChar &		operator=(char ch);
	bool				operator==(const MBChar & other) const;
	inline bool			operator!=(const MBChar & other) const;
	inline bool			operator==(char ch) const;
	inline bool			operator!=(char ch) const;

private:
	//--------
	// Instance variables
	//--------
	char				m_mbChar[MB_LEN_MAX+1];
	short				m_mbCharLen;
	wchar_t				m_wChar;

	//--------
	// Unsupported constructors and assignment operators
	//--------
	MBChar(const MBChar &);
};


//--------
// Inline implementation of operations.
//--------

inline MBChar &
MBChar::operator=(const MBChar & other)
{
	int			i;

	m_mbCharLen = other.m_mbCharLen;
	m_wChar     = other.m_wChar;
	for (i = 0; i < MB_LEN_MAX+1; i++) {
		m_mbChar[i] = other.m_mbChar[i];
	}
	return *this;
}


inline MBChar &
MBChar::operator=(char ch)
{
	m_mbCharLen = 1;
	m_mbChar[0] = ch;
	m_mbChar[1] = '\0';
	m_wChar = 0;
	return *this;
}


inline bool
MBChar::add(char ch)
{
	if (m_mbCharLen < MB_LEN_MAX) {
		m_mbChar[m_mbCharLen] = ch;
		m_mbChar[m_mbCharLen+1] = '\0';
		m_mbCharLen++;
		return true;
	}
	return false;
}


inline void
MBChar::setWChar(wchar_t wChar)
{
	m_wChar = wChar;
}


inline wchar_t
MBChar::getWChar() const
{
	return m_wChar;
}


inline bool
MBChar::isSpace() const
{
	return ::iswspace(m_wChar) != 0;
}


inline int
MBChar::length() const
{
	return m_mbCharLen;
}


inline bool
MBChar::isEmpty() const
{
	return m_mbCharLen == 0;
}


inline bool
MBChar::isFull() const
{
	return m_mbCharLen == MB_LEN_MAX;
}


inline const char *
MBChar::c_str() const
{
	return m_mbChar;
}


inline void
MBChar::reset()
{
	m_mbCharLen = 0;
	m_mbChar[0] = '\0';
	m_wChar = 0;
}


inline bool
MBChar::operator!=(const MBChar & other) const
{
	return !(*this == other);
}


inline bool
MBChar::operator==(char ch) const
{
	return m_mbCharLen == 1 && m_mbChar[0] == ch;
}


inline bool
MBChar::operator!=(char ch) const
{
	return !(m_mbCharLen == 1 && m_mbChar[0] == ch);
}


}; // namespace CONFIG4CPP_NAMESPACE
#endif
