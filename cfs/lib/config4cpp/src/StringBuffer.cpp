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
#include <config4cpp/StringBuffer.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>


namespace CONFIG4CPP_NAMESPACE {

StringBuffer::StringBuffer()
{
	m_maxSize	= CONFIG4CPP_STRING_BUFFER_INTERNAL_BUF_SIZE;
	m_buf		= m_internalBuf;
	m_buf[0]	= '\0';
	m_currSize	= 1;
}



StringBuffer::StringBuffer(const char * str)
{
	m_maxSize	= CONFIG4CPP_STRING_BUFFER_INTERNAL_BUF_SIZE;
	m_buf		= m_internalBuf;
	m_buf[0]	= '\0';
	m_currSize	= 1;

	append(str);
}



StringBuffer::StringBuffer(const StringBuffer & other)
{
	m_maxSize	= CONFIG4CPP_STRING_BUFFER_INTERNAL_BUF_SIZE;
	m_buf		= m_internalBuf;
	m_buf[0]	= '\0';
	m_currSize	= 1;

	append(other);
}



StringBuffer::~StringBuffer()
{
	if (m_buf != m_internalBuf) {
		delete [] m_buf;
	}
}



char *
StringBuffer::c_strWithOwnership()
{
	char *		result;

	if (m_buf == m_internalBuf) {
		result = new char[m_currSize];
		strcpy(result, m_buf);
	} else {
		result = m_buf;
		m_maxSize = CONFIG4CPP_STRING_BUFFER_INTERNAL_BUF_SIZE;
	}
	m_buf = m_internalBuf;
	m_buf[0] = '\0';
	m_currSize = 1;
	return result;
}



StringBuffer &
StringBuffer::append(const char *str)
{
	int				len;

	len = strlen(str);
	growIfNeeded(len);
	strcpy(&m_buf[m_currSize-1], str);
	m_currSize += len;
	return *this;
}



StringBuffer &
StringBuffer::append(const StringBuffer & other)
{
	int				len;

	len = other.length();
	growIfNeeded(len);
	strcpy(&m_buf[m_currSize-1], other.c_str());
	m_currSize += len;
	return *this;
}



StringBuffer &
StringBuffer::append(int val)
{
	char			str[64]; // Big enough

	sprintf(str, "%d", val);
	append(str);
	return *this;
}



StringBuffer &
StringBuffer::append(float val)
{
	char			str[64]; // Big enough

	sprintf(str, "%f", val);
	append(str);
	return *this;
}



StringBuffer &
StringBuffer::append(char ch)
{
	growIfNeeded(1);
	m_buf[m_currSize-1] = ch;
	m_buf[m_currSize] = '\0';
	m_currSize++;
	return *this;
}



StringBuffer &
StringBuffer::operator=(const char * str)
{
	m_buf[0]   = '\0';
	m_currSize = 1;
	append(str);
	return *this;
}



StringBuffer &
StringBuffer::operator=(const StringBuffer & other)
{
	m_buf[0]   = '\0';
	m_currSize = 1;
	append(other.m_buf);
	return *this;
}



void
StringBuffer::takeOwnershipOfStringIn(StringBuffer & other)
{
	if (other.m_buf == other.m_internalBuf) {
		m_currSize = other.m_currSize;
		strcpy(m_buf, other.c_str());
	} else {
		if (m_buf != m_internalBuf) {
			delete [] m_buf;
		}
		m_buf       = other.m_buf;
		m_currSize  = other.m_currSize;
		m_maxSize   = other.m_maxSize;
		other.m_buf = other.m_internalBuf;
	}
	other.m_maxSize	 = CONFIG4CPP_STRING_BUFFER_INTERNAL_BUF_SIZE;
	other.m_buf[0]	 = '\0';
	other.m_currSize = 1;
}



void
StringBuffer::growIfNeeded(int len)
{
	int				newSize;
	char *			newBuf;

	if (m_currSize + len < m_maxSize) {
		//--------
		// It's already big enough
		//--------
		return;
	}

	//--------
	// Have to re-allocate a larger buffer.
	//--------
	newSize = m_maxSize * 2;
	if (newSize < m_currSize + len) {
		newSize = (m_currSize+len) * 2;
	}
	newBuf = new char[newSize];
	m_maxSize = newSize;

	//--------
	// Copy old buffer contents to new buffer
	// and delete the old buffer if it was heap allocated.
	//--------
	strcpy(newBuf, m_buf);
	if (m_buf != m_internalBuf) {
		delete [] m_buf;
	}
	m_buf = newBuf;
}

}; // namespace CONFIG4CPP_NAMESPACE
