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
// #includes & #defines
//--------
#include <config4cpp/StringVector.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>


namespace CONFIG4CPP_NAMESPACE {

static char *
copyString(const char * str)
{
	char *		result;

	result = new char[strlen(str) + 1];
	strcpy(result, str);
	return result;
}



StringVector::StringVector(int initialCapacity)
{
	int				i;

	assert(initialCapacity > 0);
	m_currSize = 0;
	m_maxSize  = initialCapacity;
	m_array    = new char *[m_maxSize + 1];
	m_array[0] = 0;
}



StringVector::StringVector(const StringVector & o)
{
	int				i;

	m_currSize = o.m_currSize;
	m_maxSize  = o.m_maxSize;
	m_array    = new char *[m_maxSize + 1];
	for (i = 0; i < m_currSize; i++) {
		m_array[i] = copyString(o[i]);
	}
	m_array[m_currSize] = 0;
}


StringVector & 
StringVector::operator=(const StringVector & o)
{
	int				i;

	ensureCapacity(o.m_currSize);
	for (i = 0; i < m_currSize; i++) {
		delete m_array[i];
		m_array[i] = 0;
	}

	m_currSize = o.m_currSize;
	for (i = 0; i < m_currSize; i++) {
		m_array[i] = copyString(o[i]);
	}
	return *this;
}



StringVector::~StringVector()
{
	int				i;

	for (i = 0; i < m_currSize; i++) {
		delete [] m_array[i];
	}
	delete [] m_array;
}



void
StringVector::add(const char * str)
{
	if (m_currSize == m_maxSize) {
		ensureCapacity(m_maxSize * 2);
	}
	m_array[m_currSize] = copyString(str);
	m_array[m_currSize + 1] = 0;
	m_currSize ++;
}



void
StringVector::add(const StringBuffer & strBuf)
{
	add(strBuf.c_str());
}



void
StringVector::addWithOwnership(StringBuffer & strBuf)
{
	if (m_currSize == m_maxSize) {
		ensureCapacity(m_maxSize * 2);
	}
	m_array[m_currSize] = strBuf.c_strWithOwnership();
	m_array[m_currSize + 1] = 0;
	m_currSize ++;
}



void
StringVector::replace(int index, const char * str)
{
	assert(index < m_currSize);
	delete [] m_array[index];
	m_array[index] = copyString(str);
}



void
StringVector::replaceWithOwnership(int index, char * str)
{
	assert(index < m_currSize);
	delete [] m_array[index];
	m_array[index] = str;
}



extern "C" int
CONFIG4CPP_C_PREFIX(compareFn)(const void * p1, const void* p2)
{
	const char *		str1;
	const char *		str2;

	str1 = *((const char **)p1);
	str2 = *((const char **)p2);
	return strcmp(str1, str2);
}



void
StringVector::sort()
{
	qsort(m_array, m_currSize, sizeof(char *), CONFIG4CPP_C_PREFIX(compareFn));
}



bool
StringVector::bSearchContains(const char * str) const
{
	void *				result;

	result = bsearch(&str, m_array, m_currSize, sizeof(char *),
					 CONFIG4CPP_C_PREFIX(compareFn));
	return (result != 0);
}



int
StringVector::length() const
{
	return m_currSize;
}



void
StringVector::ensureCapacity(int size)
{
	char **			oldArray;
	int				i;

	if (size <= m_maxSize) {
		return;
	}

	oldArray  = m_array;
	m_maxSize = size;
	m_array   = new char *[m_maxSize + 1];

	for (i = 0; i < m_currSize; i++) {
		m_array[i] = oldArray[i];
	}
	m_array[m_currSize + 1] = 0;
	delete [] oldArray;
}



void
StringVector::add(const StringVector & other)
{
	int				i;
	int				otherLen;

	otherLen = other.length();
	if (m_currSize + otherLen >= m_maxSize) {
		ensureCapacity( (m_currSize + otherLen) * 2 );
	}

	for (i = 0; i < otherLen; i++) {
		add(other[i]);
	}
}



void
StringVector::addWithOwnership(StringVector & other)
{
	int				i;
	int				otherLen;

	otherLen = other.length();
	if (m_currSize + otherLen >= m_maxSize) {
		ensureCapacity( (m_currSize + otherLen) * 2 );
	}

	for (i = 0; i < otherLen; i++) {
		m_array[m_currSize] = other.m_array[i];
		other.m_array[i] = 0;
		m_currSize ++;
	}
	m_array[m_currSize + 1] = 0;
	other.m_currSize = 0;
	other.m_array[0] = 0;
}



void
StringVector::c_array(const char **& array, int & arraySize) const
{
	assert(m_array[m_currSize] == 0);
	array = (const char**)m_array;
	arraySize = m_currSize;
}



const char **
StringVector::c_array() const
{
	assert(m_array[m_currSize] == 0);
	return (const char **)m_array;
}



void
StringVector::empty()
{
	int				i;

	for (i = 0; i < m_currSize; i++) {
		delete [] m_array[i];
	}
	m_currSize = 0;
	m_array[0] = 0;
}



void
StringVector::removeLast()
{
	assert(m_currSize > 0);
	
	delete [] m_array[m_currSize-1];
	m_array[m_currSize-1] = 0;
	m_currSize --;
}



const char *
StringVector::operator[](int index) const
{
	assert(index < m_currSize);
	return m_array[index];
}

}; // namespace CONFIG4CPP_NAMESPACE
