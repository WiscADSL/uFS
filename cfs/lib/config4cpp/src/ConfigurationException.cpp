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
#include <config4cpp/ConfigurationException.h>
#include <string.h>


namespace CONFIG4CPP_NAMESPACE {

ConfigurationException::ConfigurationException(const char * str)
{
	m_str = new char[strlen(str) + 1];
	strcpy(m_str, str);
}



ConfigurationException::ConfigurationException(const ConfigurationException & o)
{
	m_str = new char[strlen(o.m_str) + 1];
	strcpy(m_str, o.m_str);
}



ConfigurationException::~ConfigurationException()
{
	delete [] m_str;
}



const char *
ConfigurationException::c_str() const
{
	return m_str;
}

}; // namespace CONFIG4CPP_NAMESPACE 
