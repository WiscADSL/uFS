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

#ifndef CONFIG4CPP_UID_IDENTIFIER_PROCESSOR_H_
#define CONFIG4CPP_UID_IDENTIFIER_PROCESSOR_H_

//--------
// #include's
//--------
#include <config4cpp/namespace.h>
#include <config4cpp/StringBuffer.h>
#include <config4cpp/ConfigurationException.h>


namespace CONFIG4CPP_NAMESPACE {

class UidIdentifierProcessor
{
public:
	//--------
	// Constructor and destructor
	//--------
	UidIdentifierProcessor();
	virtual ~UidIdentifierProcessor();

	virtual void expand(StringBuffer & spelling) throw (ConfigurationException);

	virtual const char * unexpand(
				const char *		spelling,
				StringBuffer &		buf) const throw (ConfigurationException);

private:
	//--------
	// Instance variables
	//
	// An assert() statement in the constructor checks that
	// the type of "m_count" is at least 32 bits wide.
	//--------
	long				m_count;

	//--------
	// Helper functions
	//--------
	void expandOne(StringBuffer & spelling) throw (ConfigurationException);
	const char * unexpandOne(
				const char *		spelling,
				StringBuffer &		buf) const throw (ConfigurationException);

	//--------
	// The following are not implemented
	//--------
	UidIdentifierProcessor(const UidIdentifierProcessor &);
	UidIdentifierProcessor & operator=(const UidIdentifierProcessor &);
};


}; // namespace CONFIG4CPP_NAMESPACE
#endif
