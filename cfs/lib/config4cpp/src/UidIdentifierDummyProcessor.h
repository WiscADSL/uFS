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

#ifndef CONFIG4CPP_UID_IDENTIFIER_DUMMY_PROCESSOR_H_
#define CONFIG4CPP_UID_IDENTIFIER_DUMMY_PROCESSOR_H_


//--------
// #include's
//--------
#include "UidIdentifierProcessor.h"


namespace CONFIG4CPP_NAMESPACE {

class UidIdentifierDummyProcessor
	: public UidIdentifierProcessor
{
public:
	//--------
	// Constructor and destructor
	//--------
	UidIdentifierDummyProcessor() { }
	virtual ~UidIdentifierDummyProcessor() { }

	virtual void expand(StringBuffer & spelling) throw (ConfigurationException)
	{
		StringBuffer dummy = spelling;
		UidIdentifierProcessor::expand(dummy);
	}
	virtual const char * unexpand(const char * spelling, StringBuffer &)
										const throw (ConfigurationException)
	{
		StringBuffer dummyBuf = spelling;
		(void)UidIdentifierProcessor::unexpand(spelling, dummyBuf);
		return spelling;
	}

private:
	//--------
	// The following are not implemented
	//--------
	UidIdentifierDummyProcessor(const UidIdentifierDummyProcessor &);
	UidIdentifierDummyProcessor & operator=(
					const UidIdentifierDummyProcessor &);
};


}; // namespace CONFIG4CPP_NAMESPACE
#endif
