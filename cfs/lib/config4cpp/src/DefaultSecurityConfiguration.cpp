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

#include "DefaultSecurityConfiguration.h"
#include "DefaultSecurity.h"
#include <stdio.h>
#include <stdlib.h>


namespace CONFIG4CPP_NAMESPACE {

//--------
// Define the singleton object
//--------
DefaultSecurityConfiguration DefaultSecurityConfiguration::singleton;



DefaultSecurityConfiguration::DefaultSecurityConfiguration()
{
	StringVector			dummyList;

	try {
		parse(Configuration::INPUT_STRING, m_cfgStr.getString(),
			  "Config4* default security");
		lookupList("", "allow_patterns", dummyList);
		lookupList("", "deny_patterns", dummyList);
		lookupList("", "trusted_directories", dummyList);
	} catch(const ConfigurationException & ex) {
		fprintf(stderr, "%s\n", ex.c_str());
		fflush(stderr);
		abort();
	}
}



DefaultSecurityConfiguration::~DefaultSecurityConfiguration()
{
	// Nothing to do
}

}; // namespace CONFIG4CPP_NAMESPACE
