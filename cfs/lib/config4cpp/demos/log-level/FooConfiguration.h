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

#ifndef FOO_CONFIGURATION_H_
#define FOO_CONFIGURATION_H_

#include "Logger.h"


class FooConfigurationException
{
public:
	//--------
	// Constructors and destructor
	//--------
	FooConfigurationException(const char * str);
	FooConfigurationException(const FooConfigurationException & other);
	~FooConfigurationException();

	const char * c_str() const; // Accessor

private:
	char *			m_str;

	//--------
	// Not implemented
	//--------
	FooConfigurationException();
	FooConfigurationException operator=(const FooConfigurationException &);
};


class FooConfiguration
{
public:
	FooConfiguration();
	~FooConfiguration();

	void parse(
			const char *	cfgInput,
			const char *	cfgScope = "",
			const char *	secInput = "",
			const char *	secScope = "") throw (FooConfigurationException);
	//--------
	// Public operations
	//--------
	Logger::LogLevel getLogLevel(const char * opName) const;

private:
	void *			m_cfg; // opaque pointer to Config4Cpp config object
	const char **	m_logLevels;
	int				m_numLogLevels;

	//--------
	// The following are not implemented
	//--------
	FooConfiguration & operator=(const FooConfiguration &);
	FooConfiguration(const FooConfiguration &);
};

#endif
