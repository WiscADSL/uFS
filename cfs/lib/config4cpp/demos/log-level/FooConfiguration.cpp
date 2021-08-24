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

#include "FooConfiguration.h"
#include <config4cpp/Configuration.h>
#include <config4cpp/SchemaValidator.h>
#include <string.h>
#include <stdlib.h>

using CONFIG4CPP_NAMESPACE::Configuration;
using CONFIG4CPP_NAMESPACE::ConfigurationException;
using CONFIG4CPP_NAMESPACE::SchemaValidator;



//----------------------------------------------------------------------
// class FooConfigurationException
//----------------------------------------------------------------------

FooConfigurationException::FooConfigurationException(const char * str)
{
	m_str = new char[strlen(str) + 1];
	strcpy(m_str, str);
}



FooConfigurationException::FooConfigurationException(
	const FooConfigurationException & other)
{
	m_str = new char[strlen(other.m_str) + 1];
	strcpy(m_str, other.m_str);
}



FooConfigurationException::~FooConfigurationException()
{
	delete [] m_str;
}



const char *
FooConfigurationException::c_str() const
{
	return m_str;
}





//----------------------------------------------------------------------
// class FooConfiguration
//----------------------------------------------------------------------

FooConfiguration::FooConfiguration()
{
	m_cfg = Configuration::create();
}



FooConfiguration::~FooConfiguration()
{
	((Configuration *)m_cfg)->destroy();
}



void
FooConfiguration::parse(
	const char *		cfgInput,
	const char *		cfgScope,
	const char *		secInput,
	const char *		secScope) throw (FooConfigurationException)
{
	SchemaValidator		sv;
	Configuration *		cfg = (Configuration*)m_cfg;
	const char *		schema[] = {
		"@typedef logLevel = int[0, 4]",
		"log_levels = table[string,operation-name, logLevel,log-level]",
		0,
	};

	try {
		//--------
		// Set non-default security, if supplied. Then parse the
		// configuration input. Finally, perform schema validation.
		//--------
		if (strcmp(secInput, "") != 0) {
			cfg->setSecurityConfiguration(secInput, secScope);
		}
		cfg->parse(cfgInput);
		sv.parseSchema(schema);
		sv.validate(cfg, cfgScope, "");

		//--------
		// Cache configuration variables in instance variables for 
		// faster access.
		//--------
		cfg->lookupList(cfgScope, "log_levels", m_logLevels, m_numLogLevels);
	} catch(const ConfigurationException & ex) {
		throw FooConfigurationException(ex.c_str());
	}
}



Logger::LogLevel
FooConfiguration::getLogLevel(const char * opName) const
{
	int					i;
	int					result;
	const char *		pattern;
	const char *		logLevelStr;

	for (i = 0; i < m_numLogLevels; i += 2) {
		pattern     = m_logLevels[i + 0];
		logLevelStr = m_logLevels[i + 1];
		if (Configuration::patternMatch(opName, pattern)) {
			result = atoi(logLevelStr);
			if (result > (int)Logger::DEBUG_LEVEL) {
				result = (int)Logger::DEBUG_LEVEL;
			} else if (result < 0) {
				result = 0;
			}
			return (Logger::LogLevel)result;
		}
	}
	return Logger::ERROR_LEVEL; // default log level
}

