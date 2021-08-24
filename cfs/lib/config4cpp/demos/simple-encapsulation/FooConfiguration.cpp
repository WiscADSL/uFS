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
#include "FallbackConfiguration.h"
#include <string.h>

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

FooConfiguration::FooConfiguration(bool wantDiagnostics)
{
	m_wantDiagnostics = wantDiagnostics;
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
	Configuration *		cfg = (Configuration*)m_cfg;
	SchemaValidator		sv;

	try {
		//--------
		// Set non-default security, if supplied
		// Parse config input, if supplied
		// Set fallback configuration
		//--------
		if (strcmp(secInput, "") != 0) {
			cfg->setSecurityConfiguration(secInput, secScope);
		}
		if (strcmp(cfgInput, "") != 0) {
			cfg->parse(cfgInput);
		}
		cfg->setFallbackConfiguration(Configuration::INPUT_STRING,
									  FallbackConfiguration::getString());

		//--------
		// Perform schema validation.
		//--------
		sv.wantDiagnostics(m_wantDiagnostics);
		sv.parseSchema(FallbackConfiguration::getSchema());
		sv.validate(cfg->getFallbackConfiguration(), "", "",
					SchemaValidator::FORCE_REQUIRED);
		sv.validate(cfg, cfgScope, "");

		//--------
		// Cache configuration variables in instance variables for 
		// faster access.
		//--------
		m_connectionTimeout = cfg->lookupDurationMilliseconds(cfgScope,
														"connection_timeout");
		m_rpcTimeout = cfg->lookupDurationMilliseconds(cfgScope, "rpc_timeout");
		m_idleTimeout = cfg->lookupDurationMilliseconds(cfgScope,
														"idle_timeout");
		m_logFile = cfg->lookupString(cfgScope, "log.file");
		m_logLevel = cfg->lookupInt(cfgScope, "log.level");
		m_host = cfg->lookupString(cfgScope, "host");
		m_port = cfg->lookupInt(cfgScope, "port");
	} catch(const ConfigurationException & ex) {
		throw FooConfigurationException(ex.c_str());
	}
}

