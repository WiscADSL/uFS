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
#include "ExtendedSchemaValidator.h"
#include "SchemaTypeHex.h"
#include <string.h>
#include <stdio.h>

using CONFIG4CPP_NAMESPACE::Configuration;
using CONFIG4CPP_NAMESPACE::ConfigurationException;



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
	m_hexList = 0;
	m_hexListSize = 0;
}



FooConfiguration::~FooConfiguration()
{
	((Configuration *)m_cfg)->destroy();
	delete [] m_hexList;
}



void
FooConfiguration::parse(
	const char *		cfgInput,
	const char *		scope,
	const char *		secInput,
	const char *		secScope) throw (FooConfigurationException)
{
	int							i;
	StringBuffer				localName;
	StringVector				strList;
	Configuration *				cfg = (Configuration*)m_cfg;
	ExtendedSchemaValidator		sv;
	const char *				schema[] = {
									"host     = string",
									"timeout  = durationMilliseconds",
									"hex_byte = hex[2]",
									"hex_word = hex[4]",
									"hex_list = list[hex]",
									0 // null-terminated array of strings
								};

	try {
		//--------
		// Set non-default security, if supplied.
		// Parse config input, if supplied.
		//--------
		if (strcmp(secInput, "") != 0) {
			cfg->setSecurityConfiguration(secInput, secScope);
		}
		if (strcmp(cfgInput, "") != 0) {
			cfg->parse(cfgInput);
		}

		//--------
		// Perform schema validation.
		//--------
		sv.wantDiagnostics(m_wantDiagnostics);
		sv.parseSchema(schema);
		sv.validate(cfg, scope, "");

		//--------
		// Cache configuration variables in instance variables for 
		// faster access. We use static utility operations on the
		// SchemaTypeHex class to perform lookupHex() and convert
		// list[hex] to int[].
		//--------
		m_host = cfg->lookupString(scope, "host");
		m_timeout = cfg->lookupDurationMilliseconds(scope, "timeout");
		m_hexByte = SchemaTypeHex::lookupHex(cfg, scope, "hex_byte");
		m_hexWord = SchemaTypeHex::lookupHex(cfg, scope, "hex_word");
		cfg->lookupList(scope, "hex_list", strList);
		m_hexListSize = strList.length();
		m_hexList = new int[m_hexListSize];
		for (i = 0; i < m_hexListSize; i++) {
			localName.empty();
			localName << "hex_list[" << (i+1) << "]";
			m_hexList[i] = SchemaTypeHex::stringToHex(cfg, scope,
												localName.c_str(), strList[i]);
		}
	} catch(const ConfigurationException & ex) {
		throw FooConfigurationException(ex.c_str());
	}
}

