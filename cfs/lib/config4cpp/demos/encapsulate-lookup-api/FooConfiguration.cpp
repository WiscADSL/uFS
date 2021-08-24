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
#include "FallbackConfiguration.h"
#include <stdio.h>
#include <stdlib.h>
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

FooConfiguration::FooConfiguration()
{
	m_cfg = Configuration::create();
	m_scope = 0;
}



FooConfiguration::~FooConfiguration()
{
	delete [] m_scope;
	((Configuration *)m_cfg)->destroy();
}


void
FooConfiguration::parse(
	const char *		cfgSource,
	const char *		scope) throw (FooConfigurationException)
{
	Configuration * cfg = (Configuration *)m_cfg;

	m_scope = new char[strlen(scope) + 1];
	strcpy(m_scope, scope);
	try {
		if (cfgSource != 0 && strcmp(cfgSource, "") != 0) {
			cfg->parse(cfgSource);
		}
		cfg->setFallbackConfiguration(Configuration::INPUT_STRING,
		                              FallbackConfiguration::getString());
	} catch(const ConfigurationException & ex) {
		throw FooConfigurationException(ex.c_str());
	}
}



const char *
FooConfiguration::lookupString(const char * name) const
											throw (FooConfigurationException)
{
	Configuration * cfg = (Configuration *)m_cfg;
	try {
		return cfg->lookupString(m_scope, name);
	} catch(const ConfigurationException & ex) {
		throw FooConfigurationException(ex.c_str());
	}
}



void
FooConfiguration::lookupList(
	const char *			name,
	const char **&			array,
	int &					arraySize) const throw (FooConfigurationException)
{
	Configuration * cfg = (Configuration *)m_cfg;
	try {
		cfg->lookupList(m_scope, name, array, arraySize);
	} catch(const ConfigurationException & ex) {
		throw FooConfigurationException(ex.c_str());
	}
}



int
FooConfiguration::lookupInt(const char * name) const
											throw (FooConfigurationException)
{
	Configuration * cfg = (Configuration *)m_cfg;
	try {
		return cfg->lookupInt(m_scope, name);
	} catch(const ConfigurationException & ex) {
		throw FooConfigurationException(ex.c_str());
	}
}



float
FooConfiguration::lookupFloat(const char * name) const
											throw (FooConfigurationException)
{
	Configuration * cfg = (Configuration *)m_cfg;
	try {
		return cfg->lookupFloat(m_scope, name);
	} catch(const ConfigurationException & ex) {
		throw FooConfigurationException(ex.c_str());
	}
}



bool
FooConfiguration::lookupBoolean(const char * name) const
											throw (FooConfigurationException)
{
	Configuration * cfg = (Configuration *)m_cfg;
	try {
		return cfg->lookupBoolean(m_scope, name);
	} catch(const ConfigurationException & ex) {
		throw FooConfigurationException(ex.c_str());
	}
}



int
FooConfiguration::lookupDurationMilliseconds(const char * name) const
											throw (FooConfigurationException)
{
	Configuration * cfg = (Configuration *)m_cfg;
	try {
		return cfg->lookupDurationMilliseconds(m_scope, name);
	} catch(const ConfigurationException & ex) {
		throw FooConfigurationException(ex.c_str());
	}
}



int
FooConfiguration::lookupDurationSeconds(const char * name) const
											throw (FooConfigurationException)
{
	Configuration * cfg = (Configuration *)m_cfg;
	try {
		return cfg->lookupDurationSeconds(m_scope, name);
	} catch(const ConfigurationException & ex) {
		throw FooConfigurationException(ex.c_str());
	}
}

