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

#ifndef CONFIG4CPP_CONFIG2CPP_H_
#define CONFIG4CPP_CONFIG2CPP_H_

//--------
// #include's
//--------
#include <config4cpp/namespace.h>
#include <stdio.h>

namespace CONFIG4CPP_NAMESPACE {

class Config2Cpp
{
public:
	//--------
	// Constructor and destructor
	//--------
	Config2Cpp(const char * progName);
	~Config2Cpp();

	bool parseCmdLineArgs(int argc, char ** argv);
	bool generateFiles(const char * const *	schema, int schemaSize);

	const char * cfgFileName()			{ return m_cfgFileName; }
	const char * schemaOverrideCfg()	{ return m_schemaOverrideCfg; }
	const char * schemaOverrideScope()	{ return m_schemaOverrideScope; }
	const char * className()			{ return m_className; }
	const char * cppExt()				{ return m_cppExt; }
	const char * hExt()					{ return m_hExt; }
	bool         wantSchema()			{ return m_wantSchema; }

private:
	//--------
	// Helper functions
	//--------
	bool parseNamespace(const char * str);
	void reportInvalidNamespace(const char * ns);
	void printToHeaderFile(FILE * file, int namesArraySize);
	void printToCppFile(
				FILE *					cfgFile,
				FILE *					file,
				const char * const *	namesArray,
				int						namesArraySize);
	void output(FILE * file, int ch);
	void usage(const char * unknownArg);

	//--------
	// Instance variables
	//--------
	char *		m_progName;
	char *		m_cfgFileName;
	char *		m_schemaOverrideCfg;
	char *		m_schemaOverrideScope;
	char *		m_className;
	char *		m_cppExt;
	char *		m_hExt;
	bool		m_wantSingleton;
	bool		m_wantSchema;
	int			m_namespaceArraySize;
	char **		m_namespaceArray;
};


}; // namespace CONFIG4CPP_NAMESPACE
#endif
