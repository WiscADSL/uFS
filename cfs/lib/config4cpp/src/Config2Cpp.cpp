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
#include "Config2Cpp.h"
#include <string.h>


namespace CONFIG4CPP_NAMESPACE {

//--------
// Forward declarations of static functions
//--------
static char * stringConcat(const char* s1, const char* s2, const char* s3 = "");



//--------
// Inline functions
//--------
inline char * stringCopy(const char * s1) { return stringConcat(s1, ""); }



//----------------------------------------------------------------------
// Function:	Constructor
//
// Description:	
//----------------------------------------------------------------------

Config2Cpp::Config2Cpp(const char * progName)
{
	m_progName            = stringCopy(progName);
	m_cfgFileName         = 0;
	m_schemaOverrideCfg   = 0;
	m_schemaOverrideScope = stringCopy("");
	m_className           = 0;
	m_cppExt              = 0;
	m_hExt                = 0;
	m_wantSingleton       = false;
	m_wantSchema          = true;
	m_namespaceArraySize  = 0;
	m_namespaceArray      = 0;
}



//----------------------------------------------------------------------
// Function:	Destructor
//
// Description:	
//----------------------------------------------------------------------

Config2Cpp::~Config2Cpp()
{
	int				i;

	delete [] m_progName;
	delete [] m_cfgFileName;
	delete [] m_schemaOverrideCfg;
	delete [] m_schemaOverrideScope;
	delete [] m_className;
	delete [] m_cppExt;
	delete [] m_hExt;
	for (i = 0; i < m_namespaceArraySize; i++) {
		delete [] m_namespaceArray[i];
	}
	delete [] m_namespaceArray;
}



//----------------------------------------------------------------------
// Function:	parseCmdLineArgs()
//
// Description:	
//----------------------------------------------------------------------

bool
Config2Cpp::parseCmdLineArgs(int argc, char ** argv)
{
	int				i;

	for (i = 1; i < argc; i++) {
		if (strcmp(argv[i], "-cfg") == 0) {
			if (i == argc-1) {
				usage("");
				return false;
			}
			m_cfgFileName = stringCopy(argv[i+1]);
			i++;
		} else if (strcmp(argv[i], "-noschema") == 0) {
			m_wantSchema = false;
		} else if (strcmp(argv[i], "-class") == 0) {
			if (i == argc-1) {
				usage("");
				return false;
			}
			m_className = stringCopy(argv[i+1]);
			i++;
		} else if (strcmp(argv[i], "-schemaOverrideCfg") == 0) {
			if (i == argc-1) {
				usage("");
				return false;
			}
			m_schemaOverrideCfg = stringCopy(argv[i+1]);
			i++;
		} else if (strcmp(argv[i], "-schemaOverrideScope") == 0) {
			if (i == argc-1) {
				usage("");
				return false;
			}
			m_schemaOverrideScope = stringCopy(argv[i+1]);
			i++;
		} else if (strcmp(argv[i], "-cpp") == 0) {
			if (i == argc-1) {
				usage("");
				return false;
			}
			m_cppExt = stringCopy(argv[i+1]);
			i++;
		} else if (strcmp(argv[i], "-h") == 0) {
			if (i == argc-1) {
				usage("");
				return false;
			}
			m_hExt = stringCopy(argv[i+1]);
			i++;
		} else if (strcmp(argv[i], "-namespace") == 0) {
			if (i == argc-1) {
				usage("");
				return false;
			}
			if (!parseNamespace(argv[i+1])) {
				return false;
			}
			i++;
		} else if (strcmp(argv[i], "-singleton") == 0) {
			m_wantSingleton = true;
		} else {
			usage(argv[i]);
			return false;
		}
	}
	if (m_className == 0 || m_cfgFileName == 0) {
		usage("");
		return false;
	}

	if (m_cppExt == 0) {
		m_cppExt = stringCopy(".cpp");
	}
	if (m_hExt == 0) {
		m_hExt = stringCopy(".h");
	}
	return true;
}



//----------------------------------------------------------------------
// Function:	generateFiles()
//
// Description:	
//----------------------------------------------------------------------

bool
Config2Cpp::generateFiles(
	const char * const *	schema,
	int						schemaSize)
{
	char *					msg;
	char *					cppFileName;
	char *					hFileName;
	FILE *					cfgFile;
	FILE *					cppFile;
	FILE *					hFile;

	cppFileName = stringConcat(m_className, m_cppExt);
	hFileName   = stringConcat(m_className, m_hExt);

	//--------
	// Open all the files
	//--------
	cfgFile = fopen(m_cfgFileName, "r");
	if (cfgFile == 0) {
		msg = stringConcat("cannot open '", m_cfgFileName, "'");
		perror(msg);
		delete [] msg;
		delete [] cppFileName;
		delete [] hFileName;
		return false;
	}
	cppFile = fopen(cppFileName, "w");
	if (cppFile == 0) {
		msg = stringConcat("cannot open '", cppFileName, "'");
		perror(msg);
		delete [] msg;
		delete [] cppFileName;
		delete [] hFileName;
		fclose(cfgFile);
		return false;
	}
	hFile = fopen(hFileName, "w");
	if (hFile == 0) {
		msg = stringConcat("cannot open '", hFileName, "'");
		perror(msg);
		delete [] msg;
		delete [] cppFileName;
		delete [] hFileName;
		fclose(cfgFile);
		fclose(cppFile);
		return false;
	}

	//--------
	// Generate the ".h" and ".cpp" files.
	//--------
	printToHeaderFile(hFile, schemaSize);
	printToCppFile(cfgFile, cppFile, schema, schemaSize);

	//--------
	// Tidy up
	//--------
	delete [] cppFileName;
	delete [] hFileName;
	fclose(cfgFile);
	fclose(cppFile);
	fclose(hFile);

	return true;
}



//----------------------------------------------------------------------
// END OF PUBLIC API
//----------------------------------------------------------------------





//----------------------------------------------------------------------
// Function:	parseNamespace()
//
// Description:	Break a string of the form "X::Y::Z" into an array
//              containing the individual namespace names.
//
// Note:        On success, returns true. On failure, calls usage()
//              and returns false.
//----------------------------------------------------------------------

bool
Config2Cpp::parseNamespace(const char * ns)
{
	int						i;
	int						count;
	int						len;
	int						componentLen;
	const char *			str;
	const char *			colonAddr;

	//--------
	// Complain if there are any leading or trailing ':'
	//--------
	len = strlen(ns);
	if (ns[0] == ':' || ns[len-1] == ':') {
		reportInvalidNamespace(ns);
		return false;
	}

	//--------
	// Count how many namespace components there are, and
	// complain if ':' does not come in pairs.
	//--------
	count = 1;
	str = ns;
	while (*str != '\0') {
		str ++;
		if (*str == ':') {
			str ++;
			if (*str != ':' || *(str+1) == ':') {
				reportInvalidNamespace(ns);
				return false;
			}
			str ++;
			count ++;
		}
	}

	//--------
	// Now we know the namespace is syntactically correct
	// and how many components are in it. Allocate an array
	// of strings to hold the components and iterate over the
	// namespace again to copy each component into the array.
	//--------
	m_namespaceArraySize = count;
	m_namespaceArray = new char *[m_namespaceArraySize];
	str = ns;
	for (i = 0; i < m_namespaceArraySize; i++) {
		//--------
		// Point colonAddr to next ':'
		// (or to '\0' if there are no more occurrances of ':')
		//--------
		colonAddr = strchr(str, ':');
		if (colonAddr == 0) {
			colonAddr = ns + len;
		}
		componentLen = colonAddr - str;
		m_namespaceArray[i] = new char[componentLen + 1];
		strncpy(m_namespaceArray[i], str, componentLen);
		m_namespaceArray[i][componentLen] = '\0';
		str = colonAddr + 2;
	}

#if 0
	//--------
	// Debug diagnostics
	//--------
	printf("There are %d namespace components in '%s'\n",
	       m_namespaceArraySize, ns);
	for (i = 0; i < m_namespaceArraySize; i++) {
		printf("component %d = '%s'\n", i, m_namespaceArray[i]);
	}
#endif

	return true;
}



void
Config2Cpp::reportInvalidNamespace(const char * ns)
{
	fprintf(stderr, "\nInvalid namespace '%s'", ns);
	usage("");
}



//----------------------------------------------------------------------
// Function:	usage()
//
// Description:	Prints a usage statement.
//----------------------------------------------------------------------

void
Config2Cpp::usage(const char * unknownArg)
{
	fprintf(stderr, "\n");
	if (strcmp(unknownArg, "") != 0) {
		fprintf(stderr, "unknown argument '%s'\n", unknownArg);
	}
	fprintf(stderr, "usage: %s -cfg <file.cfg> -class <class>\n",
			m_progName);
	fprintf(stderr, "options are:\n");
	fprintf(stderr, "\t-noschema            Do not generate a schema\n");
	fprintf(stderr, "\t-schemaOverrideCfg   <file.cfg>   \n");
	fprintf(stderr, "\t-schemaOverrideScope <scope> \n");
	fprintf(stderr, "\t-cpp <extension>     default is .cpp\n");
	fprintf(stderr, "\t-h   <extension>     default is .h\n");
	fprintf(stderr, "\t-namespace X::Y::Z   default is the global scope\n");
	fprintf(stderr, "\t-singleton           generate a singleton class\n");
}



//----------------------------------------------------------------------
// Function:	printToHeaderFile()
//
// Description:	Prints contents of generated .h file
//----------------------------------------------------------------------

void
Config2Cpp::printToHeaderFile(FILE * file, int schemaSize)
{
	int				i;

	fprintf(file, "//%s%s\n",
			"-----------------------------------",
			"-----------------------------------");
	fprintf(file, "// WARNING: This file was generated by %s. %s\n",
			m_progName, "Do not edit.");
	fprintf(file, "//\n");
	fprintf(file, "// Description: a class providing %s\n",
		      "access to an embedded");
	fprintf(file, "//              configuration string.\n");
	fprintf(file, "//%s%s\n",
			"-----------------------------------",
			"-----------------------------------");
	fprintf(file, "#ifndef %s_h\n", m_className);
	fprintf(file, "#define %s_h\n", m_className);
	fprintf(file, "\n");
	fprintf(file, "#include <config4cpp/Configuration.h>\n");
	fprintf(file, "\n");
	fprintf(file, "\n");
	if (m_namespaceArraySize != 0) {
		for (i = 0; i < m_namespaceArraySize; i++) {
			fprintf(file, "namespace %s {\n", m_namespaceArray[i]);
		}
		fprintf(file, "\n");
		fprintf(file, "\n");
	}
	fprintf(file, "class %s\n", m_className);
	fprintf(file, "{\n");
	fprintf(file, "public:\n");
	fprintf(file, "\t//--------\n");
	if (m_wantSingleton) {
		fprintf(file, "\t// Constructor and destructor. Ideally %s\n",
		      "these would be private ");
		fprintf(file, "\t// but they have to be public so we can %s\n",
		      "initialize the singleton.");
	} else {
		fprintf(file, "\t// Constructor and destructor.\n");
	}
	fprintf(file, "\t//--------\n");
	fprintf(file, "\t%s();\n", m_className);
	fprintf(file, "\t~%s();\n", m_className);
	fprintf(file, "\n");
	if (m_wantSingleton) {
		fprintf(file, "\t//--------\n");
		fprintf(file, "\t// Get the configuration string from %s\n",
			      "the singleton object");
		fprintf(file, "\t//--------\n");
		fprintf(file, "\tstatic const char * getString()\n");
		fprintf(file, "\t{\n");
		fprintf(file, "\t\treturn s_singleton.m_str.c_str();\n");
		fprintf(file, "\t}\n");
		if (schemaSize > 0) {
			fprintf(file, "\n");
			fprintf(file, "\t//--------\n");
			fprintf(file, "\t// Get the configuration's schema\n");
			fprintf(file, "\t//--------\n");
			fprintf(file, "\tstatic void getSchema(%s, %s)\n",
							"const char **& schema",
							"int & schemaSize");
			fprintf(file, "\t{\n");
			fprintf(file, "\t\tschema = %s.m_schema;\n",
			              "s_singleton");
			fprintf(file, "\t\tschemaSize = %d;\n", schemaSize);
			fprintf(file, "\t}\n");
			fprintf(file, "\tstatic const char ** %s() // %s\n",
			              "getSchema", "null terminated array");
			fprintf(file, "\t{\n");
			fprintf(file, "\t\treturn s_singleton.m_schema;\n");
			fprintf(file, "\t}\n");
		}
	} else {
		fprintf(file, "\t//--------\n");
		fprintf(file, "\t// Get the configuration string\n");
		fprintf(file, "\t//--------\n");
		fprintf(file, "\tconst char * getString()\n");
		fprintf(file, "\t{\n");
		fprintf(file, "\t\treturn m_str.c_str();\n");
		fprintf(file, "\t}\n");
		if (schemaSize > 0) {
			fprintf(file, "\n");
			fprintf(file, "\t//--------\n");
			fprintf(file, "\t// Get the configuration's schema\n");
			fprintf(file, "\t//--------\n");
			fprintf(file, "\tvoid getSchema(%s, %s)\n",
							"const char **& schema",
							"int & schemaSize");
			fprintf(file, "\t{\n");
			fprintf(file, "\t\tschema = m_schema;\n");
			fprintf(file, "\t\tschemaSize = %d;\n", schemaSize);
			fprintf(file, "\t}\n");
			fprintf(file, "\tconst char ** getSchema() // %s\n",
			              "null terminated array");
			fprintf(file, "\t{\n");
			fprintf(file, "\t\treturn m_schema;\n");
			fprintf(file, "\t}\n");
		}
	}
	fprintf(file, "\n");
	fprintf(file, "private:\n");
	fprintf(file, "\t//--------\n");
	fprintf(file, "\t// Variables\n");
	fprintf(file, "\t//--------\n");
	fprintf(file, "\tCONFIG4CPP_NAMESPACE::StringBuffer m_str;\n");
	if (schemaSize > 0) {
		fprintf(file, "\tconst char *                       %s[%d];\n",
			"m_schema", schemaSize + 1);
	}
	if (m_wantSingleton) {
		fprintf(file, "\tstatic %s s_singleton;\n", m_className);
	}
	fprintf(file, "\n");
	fprintf(file, "\t//--------\n");
	fprintf(file, "\t// The following are not implemented\n");
	fprintf(file, "\t//--------\n");
	fprintf(file, "\t%s & operator=(const %s &);\n",
		      m_className, m_className);
	fprintf(file, "\t%s(const %s &);\n", m_className, m_className);
	fprintf(file, "};\n");
	fprintf(file, "\n");
	fprintf(file, "\n");
	if (m_namespaceArraySize != 0) {
		for (i = 0; i < m_namespaceArraySize; i++) {
			fprintf(file, "}; // namespace %s\n",
			       m_namespaceArray[m_namespaceArraySize-(i+1)]);
		}
		fprintf(file, "\n");
		fprintf(file, "\n");
	}
	fprintf(file, "#endif\n");
}



//----------------------------------------------------------------------
// Function:	printToCppFile()
//
// Description:	Prints contents of generated .cpp file
//----------------------------------------------------------------------

void
Config2Cpp::printToCppFile(
	FILE *					cfgFile,
	FILE *					file,
	const char * const *	schema,
	int						schemaSize)
{
	int						i;
	int						ch;
	int						count;

	fprintf(file, "//%s%s\n",
			"-----------------------------------",
			"-----------------------------------");
	fprintf(file, "// WARNING: This file was generated by %s. %s\n",
			m_progName, "Do not edit.");
	fprintf(file, "//%s%s\n",
			"-----------------------------------",
			"-----------------------------------");
	fprintf(file, "\n");
	fprintf(file, "#include \"%s%s\"\n", m_className, m_hExt);
	fprintf(file, "\n\n\n\n\n");
	if (m_namespaceArraySize != 0) {
		for (i = 0; i < m_namespaceArraySize; i++) {
			fprintf(file, "namespace %s {\n", m_namespaceArray[i]);
		}
		fprintf(file, "\n");
		fprintf(file, "\n");
	}
	if (m_wantSingleton) {
		fprintf(file, "//--------\n");
		fprintf(file, "// Define the singleton object\n");
		fprintf(file, "//--------\n");
		fprintf(file, "%s %s::s_singleton;\n",
			m_className, m_className);
		fprintf(file, "\n\n");
	}
	fprintf(file, "%s::%s()\n", m_className, m_className);
	fprintf(file, "{\n");
	if (schemaSize > 0) {
		for (i = 0; i < schemaSize; i++) {
			fprintf(file, "\tm_schema[%d] = \"%s\";\n",
				i, schema[i]);
		}
		fprintf(file, "\tm_schema[%d] = 0;\n", i);
		fprintf(file, "\n");
	}
	fprintf(file, "\tm_str << \"");
	ch = fgetc(cfgFile);
	count = 0;
	while (ch != EOF) {
		if (ch == '\r') { continue; }
		count ++;
		output(file, ch);
		if (ch == '\n' || count == 50) {
			fprintf(file, "\";\n\tm_str << \"");
			count = 0;
		}
		ch = fgetc(cfgFile);
	}
	fprintf(file, "\";\n");
	fprintf(file, "}\n");
	fprintf(file, "\n\n\n\n\n");
	fprintf(file, "%s::~%s()\n", m_className, m_className);
	fprintf(file, "{\n");
	fprintf(file, "\t// Nothing to do\n");
	fprintf(file, "}\n");
	fprintf(file, "\n");
	fprintf(file, "\n");
	if (m_namespaceArraySize != 0) {
		for (i = 0; i < m_namespaceArraySize; i++) {
			fprintf(file, "}; // namespace %s\n",
			       m_namespaceArray[m_namespaceArraySize-(i+1)]);
		}
		fprintf(file, "\n");
	}
}



//----------------------------------------------------------------------
// Function:	output()
//
// Description:	Outputs a single character within a string to the
//		current output file, escaping it if necessary.
//----------------------------------------------------------------------

void
Config2Cpp::output(FILE * file, int ch)
{
	switch (ch) {
	case '\\':
		fputs("\\\\", file);
		break;
	case '\t':
		fputs("\\t", file);
		break;
	case '\n':
		fputs("\\n", file);
		break;
	case '"':
		fputs("\\\"", file);
		break;
	default:
		fputc(ch, file);
		break;
	}
}



//----------------------------------------------------------------------
// Function:	stringConcat()
//
// Description:	Concatenate strings.
//----------------------------------------------------------------------

static char *
stringConcat(const char * s1, const char * s2, const char * s3)
{
	char *			result;
	int				len;

	len = strlen(s1) + strlen(s2) + strlen(s3);
	result = new char[len + 1];
	sprintf(result, "%s%s%s", s1, s2, s3);
	return result;
}

}; // namespace CONFIG4CPP_NAMESPACE
