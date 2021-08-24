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

//--------
// #include's
//--------
#include "FooConfiguration.h"
#include "A.h"
#include "B.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <locale.h>


//--------
// Forward declarations
//--------
static void
parseCmdLineArgs(
	int					argc,
	char **				argv,
	const char *&		cfgInput,
	const char *&		cfgScope,
	const char *&		secInput,
	const char *&		secScope);
static void usage();





int
main(int argc, char ** argv)
{
	FooConfiguration *		cfg = new FooConfiguration();
	const char *			cfgInput;
	const char *			cfgScope;
	const char *			secInput;
	const char *			secScope;
	A *						aObj;
	B *						bObj;

	setlocale(LC_ALL, "");
	parseCmdLineArgs(argc, argv, cfgInput, cfgScope, secInput, secScope);

	//--------
	// Parse the configuration file.
	//--------
	try {
		cfg->parse(cfgInput, cfgScope, secInput, secScope);
	} catch(const FooConfigurationException & ex) {
		fprintf(stderr, "%s\n", ex.c_str());
		delete cfg;
		exit(1);
	}

	//--------
	// Create some application objects, and set their log levels
	// via configuration.
	//--------
	aObj = new A(cfg);
	bObj = new B(cfg);

	//--------
	// Invoke operations. Diagnostics will be written to standard output.
	//--------
	aObj->op1();
	aObj->op2();
	aObj->op3();

	bObj->op1();
	bObj->op2();
	bObj->op3();

	//--------
	// Terminate gracefully.
	//--------
	delete aObj;
	delete bObj;
	delete cfg;
	return 0;
}



static void
parseCmdLineArgs(
	int					argc,
	char **				argv,
	const char *&		cfgInput,
	const char *&		cfgScope,
	const char *&		secInput,
	const char *&		secScope)
{
	int					i;

	cfgInput = "";
	cfgScope = "";
	secInput = "";
	secScope = "";

	for (i = 1; i < argc; i++) {
		if (strcmp(argv[i], "-h") == 0) {
			usage();
		} else if (strcmp(argv[i], "-cfg") == 0) {
			if (i == argc-1) { usage(); }
			cfgInput = argv[i+1];
			i++;
		} else if (strcmp(argv[i], "-scope") == 0) {
			if (i == argc-1) { usage(); }
			cfgScope = argv[i+1];
			i++;
		} else if (strcmp(argv[i], "-sec") == 0) {
			if (i == argc-1) { usage(); }
			secInput = argv[i+1];
			i++;
		} else if (strcmp(argv[i], "-secScope") == 0) {
			if (i == argc-1) { usage(); }
			secScope = argv[i+1];
			i++;
		} else {
			fprintf(stderr, "Unrecognised option '%s'\n",
				argv[i]);
			usage();
		}
	}
	if (cfgInput[0] == '\0') {
		fprintf(stderr, "You must specify '-cfg <source>'");
		usage();
	}
}



static void
usage()
{
	fprintf(stderr,
		"\n"
		"usage: demo <options>\n"
		"\n"
		"The <options> can be:\n"
		"  -h             Print this usage statement\n"
		"  -cfg <source>  Parse specified configuration source\n"
		"  -scope         Configuration scope\n"
		"  -sec <source>  Override default security configuration\n"
		"  -secScope      Security configuration scope\n"
		"\n"
		"A configuration <source> can be one of the following:\n"
		"  file.cfg       A configuration file\n"
		"  file#file.cfg  A configuration file\n"
		"  exec#<command> Output from executing the specified command\n"
		"\n");
	exit(1);
}
