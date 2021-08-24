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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <locale.h>


//--------
// Forward declarations
//--------
static void
parseCmdLineArgs(
	int						argc,
	char **					argv,
	const char *&			cfgInput,
	const char *&			cfgScope,
	const char *&			secInput,
	const char *&			secScope,
	bool &					wantDiagnostics);
static void usage();



int
main(int argc, char ** argv)
{
	FooConfiguration *		cfg = 0;
	const char *			cfgInput;
	const char *			cfgScope;
	const char *			secInput;
	const char *			secScope;
	int						exitStatus = 0;
	bool					wantDiagnostics;

	setlocale(LC_ALL, "");
	parseCmdLineArgs(argc, argv, cfgInput, cfgScope, secInput, secScope,
					 wantDiagnostics);

	try {
		//--------
		// Initialize the configuration object.
		//--------
		cfg = new FooConfiguration(wantDiagnostics);
		cfg->parse(cfgInput, cfgScope, secInput, secScope);

		//--------
		// Query the configuration object.
		//--------
		printf("connection_timeout = %d\n", cfg->getConnectionTimeout());
		printf("rpc_timeout = %d\n", cfg->getRpcTimeout());
		printf("idle_timeout = %d\n", cfg->getIdleTimeout());
		printf("log.file = %s\n", cfg->getLogFile());
		printf("log.level = %d\n", cfg->getLogLevel());
		printf("host = %s\n", cfg->getHost());
		printf("port = %d\n", cfg->getPort());
	} catch(const FooConfigurationException & ex) {
		fprintf(stderr, "%s\n", ex.c_str());
		exitStatus = 1;
	}

	delete cfg;
	return exitStatus;
}



static void
parseCmdLineArgs(
	int						argc,
	char **					argv,
	const char *&			cfgInput,
	const char *&			cfgScope,
	const char *&			secInput,
	const char *&			secScope,
	bool &					wantDiagnostics)
{
	int						i;

	cfgInput = "";
	cfgScope = "";
	secInput = "";
	secScope = "";
	wantDiagnostics = false;

	for (i = 1; i < argc; i++) {
		if (strcmp(argv[i], "-h") == 0) {
			usage();
		} else if (strcmp(argv[i], "-diagnostics") == 0) {
			wantDiagnostics = true;
		} else if (strcmp(argv[i], "-nodiagnostics") == 0) {
			wantDiagnostics = false;
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
			fprintf(stderr, "Unrecognised option '%s'\n\n",
				argv[i]);
			usage();
		}
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
		"  -diagnostics   Prints diagnostics from schema validator\n"
		"  -nodiagnostics No diagnostics (default)\n"
		"\n"
		"A configuration <source> can be one of the following:\n"
		"  file.cfg       A configuration file\n"
		"  file#file.cfg  A configuration file\n"
		"  exec#<command> Output from executing the specified command\n"
		"\n");
	exit(1);
}

