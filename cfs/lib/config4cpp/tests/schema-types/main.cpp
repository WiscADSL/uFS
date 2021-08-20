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
#include <config4cpp/Configuration.h>
#include <config4cpp/SchemaValidator.h>
using config4cpp::Configuration;
using config4cpp::ConfigurationException;
using config4cpp::SchemaValidator;
using config4cpp::StringBuffer;
using config4cpp::StringVector;
#include <stdio.h>
#include <assert.h>
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
	const char *&			cfgFile,
	bool &					wantDiagnostics);
static void usage();



int
main(int argc, char ** argv)
{
	Configuration *			cfg = Configuration::create();
	const char *			cfgFile;
	const char *			exPattern;
	bool					wantDiagnostics;
	StringBuffer			buf;
	StringVector			testSchema;
	StringVector			goodScopes;
	StringVector			badScopes;
	SchemaValidator			fileSv;
	SchemaValidator			testSv;
	int						totalCount = 0;
	int						passedCount = 0;
	int						i;
	int						len;
	const char *			fileSchema[] = {
									"testSchema = list[string]",
									"good = scope",
									"bad = scope",
									"@ignoreScopesIn good",
									"@ignoreScopesIn bad",
									0 // null-terminated array
	};

	setlocale(LC_ALL, "");
	parseCmdLineArgs(argc, argv, cfgFile, wantDiagnostics);

	//--------
	// Parse and validate the configuration file. Then get the testSchema
	// and lists of the names of good and bad sub-scopes.
	//--------
	try {
		cfg->parse(cfgFile);
		fileSv.parseSchema(fileSchema);
		fileSv.validate(cfg, "", "");

		cfg->lookupList("testSchema", "", testSchema);
		cfg->listFullyScopedNames("good", "", Configuration::CFG_SCOPE, false,
								  goodScopes);
		cfg->listFullyScopedNames("bad", "", Configuration::CFG_SCOPE, false,
								  badScopes);
		testSv.wantDiagnostics(wantDiagnostics);
		testSv.parseSchema(testSchema.c_array());
	} catch(const ConfigurationException & ex) {
		printf("%s\n", ex.c_str());
		return 1;
	}

	//--------
	// Schema validation should succeed for every sub-scope withinin "good".
	//--------
	len = goodScopes.length();
	for (i = 0; i < len; i++) {
		try {
			testSv.validate(cfg, goodScopes[i], "");
			passedCount ++;
		} catch(const ConfigurationException & ex) {
			try {
				cfg->dump(buf, true, goodScopes[i], "");
			} catch(const ConfigurationException &ex2) {
				assert(0); // Bug!
			}
			printf("\n\n--------\n");
			printf("%s\n\n%s--------\n\n", ex.c_str(), buf.c_str());
		}
	}

	//--------
	// Schema validation should fail for every sub-scope within "bad".
	//--------
	len = badScopes.length();
	for (i = 0; i < len; i++) {
		try {
			exPattern = cfg->lookupString(badScopes[i], "exception");
			testSv.validate(cfg, badScopes[i], "");
			try {
				cfg->dump(buf, true, badScopes[i], "");
			} catch(const ConfigurationException &ex2) {
				assert(0); // Bug!
			}
			printf("\n\n--------\n");
			printf("Validation succeeded for scope '%s'\n%s--------\n\n",
					badScopes[i], buf.c_str());
		} catch(const ConfigurationException & ex) {
			if (Configuration::patternMatch(ex.c_str(), exPattern)) {
				passedCount ++;
			} else {
				printf("\n\n--------\n");
				printf("Unexpected exception for scope \"%s\"\n"
						"Pattern \"%s\" does not match "
						"exception\n\"%s\"\n--------\n\n",
						badScopes[i], exPattern, ex.c_str());
			}
		}
	}

	totalCount = goodScopes.length() + badScopes.length();
	printf("\n%d tests out of %d passed\n\n", passedCount, totalCount);

	cfg->destroy();
	return (int)(passedCount != totalCount);
}



static void
parseCmdLineArgs(
	int						argc,
	char **					argv,
	const char *&			cfgFile,
	bool &					wantDiagnostics)
{
	int						i;

	cfgFile = 0;
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
			cfgFile = argv[i+1];
			i++;
		} else {
			fprintf(stderr, "\nUnrecognised option '%s'\n", argv[i]);
			usage();
		}
	}
	if (cfgFile == 0) {
		fprintf(stderr, "\nYou must specify '-cfg <file.cfg>'\n");
		usage();
	}
}



static void
usage()
{
	fprintf(stderr,
		"\n"
		"usage: schema-testsuite <options>\n"
		"\n"
		"The <options> can be:\n"
		"  -h               Print this usage statement\n"
		"  -cfg <file.cfg>  Parse specified configuration file\n"
		"  -diagnostics     Prints diagnostics from schema validator\n"
		"  -nodiagnostics   No diagnostics (default)\n"
		"\n");
	exit(1);
}

