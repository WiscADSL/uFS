This directory contains a large collection of unit tests for the
SchemaType<Type> classes. The driver for the test suite is contained in
the "main.cpp" file. The code opens the "schema-type-tests.cfg" file
and extracts:

	- The schema in the "testSchema" variable.

	- A list of sub-scopes in the "good" scope.

	- A list of sub-scopes in the "bad" scope.

The application checks that:

	- All the "good.<sub-scope>" scopes validate against the schema.

	- All the "bad.<sub-scope>" scopes do *not* validate against the
	  schema.

With that infrastructure in place, each unit test consists of:
(1) defining schema rules that permit us to test all the built-in
schema types (without and without arguments); and (2) defining
"good.<sub-scope>" and "bad.<sub-scope>" scopes against which to test
those schema rules.

