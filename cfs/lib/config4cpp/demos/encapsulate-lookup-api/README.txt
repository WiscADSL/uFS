This demo application defines a class that provides the configuration
capabilities for a hypothetical application called "Foo". 

This application requires general purpose lookup-style operations when
accessing configuration information. The "FooConfiguration" class
provides its own lookup-style operations that internally delegate to the
lookup operations of Config4*. In this way, the "FooConfiguration" class
encapsulates the use of Config4*, thus making it easy for a maintainer
of the application to migrate to using a different configuration
technology, should the need ever arise.

The delegation logic is straightforward but a bit verbose due to the
need to surround calls to Config4* with a try-catch clause so the
information inside a Config4* exception can be copied to an
application-specific exception that is thrown. This type of delegation
logic occupies only a few lines of code for each lookup-style operation.
The verbosity arises because Config4* provides so many lookup-style
operations. Config4* has lookup operations for many different types, and
for each of these types the lookup operation is overloaded to provide a
"lookup with a default value" and a "lookup without a default value." To
keep the volume of code manageable, the demo does not provide the
"lookup with a default value" version of operations.  Instead, it uses
fallback configuration to provide default values.

Examples of running the demo:

    demo -h                     (prints a usage statement)
    demo                        (prints values from fallback configuration)
    demo -cfg runtime.cfg -scope foo

