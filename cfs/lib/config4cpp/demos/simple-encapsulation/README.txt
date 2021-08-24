This demo application defines a class that provides the configuration
capabilities for a hypothetical application called "Foo". 

This application requires only seven configuration variables so, rather
than expose general purpose lookup-style operations, the
FooConfiguration class provides accessor operations for these seven
pieces of configuration. The class surrounds calls to Config4* with a
try-catch clause so the information inside a Config4* exception can be
copied to an application-specific exception that is thrown. In these
simple ways, the FooConfiguration class encapsulates the use of
Config4*, thus making it easy for a maintainer of the application to
migrate to using a different configuration technology, should the need
ever arise.

Despite the FooConfiguration class having a simple-to-use API, it
encapsulates some powerful features of Config4*. First, the class allows
the default security policy to be overridden. Second, the class uses
fallback configuration so users can run the Foo application without a
configuration file. The source used for the fallback configuration is an
embedded string that the Makefile generates by use of the "config2cpp"
utility.

The FooConfiguration::parse() operation takes four string parameters
that specify the configuration source (a file or "exec#...") and scope
within it, and a security source and scope within it. Empty strings can
be passed for any or all of these parameters, thus making a
user-specified configuration source and user-specified security policy
optional.

If the FooConfiguration class were re-implemted to use, say, an XML
file, then the four parameters to parse() would likey be replaced by a
single parameter denoting the name of the XML file or Java properties
file. This change in the public API of FooConfiguration is likely to
have minimal knock-on effects in the rest of the Foo application. In
particular, the parsing of command-line arguments and subsequent
creation of a FooConfiguration object are the only pieces of code likely
to be affected.

Examples of running the demo:

    demo -h                     (prints a usage statement)
    demo                        (prints values from fallback configuration)
    demo -cfg runtime.cfg -scope foo

