Many applications have the ability to print diagnostic messages (as a
troubleshooting aid when something goes wrong), and use a command-line
option or variable in a configuration file to set the diagnostics level.
A primitive way to control the diagnostics level is to have a, say, a
"-d <int>" command-line option that sets the diagnostics level for the
entire application. However, this simplistic approach  can result in too
many irrelevant diagnostics messages being printed, which can hinder
attempts to diagnose a problem.

A better approach is to provide an independent diagnostic level for each
component in an application. This makes it possible to selectively turn
on diagnostics for some components, while turning off diagnostics for
other components. This flexible approach is becoming increasingly
common, though the granularity of a "component" varies widely. In some
applications, a component might be coarse-grained, such as an entire
functional subsystem or a plug-in. In other applications, a component
might be finer-grained, such as individual classes, or even individual
operations on a class.

If you want to use this "separate log-level for each component"
technique in a Config4*-based application, then you might think of using
a separate configuration variable for each component. For example:

log_level {
    component1 = "2";
    component2 = "0";
    component3 = "0";
};

However, that approach does not scale well: if you have hundreds of
components in your application, then you will need hundreds of
configuration variables to control their log levels.

A better approach is to use a two-column table that provides a
mapping from the wildcarded name of a component to a log level. For
example:

log_level = [
    # wildcarded component name   log level
    #--------------------------------------
    "A::op3",                     "4",
    "B::op1",                     "3",
    "B::*",                       "1",
    "*",                          "0",
];

The string "A::op3" denotes operation op3() in the class A. The wildcard
character ("*") matches zero or more characters, and it might be used to
match, say, the names of all operations within a specific class
("B::*"), all create-style operations, regardless of the class in which
they appear ("*::create*"), or all operations in all classes ("*").

When a component needs to determine its log level, it iterates through
the rows of the table, and uses the log level of the first matching
wildcarded entry. (Config4* provides a patternMatch() operation that an
application can use for this purpose.)

Thus, the last line of the table can specify a default log level (by
using "*" as the wildcarded component name), and earlier lines in the
table can specify a different log level for individual components (or
groups of components). This combination of wildcarding and defaulting
means that the table can remain short even if an application contains
hundreds or thousands of components.

The demo application in this directory illustrates how to use a table to
specify wildcarded log levels.

Examples of running the demo:

    demo -h                             (prints a usage statement)
    demo -cfg runtime.cfg -scope foo

