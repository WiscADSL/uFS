This demo application illustrates how to enhance the Config4* schema
validator with knowledge of additional schema types.

The "ExtendedSchemaValidator" class illustrates how to write a subclass
of "SchemaValidator". Most of the code in this class is boilerplate text
that delegates to the parent class. The important part of this class is
the implementation of registerTypes(), which registers a singleton
instance of each new schema type. For the purposes of this demo, this
operation registers the "SchemaTypeHex" class, which performs schema
validation for hexadecimal numbers.

A class that provides a new schema type must implement two operations:

	checkRule() is invoked when the schema type is used in a schema
	rule.

	isA() is invoked during schema validation of a configuration
	file.

The "SchemaTypeHex" class illustrates how to implement the above
operations. In addition, the class provides some utility functions that
might be useful to application developers: lookupHex(), stringToHex()
and isHex().

The "FooConfiguration" class encapsulates use of Config4* and the new
schema type. This class illustrates how to make use of the enhanced
schema validator and the utility functions provided by the
"SchemaTypeHex" class.

Examples of running the demo:

    demo -h                             (prints a usage statement)
    demo -cfg runtime.cfg -scope foo

