

A simple code generator for load data logging, python processing pipeline

Given a header with a C struct, this will generate:

* cpp struct with one template function that write fields into argument stream
* python code to process this corresponding output

### Requirement

* Only support scalar type, array for now
* label `// @array` to indicate this line is an array
* label `// @end` to indicate this line as the end of struct

### Example

```shell

$ ./gen_cpp_py_for_cstruct.py ./struct.hh

```
