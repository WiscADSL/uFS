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

#ifndef FOO_CONFIGURATION_H_
#define FOO_CONFIGURATION_H_


class FooConfigurationException
{
public:
	//--------
	// Constructors and destructor
	//--------
	FooConfigurationException(const char * str);
	FooConfigurationException(const FooConfigurationException & other);
	~FooConfigurationException();

	const char * c_str() const; // Accessor

private:
	char *			m_str;

	//--------
	// Not implemented
	//--------
	FooConfigurationException();
	FooConfigurationException operator=(const FooConfigurationException &);
};


class FooConfiguration
{
public:
	//--------
	// Constructor and destructor
	//--------
	FooConfiguration();
	~FooConfiguration();

	void parse(const char * cfgSource, const char * scope = "")
											throw (FooConfigurationException);

	//--------
	// Lookup-style functions.
	//--------
	const char * lookupString(const char * name) const
											throw (FooConfigurationException);
	void lookupList(
			const char *	name,
			const char **&	array,
			int &			arraySize) const throw (FooConfigurationException);

	virtual int lookupInt(const char * name) const
											throw(FooConfigurationException);
	virtual float lookupFloat(const char * name) const
											throw(FooConfigurationException);
	virtual bool lookupBoolean(const char * name) const
											throw(FooConfigurationException);
	virtual int lookupDurationMilliseconds(const char * name) const
											throw(FooConfigurationException);
	virtual int lookupDurationSeconds(const char * name) const
											throw(FooConfigurationException);

private:
	//--------
	// Instance variables
	//--------
	char *			m_scope;
	void *			m_cfg;

	//--------
	// The following are not implemented
	//--------
	FooConfiguration(const FooConfiguration &);
	FooConfiguration & operator=(const FooConfiguration &);
};


#endif
