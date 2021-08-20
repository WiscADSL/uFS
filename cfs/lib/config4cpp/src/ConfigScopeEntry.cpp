//-----------------------------------------------------------------------
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
#include "ConfigScopeEntry.h"
#include <string.h>
#include <assert.h>


namespace CONFIG4CPP_NAMESPACE {

//----------------------------------------------------------------------
// Function:	Constructor (overloaded)
//
// Description:	
//----------------------------------------------------------------------

ConfigScopeEntry::ConfigScopeEntry()
{
	m_item	= 0;
	m_next	= 0;
}


ConfigScopeEntry::ConfigScopeEntry(
		const char *		name,
		ConfigItem *		item,
		ConfigScopeEntry *	next)
{
	m_item	= item;
	m_next	= next;
}



//----------------------------------------------------------------------
// Function:	Destructor
//
// Description:	
//----------------------------------------------------------------------

ConfigScopeEntry::~ConfigScopeEntry ()
{
	delete m_item;
	delete m_next;
}



void
ConfigScopeEntry::setItem(ConfigItem * item)
{
	delete m_item;
	m_item = item;
}

}; // namespace CONFIG4CPP_NAMESPACE
