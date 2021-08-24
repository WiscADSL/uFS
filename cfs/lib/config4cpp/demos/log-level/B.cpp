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

#include "B.h"
#include "Logger.h"



B::B(const FooConfiguration * cfg)
{
	m_logLevelOp1 = cfg->getLogLevel("B::op1");
	m_logLevelOp2 = cfg->getLogLevel("B::op2");
	m_logLevelOp3 = cfg->getLogLevel("B::op3");
}



void
B::op1()
{
	log.error(m_logLevelOp1, "B::op1(): this is an error message");
	log.info(m_logLevelOp1,  "B::op1(): this is an information message");
	log.warn(m_logLevelOp1,  "B::op1(): this is a warning message");
	log.debug(m_logLevelOp1, "B::op1(): this is a debug message");
}



void
B::op2()
{
	log.error(m_logLevelOp2, "B::op2(): this is an error message");
	log.info(m_logLevelOp2,  "B::op2(): this is an information message");
	log.warn(m_logLevelOp2,  "B::op2(): this is a warning message");
	log.debug(m_logLevelOp2, "B::op2(): this is a debug message");
}



void
B::op3()
{
	log.error(m_logLevelOp3, "B::op3(): this is an error message");
	log.info(m_logLevelOp3,  "B::op3(): this is an information message");
	log.warn(m_logLevelOp3,  "B::op3(): this is a warning message");
	log.debug(m_logLevelOp3, "B::op3(): this is a debug message");
}

