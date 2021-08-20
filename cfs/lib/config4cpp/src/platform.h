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

#ifndef CONFIG4CPP_PLATFORM_H_
#define CONFIG4CPP_PLATFORM_H_

#ifdef WIN32
#include <process.h>
#	define CONFIG4CPP_OS_TYPE "windows"
#	define CONFIG4CPP_DIR_SEP "\\"
#	define CONFIG4CPP_PATH_SEP ";"
#	define CONFIG4CPP_POPEN(fileName, mode) _popen(fileName, mode)
#	define CONFIG4CPP_PCLOSE(file) _pclose(file)
#	define CONFIG4CPP_DISCARD_STDERR "2> nul"
#else 
#include <unistd.h>
#	ifndef CONFIG4CPP_OS_TYPE 
#		define CONFIG4CPP_OS_TYPE "unix"
#	endif
#	ifndef CONFIG4CPP_DIR_SEP
#		define CONFIG4CPP_DIR_SEP "/"
#	endif
#	ifndef CONFIG4CPP_PATH_SEP
#		define CONFIG4CPP_PATH_SEP ":"
#	endif
#	ifndef CONFIG4CPP_POPEN
#		define CONFIG4CPP_POPEN(fileName, mode) popen(fileName, mode)
#	endif
#	ifndef CONFIG4CPP_PCLOSE
#		define CONFIG4CPP_PCLOSE(file) pclose(file)
#	endif
#	ifndef CONFIG4CPP_DISCARD_STDERR
#		define CONFIG4CPP_DISCARD_STDERR "2> /dev/null"
#	endif
#endif
#include <config4cpp/StringBuffer.h>
#include <stdio.h>


namespace CONFIG4CPP_NAMESPACE {

extern bool execCmd(const char * cmd, StringBuffer & output);
extern bool isCmdInDir(const char * cmd, const char * dir);

//----------------------------------------------------------------------
// Some operating systems, such as Solaris
// (see http://developers.sun.com/solaris/articles/stdio_256.html),
// have an implementation of <stdio.h> in which fopen() fails if
// the file descriptor used is > 255. This suggests that config4Cpp
// should use open() instead of fopen(). However, fopen() is part of
// the standard C library and is guaranteed to be available everywhere,
// but open() is UNIX-specific and may not be available on some
// (non-UNIX) systems. The workaround is to define a class that provides
// an abstraction layer around either fopen() or open(). The
// functionality of this class is driven by the needs of the Lex class,
// which needs only to open a file for reading and then read it one
// byte at a time.
//----------------------------------------------------------------------

class BufferedFileReader
{
public:
	//--------
	// Constructors and destructor
	//--------
	BufferedFileReader();
	~BufferedFileReader();

	bool open(const char * fileName);
	int getChar(); // returns EOF on end-of-file
	bool close();
private:
	//--------
	// Instance variables
	//--------
#ifdef P_STDIO_HAS_LIMITED_FDS
#define BUFFERED_FILE_READER_BUF_SIZE 8192
	int		m_fd;
	char	m_buf[BUFFERED_FILE_READER_BUF_SIZE];
	int		m_bufIndex;
	int		m_bufLen;
#else
	FILE *	m_file;
#endif
	//--------
	// The following are not implemented
	//--------
	BufferedFileReader(const BufferedFileReader &);
	BufferedFileReader& operator=(const BufferedFileReader &);
	
};


}; // namespace CONFIG4CPP_NAMESPACE
#endif
