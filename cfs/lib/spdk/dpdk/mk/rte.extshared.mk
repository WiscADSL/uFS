# BSD LICENSE
#
# Copyright 2012-2013 6WIND S.A.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#   * Redistributions of source code must retain the above copyright
#     notice, this list of conditions and the following disclaimer.
#   * Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions and the following disclaimer in
#     the documentation and/or other materials provided with the
#     distribution.
#   * Neither the name of 6WIND S.A. nor the names of its
#     contributors may be used to endorse or promote products derived
#     from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

MAKEFLAGS += --no-print-directory

# we must create the output dir first and recall the same Makefile
# from this directory
ifeq ($(NOT_FIRST_CALL),)

NOT_FIRST_CALL = 1
export NOT_FIRST_CALL

all:
	$(Q)mkdir -p $(RTE_OUTPUT)
	$(Q)$(MAKE) -C $(RTE_OUTPUT) -f $(RTE_EXTMK) \
		S=$(RTE_SRCDIR) O=$(RTE_OUTPUT) SRCDIR=$(RTE_SRCDIR)
	@echo $(RTE_OUTPUT)/lib must be added to /etc/ld.so.conf or \
		LD_LIBRARY_PATH variable to allow binary to link with dynamic library

%::
	$(Q)mkdir -p $(RTE_OUTPUT)
	$(Q)$(MAKE) -C $(RTE_OUTPUT) -f $(RTE_EXTMK) $@ \
		S=$(RTE_SRCDIR) O=$(RTE_OUTPUT) SRCDIR=$(RTE_SRCDIR)
else
include $(RTE_SDK)/mk/rte.shared.mk
endif
