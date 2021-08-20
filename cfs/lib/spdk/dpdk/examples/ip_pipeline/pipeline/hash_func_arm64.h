/*-
 *   BSD LICENSE
 *
 *   Copyright(c) 2017 Linaro Limited. All rights reserved.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#ifndef __HASH_FUNC_ARM64_H__
#define __HASH_FUNC_ARM64_H__

#define _CRC32CX(crc, val)	\
	__asm__("crc32cx %w[c], %w[c], %x[v]":[c] "+r" (crc):[v] "r" (val))

static inline uint64_t
hash_crc_key8(void *key, void *mask, __rte_unused uint32_t key_size,
	uint64_t seed)
{
	uint64_t *k = key;
	uint64_t *m = mask;
	uint32_t crc0;

	crc0 = seed;
	_CRC32CX(crc0, k[0] & m[0]);

	return crc0;
}

static inline uint64_t
hash_crc_key16(void *key, void *mask, __rte_unused uint32_t key_size,
	uint64_t seed)
{
	uint64_t *k = key, k0;
	uint64_t *m = mask;
	uint32_t crc0, crc1;

	k0 = k[0] & m[0];

	crc0 = k0;
	_CRC32CX(crc0, seed);
	crc1 = k0 >> 32;
	_CRC32CX(crc1, k[1] & m[1]);

	crc0 ^= crc1;

	return crc0;
}

static inline uint64_t
hash_crc_key24(void *key, void *mask, __rte_unused uint32_t key_size,
	uint64_t seed)
{
	uint64_t *k = key, k0, k2;
	uint64_t *m = mask;
	uint32_t crc0, crc1;

	k0 = k[0] & m[0];
	k2 = k[2] & m[2];

	crc0 = k0;
	_CRC32CX(crc0, seed);
	crc1 = k0 >> 32;
	_CRC32CX(crc1, k[1] & m[1]);

	_CRC32CX(crc0, k2);

	crc0 ^= crc1;

	return crc0;
}

static inline uint64_t
hash_crc_key32(void *key, void *mask, __rte_unused uint32_t key_size,
	uint64_t seed)
{
	uint64_t *k = key, k0, k2;
	uint64_t *m = mask;
	uint32_t crc0, crc1, crc2, crc3;

	k0 = k[0] & m[0];
	k2 = k[2] & m[2];

	crc0 = k0;
	_CRC32CX(crc0, seed);
	crc1 = k0 >> 32;
	_CRC32CX(crc1, k[1] & m[1]);

	crc2 = k2;
	_CRC32CX(crc2, k[3] & m[3]);
	crc3 = k2 >> 32;

	_CRC32CX(crc0, crc1);
	_CRC32CX(crc2, crc3);

	crc0 ^= crc2;

	return crc0;
}

static inline uint64_t
hash_crc_key40(void *key, void *mask, __rte_unused uint32_t key_size,
	uint64_t seed)
{
	uint64_t *k = key, k0, k2;
	uint64_t *m = mask;
	uint32_t crc0, crc1, crc2, crc3;

	k0 = k[0] & m[0];
	k2 = k[2] & m[2];

	crc0 = k0;
	_CRC32CX(crc0, seed);
	crc1 = k0 >> 32;
	_CRC32CX(crc1, k[1] & m[1]);

	crc2 = k2;
	_CRC32CX(crc2, k[3] & m[3]);
	crc3 = k2 >> 32;
	_CRC32CX(crc3, k[4] & m[4]);

	_CRC32CX(crc0, crc1);
	_CRC32CX(crc2, crc3);

	crc0 ^= crc2;

	return crc0;
}

static inline uint64_t
hash_crc_key48(void *key, void *mask, __rte_unused uint32_t key_size,
	uint64_t seed)
{
	uint64_t *k = key, k0, k2, k5;
	uint64_t *m = mask;
	uint32_t crc0, crc1, crc2, crc3;

	k0 = k[0] & m[0];
	k2 = k[2] & m[2];
	k5 = k[5] & m[5];

	crc0 = k0;
	_CRC32CX(crc0, seed);
	crc1 = k0 >> 32;
	_CRC32CX(crc1, k[1] & m[1]);

	crc2 = k2;
	_CRC32CX(crc2, k[3] & m[3]);
	crc3 = k2 >> 32;
	_CRC32CX(crc3, k[4] & m[4]);

	_CRC32CX(crc0, ((uint64_t)crc1 << 32) ^ crc2);
	_CRC32CX(crc3, k5);

	crc0 ^= crc3;

	return crc0;
}

static inline uint64_t
hash_crc_key56(void *key, void *mask, __rte_unused uint32_t key_size,
	uint64_t seed)
{
	uint64_t *k = key, k0, k2, k5;
	uint64_t *m = mask;
	uint32_t crc0, crc1, crc2, crc3, crc4, crc5;

	k0 = k[0] & m[0];
	k2 = k[2] & m[2];
	k5 = k[5] & m[5];

	crc0 = k0;
	_CRC32CX(crc0, seed);
	crc1 = k0 >> 32;
	_CRC32CX(crc1, k[1] & m[1]);

	crc2 = k2;
	_CRC32CX(crc2, k[3] & m[3]);
	crc3 = k2 >> 32;
	_CRC32CX(crc3, k[4] & m[4]);

	crc4 = k5;
	 _CRC32CX(crc4, k[6] & m[6]);
	crc5 = k5 >> 32;

	_CRC32CX(crc0, ((uint64_t)crc1 << 32) ^ crc2);
	_CRC32CX(crc3, ((uint64_t)crc4 << 32) ^ crc5);

	crc0 ^= crc3;

	return crc0;
}

static inline uint64_t
hash_crc_key64(void *key, void *mask, __rte_unused uint32_t key_size,
	uint64_t seed)
{
	uint64_t *k = key, k0, k2, k5;
	uint64_t *m = mask;
	uint32_t crc0, crc1, crc2, crc3, crc4, crc5;

	k0 = k[0] & m[0];
	k2 = k[2] & m[2];
	k5 = k[5] & m[5];

	crc0 = k0;
	_CRC32CX(crc0, seed);
	crc1 = k0 >> 32;
	_CRC32CX(crc1, k[1] & m[1]);

	crc2 = k2;
	_CRC32CX(crc2, k[3] & m[3]);
	crc3 = k2 >> 32;
	_CRC32CX(crc3, k[4] & m[4]);

	crc4 = k5;
	 _CRC32CX(crc4, k[6] & m[6]);
	crc5 = k5 >> 32;
	_CRC32CX(crc5, k[7] & m[7]);

	_CRC32CX(crc0, ((uint64_t)crc1 << 32) ^ crc2);
	_CRC32CX(crc3, ((uint64_t)crc4 << 32) ^ crc5);

	crc0 ^= crc3;

	return crc0;
}

#define hash_default_key8			hash_crc_key8
#define hash_default_key16			hash_crc_key16
#define hash_default_key24			hash_crc_key24
#define hash_default_key32			hash_crc_key32
#define hash_default_key40			hash_crc_key40
#define hash_default_key48			hash_crc_key48
#define hash_default_key56			hash_crc_key56
#define hash_default_key64			hash_crc_key64

#endif
