/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2015 Intel Corporation
 */

#ifndef _AESNI_MB_OPS_H_
#define _AESNI_MB_OPS_H_

#ifndef LINUX
#define LINUX
#endif

#include <mb_mgr.h>
#include <aux_funcs.h>

enum aesni_mb_vector_mode {
	RTE_AESNI_MB_NOT_SUPPORTED = 0,
	RTE_AESNI_MB_SSE,
	RTE_AESNI_MB_AVX,
	RTE_AESNI_MB_AVX2,
	RTE_AESNI_MB_AVX512
};

typedef void (*md5_one_block_t)(const void *data, void *digest);

typedef void (*sha1_one_block_t)(const void *data, void *digest);
typedef void (*sha224_one_block_t)(const void *data, void *digest);
typedef void (*sha256_one_block_t)(const void *data, void *digest);
typedef void (*sha384_one_block_t)(const void *data, void *digest);
typedef void (*sha512_one_block_t)(const void *data, void *digest);

typedef void (*aes_keyexp_128_t)
		(const void *key, void *enc_exp_keys, void *dec_exp_keys);
typedef void (*aes_keyexp_192_t)
		(const void *key, void *enc_exp_keys, void *dec_exp_keys);
typedef void (*aes_keyexp_256_t)
		(const void *key, void *enc_exp_keys, void *dec_exp_keys);

typedef void (*aes_xcbc_expand_key_t)
		(const void *key, void *exp_k1, void *k2, void *k3);

/** Multi-buffer library function pointer table */
struct aesni_mb_op_fns {
	struct {
		init_mb_mgr_t init_mgr;
		/**< Initialise scheduler  */
		get_next_job_t get_next;
		/**< Get next free job structure */
		submit_job_t submit;
		/**< Submit job to scheduler */
		get_completed_job_t get_completed_job;
		/**< Get completed job */
		flush_job_t flush_job;
		/**< flush jobs from manager */
	} job;
	/**< multi buffer manager functions */

	struct {
		struct {
			md5_one_block_t md5;
			/**< MD5 one block hash */
			sha1_one_block_t sha1;
			/**< SHA1 one block hash */
			sha224_one_block_t sha224;
			/**< SHA224 one block hash */
			sha256_one_block_t sha256;
			/**< SHA256 one block hash */
			sha384_one_block_t sha384;
			/**< SHA384 one block hash */
			sha512_one_block_t sha512;
			/**< SHA512 one block hash */
		} one_block;
		/**< one block hash functions */

		struct {
			aes_keyexp_128_t aes128;
			/**< AES128 key expansions */
			aes_keyexp_192_t aes192;
			/**< AES192 key expansions */
			aes_keyexp_256_t aes256;
			/**< AES256 key expansions */

			aes_xcbc_expand_key_t aes_xcbc;
			/**< AES XCBC key expansions */
		} keyexp;
		/**< Key expansion functions */
	} aux;
	/**< Auxiliary functions */
};


static const struct aesni_mb_op_fns job_ops[] = {
		[RTE_AESNI_MB_NOT_SUPPORTED] = {
			.job = {
				NULL
			},
			.aux = {
				.one_block = {
					NULL
				},
				.keyexp = {
					NULL
				}
			}
		},
		[RTE_AESNI_MB_SSE] = {
			.job = {
				init_mb_mgr_sse,
				get_next_job_sse,
				submit_job_sse,
				get_completed_job_sse,
				flush_job_sse
			},
			.aux = {
				.one_block = {
					md5_one_block_sse,
					sha1_one_block_sse,
					sha224_one_block_sse,
					sha256_one_block_sse,
					sha384_one_block_sse,
					sha512_one_block_sse
				},
				.keyexp = {
					aes_keyexp_128_sse,
					aes_keyexp_192_sse,
					aes_keyexp_256_sse,
					aes_xcbc_expand_key_sse
				}
			}
		},
		[RTE_AESNI_MB_AVX] = {
			.job = {
				init_mb_mgr_avx,
				get_next_job_avx,
				submit_job_avx,
				get_completed_job_avx,
				flush_job_avx
			},
			.aux = {
				.one_block = {
					md5_one_block_avx,
					sha1_one_block_avx,
					sha224_one_block_avx,
					sha256_one_block_avx,
					sha384_one_block_avx,
					sha512_one_block_avx
				},
				.keyexp = {
					aes_keyexp_128_avx,
					aes_keyexp_192_avx,
					aes_keyexp_256_avx,
					aes_xcbc_expand_key_avx
				}
			}
		},
		[RTE_AESNI_MB_AVX2] = {
			.job = {
				init_mb_mgr_avx2,
				get_next_job_avx2,
				submit_job_avx2,
				get_completed_job_avx2,
				flush_job_avx2
			},
			.aux = {
				.one_block = {
					md5_one_block_avx2,
					sha1_one_block_avx2,
					sha224_one_block_avx2,
					sha256_one_block_avx2,
					sha384_one_block_avx2,
					sha512_one_block_avx2
				},
				.keyexp = {
					aes_keyexp_128_avx2,
					aes_keyexp_192_avx2,
					aes_keyexp_256_avx2,
					aes_xcbc_expand_key_avx2
				}
			}
		},
		[RTE_AESNI_MB_AVX512] = {
			.job = {
				init_mb_mgr_avx512,
				get_next_job_avx512,
				submit_job_avx512,
				get_completed_job_avx512,
				flush_job_avx512
			},
			.aux = {
				.one_block = {
					md5_one_block_avx512,
					sha1_one_block_avx512,
					sha224_one_block_avx512,
					sha256_one_block_avx512,
					sha384_one_block_avx512,
					sha512_one_block_avx512
				},
				.keyexp = {
					aes_keyexp_128_avx512,
					aes_keyexp_192_avx512,
					aes_keyexp_256_avx512,
					aes_xcbc_expand_key_avx512
				}
			}
		}
};


#endif /* _AESNI_MB_OPS_H_ */
