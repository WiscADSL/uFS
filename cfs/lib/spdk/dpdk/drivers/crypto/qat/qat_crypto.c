/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2015-2017 Intel Corporation
 */

#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>
#include <inttypes.h>
#include <errno.h>
#include <sys/queue.h>
#include <stdarg.h>

#include <rte_common.h>
#include <rte_log.h>
#include <rte_debug.h>
#include <rte_memory.h>
#include <rte_tailq.h>
#include <rte_malloc.h>
#include <rte_launch.h>
#include <rte_eal.h>
#include <rte_per_lcore.h>
#include <rte_lcore.h>
#include <rte_branch_prediction.h>
#include <rte_mempool.h>
#include <rte_mbuf.h>
#include <rte_string_fns.h>
#include <rte_spinlock.h>
#include <rte_hexdump.h>
#include <rte_crypto_sym.h>
#include <rte_byteorder.h>
#include <rte_pci.h>
#include <rte_bus_pci.h>

#include <openssl/evp.h>

#include "qat_logs.h"
#include "qat_algs.h"
#include "qat_crypto.h"
#include "adf_transport_access_macros.h"

#define BYTE_LENGTH    8
/* bpi is only used for partial blocks of DES and AES
 * so AES block len can be assumed as max len for iv, src and dst
 */
#define BPI_MAX_ENCR_IV_LEN ICP_QAT_HW_AES_BLK_SZ

static int
qat_is_cipher_alg_supported(enum rte_crypto_cipher_algorithm algo,
		struct qat_pmd_private *internals) {
	int i = 0;
	const struct rte_cryptodev_capabilities *capability;

	while ((capability = &(internals->qat_dev_capabilities[i++]))->op !=
			RTE_CRYPTO_OP_TYPE_UNDEFINED) {
		if (capability->op != RTE_CRYPTO_OP_TYPE_SYMMETRIC)
			continue;

		if (capability->sym.xform_type != RTE_CRYPTO_SYM_XFORM_CIPHER)
			continue;

		if (capability->sym.cipher.algo == algo)
			return 1;
	}
	return 0;
}

static int
qat_is_auth_alg_supported(enum rte_crypto_auth_algorithm algo,
		struct qat_pmd_private *internals) {
	int i = 0;
	const struct rte_cryptodev_capabilities *capability;

	while ((capability = &(internals->qat_dev_capabilities[i++]))->op !=
			RTE_CRYPTO_OP_TYPE_UNDEFINED) {
		if (capability->op != RTE_CRYPTO_OP_TYPE_SYMMETRIC)
			continue;

		if (capability->sym.xform_type != RTE_CRYPTO_SYM_XFORM_AUTH)
			continue;

		if (capability->sym.auth.algo == algo)
			return 1;
	}
	return 0;
}

/** Encrypt a single partial block
 *  Depends on openssl libcrypto
 *  Uses ECB+XOR to do CFB encryption, same result, more performant
 */
static inline int
bpi_cipher_encrypt(uint8_t *src, uint8_t *dst,
		uint8_t *iv, int ivlen, int srclen,
		void *bpi_ctx)
{
	EVP_CIPHER_CTX *ctx = (EVP_CIPHER_CTX *)bpi_ctx;
	int encrypted_ivlen;
	uint8_t encrypted_iv[BPI_MAX_ENCR_IV_LEN];
	uint8_t *encr = encrypted_iv;

	/* ECB method: encrypt the IV, then XOR this with plaintext */
	if (EVP_EncryptUpdate(ctx, encrypted_iv, &encrypted_ivlen, iv, ivlen)
								<= 0)
		goto cipher_encrypt_err;

	for (; srclen != 0; --srclen, ++dst, ++src, ++encr)
		*dst = *src ^ *encr;

	return 0;

cipher_encrypt_err:
	PMD_DRV_LOG(ERR, "libcrypto ECB cipher encrypt failed");
	return -EINVAL;
}

/** Decrypt a single partial block
 *  Depends on openssl libcrypto
 *  Uses ECB+XOR to do CFB encryption, same result, more performant
 */
static inline int
bpi_cipher_decrypt(uint8_t *src, uint8_t *dst,
		uint8_t *iv, int ivlen, int srclen,
		void *bpi_ctx)
{
	EVP_CIPHER_CTX *ctx = (EVP_CIPHER_CTX *)bpi_ctx;
	int encrypted_ivlen;
	uint8_t encrypted_iv[BPI_MAX_ENCR_IV_LEN];
	uint8_t *encr = encrypted_iv;

	/* ECB method: encrypt (not decrypt!) the IV, then XOR with plaintext */
	if (EVP_EncryptUpdate(ctx, encrypted_iv, &encrypted_ivlen, iv, ivlen)
								<= 0)
		goto cipher_decrypt_err;

	for (; srclen != 0; --srclen, ++dst, ++src, ++encr)
		*dst = *src ^ *encr;

	return 0;

cipher_decrypt_err:
	PMD_DRV_LOG(ERR, "libcrypto ECB cipher decrypt for BPI IV failed");
	return -EINVAL;
}

/** Creates a context in either AES or DES in ECB mode
 *  Depends on openssl libcrypto
 */
static int
bpi_cipher_ctx_init(enum rte_crypto_cipher_algorithm cryptodev_algo,
		enum rte_crypto_cipher_operation direction __rte_unused,
		uint8_t *key, void **ctx)
{
	const EVP_CIPHER *algo = NULL;
	int ret;
	*ctx = EVP_CIPHER_CTX_new();

	if (*ctx == NULL) {
		ret = -ENOMEM;
		goto ctx_init_err;
	}

	if (cryptodev_algo == RTE_CRYPTO_CIPHER_DES_DOCSISBPI)
		algo = EVP_des_ecb();
	else
		algo = EVP_aes_128_ecb();

	/* IV will be ECB encrypted whether direction is encrypt or decrypt*/
	if (EVP_EncryptInit_ex(*ctx, algo, NULL, key, 0) != 1) {
		ret = -EINVAL;
		goto ctx_init_err;
	}

	return 0;

ctx_init_err:
	if (*ctx != NULL)
		EVP_CIPHER_CTX_free(*ctx);
	return ret;
}

/** Frees a context previously created
 *  Depends on openssl libcrypto
 */
static void
bpi_cipher_ctx_free(void *bpi_ctx)
{
	if (bpi_ctx != NULL)
		EVP_CIPHER_CTX_free((EVP_CIPHER_CTX *)bpi_ctx);
}

static inline uint32_t
adf_modulo(uint32_t data, uint32_t shift);

static inline int
qat_write_hw_desc_entry(struct rte_crypto_op *op, uint8_t *out_msg,
		struct qat_crypto_op_cookie *qat_op_cookie, struct qat_qp *qp);

void
qat_crypto_sym_clear_session(struct rte_cryptodev *dev,
		struct rte_cryptodev_sym_session *sess)
{
	PMD_INIT_FUNC_TRACE();
	uint8_t index = dev->driver_id;
	void *sess_priv = get_session_private_data(sess, index);
	struct qat_session *s = (struct qat_session *)sess_priv;

	if (sess_priv) {
		if (s->bpi_ctx)
			bpi_cipher_ctx_free(s->bpi_ctx);
		memset(s, 0, qat_crypto_sym_get_session_private_size(dev));
		struct rte_mempool *sess_mp = rte_mempool_from_obj(sess_priv);
		set_session_private_data(sess, index, NULL);
		rte_mempool_put(sess_mp, sess_priv);
	}
}

static int
qat_get_cmd_id(const struct rte_crypto_sym_xform *xform)
{
	/* Cipher Only */
	if (xform->type == RTE_CRYPTO_SYM_XFORM_CIPHER && xform->next == NULL)
		return ICP_QAT_FW_LA_CMD_CIPHER;

	/* Authentication Only */
	if (xform->type == RTE_CRYPTO_SYM_XFORM_AUTH && xform->next == NULL)
		return ICP_QAT_FW_LA_CMD_AUTH;

	/* AEAD */
	if (xform->type == RTE_CRYPTO_SYM_XFORM_AEAD) {
		/* AES-GCM and AES-CCM works with different direction
		 * GCM first encrypts and generate hash where AES-CCM
		 * first generate hash and encrypts. Similar relation
		 * applies to decryption.
		 */
		if (xform->aead.op == RTE_CRYPTO_AEAD_OP_ENCRYPT)
			if (xform->aead.algo == RTE_CRYPTO_AEAD_AES_GCM)
				return ICP_QAT_FW_LA_CMD_CIPHER_HASH;
			else
				return ICP_QAT_FW_LA_CMD_HASH_CIPHER;
		else
			if (xform->aead.algo == RTE_CRYPTO_AEAD_AES_GCM)
				return ICP_QAT_FW_LA_CMD_HASH_CIPHER;
			else
				return ICP_QAT_FW_LA_CMD_CIPHER_HASH;
	}

	if (xform->next == NULL)
		return -1;

	/* Cipher then Authenticate */
	if (xform->type == RTE_CRYPTO_SYM_XFORM_CIPHER &&
			xform->next->type == RTE_CRYPTO_SYM_XFORM_AUTH)
		return ICP_QAT_FW_LA_CMD_CIPHER_HASH;

	/* Authenticate then Cipher */
	if (xform->type == RTE_CRYPTO_SYM_XFORM_AUTH &&
			xform->next->type == RTE_CRYPTO_SYM_XFORM_CIPHER)
		return ICP_QAT_FW_LA_CMD_HASH_CIPHER;

	return -1;
}

static struct rte_crypto_auth_xform *
qat_get_auth_xform(struct rte_crypto_sym_xform *xform)
{
	do {
		if (xform->type == RTE_CRYPTO_SYM_XFORM_AUTH)
			return &xform->auth;

		xform = xform->next;
	} while (xform);

	return NULL;
}

static struct rte_crypto_cipher_xform *
qat_get_cipher_xform(struct rte_crypto_sym_xform *xform)
{
	do {
		if (xform->type == RTE_CRYPTO_SYM_XFORM_CIPHER)
			return &xform->cipher;

		xform = xform->next;
	} while (xform);

	return NULL;
}

int
qat_crypto_sym_configure_session_cipher(struct rte_cryptodev *dev,
		struct rte_crypto_sym_xform *xform,
		struct qat_session *session)
{
	struct qat_pmd_private *internals = dev->data->dev_private;
	struct rte_crypto_cipher_xform *cipher_xform = NULL;
	int ret;

	/* Get cipher xform from crypto xform chain */
	cipher_xform = qat_get_cipher_xform(xform);

	session->cipher_iv.offset = cipher_xform->iv.offset;
	session->cipher_iv.length = cipher_xform->iv.length;

	switch (cipher_xform->algo) {
	case RTE_CRYPTO_CIPHER_AES_CBC:
		if (qat_alg_validate_aes_key(cipher_xform->key.length,
				&session->qat_cipher_alg) != 0) {
			PMD_DRV_LOG(ERR, "Invalid AES cipher key size");
			ret = -EINVAL;
			goto error_out;
		}
		session->qat_mode = ICP_QAT_HW_CIPHER_CBC_MODE;
		break;
	case RTE_CRYPTO_CIPHER_AES_CTR:
		if (qat_alg_validate_aes_key(cipher_xform->key.length,
				&session->qat_cipher_alg) != 0) {
			PMD_DRV_LOG(ERR, "Invalid AES cipher key size");
			ret = -EINVAL;
			goto error_out;
		}
		session->qat_mode = ICP_QAT_HW_CIPHER_CTR_MODE;
		break;
	case RTE_CRYPTO_CIPHER_SNOW3G_UEA2:
		if (qat_alg_validate_snow3g_key(cipher_xform->key.length,
					&session->qat_cipher_alg) != 0) {
			PMD_DRV_LOG(ERR, "Invalid SNOW 3G cipher key size");
			ret = -EINVAL;
			goto error_out;
		}
		session->qat_mode = ICP_QAT_HW_CIPHER_ECB_MODE;
		break;
	case RTE_CRYPTO_CIPHER_NULL:
		session->qat_mode = ICP_QAT_HW_CIPHER_ECB_MODE;
		break;
	case RTE_CRYPTO_CIPHER_KASUMI_F8:
		if (qat_alg_validate_kasumi_key(cipher_xform->key.length,
					&session->qat_cipher_alg) != 0) {
			PMD_DRV_LOG(ERR, "Invalid KASUMI cipher key size");
			ret = -EINVAL;
			goto error_out;
		}
		session->qat_mode = ICP_QAT_HW_CIPHER_F8_MODE;
		break;
	case RTE_CRYPTO_CIPHER_3DES_CBC:
		if (qat_alg_validate_3des_key(cipher_xform->key.length,
				&session->qat_cipher_alg) != 0) {
			PMD_DRV_LOG(ERR, "Invalid 3DES cipher key size");
			ret = -EINVAL;
			goto error_out;
		}
		session->qat_mode = ICP_QAT_HW_CIPHER_CBC_MODE;
		break;
	case RTE_CRYPTO_CIPHER_DES_CBC:
		if (qat_alg_validate_des_key(cipher_xform->key.length,
				&session->qat_cipher_alg) != 0) {
			PMD_DRV_LOG(ERR, "Invalid DES cipher key size");
			ret = -EINVAL;
			goto error_out;
		}
		session->qat_mode = ICP_QAT_HW_CIPHER_CBC_MODE;
		break;
	case RTE_CRYPTO_CIPHER_3DES_CTR:
		if (qat_alg_validate_3des_key(cipher_xform->key.length,
				&session->qat_cipher_alg) != 0) {
			PMD_DRV_LOG(ERR, "Invalid 3DES cipher key size");
			ret = -EINVAL;
			goto error_out;
		}
		session->qat_mode = ICP_QAT_HW_CIPHER_CTR_MODE;
		break;
	case RTE_CRYPTO_CIPHER_DES_DOCSISBPI:
		ret = bpi_cipher_ctx_init(
					cipher_xform->algo,
					cipher_xform->op,
					cipher_xform->key.data,
					&session->bpi_ctx);
		if (ret != 0) {
			PMD_DRV_LOG(ERR, "failed to create DES BPI ctx");
			goto error_out;
		}
		if (qat_alg_validate_des_key(cipher_xform->key.length,
				&session->qat_cipher_alg) != 0) {
			PMD_DRV_LOG(ERR, "Invalid DES cipher key size");
			ret = -EINVAL;
			goto error_out;
		}
		session->qat_mode = ICP_QAT_HW_CIPHER_CBC_MODE;
		break;
	case RTE_CRYPTO_CIPHER_AES_DOCSISBPI:
		ret = bpi_cipher_ctx_init(
					cipher_xform->algo,
					cipher_xform->op,
					cipher_xform->key.data,
					&session->bpi_ctx);
		if (ret != 0) {
			PMD_DRV_LOG(ERR, "failed to create AES BPI ctx");
			goto error_out;
		}
		if (qat_alg_validate_aes_docsisbpi_key(cipher_xform->key.length,
				&session->qat_cipher_alg) != 0) {
			PMD_DRV_LOG(ERR, "Invalid AES DOCSISBPI key size");
			ret = -EINVAL;
			goto error_out;
		}
		session->qat_mode = ICP_QAT_HW_CIPHER_CBC_MODE;
		break;
	case RTE_CRYPTO_CIPHER_ZUC_EEA3:
		if (!qat_is_cipher_alg_supported(
			cipher_xform->algo, internals)) {
			PMD_DRV_LOG(ERR, "%s not supported on this device",
				rte_crypto_cipher_algorithm_strings
					[cipher_xform->algo]);
			ret = -ENOTSUP;
			goto error_out;
		}
		if (qat_alg_validate_zuc_key(cipher_xform->key.length,
				&session->qat_cipher_alg) != 0) {
			PMD_DRV_LOG(ERR, "Invalid ZUC cipher key size");
			ret = -EINVAL;
			goto error_out;
		}
		session->qat_mode = ICP_QAT_HW_CIPHER_ECB_MODE;
		break;
	case RTE_CRYPTO_CIPHER_3DES_ECB:
	case RTE_CRYPTO_CIPHER_AES_ECB:
	case RTE_CRYPTO_CIPHER_AES_F8:
	case RTE_CRYPTO_CIPHER_AES_XTS:
	case RTE_CRYPTO_CIPHER_ARC4:
		PMD_DRV_LOG(ERR, "Crypto QAT PMD: Unsupported Cipher alg %u",
				cipher_xform->algo);
		ret = -ENOTSUP;
		goto error_out;
	default:
		PMD_DRV_LOG(ERR, "Crypto: Undefined Cipher specified %u\n",
				cipher_xform->algo);
		ret = -EINVAL;
		goto error_out;
	}

	if (cipher_xform->op == RTE_CRYPTO_CIPHER_OP_ENCRYPT)
		session->qat_dir = ICP_QAT_HW_CIPHER_ENCRYPT;
	else
		session->qat_dir = ICP_QAT_HW_CIPHER_DECRYPT;

	if (qat_alg_aead_session_create_content_desc_cipher(session,
						cipher_xform->key.data,
						cipher_xform->key.length)) {
		ret = -EINVAL;
		goto error_out;
	}

	return 0;

error_out:
	if (session->bpi_ctx) {
		bpi_cipher_ctx_free(session->bpi_ctx);
		session->bpi_ctx = NULL;
	}
	return ret;
}

int
qat_crypto_sym_configure_session(struct rte_cryptodev *dev,
		struct rte_crypto_sym_xform *xform,
		struct rte_cryptodev_sym_session *sess,
		struct rte_mempool *mempool)
{
	void *sess_private_data;
	int ret;

	if (rte_mempool_get(mempool, &sess_private_data)) {
		CDEV_LOG_ERR(
			"Couldn't get object from session mempool");
		return -ENOMEM;
	}

	ret = qat_crypto_set_session_parameters(dev, xform, sess_private_data);
	if (ret != 0) {
		PMD_DRV_LOG(ERR, "Crypto QAT PMD: failed to configure "
				"session parameters");

		/* Return session to mempool */
		rte_mempool_put(mempool, sess_private_data);
		return ret;
	}

	set_session_private_data(sess, dev->driver_id,
		sess_private_data);

	return 0;
}

int
qat_crypto_set_session_parameters(struct rte_cryptodev *dev,
		struct rte_crypto_sym_xform *xform, void *session_private)
{
	struct qat_session *session = session_private;
	int ret;

	int qat_cmd_id;
	PMD_INIT_FUNC_TRACE();

	/* Set context descriptor physical address */
	session->cd_paddr = rte_mempool_virt2iova(session) +
			offsetof(struct qat_session, cd);

	session->min_qat_dev_gen = QAT_GEN1;

	/* Get requested QAT command id */
	qat_cmd_id = qat_get_cmd_id(xform);
	if (qat_cmd_id < 0 || qat_cmd_id >= ICP_QAT_FW_LA_CMD_DELIMITER) {
		PMD_DRV_LOG(ERR, "Unsupported xform chain requested");
		return -ENOTSUP;
	}
	session->qat_cmd = (enum icp_qat_fw_la_cmd_id)qat_cmd_id;
	switch (session->qat_cmd) {
	case ICP_QAT_FW_LA_CMD_CIPHER:
		ret = qat_crypto_sym_configure_session_cipher(dev, xform, session);
		if (ret < 0)
			return ret;
		break;
	case ICP_QAT_FW_LA_CMD_AUTH:
		ret = qat_crypto_sym_configure_session_auth(dev, xform, session);
		if (ret < 0)
			return ret;
		break;
	case ICP_QAT_FW_LA_CMD_CIPHER_HASH:
		if (xform->type == RTE_CRYPTO_SYM_XFORM_AEAD) {
			ret = qat_crypto_sym_configure_session_aead(xform,
					session);
			if (ret < 0)
				return ret;
		} else {
			ret = qat_crypto_sym_configure_session_cipher(dev,
					xform, session);
			if (ret < 0)
				return ret;
			ret = qat_crypto_sym_configure_session_auth(dev,
					xform, session);
			if (ret < 0)
				return ret;
		}
		break;
	case ICP_QAT_FW_LA_CMD_HASH_CIPHER:
		if (xform->type == RTE_CRYPTO_SYM_XFORM_AEAD) {
			ret = qat_crypto_sym_configure_session_aead(xform,
					session);
			if (ret < 0)
				return ret;
		} else {
			ret = qat_crypto_sym_configure_session_auth(dev,
					xform, session);
			if (ret < 0)
				return ret;
			ret = qat_crypto_sym_configure_session_cipher(dev,
					xform, session);
			if (ret < 0)
				return ret;
		}
		break;
	case ICP_QAT_FW_LA_CMD_TRNG_GET_RANDOM:
	case ICP_QAT_FW_LA_CMD_TRNG_TEST:
	case ICP_QAT_FW_LA_CMD_SSL3_KEY_DERIVE:
	case ICP_QAT_FW_LA_CMD_TLS_V1_1_KEY_DERIVE:
	case ICP_QAT_FW_LA_CMD_TLS_V1_2_KEY_DERIVE:
	case ICP_QAT_FW_LA_CMD_MGF1:
	case ICP_QAT_FW_LA_CMD_AUTH_PRE_COMP:
	case ICP_QAT_FW_LA_CMD_CIPHER_PRE_COMP:
	case ICP_QAT_FW_LA_CMD_DELIMITER:
	PMD_DRV_LOG(ERR, "Unsupported Service %u",
		session->qat_cmd);
		return -ENOTSUP;
	default:
	PMD_DRV_LOG(ERR, "Unsupported Service %u",
		session->qat_cmd);
		return -ENOTSUP;
	}

	return 0;
}

int
qat_crypto_sym_configure_session_auth(struct rte_cryptodev *dev,
				struct rte_crypto_sym_xform *xform,
				struct qat_session *session)
{
	struct rte_crypto_auth_xform *auth_xform = NULL;
	struct qat_pmd_private *internals = dev->data->dev_private;
	auth_xform = qat_get_auth_xform(xform);
	uint8_t *key_data = auth_xform->key.data;
	uint8_t key_length = auth_xform->key.length;

	switch (auth_xform->algo) {
	case RTE_CRYPTO_AUTH_SHA1_HMAC:
		session->qat_hash_alg = ICP_QAT_HW_AUTH_ALGO_SHA1;
		break;
	case RTE_CRYPTO_AUTH_SHA224_HMAC:
		session->qat_hash_alg = ICP_QAT_HW_AUTH_ALGO_SHA224;
		break;
	case RTE_CRYPTO_AUTH_SHA256_HMAC:
		session->qat_hash_alg = ICP_QAT_HW_AUTH_ALGO_SHA256;
		break;
	case RTE_CRYPTO_AUTH_SHA384_HMAC:
		session->qat_hash_alg = ICP_QAT_HW_AUTH_ALGO_SHA384;
		break;
	case RTE_CRYPTO_AUTH_SHA512_HMAC:
		session->qat_hash_alg = ICP_QAT_HW_AUTH_ALGO_SHA512;
		break;
	case RTE_CRYPTO_AUTH_AES_XCBC_MAC:
		session->qat_hash_alg = ICP_QAT_HW_AUTH_ALGO_AES_XCBC_MAC;
		break;
	case RTE_CRYPTO_AUTH_AES_GMAC:
		if (qat_alg_validate_aes_key(auth_xform->key.length,
				&session->qat_cipher_alg) != 0) {
			PMD_DRV_LOG(ERR, "Invalid AES key size");
			return -EINVAL;
		}
		session->qat_mode = ICP_QAT_HW_CIPHER_CTR_MODE;
		session->qat_hash_alg = ICP_QAT_HW_AUTH_ALGO_GALOIS_128;

		break;
	case RTE_CRYPTO_AUTH_SNOW3G_UIA2:
		session->qat_hash_alg = ICP_QAT_HW_AUTH_ALGO_SNOW_3G_UIA2;
		break;
	case RTE_CRYPTO_AUTH_MD5_HMAC:
		session->qat_hash_alg = ICP_QAT_HW_AUTH_ALGO_MD5;
		break;
	case RTE_CRYPTO_AUTH_NULL:
		session->qat_hash_alg = ICP_QAT_HW_AUTH_ALGO_NULL;
		break;
	case RTE_CRYPTO_AUTH_KASUMI_F9:
		session->qat_hash_alg = ICP_QAT_HW_AUTH_ALGO_KASUMI_F9;
		break;
	case RTE_CRYPTO_AUTH_ZUC_EIA3:
		if (!qat_is_auth_alg_supported(auth_xform->algo, internals)) {
			PMD_DRV_LOG(ERR, "%s not supported on this device",
				rte_crypto_auth_algorithm_strings
				[auth_xform->algo]);
			return -ENOTSUP;
		}
		session->qat_hash_alg = ICP_QAT_HW_AUTH_ALGO_ZUC_3G_128_EIA3;
		break;
	case RTE_CRYPTO_AUTH_SHA1:
	case RTE_CRYPTO_AUTH_SHA256:
	case RTE_CRYPTO_AUTH_SHA512:
	case RTE_CRYPTO_AUTH_SHA224:
	case RTE_CRYPTO_AUTH_SHA384:
	case RTE_CRYPTO_AUTH_MD5:
	case RTE_CRYPTO_AUTH_AES_CMAC:
	case RTE_CRYPTO_AUTH_AES_CBC_MAC:
		PMD_DRV_LOG(ERR, "Crypto: Unsupported hash alg %u",
				auth_xform->algo);
		return -ENOTSUP;
	default:
		PMD_DRV_LOG(ERR, "Crypto: Undefined Hash algo %u specified",
				auth_xform->algo);
		return -EINVAL;
	}

	session->auth_iv.offset = auth_xform->iv.offset;
	session->auth_iv.length = auth_xform->iv.length;

	if (auth_xform->algo == RTE_CRYPTO_AUTH_AES_GMAC) {
		if (auth_xform->op == RTE_CRYPTO_AUTH_OP_GENERATE) {
			session->qat_cmd = ICP_QAT_FW_LA_CMD_CIPHER_HASH;
			session->qat_dir = ICP_QAT_HW_CIPHER_ENCRYPT;
			/*
			 * It needs to create cipher desc content first,
			 * then authentication
			 */
			if (qat_alg_aead_session_create_content_desc_cipher(session,
						auth_xform->key.data,
						auth_xform->key.length))
				return -EINVAL;

			if (qat_alg_aead_session_create_content_desc_auth(session,
						key_data,
						key_length,
						0,
						auth_xform->digest_length,
						auth_xform->op))
				return -EINVAL;
		} else {
			session->qat_cmd = ICP_QAT_FW_LA_CMD_HASH_CIPHER;
			session->qat_dir = ICP_QAT_HW_CIPHER_DECRYPT;
			/*
			 * It needs to create authentication desc content first,
			 * then cipher
			 */
			if (qat_alg_aead_session_create_content_desc_auth(session,
					key_data,
					key_length,
					0,
					auth_xform->digest_length,
					auth_xform->op))
				return -EINVAL;

			if (qat_alg_aead_session_create_content_desc_cipher(session,
						auth_xform->key.data,
						auth_xform->key.length))
				return -EINVAL;
		}
		/* Restore to authentication only only */
		session->qat_cmd = ICP_QAT_FW_LA_CMD_AUTH;
	} else {
		if (qat_alg_aead_session_create_content_desc_auth(session,
				key_data,
				key_length,
				0,
				auth_xform->digest_length,
				auth_xform->op))
			return -EINVAL;
	}

	session->digest_length = auth_xform->digest_length;
	return 0;
}

int
qat_crypto_sym_configure_session_aead(struct rte_crypto_sym_xform *xform,
				struct qat_session *session)
{
	struct rte_crypto_aead_xform *aead_xform = &xform->aead;
	enum rte_crypto_auth_operation crypto_operation;

	/*
	 * Store AEAD IV parameters as cipher IV,
	 * to avoid unnecessary memory usage
	 */
	session->cipher_iv.offset = xform->aead.iv.offset;
	session->cipher_iv.length = xform->aead.iv.length;

	switch (aead_xform->algo) {
	case RTE_CRYPTO_AEAD_AES_GCM:
		if (qat_alg_validate_aes_key(aead_xform->key.length,
				&session->qat_cipher_alg) != 0) {
			PMD_DRV_LOG(ERR, "Invalid AES key size");
			return -EINVAL;
		}
		session->qat_mode = ICP_QAT_HW_CIPHER_CTR_MODE;
		session->qat_hash_alg = ICP_QAT_HW_AUTH_ALGO_GALOIS_128;
		break;
	case RTE_CRYPTO_AEAD_AES_CCM:
		if (qat_alg_validate_aes_key(aead_xform->key.length,
				&session->qat_cipher_alg) != 0) {
			PMD_DRV_LOG(ERR, "Invalid AES key size");
			return -EINVAL;
		}
		session->qat_mode = ICP_QAT_HW_CIPHER_CTR_MODE;
		session->qat_hash_alg = ICP_QAT_HW_AUTH_ALGO_AES_CBC_MAC;
		break;
	default:
		PMD_DRV_LOG(ERR, "Crypto: Undefined AEAD specified %u\n",
				aead_xform->algo);
		return -EINVAL;
	}

	if ((aead_xform->op == RTE_CRYPTO_AEAD_OP_ENCRYPT &&
			aead_xform->algo == RTE_CRYPTO_AEAD_AES_GCM) ||
			(aead_xform->op == RTE_CRYPTO_AEAD_OP_DECRYPT &&
			aead_xform->algo == RTE_CRYPTO_AEAD_AES_CCM)) {
		session->qat_dir = ICP_QAT_HW_CIPHER_ENCRYPT;
		/*
		 * It needs to create cipher desc content first,
		 * then authentication
		 */

		crypto_operation = aead_xform->algo == RTE_CRYPTO_AEAD_AES_GCM ?
			RTE_CRYPTO_AUTH_OP_GENERATE : RTE_CRYPTO_AUTH_OP_VERIFY;

		if (qat_alg_aead_session_create_content_desc_cipher(session,
					aead_xform->key.data,
					aead_xform->key.length))
			return -EINVAL;

		if (qat_alg_aead_session_create_content_desc_auth(session,
					aead_xform->key.data,
					aead_xform->key.length,
					aead_xform->aad_length,
					aead_xform->digest_length,
					crypto_operation))
			return -EINVAL;
	} else {
		session->qat_dir = ICP_QAT_HW_CIPHER_DECRYPT;
		/*
		 * It needs to create authentication desc content first,
		 * then cipher
		 */

		crypto_operation = aead_xform->algo == RTE_CRYPTO_AEAD_AES_GCM ?
			RTE_CRYPTO_AUTH_OP_VERIFY : RTE_CRYPTO_AUTH_OP_GENERATE;

		if (qat_alg_aead_session_create_content_desc_auth(session,
					aead_xform->key.data,
					aead_xform->key.length,
					aead_xform->aad_length,
					aead_xform->digest_length,
					crypto_operation))
			return -EINVAL;

		if (qat_alg_aead_session_create_content_desc_cipher(session,
					aead_xform->key.data,
					aead_xform->key.length))
			return -EINVAL;
	}

	session->digest_length = aead_xform->digest_length;
	return 0;
}

unsigned qat_crypto_sym_get_session_private_size(
		struct rte_cryptodev *dev __rte_unused)
{
	return RTE_ALIGN_CEIL(sizeof(struct qat_session), 8);
}

static inline uint32_t
qat_bpicipher_preprocess(struct qat_session *ctx,
				struct rte_crypto_op *op)
{
	int block_len = qat_cipher_get_block_size(ctx->qat_cipher_alg);
	struct rte_crypto_sym_op *sym_op = op->sym;
	uint8_t last_block_len = block_len > 0 ?
			sym_op->cipher.data.length % block_len : 0;

	if (last_block_len &&
			ctx->qat_dir == ICP_QAT_HW_CIPHER_DECRYPT) {

		/* Decrypt last block */
		uint8_t *last_block, *dst, *iv;
		uint32_t last_block_offset = sym_op->cipher.data.offset +
				sym_op->cipher.data.length - last_block_len;
		last_block = (uint8_t *) rte_pktmbuf_mtod_offset(sym_op->m_src,
				uint8_t *, last_block_offset);

		if (unlikely(sym_op->m_dst != NULL))
			/* out-of-place operation (OOP) */
			dst = (uint8_t *) rte_pktmbuf_mtod_offset(sym_op->m_dst,
						uint8_t *, last_block_offset);
		else
			dst = last_block;

		if (last_block_len < sym_op->cipher.data.length)
			/* use previous block ciphertext as IV */
			iv = last_block - block_len;
		else
			/* runt block, i.e. less than one full block */
			iv = rte_crypto_op_ctod_offset(op, uint8_t *,
					ctx->cipher_iv.offset);

#ifdef RTE_LIBRTE_PMD_QAT_DEBUG_TX
		rte_hexdump(stdout, "BPI: src before pre-process:", last_block,
			last_block_len);
		if (sym_op->m_dst != NULL)
			rte_hexdump(stdout, "BPI: dst before pre-process:", dst,
				last_block_len);
#endif
		bpi_cipher_decrypt(last_block, dst, iv, block_len,
				last_block_len, ctx->bpi_ctx);
#ifdef RTE_LIBRTE_PMD_QAT_DEBUG_TX
		rte_hexdump(stdout, "BPI: src after pre-process:", last_block,
			last_block_len);
		if (sym_op->m_dst != NULL)
			rte_hexdump(stdout, "BPI: dst after pre-process:", dst,
				last_block_len);
#endif
	}

	return sym_op->cipher.data.length - last_block_len;
}

static inline uint32_t
qat_bpicipher_postprocess(struct qat_session *ctx,
				struct rte_crypto_op *op)
{
	int block_len = qat_cipher_get_block_size(ctx->qat_cipher_alg);
	struct rte_crypto_sym_op *sym_op = op->sym;
	uint8_t last_block_len = block_len > 0 ?
			sym_op->cipher.data.length % block_len : 0;

	if (last_block_len > 0 &&
			ctx->qat_dir == ICP_QAT_HW_CIPHER_ENCRYPT) {

		/* Encrypt last block */
		uint8_t *last_block, *dst, *iv;
		uint32_t last_block_offset;

		last_block_offset = sym_op->cipher.data.offset +
				sym_op->cipher.data.length - last_block_len;
		last_block = (uint8_t *) rte_pktmbuf_mtod_offset(sym_op->m_src,
				uint8_t *, last_block_offset);

		if (unlikely(sym_op->m_dst != NULL))
			/* out-of-place operation (OOP) */
			dst = (uint8_t *) rte_pktmbuf_mtod_offset(sym_op->m_dst,
						uint8_t *, last_block_offset);
		else
			dst = last_block;

		if (last_block_len < sym_op->cipher.data.length)
			/* use previous block ciphertext as IV */
			iv = dst - block_len;
		else
			/* runt block, i.e. less than one full block */
			iv = rte_crypto_op_ctod_offset(op, uint8_t *,
					ctx->cipher_iv.offset);

#ifdef RTE_LIBRTE_PMD_QAT_DEBUG_RX
		rte_hexdump(stdout, "BPI: src before post-process:", last_block,
			last_block_len);
		if (sym_op->m_dst != NULL)
			rte_hexdump(stdout, "BPI: dst before post-process:",
					dst, last_block_len);
#endif
		bpi_cipher_encrypt(last_block, dst, iv, block_len,
				last_block_len, ctx->bpi_ctx);
#ifdef RTE_LIBRTE_PMD_QAT_DEBUG_RX
		rte_hexdump(stdout, "BPI: src after post-process:", last_block,
			last_block_len);
		if (sym_op->m_dst != NULL)
			rte_hexdump(stdout, "BPI: dst after post-process:", dst,
				last_block_len);
#endif
	}
	return sym_op->cipher.data.length - last_block_len;
}

static inline void
txq_write_tail(struct qat_qp *qp, struct qat_queue *q) {
	WRITE_CSR_RING_TAIL(qp->mmap_bar_addr, q->hw_bundle_number,
			q->hw_queue_number, q->tail);
	q->nb_pending_requests = 0;
	q->csr_tail = q->tail;
}

uint16_t
qat_pmd_enqueue_op_burst(void *qp, struct rte_crypto_op **ops,
		uint16_t nb_ops)
{
	register struct qat_queue *queue;
	struct qat_qp *tmp_qp = (struct qat_qp *)qp;
	register uint32_t nb_ops_sent = 0;
	register struct rte_crypto_op **cur_op = ops;
	register int ret;
	uint16_t nb_ops_possible = nb_ops;
	register uint8_t *base_addr;
	register uint32_t tail;
	int overflow;

	if (unlikely(nb_ops == 0))
		return 0;

	/* read params used a lot in main loop into registers */
	queue = &(tmp_qp->tx_q);
	base_addr = (uint8_t *)queue->base_addr;
	tail = queue->tail;

	/* Find how many can actually fit on the ring */
	tmp_qp->inflights16 += nb_ops;
	overflow = tmp_qp->inflights16 - queue->max_inflights;
	if (overflow > 0) {
		tmp_qp->inflights16 -= overflow;
		nb_ops_possible = nb_ops - overflow;
		if (nb_ops_possible == 0)
			return 0;
	}

	while (nb_ops_sent != nb_ops_possible) {
		ret = qat_write_hw_desc_entry(*cur_op, base_addr + tail,
			tmp_qp->op_cookies[tail / queue->msg_size], tmp_qp);
		if (ret != 0) {
			tmp_qp->stats.enqueue_err_count++;
			/*
			 * This message cannot be enqueued,
			 * decrease number of ops that wasn't sent
			 */
			tmp_qp->inflights16 -= nb_ops_possible - nb_ops_sent;
			if (nb_ops_sent == 0)
				return 0;
			goto kick_tail;
		}

		tail = adf_modulo(tail + queue->msg_size, queue->modulo);
		nb_ops_sent++;
		cur_op++;
	}
kick_tail:
	queue->tail = tail;
	tmp_qp->stats.enqueued_count += nb_ops_sent;
	queue->nb_pending_requests += nb_ops_sent;
	if (tmp_qp->inflights16 < QAT_CSR_TAIL_FORCE_WRITE_THRESH ||
			queue->nb_pending_requests > QAT_CSR_TAIL_WRITE_THRESH) {
		txq_write_tail(tmp_qp, queue);
	}
	return nb_ops_sent;
}

static inline
void rxq_free_desc(struct qat_qp *qp, struct qat_queue *q)
{
	uint32_t old_head, new_head;
	uint32_t max_head;

	old_head = q->csr_head;
	new_head = q->head;
	max_head = qp->nb_descriptors * q->msg_size;

	/* write out free descriptors */
	void *cur_desc = (uint8_t *)q->base_addr + old_head;

	if (new_head < old_head) {
		memset(cur_desc, ADF_RING_EMPTY_SIG_BYTE, max_head - old_head);
		memset(q->base_addr, ADF_RING_EMPTY_SIG_BYTE, new_head);
	} else {
		memset(cur_desc, ADF_RING_EMPTY_SIG_BYTE, new_head - old_head);
	}
	q->nb_processed_responses = 0;
	q->csr_head = new_head;

	/* write current head to CSR */
	WRITE_CSR_RING_HEAD(qp->mmap_bar_addr, q->hw_bundle_number,
			    q->hw_queue_number, new_head);
}

uint16_t
qat_pmd_dequeue_op_burst(void *qp, struct rte_crypto_op **ops,
		uint16_t nb_ops)
{
	struct qat_queue *rx_queue, *tx_queue;
	struct qat_qp *tmp_qp = (struct qat_qp *)qp;
	uint32_t msg_counter = 0;
	struct rte_crypto_op *rx_op;
	struct icp_qat_fw_comn_resp *resp_msg;
	uint32_t head;

	rx_queue = &(tmp_qp->rx_q);
	tx_queue = &(tmp_qp->tx_q);
	head = rx_queue->head;
	resp_msg = (struct icp_qat_fw_comn_resp *)
			((uint8_t *)rx_queue->base_addr + head);

	while (*(uint32_t *)resp_msg != ADF_RING_EMPTY_SIG &&
			msg_counter != nb_ops) {
		rx_op = (struct rte_crypto_op *)(uintptr_t)
				(resp_msg->opaque_data);

#ifdef RTE_LIBRTE_PMD_QAT_DEBUG_RX
		rte_hexdump(stdout, "qat_response:", (uint8_t *)resp_msg,
			sizeof(struct icp_qat_fw_comn_resp));
#endif
		if (ICP_QAT_FW_COMN_STATUS_FLAG_OK !=
				ICP_QAT_FW_COMN_RESP_CRYPTO_STAT_GET(
					resp_msg->comn_hdr.comn_status)) {
			rx_op->status = RTE_CRYPTO_OP_STATUS_AUTH_FAILED;
		} else {
			struct qat_session *sess = (struct qat_session *)
					get_session_private_data(
					rx_op->sym->session,
					cryptodev_qat_driver_id);

			if (sess->bpi_ctx)
				qat_bpicipher_postprocess(sess, rx_op);
			rx_op->status = RTE_CRYPTO_OP_STATUS_SUCCESS;
		}

		head = adf_modulo(head + rx_queue->msg_size, rx_queue->modulo);
		resp_msg = (struct icp_qat_fw_comn_resp *)
				((uint8_t *)rx_queue->base_addr + head);
		*ops = rx_op;
		ops++;
		msg_counter++;
	}
	if (msg_counter > 0) {
		rx_queue->head = head;
		tmp_qp->stats.dequeued_count += msg_counter;
		rx_queue->nb_processed_responses += msg_counter;
		tmp_qp->inflights16 -= msg_counter;

		if (rx_queue->nb_processed_responses > QAT_CSR_HEAD_WRITE_THRESH)
			rxq_free_desc(tmp_qp, rx_queue);
	}
	/* also check if tail needs to be advanced */
	if (tmp_qp->inflights16 <= QAT_CSR_TAIL_FORCE_WRITE_THRESH &&
			tx_queue->tail != tx_queue->csr_tail) {
		txq_write_tail(tmp_qp, tx_queue);
	}
	return msg_counter;
}

static inline int
qat_sgl_fill_array(struct rte_mbuf *buf, uint64_t buff_start,
		struct qat_alg_buf_list *list, uint32_t data_len)
{
	int nr = 1;

	uint32_t buf_len = rte_pktmbuf_iova(buf) -
			buff_start + rte_pktmbuf_data_len(buf);

	list->bufers[0].addr = buff_start;
	list->bufers[0].resrvd = 0;
	list->bufers[0].len = buf_len;

	if (data_len <= buf_len) {
		list->num_bufs = nr;
		list->bufers[0].len = data_len;
		return 0;
	}

	buf = buf->next;
	while (buf) {
		if (unlikely(nr == QAT_SGL_MAX_NUMBER)) {
			PMD_DRV_LOG(ERR, "QAT PMD exceeded size of QAT SGL"
					" entry(%u)",
					QAT_SGL_MAX_NUMBER);
			return -EINVAL;
		}

		list->bufers[nr].len = rte_pktmbuf_data_len(buf);
		list->bufers[nr].resrvd = 0;
		list->bufers[nr].addr = rte_pktmbuf_iova(buf);

		buf_len += list->bufers[nr].len;
		buf = buf->next;

		if (buf_len > data_len) {
			list->bufers[nr].len -=
				buf_len - data_len;
			buf = NULL;
		}
		++nr;
	}
	list->num_bufs = nr;

	return 0;
}

static inline void
set_cipher_iv(uint16_t iv_length, uint16_t iv_offset,
		struct icp_qat_fw_la_cipher_req_params *cipher_param,
		struct rte_crypto_op *op,
		struct icp_qat_fw_la_bulk_req *qat_req)
{
	/* copy IV into request if it fits */
	if (iv_length <= sizeof(cipher_param->u.cipher_IV_array)) {
		rte_memcpy(cipher_param->u.cipher_IV_array,
				rte_crypto_op_ctod_offset(op, uint8_t *,
					iv_offset),
				iv_length);
	} else {
		ICP_QAT_FW_LA_CIPH_IV_FLD_FLAG_SET(
				qat_req->comn_hdr.serv_specif_flags,
				ICP_QAT_FW_CIPH_IV_64BIT_PTR);
		cipher_param->u.s.cipher_IV_ptr =
				rte_crypto_op_ctophys_offset(op,
					iv_offset);
	}
}

/** Set IV for CCM is special case, 0th byte is set to q-1
 *  where q is padding of nonce in 16 byte block
 */
static inline void
set_cipher_iv_ccm(uint16_t iv_length, uint16_t iv_offset,
		struct icp_qat_fw_la_cipher_req_params *cipher_param,
		struct rte_crypto_op *op, uint8_t q, uint8_t aad_len_field_sz)
{
	rte_memcpy(((uint8_t *)cipher_param->u.cipher_IV_array) +
			ICP_QAT_HW_CCM_NONCE_OFFSET,
			rte_crypto_op_ctod_offset(op, uint8_t *,
				iv_offset) + ICP_QAT_HW_CCM_NONCE_OFFSET,
			iv_length);
	*(uint8_t *)&cipher_param->u.cipher_IV_array[0] =
			q - ICP_QAT_HW_CCM_NONCE_OFFSET;

	if (aad_len_field_sz)
		rte_memcpy(&op->sym->aead.aad.data[ICP_QAT_HW_CCM_NONCE_OFFSET],
			rte_crypto_op_ctod_offset(op, uint8_t *,
				iv_offset) + ICP_QAT_HW_CCM_NONCE_OFFSET,
			iv_length);
}

static inline int
qat_write_hw_desc_entry(struct rte_crypto_op *op, uint8_t *out_msg,
		struct qat_crypto_op_cookie *qat_op_cookie, struct qat_qp *qp)
{
	int ret = 0;
	struct qat_session *ctx;
	struct icp_qat_fw_la_cipher_req_params *cipher_param;
	struct icp_qat_fw_la_auth_req_params *auth_param;
	register struct icp_qat_fw_la_bulk_req *qat_req;
	uint8_t do_auth = 0, do_cipher = 0, do_aead = 0;
	uint32_t cipher_len = 0, cipher_ofs = 0;
	uint32_t auth_len = 0, auth_ofs = 0;
	uint32_t min_ofs = 0;
	uint64_t src_buf_start = 0, dst_buf_start = 0;
	uint8_t do_sgl = 0;

#ifdef RTE_LIBRTE_PMD_QAT_DEBUG_TX
	if (unlikely(op->type != RTE_CRYPTO_OP_TYPE_SYMMETRIC)) {
		PMD_DRV_LOG(ERR, "QAT PMD only supports symmetric crypto "
				"operation requests, op (%p) is not a "
				"symmetric operation.", op);
		return -EINVAL;
	}
#endif
	if (unlikely(op->sess_type == RTE_CRYPTO_OP_SESSIONLESS)) {
		PMD_DRV_LOG(ERR, "QAT PMD only supports session oriented"
				" requests, op (%p) is sessionless.", op);
		return -EINVAL;
	}

	ctx = (struct qat_session *)get_session_private_data(
			op->sym->session, cryptodev_qat_driver_id);

	if (unlikely(ctx == NULL)) {
		PMD_DRV_LOG(ERR, "Session was not created for this device");
		return -EINVAL;
	}

	if (unlikely(ctx->min_qat_dev_gen > qp->qat_dev_gen)) {
		PMD_DRV_LOG(ERR, "Session alg not supported on this device gen");
		op->status = RTE_CRYPTO_OP_STATUS_INVALID_SESSION;
		return -EINVAL;
	}



	qat_req = (struct icp_qat_fw_la_bulk_req *)out_msg;
	rte_mov128((uint8_t *)qat_req, (const uint8_t *)&(ctx->fw_req));
	qat_req->comn_mid.opaque_data = (uint64_t)(uintptr_t)op;
	cipher_param = (void *)&qat_req->serv_specif_rqpars;
	auth_param = (void *)((uint8_t *)cipher_param + sizeof(*cipher_param));

	if (ctx->qat_cmd == ICP_QAT_FW_LA_CMD_HASH_CIPHER ||
			ctx->qat_cmd == ICP_QAT_FW_LA_CMD_CIPHER_HASH) {
		/* AES-GCM or AES-CCM */
		if (ctx->qat_hash_alg == ICP_QAT_HW_AUTH_ALGO_GALOIS_128 ||
				ctx->qat_hash_alg == ICP_QAT_HW_AUTH_ALGO_GALOIS_64 ||
				(ctx->qat_cipher_alg == ICP_QAT_HW_CIPHER_ALGO_AES128
				&& ctx->qat_mode == ICP_QAT_HW_CIPHER_CTR_MODE
				&& ctx->qat_hash_alg ==
						ICP_QAT_HW_AUTH_ALGO_AES_CBC_MAC)) {
			do_aead = 1;
		} else {
			do_auth = 1;
			do_cipher = 1;
		}
	} else if (ctx->qat_cmd == ICP_QAT_FW_LA_CMD_AUTH) {
		do_auth = 1;
		do_cipher = 0;
	} else if (ctx->qat_cmd == ICP_QAT_FW_LA_CMD_CIPHER) {
		do_auth = 0;
		do_cipher = 1;
	}

	if (do_cipher) {

		if (ctx->qat_cipher_alg ==
					 ICP_QAT_HW_CIPHER_ALGO_SNOW_3G_UEA2 ||
			ctx->qat_cipher_alg == ICP_QAT_HW_CIPHER_ALGO_KASUMI ||
			ctx->qat_cipher_alg ==
				ICP_QAT_HW_CIPHER_ALGO_ZUC_3G_128_EEA3) {

			if (unlikely(
				(cipher_param->cipher_length % BYTE_LENGTH != 0)
				 || (cipher_param->cipher_offset
							% BYTE_LENGTH != 0))) {
				PMD_DRV_LOG(ERR,
		  "SNOW3G/KASUMI/ZUC in QAT PMD only supports byte aligned values");
				op->status = RTE_CRYPTO_OP_STATUS_INVALID_ARGS;
				return -EINVAL;
			}
			cipher_len = op->sym->cipher.data.length >> 3;
			cipher_ofs = op->sym->cipher.data.offset >> 3;

		} else if (ctx->bpi_ctx) {
			/* DOCSIS - only send complete blocks to device
			 * Process any partial block using CFB mode.
			 * Even if 0 complete blocks, still send this to device
			 * to get into rx queue for post-process and dequeuing
			 */
			cipher_len = qat_bpicipher_preprocess(ctx, op);
			cipher_ofs = op->sym->cipher.data.offset;
		} else {
			cipher_len = op->sym->cipher.data.length;
			cipher_ofs = op->sym->cipher.data.offset;
		}

		set_cipher_iv(ctx->cipher_iv.length, ctx->cipher_iv.offset,
				cipher_param, op, qat_req);
		min_ofs = cipher_ofs;
	}

	if (do_auth) {

		if (ctx->qat_hash_alg == ICP_QAT_HW_AUTH_ALGO_SNOW_3G_UIA2 ||
			ctx->qat_hash_alg == ICP_QAT_HW_AUTH_ALGO_KASUMI_F9 ||
			ctx->qat_hash_alg ==
				ICP_QAT_HW_AUTH_ALGO_ZUC_3G_128_EIA3) {
			if (unlikely((auth_param->auth_off % BYTE_LENGTH != 0)
				|| (auth_param->auth_len % BYTE_LENGTH != 0))) {
				PMD_DRV_LOG(ERR,
		"For SNOW3G/KASUMI/ZUC, QAT PMD only supports byte aligned values");
				op->status = RTE_CRYPTO_OP_STATUS_INVALID_ARGS;
				return -EINVAL;
			}
			auth_ofs = op->sym->auth.data.offset >> 3;
			auth_len = op->sym->auth.data.length >> 3;

			auth_param->u1.aad_adr =
					rte_crypto_op_ctophys_offset(op,
							ctx->auth_iv.offset);

		} else if (ctx->qat_hash_alg ==
					ICP_QAT_HW_AUTH_ALGO_GALOIS_128 ||
				ctx->qat_hash_alg ==
					ICP_QAT_HW_AUTH_ALGO_GALOIS_64) {
			/* AES-GMAC */
			set_cipher_iv(ctx->auth_iv.length,
				ctx->auth_iv.offset,
				cipher_param, op, qat_req);
			auth_ofs = op->sym->auth.data.offset;
			auth_len = op->sym->auth.data.length;

			auth_param->u1.aad_adr = 0;
			auth_param->u2.aad_sz = 0;

			/*
			 * If len(iv)==12B fw computes J0
			 */
			if (ctx->auth_iv.length == 12) {
				ICP_QAT_FW_LA_GCM_IV_LEN_FLAG_SET(
					qat_req->comn_hdr.serv_specif_flags,
					ICP_QAT_FW_LA_GCM_IV_LEN_12_OCTETS);

			}
		} else {
			auth_ofs = op->sym->auth.data.offset;
			auth_len = op->sym->auth.data.length;

		}
		min_ofs = auth_ofs;

		if (likely(ctx->qat_hash_alg != ICP_QAT_HW_AUTH_ALGO_NULL))
			auth_param->auth_res_addr =
					op->sym->auth.digest.phys_addr;

	}

	if (do_aead) {
		/*
		 * This address may used for setting AAD physical pointer
		 * into IV offset from op
		 */
		rte_iova_t aad_phys_addr_aead = op->sym->aead.aad.phys_addr;
		if (ctx->qat_hash_alg ==
				ICP_QAT_HW_AUTH_ALGO_GALOIS_128 ||
				ctx->qat_hash_alg ==
					ICP_QAT_HW_AUTH_ALGO_GALOIS_64) {
			/*
			 * If len(iv)==12B fw computes J0
			 */
			if (ctx->cipher_iv.length == 12) {
				ICP_QAT_FW_LA_GCM_IV_LEN_FLAG_SET(
					qat_req->comn_hdr.serv_specif_flags,
					ICP_QAT_FW_LA_GCM_IV_LEN_12_OCTETS);
			}

			set_cipher_iv(ctx->cipher_iv.length,
					ctx->cipher_iv.offset,
					cipher_param, op, qat_req);

		} else if (ctx->qat_hash_alg ==
				ICP_QAT_HW_AUTH_ALGO_AES_CBC_MAC) {

			/* In case of AES-CCM this may point to user selected memory
			 * or iv offset in cypto_op
			 */
			uint8_t *aad_data = op->sym->aead.aad.data;
			/* This is true AAD length, it not includes 18 bytes of
			 * preceding data
			 */
			uint8_t aad_ccm_real_len = 0;

			uint8_t aad_len_field_sz = 0;
			uint32_t msg_len_be =
					rte_bswap32(op->sym->aead.data.length);

			if (ctx->aad_len > ICP_QAT_HW_CCM_AAD_DATA_OFFSET) {
				aad_len_field_sz = ICP_QAT_HW_CCM_AAD_LEN_INFO;
				aad_ccm_real_len = ctx->aad_len -
					ICP_QAT_HW_CCM_AAD_B0_LEN -
					ICP_QAT_HW_CCM_AAD_LEN_INFO;
			} else {
				/*
				 * aad_len not greater than 18, so no actual aad data,
				 * then use IV after op for B0 block
				 */
				aad_data = rte_crypto_op_ctod_offset(op, uint8_t *,
						ctx->cipher_iv.offset);
				aad_phys_addr_aead =
						rte_crypto_op_ctophys_offset(op,
								ctx->cipher_iv.offset);
			}

			uint8_t q = ICP_QAT_HW_CCM_NQ_CONST - ctx->cipher_iv.length;

			aad_data[0] = ICP_QAT_HW_CCM_BUILD_B0_FLAGS(aad_len_field_sz,
							ctx->digest_length, q);

			if (q > ICP_QAT_HW_CCM_MSG_LEN_MAX_FIELD_SIZE) {
				memcpy(aad_data	+ ctx->cipher_iv.length +
					ICP_QAT_HW_CCM_NONCE_OFFSET
					+ (q - ICP_QAT_HW_CCM_MSG_LEN_MAX_FIELD_SIZE),
					(uint8_t *)&msg_len_be,
					ICP_QAT_HW_CCM_MSG_LEN_MAX_FIELD_SIZE);
			} else {
				memcpy(aad_data	+ ctx->cipher_iv.length +
					ICP_QAT_HW_CCM_NONCE_OFFSET,
					(uint8_t *)&msg_len_be
					+ (ICP_QAT_HW_CCM_MSG_LEN_MAX_FIELD_SIZE
					- q), q);
			}

			if (aad_len_field_sz > 0) {
				*(uint16_t *)&aad_data[ICP_QAT_HW_CCM_AAD_B0_LEN]
						= rte_bswap16(aad_ccm_real_len);

				if ((aad_ccm_real_len + aad_len_field_sz)
						% ICP_QAT_HW_CCM_AAD_B0_LEN) {
					uint8_t pad_len = 0;
					uint8_t pad_idx = 0;

					pad_len = ICP_QAT_HW_CCM_AAD_B0_LEN -
						((aad_ccm_real_len + aad_len_field_sz) %
							ICP_QAT_HW_CCM_AAD_B0_LEN);
					pad_idx = ICP_QAT_HW_CCM_AAD_B0_LEN +
						aad_ccm_real_len + aad_len_field_sz;
					memset(&aad_data[pad_idx],
							0, pad_len);
				}

			}

			set_cipher_iv_ccm(ctx->cipher_iv.length,
					ctx->cipher_iv.offset,
					cipher_param, op, q,
					aad_len_field_sz);

		}

		cipher_len = op->sym->aead.data.length;
		cipher_ofs = op->sym->aead.data.offset;
		auth_len = op->sym->aead.data.length;
		auth_ofs = op->sym->aead.data.offset;

		auth_param->u1.aad_adr = aad_phys_addr_aead;
		auth_param->auth_res_addr = op->sym->aead.digest.phys_addr;
		min_ofs = op->sym->aead.data.offset;
	}

	if (op->sym->m_src->next || (op->sym->m_dst && op->sym->m_dst->next))
		do_sgl = 1;

	/* adjust for chain case */
	if (do_cipher && do_auth)
		min_ofs = cipher_ofs < auth_ofs ? cipher_ofs : auth_ofs;

	if (unlikely(min_ofs >= rte_pktmbuf_data_len(op->sym->m_src) && do_sgl))
		min_ofs = 0;

	if (unlikely(op->sym->m_dst != NULL)) {
		/* Out-of-place operation (OOP)
		 * Don't align DMA start. DMA the minimum data-set
		 * so as not to overwrite data in dest buffer
		 */
		src_buf_start =
			rte_pktmbuf_iova_offset(op->sym->m_src, min_ofs);
		dst_buf_start =
			rte_pktmbuf_iova_offset(op->sym->m_dst, min_ofs);

	} else {
		/* In-place operation
		 * Start DMA at nearest aligned address below min_ofs
		 */
		src_buf_start =
			rte_pktmbuf_iova_offset(op->sym->m_src, min_ofs)
						& QAT_64_BTYE_ALIGN_MASK;

		if (unlikely((rte_pktmbuf_iova(op->sym->m_src) -
					rte_pktmbuf_headroom(op->sym->m_src))
							> src_buf_start)) {
			/* alignment has pushed addr ahead of start of mbuf
			 * so revert and take the performance hit
			 */
			src_buf_start =
				rte_pktmbuf_iova_offset(op->sym->m_src,
								min_ofs);
		}
		dst_buf_start = src_buf_start;
	}

	if (do_cipher || do_aead) {
		cipher_param->cipher_offset =
				(uint32_t)rte_pktmbuf_iova_offset(
				op->sym->m_src, cipher_ofs) - src_buf_start;
		cipher_param->cipher_length = cipher_len;
	} else {
		cipher_param->cipher_offset = 0;
		cipher_param->cipher_length = 0;
	}

	if (do_auth || do_aead) {
		auth_param->auth_off = (uint32_t)rte_pktmbuf_iova_offset(
				op->sym->m_src, auth_ofs) - src_buf_start;
		auth_param->auth_len = auth_len;
	} else {
		auth_param->auth_off = 0;
		auth_param->auth_len = 0;
	}

	qat_req->comn_mid.dst_length =
		qat_req->comn_mid.src_length =
		(cipher_param->cipher_offset + cipher_param->cipher_length)
		> (auth_param->auth_off + auth_param->auth_len) ?
		(cipher_param->cipher_offset + cipher_param->cipher_length)
		: (auth_param->auth_off + auth_param->auth_len);

	if (do_sgl) {

		ICP_QAT_FW_COMN_PTR_TYPE_SET(qat_req->comn_hdr.comn_req_flags,
				QAT_COMN_PTR_TYPE_SGL);
		ret = qat_sgl_fill_array(op->sym->m_src, src_buf_start,
				&qat_op_cookie->qat_sgl_list_src,
				qat_req->comn_mid.src_length);
		if (ret) {
			PMD_DRV_LOG(ERR, "QAT PMD Cannot fill sgl array");
			return ret;
		}

		if (likely(op->sym->m_dst == NULL))
			qat_req->comn_mid.dest_data_addr =
				qat_req->comn_mid.src_data_addr =
				qat_op_cookie->qat_sgl_src_phys_addr;
		else {
			ret = qat_sgl_fill_array(op->sym->m_dst,
					dst_buf_start,
					&qat_op_cookie->qat_sgl_list_dst,
						qat_req->comn_mid.dst_length);

			if (ret) {
				PMD_DRV_LOG(ERR, "QAT PMD Cannot "
						"fill sgl array");
				return ret;
			}

			qat_req->comn_mid.src_data_addr =
				qat_op_cookie->qat_sgl_src_phys_addr;
			qat_req->comn_mid.dest_data_addr =
					qat_op_cookie->qat_sgl_dst_phys_addr;
		}
	} else {
		qat_req->comn_mid.src_data_addr = src_buf_start;
		qat_req->comn_mid.dest_data_addr = dst_buf_start;
	}

#ifdef RTE_LIBRTE_PMD_QAT_DEBUG_TX
	rte_hexdump(stdout, "qat_req:", qat_req,
			sizeof(struct icp_qat_fw_la_bulk_req));
	rte_hexdump(stdout, "src_data:",
			rte_pktmbuf_mtod(op->sym->m_src, uint8_t*),
			rte_pktmbuf_data_len(op->sym->m_src));
	if (do_cipher) {
		uint8_t *cipher_iv_ptr = rte_crypto_op_ctod_offset(op,
						uint8_t *,
						ctx->cipher_iv.offset);
		rte_hexdump(stdout, "cipher iv:", cipher_iv_ptr,
				ctx->cipher_iv.length);
	}

	if (do_auth) {
		if (ctx->auth_iv.length) {
			uint8_t *auth_iv_ptr = rte_crypto_op_ctod_offset(op,
							uint8_t *,
							ctx->auth_iv.offset);
			rte_hexdump(stdout, "auth iv:", auth_iv_ptr,
						ctx->auth_iv.length);
		}
		rte_hexdump(stdout, "digest:", op->sym->auth.digest.data,
				ctx->digest_length);
	}

	if (do_aead) {
		rte_hexdump(stdout, "digest:", op->sym->aead.digest.data,
				ctx->digest_length);
		rte_hexdump(stdout, "aad:", op->sym->aead.aad.data,
				ctx->aad_len);
	}
#endif
	return 0;
}

static inline uint32_t adf_modulo(uint32_t data, uint32_t shift)
{
	uint32_t div = data >> shift;
	uint32_t mult = div << shift;

	return data - mult;
}

int qat_dev_config(__rte_unused struct rte_cryptodev *dev,
		__rte_unused struct rte_cryptodev_config *config)
{
	PMD_INIT_FUNC_TRACE();
	return 0;
}

int qat_dev_start(__rte_unused struct rte_cryptodev *dev)
{
	PMD_INIT_FUNC_TRACE();
	return 0;
}

void qat_dev_stop(__rte_unused struct rte_cryptodev *dev)
{
	PMD_INIT_FUNC_TRACE();
}

int qat_dev_close(struct rte_cryptodev *dev)
{
	int i, ret;

	PMD_INIT_FUNC_TRACE();

	for (i = 0; i < dev->data->nb_queue_pairs; i++) {
		ret = qat_crypto_sym_qp_release(dev, i);
		if (ret < 0)
			return ret;
	}

	return 0;
}

void qat_dev_info_get(struct rte_cryptodev *dev,
			struct rte_cryptodev_info *info)
{
	struct qat_pmd_private *internals = dev->data->dev_private;

	PMD_INIT_FUNC_TRACE();
	if (info != NULL) {
		info->max_nb_queue_pairs =
				ADF_NUM_SYM_QPS_PER_BUNDLE *
				ADF_NUM_BUNDLES_PER_DEV;
		info->feature_flags = dev->feature_flags;
		info->capabilities = internals->qat_dev_capabilities;
		info->sym.max_nb_sessions = internals->max_nb_sessions;
		info->driver_id = cryptodev_qat_driver_id;
		info->pci_dev = RTE_DEV_TO_PCI(dev->device);
	}
}

void qat_crypto_sym_stats_get(struct rte_cryptodev *dev,
		struct rte_cryptodev_stats *stats)
{
	int i;
	struct qat_qp **qp = (struct qat_qp **)(dev->data->queue_pairs);

	PMD_INIT_FUNC_TRACE();
	if (stats == NULL) {
		PMD_DRV_LOG(ERR, "invalid stats ptr NULL");
		return;
	}
	for (i = 0; i < dev->data->nb_queue_pairs; i++) {
		if (qp[i] == NULL) {
			PMD_DRV_LOG(DEBUG, "Uninitialised queue pair");
			continue;
		}

		stats->enqueued_count += qp[i]->stats.enqueued_count;
		stats->dequeued_count += qp[i]->stats.dequeued_count;
		stats->enqueue_err_count += qp[i]->stats.enqueue_err_count;
		stats->dequeue_err_count += qp[i]->stats.dequeue_err_count;
	}
}

void qat_crypto_sym_stats_reset(struct rte_cryptodev *dev)
{
	int i;
	struct qat_qp **qp = (struct qat_qp **)(dev->data->queue_pairs);

	PMD_INIT_FUNC_TRACE();
	for (i = 0; i < dev->data->nb_queue_pairs; i++)
		memset(&(qp[i]->stats), 0, sizeof(qp[i]->stats));
	PMD_DRV_LOG(DEBUG, "QAT crypto: stats cleared");
}
