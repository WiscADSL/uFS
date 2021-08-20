/* SPDX-License-Identifier: BSD-3-Clause
 *
 *   Copyright (c) 2016 Freescale Semiconductor, Inc. All rights reserved.
 *   Copyright 2016 NXP
 *
 */

#include <time.h>
#include <net/if.h>

#include <rte_mbuf.h>
#include <rte_cryptodev.h>
#include <rte_security_driver.h>
#include <rte_malloc.h>
#include <rte_memcpy.h>
#include <rte_string_fns.h>
#include <rte_cycles.h>
#include <rte_kvargs.h>
#include <rte_dev.h>
#include <rte_cryptodev_pmd.h>
#include <rte_common.h>
#include <rte_fslmc.h>
#include <fslmc_vfio.h>
#include <dpaa2_hw_pvt.h>
#include <dpaa2_hw_dpio.h>
#include <dpaa2_hw_mempool.h>
#include <fsl_dpseci.h>
#include <fsl_mc_sys.h>

#include "dpaa2_sec_priv.h"
#include "dpaa2_sec_logs.h"

/* RTA header files */
#include <hw/desc/ipsec.h>
#include <hw/desc/algo.h>

/* Minimum job descriptor consists of a oneword job descriptor HEADER and
 * a pointer to the shared descriptor
 */
#define MIN_JOB_DESC_SIZE	(CAAM_CMD_SZ + CAAM_PTR_SZ)
#define FSL_VENDOR_ID           0x1957
#define FSL_DEVICE_ID           0x410
#define FSL_SUBSYSTEM_SEC       1
#define FSL_MC_DPSECI_DEVID     3

#define NO_PREFETCH 0
/* FLE_POOL_NUM_BUFS is set as per the ipsec-secgw application */
#define FLE_POOL_NUM_BUFS	32000
#define FLE_POOL_BUF_SIZE	256
#define FLE_POOL_CACHE_SIZE	512
#define FLE_SG_MEM_SIZE		2048
#define SEC_FLC_DHR_OUTBOUND	-114
#define SEC_FLC_DHR_INBOUND	0

enum rta_sec_era rta_sec_era = RTA_SEC_ERA_8;

static uint8_t cryptodev_driver_id;

static inline int
build_proto_fd(dpaa2_sec_session *sess,
	       struct rte_crypto_op *op,
	       struct qbman_fd *fd, uint16_t bpid)
{
	struct rte_crypto_sym_op *sym_op = op->sym;
	struct ctxt_priv *priv = sess->ctxt;
	struct sec_flow_context *flc;
	struct rte_mbuf *mbuf = sym_op->m_src;

	if (likely(bpid < MAX_BPID))
		DPAA2_SET_FD_BPID(fd, bpid);
	else
		DPAA2_SET_FD_IVP(fd);

	/* Save the shared descriptor */
	flc = &priv->flc_desc[0].flc;

	DPAA2_SET_FD_ADDR(fd, DPAA2_MBUF_VADDR_TO_IOVA(sym_op->m_src));
	DPAA2_SET_FD_OFFSET(fd, sym_op->m_src->data_off);
	DPAA2_SET_FD_LEN(fd, sym_op->m_src->pkt_len);
	DPAA2_SET_FD_FLC(fd, ((uint64_t)flc));

	/* save physical address of mbuf */
	op->sym->aead.digest.phys_addr = mbuf->buf_iova;
	mbuf->buf_iova = (uint64_t)op;

	return 0;
}

static inline int
build_authenc_gcm_sg_fd(dpaa2_sec_session *sess,
		 struct rte_crypto_op *op,
		 struct qbman_fd *fd, __rte_unused uint16_t bpid)
{
	struct rte_crypto_sym_op *sym_op = op->sym;
	struct ctxt_priv *priv = sess->ctxt;
	struct qbman_fle *fle, *sge, *ip_fle, *op_fle;
	struct sec_flow_context *flc;
	uint32_t auth_only_len = sess->ext_params.aead_ctxt.auth_only_len;
	int icv_len = sess->digest_length;
	uint8_t *old_icv;
	struct rte_mbuf *mbuf;
	uint8_t *IV_ptr = rte_crypto_op_ctod_offset(op, uint8_t *,
			sess->iv.offset);

	PMD_INIT_FUNC_TRACE();

	if (sym_op->m_dst)
		mbuf = sym_op->m_dst;
	else
		mbuf = sym_op->m_src;

	/* first FLE entry used to store mbuf and session ctxt */
	fle = (struct qbman_fle *)rte_malloc(NULL, FLE_SG_MEM_SIZE,
			RTE_CACHE_LINE_SIZE);
	if (unlikely(!fle)) {
		RTE_LOG(ERR, PMD, "GCM SG: Memory alloc failed for SGE\n");
		return -1;
	}
	memset(fle, 0, FLE_SG_MEM_SIZE);
	DPAA2_SET_FLE_ADDR(fle, DPAA2_OP_VADDR_TO_IOVA(op));
	DPAA2_FLE_SAVE_CTXT(fle, priv);

	op_fle = fle + 1;
	ip_fle = fle + 2;
	sge = fle + 3;

	/* Save the shared descriptor */
	flc = &priv->flc_desc[0].flc;

	/* Configure FD as a FRAME LIST */
	DPAA2_SET_FD_ADDR(fd, DPAA2_VADDR_TO_IOVA(op_fle));
	DPAA2_SET_FD_COMPOUND_FMT(fd);
	DPAA2_SET_FD_FLC(fd, DPAA2_VADDR_TO_IOVA(flc));

	PMD_TX_LOG(DEBUG, "GCM SG: auth_off: 0x%x/length %d, digest-len=%d\n"
		   "iv-len=%d data_off: 0x%x\n",
		   sym_op->aead.data.offset,
		   sym_op->aead.data.length,
		   sym_op->aead.digest.length,
		   sess->iv.length,
		   sym_op->m_src->data_off);

	/* Configure Output FLE with Scatter/Gather Entry */
	DPAA2_SET_FLE_SG_EXT(op_fle);
	DPAA2_SET_FLE_ADDR(op_fle, DPAA2_VADDR_TO_IOVA(sge));

	if (auth_only_len)
		DPAA2_SET_FLE_INTERNAL_JD(op_fle, auth_only_len);

	op_fle->length = (sess->dir == DIR_ENC) ?
			(sym_op->aead.data.length + icv_len + auth_only_len) :
			sym_op->aead.data.length + auth_only_len;

	/* Configure Output SGE for Encap/Decap */
	DPAA2_SET_FLE_ADDR(sge, DPAA2_MBUF_VADDR_TO_IOVA(mbuf));
	DPAA2_SET_FLE_OFFSET(sge, mbuf->data_off + sym_op->aead.data.offset -
								auth_only_len);
	sge->length = mbuf->data_len - sym_op->aead.data.offset + auth_only_len;

	mbuf = mbuf->next;
	/* o/p segs */
	while (mbuf) {
		sge++;
		DPAA2_SET_FLE_ADDR(sge, DPAA2_MBUF_VADDR_TO_IOVA(mbuf));
		DPAA2_SET_FLE_OFFSET(sge, mbuf->data_off);
		sge->length = mbuf->data_len;
		mbuf = mbuf->next;
	}
	sge->length -= icv_len;

	if (sess->dir == DIR_ENC) {
		sge++;
		DPAA2_SET_FLE_ADDR(sge,
				DPAA2_VADDR_TO_IOVA(sym_op->aead.digest.data));
		sge->length = icv_len;
	}
	DPAA2_SET_FLE_FIN(sge);

	sge++;
	mbuf = sym_op->m_src;

	/* Configure Input FLE with Scatter/Gather Entry */
	DPAA2_SET_FLE_ADDR(ip_fle, DPAA2_VADDR_TO_IOVA(sge));
	DPAA2_SET_FLE_SG_EXT(ip_fle);
	DPAA2_SET_FLE_FIN(ip_fle);
	ip_fle->length = (sess->dir == DIR_ENC) ?
		(sym_op->aead.data.length + sess->iv.length + auth_only_len) :
		(sym_op->aead.data.length + sess->iv.length + auth_only_len +
		 icv_len);

	/* Configure Input SGE for Encap/Decap */
	DPAA2_SET_FLE_ADDR(sge, DPAA2_VADDR_TO_IOVA(IV_ptr));
	sge->length = sess->iv.length;

	sge++;
	if (auth_only_len) {
		DPAA2_SET_FLE_ADDR(sge,
				DPAA2_VADDR_TO_IOVA(sym_op->aead.aad.data));
		sge->length = auth_only_len;
		sge++;
	}

	DPAA2_SET_FLE_ADDR(sge, DPAA2_MBUF_VADDR_TO_IOVA(mbuf));
	DPAA2_SET_FLE_OFFSET(sge, sym_op->aead.data.offset +
				mbuf->data_off);
	sge->length = mbuf->data_len - sym_op->aead.data.offset;

	mbuf = mbuf->next;
	/* i/p segs */
	while (mbuf) {
		sge++;
		DPAA2_SET_FLE_ADDR(sge, DPAA2_MBUF_VADDR_TO_IOVA(mbuf));
		DPAA2_SET_FLE_OFFSET(sge, mbuf->data_off);
		sge->length = mbuf->data_len;
		mbuf = mbuf->next;
	}

	if (sess->dir == DIR_DEC) {
		sge++;
		old_icv = (uint8_t *)(sge + 1);
		memcpy(old_icv,	sym_op->aead.digest.data, icv_len);
		DPAA2_SET_FLE_ADDR(sge, DPAA2_VADDR_TO_IOVA(old_icv));
		sge->length = icv_len;
	}

	DPAA2_SET_FLE_FIN(sge);
	if (auth_only_len) {
		DPAA2_SET_FLE_INTERNAL_JD(ip_fle, auth_only_len);
		DPAA2_SET_FD_INTERNAL_JD(fd, auth_only_len);
	}
	DPAA2_SET_FD_LEN(fd, ip_fle->length);

	return 0;
}

static inline int
build_authenc_gcm_fd(dpaa2_sec_session *sess,
		     struct rte_crypto_op *op,
		     struct qbman_fd *fd, uint16_t bpid)
{
	struct rte_crypto_sym_op *sym_op = op->sym;
	struct ctxt_priv *priv = sess->ctxt;
	struct qbman_fle *fle, *sge;
	struct sec_flow_context *flc;
	uint32_t auth_only_len = sess->ext_params.aead_ctxt.auth_only_len;
	int icv_len = sess->digest_length, retval;
	uint8_t *old_icv;
	struct rte_mbuf *dst;
	uint8_t *IV_ptr = rte_crypto_op_ctod_offset(op, uint8_t *,
			sess->iv.offset);

	PMD_INIT_FUNC_TRACE();

	if (sym_op->m_dst)
		dst = sym_op->m_dst;
	else
		dst = sym_op->m_src;

	/* TODO we are using the first FLE entry to store Mbuf and session ctxt.
	 * Currently we donot know which FLE has the mbuf stored.
	 * So while retreiving we can go back 1 FLE from the FD -ADDR
	 * to get the MBUF Addr from the previous FLE.
	 * We can have a better approach to use the inline Mbuf
	 */
	retval = rte_mempool_get(priv->fle_pool, (void **)(&fle));
	if (retval) {
		RTE_LOG(ERR, PMD, "GCM: Memory alloc failed for SGE\n");
		return -1;
	}
	memset(fle, 0, FLE_POOL_BUF_SIZE);
	DPAA2_SET_FLE_ADDR(fle, DPAA2_OP_VADDR_TO_IOVA(op));
	DPAA2_FLE_SAVE_CTXT(fle, priv);
	fle = fle + 1;
	sge = fle + 2;
	if (likely(bpid < MAX_BPID)) {
		DPAA2_SET_FD_BPID(fd, bpid);
		DPAA2_SET_FLE_BPID(fle, bpid);
		DPAA2_SET_FLE_BPID(fle + 1, bpid);
		DPAA2_SET_FLE_BPID(sge, bpid);
		DPAA2_SET_FLE_BPID(sge + 1, bpid);
		DPAA2_SET_FLE_BPID(sge + 2, bpid);
		DPAA2_SET_FLE_BPID(sge + 3, bpid);
	} else {
		DPAA2_SET_FD_IVP(fd);
		DPAA2_SET_FLE_IVP(fle);
		DPAA2_SET_FLE_IVP((fle + 1));
		DPAA2_SET_FLE_IVP(sge);
		DPAA2_SET_FLE_IVP((sge + 1));
		DPAA2_SET_FLE_IVP((sge + 2));
		DPAA2_SET_FLE_IVP((sge + 3));
	}

	/* Save the shared descriptor */
	flc = &priv->flc_desc[0].flc;
	/* Configure FD as a FRAME LIST */
	DPAA2_SET_FD_ADDR(fd, DPAA2_VADDR_TO_IOVA(fle));
	DPAA2_SET_FD_COMPOUND_FMT(fd);
	DPAA2_SET_FD_FLC(fd, DPAA2_VADDR_TO_IOVA(flc));

	PMD_TX_LOG(DEBUG, "GCM: auth_off: 0x%x/length %d, digest-len=%d\n"
		   "iv-len=%d data_off: 0x%x\n",
		   sym_op->aead.data.offset,
		   sym_op->aead.data.length,
		   sym_op->aead.digest.length,
		   sess->iv.length,
		   sym_op->m_src->data_off);

	/* Configure Output FLE with Scatter/Gather Entry */
	DPAA2_SET_FLE_ADDR(fle, DPAA2_VADDR_TO_IOVA(sge));
	if (auth_only_len)
		DPAA2_SET_FLE_INTERNAL_JD(fle, auth_only_len);
	fle->length = (sess->dir == DIR_ENC) ?
			(sym_op->aead.data.length + icv_len + auth_only_len) :
			sym_op->aead.data.length + auth_only_len;

	DPAA2_SET_FLE_SG_EXT(fle);

	/* Configure Output SGE for Encap/Decap */
	DPAA2_SET_FLE_ADDR(sge, DPAA2_MBUF_VADDR_TO_IOVA(dst));
	DPAA2_SET_FLE_OFFSET(sge, sym_op->aead.data.offset +
				dst->data_off - auth_only_len);
	sge->length = sym_op->aead.data.length + auth_only_len;

	if (sess->dir == DIR_ENC) {
		sge++;
		DPAA2_SET_FLE_ADDR(sge,
				DPAA2_VADDR_TO_IOVA(sym_op->aead.digest.data));
		sge->length = sess->digest_length;
		DPAA2_SET_FD_LEN(fd, (sym_op->aead.data.length +
					sess->iv.length + auth_only_len));
	}
	DPAA2_SET_FLE_FIN(sge);

	sge++;
	fle++;

	/* Configure Input FLE with Scatter/Gather Entry */
	DPAA2_SET_FLE_ADDR(fle, DPAA2_VADDR_TO_IOVA(sge));
	DPAA2_SET_FLE_SG_EXT(fle);
	DPAA2_SET_FLE_FIN(fle);
	fle->length = (sess->dir == DIR_ENC) ?
		(sym_op->aead.data.length + sess->iv.length + auth_only_len) :
		(sym_op->aead.data.length + sess->iv.length + auth_only_len +
		 sess->digest_length);

	/* Configure Input SGE for Encap/Decap */
	DPAA2_SET_FLE_ADDR(sge, DPAA2_VADDR_TO_IOVA(IV_ptr));
	sge->length = sess->iv.length;
	sge++;
	if (auth_only_len) {
		DPAA2_SET_FLE_ADDR(sge,
				DPAA2_VADDR_TO_IOVA(sym_op->aead.aad.data));
		sge->length = auth_only_len;
		DPAA2_SET_FLE_BPID(sge, bpid);
		sge++;
	}

	DPAA2_SET_FLE_ADDR(sge, DPAA2_MBUF_VADDR_TO_IOVA(sym_op->m_src));
	DPAA2_SET_FLE_OFFSET(sge, sym_op->aead.data.offset +
				sym_op->m_src->data_off);
	sge->length = sym_op->aead.data.length;
	if (sess->dir == DIR_DEC) {
		sge++;
		old_icv = (uint8_t *)(sge + 1);
		memcpy(old_icv,	sym_op->aead.digest.data,
		       sess->digest_length);
		DPAA2_SET_FLE_ADDR(sge, DPAA2_VADDR_TO_IOVA(old_icv));
		sge->length = sess->digest_length;
		DPAA2_SET_FD_LEN(fd, (sym_op->aead.data.length +
				 sess->digest_length +
				 sess->iv.length +
				 auth_only_len));
	}
	DPAA2_SET_FLE_FIN(sge);

	if (auth_only_len) {
		DPAA2_SET_FLE_INTERNAL_JD(fle, auth_only_len);
		DPAA2_SET_FD_INTERNAL_JD(fd, auth_only_len);
	}

	return 0;
}

static inline int
build_authenc_sg_fd(dpaa2_sec_session *sess,
		 struct rte_crypto_op *op,
		 struct qbman_fd *fd, __rte_unused uint16_t bpid)
{
	struct rte_crypto_sym_op *sym_op = op->sym;
	struct ctxt_priv *priv = sess->ctxt;
	struct qbman_fle *fle, *sge, *ip_fle, *op_fle;
	struct sec_flow_context *flc;
	uint32_t auth_only_len = sym_op->auth.data.length -
				sym_op->cipher.data.length;
	int icv_len = sess->digest_length;
	uint8_t *old_icv;
	struct rte_mbuf *mbuf;
	uint8_t *iv_ptr = rte_crypto_op_ctod_offset(op, uint8_t *,
			sess->iv.offset);

	PMD_INIT_FUNC_TRACE();

	if (sym_op->m_dst)
		mbuf = sym_op->m_dst;
	else
		mbuf = sym_op->m_src;

	/* first FLE entry used to store mbuf and session ctxt */
	fle = (struct qbman_fle *)rte_malloc(NULL, FLE_SG_MEM_SIZE,
			RTE_CACHE_LINE_SIZE);
	if (unlikely(!fle)) {
		RTE_LOG(ERR, PMD, "AUTHENC SG: Memory alloc failed for SGE\n");
		return -1;
	}
	memset(fle, 0, FLE_SG_MEM_SIZE);
	DPAA2_SET_FLE_ADDR(fle, DPAA2_OP_VADDR_TO_IOVA(op));
	DPAA2_FLE_SAVE_CTXT(fle, priv);

	op_fle = fle + 1;
	ip_fle = fle + 2;
	sge = fle + 3;

	/* Save the shared descriptor */
	flc = &priv->flc_desc[0].flc;

	/* Configure FD as a FRAME LIST */
	DPAA2_SET_FD_ADDR(fd, DPAA2_VADDR_TO_IOVA(op_fle));
	DPAA2_SET_FD_COMPOUND_FMT(fd);
	DPAA2_SET_FD_FLC(fd, DPAA2_VADDR_TO_IOVA(flc));

	PMD_TX_LOG(DEBUG,
			"AUTHENC SG: auth_off: 0x%x/length %d, digest-len=%d\n"
			"cipher_off: 0x%x/length %d, iv-len=%d data_off: 0x%x\n",
		   sym_op->auth.data.offset,
		   sym_op->auth.data.length,
		   sym_op->auth.digest.length,
		   sym_op->cipher.data.offset,
		   sym_op->cipher.data.length,
		   sym_op->cipher.iv.length,
		   sym_op->m_src->data_off);

	/* Configure Output FLE with Scatter/Gather Entry */
	DPAA2_SET_FLE_SG_EXT(op_fle);
	DPAA2_SET_FLE_ADDR(op_fle, DPAA2_VADDR_TO_IOVA(sge));

	if (auth_only_len)
		DPAA2_SET_FLE_INTERNAL_JD(op_fle, auth_only_len);

	op_fle->length = (sess->dir == DIR_ENC) ?
			(sym_op->cipher.data.length + icv_len) :
			sym_op->cipher.data.length;

	/* Configure Output SGE for Encap/Decap */
	DPAA2_SET_FLE_ADDR(sge, DPAA2_MBUF_VADDR_TO_IOVA(mbuf));
	DPAA2_SET_FLE_OFFSET(sge, mbuf->data_off + sym_op->auth.data.offset);
	sge->length = mbuf->data_len - sym_op->auth.data.offset;

	mbuf = mbuf->next;
	/* o/p segs */
	while (mbuf) {
		sge++;
		DPAA2_SET_FLE_ADDR(sge, DPAA2_MBUF_VADDR_TO_IOVA(mbuf));
		DPAA2_SET_FLE_OFFSET(sge, mbuf->data_off);
		sge->length = mbuf->data_len;
		mbuf = mbuf->next;
	}
	sge->length -= icv_len;

	if (sess->dir == DIR_ENC) {
		sge++;
		DPAA2_SET_FLE_ADDR(sge,
				DPAA2_VADDR_TO_IOVA(sym_op->auth.digest.data));
		sge->length = icv_len;
	}
	DPAA2_SET_FLE_FIN(sge);

	sge++;
	mbuf = sym_op->m_src;

	/* Configure Input FLE with Scatter/Gather Entry */
	DPAA2_SET_FLE_ADDR(ip_fle, DPAA2_VADDR_TO_IOVA(sge));
	DPAA2_SET_FLE_SG_EXT(ip_fle);
	DPAA2_SET_FLE_FIN(ip_fle);
	ip_fle->length = (sess->dir == DIR_ENC) ?
			(sym_op->auth.data.length + sess->iv.length) :
			(sym_op->auth.data.length + sess->iv.length +
			 icv_len);

	/* Configure Input SGE for Encap/Decap */
	DPAA2_SET_FLE_ADDR(sge, DPAA2_VADDR_TO_IOVA(iv_ptr));
	sge->length = sess->iv.length;

	sge++;
	DPAA2_SET_FLE_ADDR(sge, DPAA2_MBUF_VADDR_TO_IOVA(mbuf));
	DPAA2_SET_FLE_OFFSET(sge, sym_op->auth.data.offset +
				mbuf->data_off);
	sge->length = mbuf->data_len - sym_op->auth.data.offset;

	mbuf = mbuf->next;
	/* i/p segs */
	while (mbuf) {
		sge++;
		DPAA2_SET_FLE_ADDR(sge, DPAA2_MBUF_VADDR_TO_IOVA(mbuf));
		DPAA2_SET_FLE_OFFSET(sge, mbuf->data_off);
		sge->length = mbuf->data_len;
		mbuf = mbuf->next;
	}
	sge->length -= icv_len;

	if (sess->dir == DIR_DEC) {
		sge++;
		old_icv = (uint8_t *)(sge + 1);
		memcpy(old_icv,	sym_op->auth.digest.data,
		       icv_len);
		DPAA2_SET_FLE_ADDR(sge, DPAA2_VADDR_TO_IOVA(old_icv));
		sge->length = icv_len;
	}

	DPAA2_SET_FLE_FIN(sge);
	if (auth_only_len) {
		DPAA2_SET_FLE_INTERNAL_JD(ip_fle, auth_only_len);
		DPAA2_SET_FD_INTERNAL_JD(fd, auth_only_len);
	}
	DPAA2_SET_FD_LEN(fd, ip_fle->length);

	return 0;
}

static inline int
build_authenc_fd(dpaa2_sec_session *sess,
		 struct rte_crypto_op *op,
		 struct qbman_fd *fd, uint16_t bpid)
{
	struct rte_crypto_sym_op *sym_op = op->sym;
	struct ctxt_priv *priv = sess->ctxt;
	struct qbman_fle *fle, *sge;
	struct sec_flow_context *flc;
	uint32_t auth_only_len = sym_op->auth.data.length -
				sym_op->cipher.data.length;
	int icv_len = sess->digest_length, retval;
	uint8_t *old_icv;
	uint8_t *iv_ptr = rte_crypto_op_ctod_offset(op, uint8_t *,
			sess->iv.offset);
	struct rte_mbuf *dst;

	PMD_INIT_FUNC_TRACE();

	if (sym_op->m_dst)
		dst = sym_op->m_dst;
	else
		dst = sym_op->m_src;

	/* we are using the first FLE entry to store Mbuf.
	 * Currently we donot know which FLE has the mbuf stored.
	 * So while retreiving we can go back 1 FLE from the FD -ADDR
	 * to get the MBUF Addr from the previous FLE.
	 * We can have a better approach to use the inline Mbuf
	 */
	retval = rte_mempool_get(priv->fle_pool, (void **)(&fle));
	if (retval) {
		RTE_LOG(ERR, PMD, "Memory alloc failed for SGE\n");
		return -1;
	}
	memset(fle, 0, FLE_POOL_BUF_SIZE);
	DPAA2_SET_FLE_ADDR(fle, DPAA2_OP_VADDR_TO_IOVA(op));
	DPAA2_FLE_SAVE_CTXT(fle, priv);
	fle = fle + 1;
	sge = fle + 2;
	if (likely(bpid < MAX_BPID)) {
		DPAA2_SET_FD_BPID(fd, bpid);
		DPAA2_SET_FLE_BPID(fle, bpid);
		DPAA2_SET_FLE_BPID(fle + 1, bpid);
		DPAA2_SET_FLE_BPID(sge, bpid);
		DPAA2_SET_FLE_BPID(sge + 1, bpid);
		DPAA2_SET_FLE_BPID(sge + 2, bpid);
		DPAA2_SET_FLE_BPID(sge + 3, bpid);
	} else {
		DPAA2_SET_FD_IVP(fd);
		DPAA2_SET_FLE_IVP(fle);
		DPAA2_SET_FLE_IVP((fle + 1));
		DPAA2_SET_FLE_IVP(sge);
		DPAA2_SET_FLE_IVP((sge + 1));
		DPAA2_SET_FLE_IVP((sge + 2));
		DPAA2_SET_FLE_IVP((sge + 3));
	}

	/* Save the shared descriptor */
	flc = &priv->flc_desc[0].flc;
	/* Configure FD as a FRAME LIST */
	DPAA2_SET_FD_ADDR(fd, DPAA2_VADDR_TO_IOVA(fle));
	DPAA2_SET_FD_COMPOUND_FMT(fd);
	DPAA2_SET_FD_FLC(fd, DPAA2_VADDR_TO_IOVA(flc));

	PMD_TX_LOG(DEBUG, "AUTHENC: auth_off: 0x%x/length %d, digest-len=%d\n"
		   "cipher_off: 0x%x/length %d, iv-len=%d data_off: 0x%x\n",
		   sym_op->auth.data.offset,
		   sym_op->auth.data.length,
		   sess->digest_length,
		   sym_op->cipher.data.offset,
		   sym_op->cipher.data.length,
		   sess->iv.length,
		   sym_op->m_src->data_off);

	/* Configure Output FLE with Scatter/Gather Entry */
	DPAA2_SET_FLE_ADDR(fle, DPAA2_VADDR_TO_IOVA(sge));
	if (auth_only_len)
		DPAA2_SET_FLE_INTERNAL_JD(fle, auth_only_len);
	fle->length = (sess->dir == DIR_ENC) ?
			(sym_op->cipher.data.length + icv_len) :
			sym_op->cipher.data.length;

	DPAA2_SET_FLE_SG_EXT(fle);

	/* Configure Output SGE for Encap/Decap */
	DPAA2_SET_FLE_ADDR(sge, DPAA2_MBUF_VADDR_TO_IOVA(dst));
	DPAA2_SET_FLE_OFFSET(sge, sym_op->cipher.data.offset +
				dst->data_off);
	sge->length = sym_op->cipher.data.length;

	if (sess->dir == DIR_ENC) {
		sge++;
		DPAA2_SET_FLE_ADDR(sge,
				DPAA2_VADDR_TO_IOVA(sym_op->auth.digest.data));
		sge->length = sess->digest_length;
		DPAA2_SET_FD_LEN(fd, (sym_op->auth.data.length +
					sess->iv.length));
	}
	DPAA2_SET_FLE_FIN(sge);

	sge++;
	fle++;

	/* Configure Input FLE with Scatter/Gather Entry */
	DPAA2_SET_FLE_ADDR(fle, DPAA2_VADDR_TO_IOVA(sge));
	DPAA2_SET_FLE_SG_EXT(fle);
	DPAA2_SET_FLE_FIN(fle);
	fle->length = (sess->dir == DIR_ENC) ?
			(sym_op->auth.data.length + sess->iv.length) :
			(sym_op->auth.data.length + sess->iv.length +
			 sess->digest_length);

	/* Configure Input SGE for Encap/Decap */
	DPAA2_SET_FLE_ADDR(sge, DPAA2_VADDR_TO_IOVA(iv_ptr));
	sge->length = sess->iv.length;
	sge++;

	DPAA2_SET_FLE_ADDR(sge, DPAA2_MBUF_VADDR_TO_IOVA(sym_op->m_src));
	DPAA2_SET_FLE_OFFSET(sge, sym_op->auth.data.offset +
				sym_op->m_src->data_off);
	sge->length = sym_op->auth.data.length;
	if (sess->dir == DIR_DEC) {
		sge++;
		old_icv = (uint8_t *)(sge + 1);
		memcpy(old_icv,	sym_op->auth.digest.data,
		       sess->digest_length);
		DPAA2_SET_FLE_ADDR(sge, DPAA2_VADDR_TO_IOVA(old_icv));
		sge->length = sess->digest_length;
		DPAA2_SET_FD_LEN(fd, (sym_op->auth.data.length +
				 sess->digest_length +
				 sess->iv.length));
	}
	DPAA2_SET_FLE_FIN(sge);
	if (auth_only_len) {
		DPAA2_SET_FLE_INTERNAL_JD(fle, auth_only_len);
		DPAA2_SET_FD_INTERNAL_JD(fd, auth_only_len);
	}
	return 0;
}

static inline int build_auth_sg_fd(
		dpaa2_sec_session *sess,
		struct rte_crypto_op *op,
		struct qbman_fd *fd,
		__rte_unused uint16_t bpid)
{
	struct rte_crypto_sym_op *sym_op = op->sym;
	struct qbman_fle *fle, *sge, *ip_fle, *op_fle;
	struct sec_flow_context *flc;
	struct ctxt_priv *priv = sess->ctxt;
	uint8_t *old_digest;
	struct rte_mbuf *mbuf;

	PMD_INIT_FUNC_TRACE();

	mbuf = sym_op->m_src;
	fle = (struct qbman_fle *)rte_malloc(NULL, FLE_SG_MEM_SIZE,
			RTE_CACHE_LINE_SIZE);
	if (unlikely(!fle)) {
		RTE_LOG(ERR, PMD, "AUTH SG: Memory alloc failed for SGE\n");
		return -1;
	}
	memset(fle, 0, FLE_SG_MEM_SIZE);
	/* first FLE entry used to store mbuf and session ctxt */
	DPAA2_SET_FLE_ADDR(fle, DPAA2_OP_VADDR_TO_IOVA(op));
	DPAA2_FLE_SAVE_CTXT(fle, priv);
	op_fle = fle + 1;
	ip_fle = fle + 2;
	sge = fle + 3;

	flc = &priv->flc_desc[DESC_INITFINAL].flc;
	/* sg FD */
	DPAA2_SET_FD_FLC(fd, DPAA2_VADDR_TO_IOVA(flc));
	DPAA2_SET_FD_ADDR(fd, DPAA2_VADDR_TO_IOVA(op_fle));
	DPAA2_SET_FD_COMPOUND_FMT(fd);

	/* o/p fle */
	DPAA2_SET_FLE_ADDR(op_fle,
				DPAA2_VADDR_TO_IOVA(sym_op->auth.digest.data));
	op_fle->length = sess->digest_length;

	/* i/p fle */
	DPAA2_SET_FLE_SG_EXT(ip_fle);
	DPAA2_SET_FLE_ADDR(ip_fle, DPAA2_VADDR_TO_IOVA(sge));
	/* i/p 1st seg */
	DPAA2_SET_FLE_ADDR(sge, DPAA2_MBUF_VADDR_TO_IOVA(mbuf));
	DPAA2_SET_FLE_OFFSET(sge, sym_op->auth.data.offset + mbuf->data_off);
	sge->length = mbuf->data_len - sym_op->auth.data.offset;

	/* i/p segs */
	mbuf = mbuf->next;
	while (mbuf) {
		sge++;
		DPAA2_SET_FLE_ADDR(sge, DPAA2_MBUF_VADDR_TO_IOVA(mbuf));
		DPAA2_SET_FLE_OFFSET(sge, mbuf->data_off);
		sge->length = mbuf->data_len;
		mbuf = mbuf->next;
	}
	if (sess->dir == DIR_ENC) {
		/* Digest calculation case */
		sge->length -= sess->digest_length;
		ip_fle->length = sym_op->auth.data.length;
	} else {
		/* Digest verification case */
		sge++;
		old_digest = (uint8_t *)(sge + 1);
		rte_memcpy(old_digest, sym_op->auth.digest.data,
			   sess->digest_length);
		DPAA2_SET_FLE_ADDR(sge, DPAA2_VADDR_TO_IOVA(old_digest));
		sge->length = sess->digest_length;
		ip_fle->length = sym_op->auth.data.length +
				sess->digest_length;
	}
	DPAA2_SET_FLE_FIN(sge);
	DPAA2_SET_FLE_FIN(ip_fle);
	DPAA2_SET_FD_LEN(fd, ip_fle->length);

	return 0;
}

static inline int
build_auth_fd(dpaa2_sec_session *sess, struct rte_crypto_op *op,
	      struct qbman_fd *fd, uint16_t bpid)
{
	struct rte_crypto_sym_op *sym_op = op->sym;
	struct qbman_fle *fle, *sge;
	struct sec_flow_context *flc;
	struct ctxt_priv *priv = sess->ctxt;
	uint8_t *old_digest;
	int retval;

	PMD_INIT_FUNC_TRACE();

	retval = rte_mempool_get(priv->fle_pool, (void **)(&fle));
	if (retval) {
		RTE_LOG(ERR, PMD, "AUTH Memory alloc failed for SGE\n");
		return -1;
	}
	memset(fle, 0, FLE_POOL_BUF_SIZE);
	/* TODO we are using the first FLE entry to store Mbuf.
	 * Currently we donot know which FLE has the mbuf stored.
	 * So while retreiving we can go back 1 FLE from the FD -ADDR
	 * to get the MBUF Addr from the previous FLE.
	 * We can have a better approach to use the inline Mbuf
	 */
	DPAA2_SET_FLE_ADDR(fle, DPAA2_OP_VADDR_TO_IOVA(op));
	DPAA2_FLE_SAVE_CTXT(fle, priv);
	fle = fle + 1;

	if (likely(bpid < MAX_BPID)) {
		DPAA2_SET_FD_BPID(fd, bpid);
		DPAA2_SET_FLE_BPID(fle, bpid);
		DPAA2_SET_FLE_BPID(fle + 1, bpid);
	} else {
		DPAA2_SET_FD_IVP(fd);
		DPAA2_SET_FLE_IVP(fle);
		DPAA2_SET_FLE_IVP((fle + 1));
	}
	flc = &priv->flc_desc[DESC_INITFINAL].flc;
	DPAA2_SET_FD_FLC(fd, DPAA2_VADDR_TO_IOVA(flc));

	DPAA2_SET_FLE_ADDR(fle, DPAA2_VADDR_TO_IOVA(sym_op->auth.digest.data));
	fle->length = sess->digest_length;

	DPAA2_SET_FD_ADDR(fd, DPAA2_VADDR_TO_IOVA(fle));
	DPAA2_SET_FD_COMPOUND_FMT(fd);
	fle++;

	if (sess->dir == DIR_ENC) {
		DPAA2_SET_FLE_ADDR(fle,
				   DPAA2_MBUF_VADDR_TO_IOVA(sym_op->m_src));
		DPAA2_SET_FLE_OFFSET(fle, sym_op->auth.data.offset +
				     sym_op->m_src->data_off);
		DPAA2_SET_FD_LEN(fd, sym_op->auth.data.length);
		fle->length = sym_op->auth.data.length;
	} else {
		sge = fle + 2;
		DPAA2_SET_FLE_SG_EXT(fle);
		DPAA2_SET_FLE_ADDR(fle, DPAA2_VADDR_TO_IOVA(sge));

		if (likely(bpid < MAX_BPID)) {
			DPAA2_SET_FLE_BPID(sge, bpid);
			DPAA2_SET_FLE_BPID(sge + 1, bpid);
		} else {
			DPAA2_SET_FLE_IVP(sge);
			DPAA2_SET_FLE_IVP((sge + 1));
		}
		DPAA2_SET_FLE_ADDR(sge,
				   DPAA2_MBUF_VADDR_TO_IOVA(sym_op->m_src));
		DPAA2_SET_FLE_OFFSET(sge, sym_op->auth.data.offset +
				     sym_op->m_src->data_off);

		DPAA2_SET_FD_LEN(fd, sym_op->auth.data.length +
				 sess->digest_length);
		sge->length = sym_op->auth.data.length;
		sge++;
		old_digest = (uint8_t *)(sge + 1);
		rte_memcpy(old_digest, sym_op->auth.digest.data,
			   sess->digest_length);
		DPAA2_SET_FLE_ADDR(sge, DPAA2_VADDR_TO_IOVA(old_digest));
		sge->length = sess->digest_length;
		fle->length = sym_op->auth.data.length +
				sess->digest_length;
		DPAA2_SET_FLE_FIN(sge);
	}
	DPAA2_SET_FLE_FIN(fle);

	return 0;
}

static int
build_cipher_sg_fd(dpaa2_sec_session *sess, struct rte_crypto_op *op,
		struct qbman_fd *fd, __rte_unused uint16_t bpid)
{
	struct rte_crypto_sym_op *sym_op = op->sym;
	struct qbman_fle *ip_fle, *op_fle, *sge, *fle;
	struct sec_flow_context *flc;
	struct ctxt_priv *priv = sess->ctxt;
	struct rte_mbuf *mbuf;
	uint8_t *iv_ptr = rte_crypto_op_ctod_offset(op, uint8_t *,
			sess->iv.offset);

	PMD_INIT_FUNC_TRACE();

	if (sym_op->m_dst)
		mbuf = sym_op->m_dst;
	else
		mbuf = sym_op->m_src;

	fle = (struct qbman_fle *)rte_malloc(NULL, FLE_SG_MEM_SIZE,
			RTE_CACHE_LINE_SIZE);
	if (!fle) {
		RTE_LOG(ERR, PMD, "CIPHER SG: Memory alloc failed for SGE\n");
		return -1;
	}
	memset(fle, 0, FLE_SG_MEM_SIZE);
	/* first FLE entry used to store mbuf and session ctxt */
	DPAA2_SET_FLE_ADDR(fle, DPAA2_OP_VADDR_TO_IOVA(op));
	DPAA2_FLE_SAVE_CTXT(fle, priv);

	op_fle = fle + 1;
	ip_fle = fle + 2;
	sge = fle + 3;

	flc = &priv->flc_desc[0].flc;

	PMD_TX_LOG(DEBUG,
			"CIPHER SG: cipher_off: 0x%x/length %d,ivlen=%d data_off: 0x%x",
		   sym_op->cipher.data.offset,
		   sym_op->cipher.data.length,
		   sym_op->cipher.iv.length,
		   sym_op->m_src->data_off);

	/* o/p fle */
	DPAA2_SET_FLE_ADDR(op_fle, DPAA2_VADDR_TO_IOVA(sge));
	op_fle->length = sym_op->cipher.data.length;
	DPAA2_SET_FLE_SG_EXT(op_fle);

	/* o/p 1st seg */
	DPAA2_SET_FLE_ADDR(sge, DPAA2_MBUF_VADDR_TO_IOVA(mbuf));
	DPAA2_SET_FLE_OFFSET(sge, sym_op->cipher.data.offset + mbuf->data_off);
	sge->length = mbuf->data_len - sym_op->cipher.data.offset;

	mbuf = mbuf->next;
	/* o/p segs */
	while (mbuf) {
		sge++;
		DPAA2_SET_FLE_ADDR(sge, DPAA2_MBUF_VADDR_TO_IOVA(mbuf));
		DPAA2_SET_FLE_OFFSET(sge, mbuf->data_off);
		sge->length = mbuf->data_len;
		mbuf = mbuf->next;
	}
	DPAA2_SET_FLE_FIN(sge);

	PMD_TX_LOG(DEBUG,
			"CIPHER SG: 1 - flc = %p, fle = %p FLEaddr = %x-%x, length %d",
			flc, fle, fle->addr_hi, fle->addr_lo,
			fle->length);

	/* i/p fle */
	mbuf = sym_op->m_src;
	sge++;
	DPAA2_SET_FLE_ADDR(ip_fle, DPAA2_VADDR_TO_IOVA(sge));
	ip_fle->length = sess->iv.length + sym_op->cipher.data.length;
	DPAA2_SET_FLE_SG_EXT(ip_fle);

	/* i/p IV */
	DPAA2_SET_FLE_ADDR(sge, DPAA2_VADDR_TO_IOVA(iv_ptr));
	DPAA2_SET_FLE_OFFSET(sge, 0);
	sge->length = sess->iv.length;

	sge++;

	/* i/p 1st seg */
	DPAA2_SET_FLE_ADDR(sge, DPAA2_MBUF_VADDR_TO_IOVA(mbuf));
	DPAA2_SET_FLE_OFFSET(sge, sym_op->cipher.data.offset +
			     mbuf->data_off);
	sge->length = mbuf->data_len - sym_op->cipher.data.offset;

	mbuf = mbuf->next;
	/* i/p segs */
	while (mbuf) {
		sge++;
		DPAA2_SET_FLE_ADDR(sge, DPAA2_MBUF_VADDR_TO_IOVA(mbuf));
		DPAA2_SET_FLE_OFFSET(sge, mbuf->data_off);
		sge->length = mbuf->data_len;
		mbuf = mbuf->next;
	}
	DPAA2_SET_FLE_FIN(sge);
	DPAA2_SET_FLE_FIN(ip_fle);

	/* sg fd */
	DPAA2_SET_FD_ADDR(fd, DPAA2_VADDR_TO_IOVA(op_fle));
	DPAA2_SET_FD_LEN(fd, ip_fle->length);
	DPAA2_SET_FD_COMPOUND_FMT(fd);
	DPAA2_SET_FD_FLC(fd, DPAA2_VADDR_TO_IOVA(flc));

	PMD_TX_LOG(DEBUG,
			"CIPHER SG: fdaddr =%p bpid =%d meta =%d off =%d, len =%d",
		   (void *)DPAA2_GET_FD_ADDR(fd),
		   DPAA2_GET_FD_BPID(fd),
		   rte_dpaa2_bpid_info[bpid].meta_data_size,
		   DPAA2_GET_FD_OFFSET(fd),
		   DPAA2_GET_FD_LEN(fd));
	return 0;
}

static int
build_cipher_fd(dpaa2_sec_session *sess, struct rte_crypto_op *op,
		struct qbman_fd *fd, uint16_t bpid)
{
	struct rte_crypto_sym_op *sym_op = op->sym;
	struct qbman_fle *fle, *sge;
	int retval;
	struct sec_flow_context *flc;
	struct ctxt_priv *priv = sess->ctxt;
	uint8_t *iv_ptr = rte_crypto_op_ctod_offset(op, uint8_t *,
			sess->iv.offset);
	struct rte_mbuf *dst;

	PMD_INIT_FUNC_TRACE();

	if (sym_op->m_dst)
		dst = sym_op->m_dst;
	else
		dst = sym_op->m_src;

	retval = rte_mempool_get(priv->fle_pool, (void **)(&fle));
	if (retval) {
		RTE_LOG(ERR, PMD, "CIPHER: Memory alloc failed for SGE\n");
		return -1;
	}
	memset(fle, 0, FLE_POOL_BUF_SIZE);
	/* TODO we are using the first FLE entry to store Mbuf.
	 * Currently we donot know which FLE has the mbuf stored.
	 * So while retreiving we can go back 1 FLE from the FD -ADDR
	 * to get the MBUF Addr from the previous FLE.
	 * We can have a better approach to use the inline Mbuf
	 */
	DPAA2_SET_FLE_ADDR(fle, DPAA2_OP_VADDR_TO_IOVA(op));
	DPAA2_FLE_SAVE_CTXT(fle, priv);
	fle = fle + 1;
	sge = fle + 2;

	if (likely(bpid < MAX_BPID)) {
		DPAA2_SET_FD_BPID(fd, bpid);
		DPAA2_SET_FLE_BPID(fle, bpid);
		DPAA2_SET_FLE_BPID(fle + 1, bpid);
		DPAA2_SET_FLE_BPID(sge, bpid);
		DPAA2_SET_FLE_BPID(sge + 1, bpid);
	} else {
		DPAA2_SET_FD_IVP(fd);
		DPAA2_SET_FLE_IVP(fle);
		DPAA2_SET_FLE_IVP((fle + 1));
		DPAA2_SET_FLE_IVP(sge);
		DPAA2_SET_FLE_IVP((sge + 1));
	}

	flc = &priv->flc_desc[0].flc;
	DPAA2_SET_FD_ADDR(fd, DPAA2_VADDR_TO_IOVA(fle));
	DPAA2_SET_FD_LEN(fd, sym_op->cipher.data.length +
			 sess->iv.length);
	DPAA2_SET_FD_COMPOUND_FMT(fd);
	DPAA2_SET_FD_FLC(fd, DPAA2_VADDR_TO_IOVA(flc));

	PMD_TX_LOG(DEBUG,
			"CIPHER: cipher_off: 0x%x/length %d, ivlen=%d, data_off: 0x%x",
		   sym_op->cipher.data.offset,
		   sym_op->cipher.data.length,
		   sess->iv.length,
		   sym_op->m_src->data_off);

	DPAA2_SET_FLE_ADDR(fle, DPAA2_MBUF_VADDR_TO_IOVA(dst));
	DPAA2_SET_FLE_OFFSET(fle, sym_op->cipher.data.offset +
			     dst->data_off);

	fle->length = sym_op->cipher.data.length + sess->iv.length;

	PMD_TX_LOG(DEBUG,
			"CIPHER: 1 - flc = %p, fle = %p FLEaddr = %x-%x, length %d",
			flc, fle, fle->addr_hi, fle->addr_lo,
			fle->length);

	fle++;

	DPAA2_SET_FLE_ADDR(fle, DPAA2_VADDR_TO_IOVA(sge));
	fle->length = sym_op->cipher.data.length + sess->iv.length;

	DPAA2_SET_FLE_SG_EXT(fle);

	DPAA2_SET_FLE_ADDR(sge, DPAA2_VADDR_TO_IOVA(iv_ptr));
	sge->length = sess->iv.length;

	sge++;
	DPAA2_SET_FLE_ADDR(sge, DPAA2_MBUF_VADDR_TO_IOVA(sym_op->m_src));
	DPAA2_SET_FLE_OFFSET(sge, sym_op->cipher.data.offset +
			     sym_op->m_src->data_off);

	sge->length = sym_op->cipher.data.length;
	DPAA2_SET_FLE_FIN(sge);
	DPAA2_SET_FLE_FIN(fle);

	PMD_TX_LOG(DEBUG,
			"CIPHER: fdaddr =%p bpid =%d meta =%d off =%d, len =%d",
		   (void *)DPAA2_GET_FD_ADDR(fd),
		   DPAA2_GET_FD_BPID(fd),
		   rte_dpaa2_bpid_info[bpid].meta_data_size,
		   DPAA2_GET_FD_OFFSET(fd),
		   DPAA2_GET_FD_LEN(fd));

	return 0;
}

static inline int
build_sec_fd(struct rte_crypto_op *op,
	     struct qbman_fd *fd, uint16_t bpid)
{
	int ret = -1;
	dpaa2_sec_session *sess;

	PMD_INIT_FUNC_TRACE();

	if (op->sess_type == RTE_CRYPTO_OP_WITH_SESSION)
		sess = (dpaa2_sec_session *)get_session_private_data(
				op->sym->session, cryptodev_driver_id);
	else if (op->sess_type == RTE_CRYPTO_OP_SECURITY_SESSION)
		sess = (dpaa2_sec_session *)get_sec_session_private_data(
				op->sym->sec_session);
	else
		return -1;

	/* Segmented buffer */
	if (unlikely(!rte_pktmbuf_is_contiguous(op->sym->m_src))) {
		switch (sess->ctxt_type) {
		case DPAA2_SEC_CIPHER:
			ret = build_cipher_sg_fd(sess, op, fd, bpid);
			break;
		case DPAA2_SEC_AUTH:
			ret = build_auth_sg_fd(sess, op, fd, bpid);
			break;
		case DPAA2_SEC_AEAD:
			ret = build_authenc_gcm_sg_fd(sess, op, fd, bpid);
			break;
		case DPAA2_SEC_CIPHER_HASH:
			ret = build_authenc_sg_fd(sess, op, fd, bpid);
			break;
		case DPAA2_SEC_HASH_CIPHER:
		default:
			RTE_LOG(ERR, PMD, "error: Unsupported session\n");
		}
	} else {
		switch (sess->ctxt_type) {
		case DPAA2_SEC_CIPHER:
			ret = build_cipher_fd(sess, op, fd, bpid);
			break;
		case DPAA2_SEC_AUTH:
			ret = build_auth_fd(sess, op, fd, bpid);
			break;
		case DPAA2_SEC_AEAD:
			ret = build_authenc_gcm_fd(sess, op, fd, bpid);
			break;
		case DPAA2_SEC_CIPHER_HASH:
			ret = build_authenc_fd(sess, op, fd, bpid);
			break;
		case DPAA2_SEC_IPSEC:
			ret = build_proto_fd(sess, op, fd, bpid);
			break;
		case DPAA2_SEC_HASH_CIPHER:
		default:
			RTE_LOG(ERR, PMD, "error: Unsupported session\n");
		}
	}
	return ret;
}

static uint16_t
dpaa2_sec_enqueue_burst(void *qp, struct rte_crypto_op **ops,
			uint16_t nb_ops)
{
	/* Function to transmit the frames to given device and VQ*/
	uint32_t loop;
	int32_t ret;
	struct qbman_fd fd_arr[MAX_TX_RING_SLOTS];
	uint32_t frames_to_send;
	struct qbman_eq_desc eqdesc;
	struct dpaa2_sec_qp *dpaa2_qp = (struct dpaa2_sec_qp *)qp;
	struct qbman_swp *swp;
	uint16_t num_tx = 0;
	/*todo - need to support multiple buffer pools */
	uint16_t bpid;
	struct rte_mempool *mb_pool;

	if (unlikely(nb_ops == 0))
		return 0;

	if (ops[0]->sess_type == RTE_CRYPTO_OP_SESSIONLESS) {
		RTE_LOG(ERR, PMD, "sessionless crypto op not supported\n");
		return 0;
	}
	/*Prepare enqueue descriptor*/
	qbman_eq_desc_clear(&eqdesc);
	qbman_eq_desc_set_no_orp(&eqdesc, DPAA2_EQ_RESP_ERR_FQ);
	qbman_eq_desc_set_response(&eqdesc, 0, 0);
	qbman_eq_desc_set_fq(&eqdesc, dpaa2_qp->tx_vq.fqid);

	if (!DPAA2_PER_LCORE_SEC_DPIO) {
		ret = dpaa2_affine_qbman_swp_sec();
		if (ret) {
			RTE_LOG(ERR, PMD, "Failure in affining portal\n");
			return 0;
		}
	}
	swp = DPAA2_PER_LCORE_SEC_PORTAL;

	while (nb_ops) {
		frames_to_send = (nb_ops >> 3) ? MAX_TX_RING_SLOTS : nb_ops;

		for (loop = 0; loop < frames_to_send; loop++) {
			/*Clear the unused FD fields before sending*/
			memset(&fd_arr[loop], 0, sizeof(struct qbman_fd));
			mb_pool = (*ops)->sym->m_src->pool;
			bpid = mempool_to_bpid(mb_pool);
			ret = build_sec_fd(*ops, &fd_arr[loop], bpid);
			if (ret) {
				PMD_DRV_LOG(ERR, "error: Improper packet"
					    " contents for crypto operation\n");
				goto skip_tx;
			}
			ops++;
		}
		loop = 0;
		while (loop < frames_to_send) {
			loop += qbman_swp_enqueue_multiple(swp, &eqdesc,
							&fd_arr[loop],
							NULL,
							frames_to_send - loop);
		}

		num_tx += frames_to_send;
		nb_ops -= frames_to_send;
	}
skip_tx:
	dpaa2_qp->tx_vq.tx_pkts += num_tx;
	dpaa2_qp->tx_vq.err_pkts += nb_ops;
	return num_tx;
}

static inline struct rte_crypto_op *
sec_simple_fd_to_mbuf(const struct qbman_fd *fd, __rte_unused uint8_t id)
{
	struct rte_crypto_op *op;
	uint16_t len = DPAA2_GET_FD_LEN(fd);
	uint16_t diff = 0;
	dpaa2_sec_session *sess_priv;

	struct rte_mbuf *mbuf = DPAA2_INLINE_MBUF_FROM_BUF(
		DPAA2_IOVA_TO_VADDR(DPAA2_GET_FD_ADDR(fd)),
		rte_dpaa2_bpid_info[DPAA2_GET_FD_BPID(fd)].meta_data_size);

	op = (struct rte_crypto_op *)mbuf->buf_iova;
	mbuf->buf_iova = op->sym->aead.digest.phys_addr;
	op->sym->aead.digest.phys_addr = 0L;

	sess_priv = (dpaa2_sec_session *)get_sec_session_private_data(
				op->sym->sec_session);
	if (sess_priv->dir == DIR_ENC)
		mbuf->data_off += SEC_FLC_DHR_OUTBOUND;
	else
		mbuf->data_off += SEC_FLC_DHR_INBOUND;
	diff = len - mbuf->pkt_len;
	mbuf->pkt_len += diff;
	mbuf->data_len += diff;

	return op;
}

static inline struct rte_crypto_op *
sec_fd_to_mbuf(const struct qbman_fd *fd, uint8_t driver_id)
{
	struct qbman_fle *fle;
	struct rte_crypto_op *op;
	struct ctxt_priv *priv;
	struct rte_mbuf *dst, *src;

	if (DPAA2_FD_GET_FORMAT(fd) == qbman_fd_single)
		return sec_simple_fd_to_mbuf(fd, driver_id);

	fle = (struct qbman_fle *)DPAA2_IOVA_TO_VADDR(DPAA2_GET_FD_ADDR(fd));

	PMD_RX_LOG(DEBUG, "FLE addr = %x - %x, offset = %x",
		   fle->addr_hi, fle->addr_lo, fle->fin_bpid_offset);

	/* we are using the first FLE entry to store Mbuf.
	 * Currently we donot know which FLE has the mbuf stored.
	 * So while retreiving we can go back 1 FLE from the FD -ADDR
	 * to get the MBUF Addr from the previous FLE.
	 * We can have a better approach to use the inline Mbuf
	 */

	if (unlikely(DPAA2_GET_FD_IVP(fd))) {
		/* TODO complete it. */
		RTE_LOG(ERR, PMD, "error: Non inline buffer - WHAT to DO?\n");
		return NULL;
	}
	op = (struct rte_crypto_op *)DPAA2_IOVA_TO_VADDR(
			DPAA2_GET_FLE_ADDR((fle - 1)));

	/* Prefeth op */
	src = op->sym->m_src;
	rte_prefetch0(src);

	if (op->sym->m_dst) {
		dst = op->sym->m_dst;
		rte_prefetch0(dst);
	} else
		dst = src;

	PMD_RX_LOG(DEBUG, "mbuf %p BMAN buf addr %p",
		   (void *)dst, dst->buf_addr);

	PMD_RX_LOG(DEBUG, "fdaddr =%p bpid =%d meta =%d off =%d, len =%d",
		   (void *)DPAA2_GET_FD_ADDR(fd),
		   DPAA2_GET_FD_BPID(fd),
		   rte_dpaa2_bpid_info[DPAA2_GET_FD_BPID(fd)].meta_data_size,
		   DPAA2_GET_FD_OFFSET(fd),
		   DPAA2_GET_FD_LEN(fd));

	/* free the fle memory */
	if (likely(rte_pktmbuf_is_contiguous(src))) {
		priv = (struct ctxt_priv *)DPAA2_GET_FLE_CTXT(fle - 1);
		rte_mempool_put(priv->fle_pool, (void *)(fle-1));
	} else
		rte_free((void *)(fle-1));

	return op;
}

static uint16_t
dpaa2_sec_dequeue_burst(void *qp, struct rte_crypto_op **ops,
			uint16_t nb_ops)
{
	/* Function is responsible to receive frames for a given device and VQ*/
	struct dpaa2_sec_qp *dpaa2_qp = (struct dpaa2_sec_qp *)qp;
	struct rte_cryptodev *dev =
			(struct rte_cryptodev *)(dpaa2_qp->rx_vq.dev);
	struct qbman_result *dq_storage;
	uint32_t fqid = dpaa2_qp->rx_vq.fqid;
	int ret, num_rx = 0;
	uint8_t is_last = 0, status;
	struct qbman_swp *swp;
	const struct qbman_fd *fd;
	struct qbman_pull_desc pulldesc;

	if (!DPAA2_PER_LCORE_SEC_DPIO) {
		ret = dpaa2_affine_qbman_swp_sec();
		if (ret) {
			RTE_LOG(ERR, PMD, "Failure in affining portal\n");
			return 0;
		}
	}
	swp = DPAA2_PER_LCORE_SEC_PORTAL;
	dq_storage = dpaa2_qp->rx_vq.q_storage->dq_storage[0];

	qbman_pull_desc_clear(&pulldesc);
	qbman_pull_desc_set_numframes(&pulldesc,
				      (nb_ops > DPAA2_DQRR_RING_SIZE) ?
				      DPAA2_DQRR_RING_SIZE : nb_ops);
	qbman_pull_desc_set_fq(&pulldesc, fqid);
	qbman_pull_desc_set_storage(&pulldesc, dq_storage,
				    (dma_addr_t)DPAA2_VADDR_TO_IOVA(dq_storage),
				    1);

	/*Issue a volatile dequeue command. */
	while (1) {
		if (qbman_swp_pull(swp, &pulldesc)) {
			RTE_LOG(WARNING, PMD,
				"SEC VDQ command is not issued : QBMAN busy\n");
			/* Portal was busy, try again */
			continue;
		}
		break;
	};

	/* Receive the packets till Last Dequeue entry is found with
	 * respect to the above issues PULL command.
	 */
	while (!is_last) {
		/* Check if the previous issued command is completed.
		 * Also seems like the SWP is shared between the Ethernet Driver
		 * and the SEC driver.
		 */
		while (!qbman_check_command_complete(dq_storage))
			;

		/* Loop until the dq_storage is updated with
		 * new token by QBMAN
		 */
		while (!qbman_check_new_result(dq_storage))
			;
		/* Check whether Last Pull command is Expired and
		 * setting Condition for Loop termination
		 */
		if (qbman_result_DQ_is_pull_complete(dq_storage)) {
			is_last = 1;
			/* Check for valid frame. */
			status = (uint8_t)qbman_result_DQ_flags(dq_storage);
			if (unlikely(
				(status & QBMAN_DQ_STAT_VALIDFRAME) == 0)) {
				PMD_RX_LOG(DEBUG, "No frame is delivered");
				continue;
			}
		}

		fd = qbman_result_DQ_fd(dq_storage);
		ops[num_rx] = sec_fd_to_mbuf(fd, dev->driver_id);

		if (unlikely(fd->simple.frc)) {
			/* TODO Parse SEC errors */
			RTE_LOG(ERR, PMD, "SEC returned Error - %x\n",
				fd->simple.frc);
			ops[num_rx]->status = RTE_CRYPTO_OP_STATUS_ERROR;
		} else {
			ops[num_rx]->status = RTE_CRYPTO_OP_STATUS_SUCCESS;
		}

		num_rx++;
		dq_storage++;
	} /* End of Packet Rx loop */

	dpaa2_qp->rx_vq.rx_pkts += num_rx;

	PMD_RX_LOG(DEBUG, "SEC Received %d Packets", num_rx);
	/*Return the total number of packets received to DPAA2 app*/
	return num_rx;
}

/** Release queue pair */
static int
dpaa2_sec_queue_pair_release(struct rte_cryptodev *dev, uint16_t queue_pair_id)
{
	struct dpaa2_sec_qp *qp =
		(struct dpaa2_sec_qp *)dev->data->queue_pairs[queue_pair_id];

	PMD_INIT_FUNC_TRACE();

	if (qp->rx_vq.q_storage) {
		dpaa2_free_dq_storage(qp->rx_vq.q_storage);
		rte_free(qp->rx_vq.q_storage);
	}
	rte_free(qp);

	dev->data->queue_pairs[queue_pair_id] = NULL;

	return 0;
}

/** Setup a queue pair */
static int
dpaa2_sec_queue_pair_setup(struct rte_cryptodev *dev, uint16_t qp_id,
		__rte_unused const struct rte_cryptodev_qp_conf *qp_conf,
		__rte_unused int socket_id,
		__rte_unused struct rte_mempool *session_pool)
{
	struct dpaa2_sec_dev_private *priv = dev->data->dev_private;
	struct dpaa2_sec_qp *qp;
	struct fsl_mc_io *dpseci = (struct fsl_mc_io *)priv->hw;
	struct dpseci_rx_queue_cfg cfg;
	int32_t retcode;

	PMD_INIT_FUNC_TRACE();

	/* If qp is already in use free ring memory and qp metadata. */
	if (dev->data->queue_pairs[qp_id] != NULL) {
		PMD_DRV_LOG(INFO, "QP already setup");
		return 0;
	}

	PMD_DRV_LOG(DEBUG, "dev =%p, queue =%d, conf =%p",
		    dev, qp_id, qp_conf);

	memset(&cfg, 0, sizeof(struct dpseci_rx_queue_cfg));

	qp = rte_malloc(NULL, sizeof(struct dpaa2_sec_qp),
			RTE_CACHE_LINE_SIZE);
	if (!qp) {
		RTE_LOG(ERR, PMD, "malloc failed for rx/tx queues\n");
		return -1;
	}

	qp->rx_vq.dev = dev;
	qp->tx_vq.dev = dev;
	qp->rx_vq.q_storage = rte_malloc("sec dq storage",
		sizeof(struct queue_storage_info_t),
		RTE_CACHE_LINE_SIZE);
	if (!qp->rx_vq.q_storage) {
		RTE_LOG(ERR, PMD, "malloc failed for q_storage\n");
		return -1;
	}
	memset(qp->rx_vq.q_storage, 0, sizeof(struct queue_storage_info_t));

	if (dpaa2_alloc_dq_storage(qp->rx_vq.q_storage)) {
		RTE_LOG(ERR, PMD, "dpaa2_alloc_dq_storage failed\n");
		return -1;
	}

	dev->data->queue_pairs[qp_id] = qp;

	cfg.options = cfg.options | DPSECI_QUEUE_OPT_USER_CTX;
	cfg.user_ctx = (uint64_t)(&qp->rx_vq);
	retcode = dpseci_set_rx_queue(dpseci, CMD_PRI_LOW, priv->token,
				      qp_id, &cfg);
	return retcode;
}

/** Start queue pair */
static int
dpaa2_sec_queue_pair_start(__rte_unused struct rte_cryptodev *dev,
			   __rte_unused uint16_t queue_pair_id)
{
	PMD_INIT_FUNC_TRACE();

	return 0;
}

/** Stop queue pair */
static int
dpaa2_sec_queue_pair_stop(__rte_unused struct rte_cryptodev *dev,
			  __rte_unused uint16_t queue_pair_id)
{
	PMD_INIT_FUNC_TRACE();

	return 0;
}

/** Return the number of allocated queue pairs */
static uint32_t
dpaa2_sec_queue_pair_count(struct rte_cryptodev *dev)
{
	PMD_INIT_FUNC_TRACE();

	return dev->data->nb_queue_pairs;
}

/** Returns the size of the aesni gcm session structure */
static unsigned int
dpaa2_sec_session_get_size(struct rte_cryptodev *dev __rte_unused)
{
	PMD_INIT_FUNC_TRACE();

	return sizeof(dpaa2_sec_session);
}

static int
dpaa2_sec_cipher_init(struct rte_cryptodev *dev,
		      struct rte_crypto_sym_xform *xform,
		      dpaa2_sec_session *session)
{
	struct dpaa2_sec_dev_private *dev_priv = dev->data->dev_private;
	struct alginfo cipherdata;
	int bufsize, i;
	struct ctxt_priv *priv;
	struct sec_flow_context *flc;

	PMD_INIT_FUNC_TRACE();

	/* For SEC CIPHER only one descriptor is required. */
	priv = (struct ctxt_priv *)rte_zmalloc(NULL,
			sizeof(struct ctxt_priv) + sizeof(struct sec_flc_desc),
			RTE_CACHE_LINE_SIZE);
	if (priv == NULL) {
		RTE_LOG(ERR, PMD, "No Memory for priv CTXT\n");
		return -1;
	}

	priv->fle_pool = dev_priv->fle_pool;

	flc = &priv->flc_desc[0].flc;

	session->cipher_key.data = rte_zmalloc(NULL, xform->cipher.key.length,
			RTE_CACHE_LINE_SIZE);
	if (session->cipher_key.data == NULL) {
		RTE_LOG(ERR, PMD, "No Memory for cipher key\n");
		rte_free(priv);
		return -1;
	}
	session->cipher_key.length = xform->cipher.key.length;

	memcpy(session->cipher_key.data, xform->cipher.key.data,
	       xform->cipher.key.length);
	cipherdata.key = (uint64_t)session->cipher_key.data;
	cipherdata.keylen = session->cipher_key.length;
	cipherdata.key_enc_flags = 0;
	cipherdata.key_type = RTA_DATA_IMM;

	/* Set IV parameters */
	session->iv.offset = xform->cipher.iv.offset;
	session->iv.length = xform->cipher.iv.length;

	switch (xform->cipher.algo) {
	case RTE_CRYPTO_CIPHER_AES_CBC:
		cipherdata.algtype = OP_ALG_ALGSEL_AES;
		cipherdata.algmode = OP_ALG_AAI_CBC;
		session->cipher_alg = RTE_CRYPTO_CIPHER_AES_CBC;
		break;
	case RTE_CRYPTO_CIPHER_3DES_CBC:
		cipherdata.algtype = OP_ALG_ALGSEL_3DES;
		cipherdata.algmode = OP_ALG_AAI_CBC;
		session->cipher_alg = RTE_CRYPTO_CIPHER_3DES_CBC;
		break;
	case RTE_CRYPTO_CIPHER_AES_CTR:
		cipherdata.algtype = OP_ALG_ALGSEL_AES;
		cipherdata.algmode = OP_ALG_AAI_CTR;
		session->cipher_alg = RTE_CRYPTO_CIPHER_AES_CTR;
		break;
	case RTE_CRYPTO_CIPHER_3DES_CTR:
	case RTE_CRYPTO_CIPHER_AES_ECB:
	case RTE_CRYPTO_CIPHER_3DES_ECB:
	case RTE_CRYPTO_CIPHER_AES_XTS:
	case RTE_CRYPTO_CIPHER_AES_F8:
	case RTE_CRYPTO_CIPHER_ARC4:
	case RTE_CRYPTO_CIPHER_KASUMI_F8:
	case RTE_CRYPTO_CIPHER_SNOW3G_UEA2:
	case RTE_CRYPTO_CIPHER_ZUC_EEA3:
	case RTE_CRYPTO_CIPHER_NULL:
		RTE_LOG(ERR, PMD, "Crypto: Unsupported Cipher alg %u\n",
			xform->cipher.algo);
		goto error_out;
	default:
		RTE_LOG(ERR, PMD, "Crypto: Undefined Cipher specified %u\n",
			xform->cipher.algo);
		goto error_out;
	}
	session->dir = (xform->cipher.op == RTE_CRYPTO_CIPHER_OP_ENCRYPT) ?
				DIR_ENC : DIR_DEC;

	bufsize = cnstr_shdsc_blkcipher(priv->flc_desc[0].desc, 1, 0,
					&cipherdata, NULL, session->iv.length,
					session->dir);
	if (bufsize < 0) {
		RTE_LOG(ERR, PMD, "Crypto: Descriptor build failed\n");
		goto error_out;
	}
	flc->dhr = 0;
	flc->bpv0 = 0x1;
	flc->mode_bits = 0x8000;

	flc->word1_sdl = (uint8_t)bufsize;
	flc->word2_rflc_31_0 = lower_32_bits(
			(uint64_t)&(((struct dpaa2_sec_qp *)
			dev->data->queue_pairs[0])->rx_vq));
	flc->word3_rflc_63_32 = upper_32_bits(
			(uint64_t)&(((struct dpaa2_sec_qp *)
			dev->data->queue_pairs[0])->rx_vq));
	session->ctxt = priv;

	for (i = 0; i < bufsize; i++)
		PMD_DRV_LOG(DEBUG, "DESC[%d]:0x%x\n",
			    i, priv->flc_desc[0].desc[i]);

	return 0;

error_out:
	rte_free(session->cipher_key.data);
	rte_free(priv);
	return -1;
}

static int
dpaa2_sec_auth_init(struct rte_cryptodev *dev,
		    struct rte_crypto_sym_xform *xform,
		    dpaa2_sec_session *session)
{
	struct dpaa2_sec_dev_private *dev_priv = dev->data->dev_private;
	struct alginfo authdata;
	unsigned int bufsize, i;
	struct ctxt_priv *priv;
	struct sec_flow_context *flc;

	PMD_INIT_FUNC_TRACE();

	/* For SEC AUTH three descriptors are required for various stages */
	priv = (struct ctxt_priv *)rte_zmalloc(NULL,
			sizeof(struct ctxt_priv) + 3 *
			sizeof(struct sec_flc_desc),
			RTE_CACHE_LINE_SIZE);
	if (priv == NULL) {
		RTE_LOG(ERR, PMD, "No Memory for priv CTXT\n");
		return -1;
	}

	priv->fle_pool = dev_priv->fle_pool;
	flc = &priv->flc_desc[DESC_INITFINAL].flc;

	session->auth_key.data = rte_zmalloc(NULL, xform->auth.key.length,
			RTE_CACHE_LINE_SIZE);
	if (session->auth_key.data == NULL) {
		RTE_LOG(ERR, PMD, "No Memory for auth key\n");
		rte_free(priv);
		return -1;
	}
	session->auth_key.length = xform->auth.key.length;

	memcpy(session->auth_key.data, xform->auth.key.data,
	       xform->auth.key.length);
	authdata.key = (uint64_t)session->auth_key.data;
	authdata.keylen = session->auth_key.length;
	authdata.key_enc_flags = 0;
	authdata.key_type = RTA_DATA_IMM;

	session->digest_length = xform->auth.digest_length;

	switch (xform->auth.algo) {
	case RTE_CRYPTO_AUTH_SHA1_HMAC:
		authdata.algtype = OP_ALG_ALGSEL_SHA1;
		authdata.algmode = OP_ALG_AAI_HMAC;
		session->auth_alg = RTE_CRYPTO_AUTH_SHA1_HMAC;
		break;
	case RTE_CRYPTO_AUTH_MD5_HMAC:
		authdata.algtype = OP_ALG_ALGSEL_MD5;
		authdata.algmode = OP_ALG_AAI_HMAC;
		session->auth_alg = RTE_CRYPTO_AUTH_MD5_HMAC;
		break;
	case RTE_CRYPTO_AUTH_SHA256_HMAC:
		authdata.algtype = OP_ALG_ALGSEL_SHA256;
		authdata.algmode = OP_ALG_AAI_HMAC;
		session->auth_alg = RTE_CRYPTO_AUTH_SHA256_HMAC;
		break;
	case RTE_CRYPTO_AUTH_SHA384_HMAC:
		authdata.algtype = OP_ALG_ALGSEL_SHA384;
		authdata.algmode = OP_ALG_AAI_HMAC;
		session->auth_alg = RTE_CRYPTO_AUTH_SHA384_HMAC;
		break;
	case RTE_CRYPTO_AUTH_SHA512_HMAC:
		authdata.algtype = OP_ALG_ALGSEL_SHA512;
		authdata.algmode = OP_ALG_AAI_HMAC;
		session->auth_alg = RTE_CRYPTO_AUTH_SHA512_HMAC;
		break;
	case RTE_CRYPTO_AUTH_SHA224_HMAC:
		authdata.algtype = OP_ALG_ALGSEL_SHA224;
		authdata.algmode = OP_ALG_AAI_HMAC;
		session->auth_alg = RTE_CRYPTO_AUTH_SHA224_HMAC;
		break;
	case RTE_CRYPTO_AUTH_AES_XCBC_MAC:
	case RTE_CRYPTO_AUTH_SNOW3G_UIA2:
	case RTE_CRYPTO_AUTH_NULL:
	case RTE_CRYPTO_AUTH_SHA1:
	case RTE_CRYPTO_AUTH_SHA256:
	case RTE_CRYPTO_AUTH_SHA512:
	case RTE_CRYPTO_AUTH_SHA224:
	case RTE_CRYPTO_AUTH_SHA384:
	case RTE_CRYPTO_AUTH_MD5:
	case RTE_CRYPTO_AUTH_AES_GMAC:
	case RTE_CRYPTO_AUTH_KASUMI_F9:
	case RTE_CRYPTO_AUTH_AES_CMAC:
	case RTE_CRYPTO_AUTH_AES_CBC_MAC:
	case RTE_CRYPTO_AUTH_ZUC_EIA3:
		RTE_LOG(ERR, PMD, "Crypto: Unsupported auth alg %u\n",
			xform->auth.algo);
		goto error_out;
	default:
		RTE_LOG(ERR, PMD, "Crypto: Undefined Auth specified %u\n",
			xform->auth.algo);
		goto error_out;
	}
	session->dir = (xform->auth.op == RTE_CRYPTO_AUTH_OP_GENERATE) ?
				DIR_ENC : DIR_DEC;

	bufsize = cnstr_shdsc_hmac(priv->flc_desc[DESC_INITFINAL].desc,
				   1, 0, &authdata, !session->dir,
				   session->digest_length);

	flc->word1_sdl = (uint8_t)bufsize;
	flc->word2_rflc_31_0 = lower_32_bits(
			(uint64_t)&(((struct dpaa2_sec_qp *)
			dev->data->queue_pairs[0])->rx_vq));
	flc->word3_rflc_63_32 = upper_32_bits(
			(uint64_t)&(((struct dpaa2_sec_qp *)
			dev->data->queue_pairs[0])->rx_vq));
	session->ctxt = priv;
	for (i = 0; i < bufsize; i++)
		PMD_DRV_LOG(DEBUG, "DESC[%d]:0x%x\n",
			    i, priv->flc_desc[DESC_INITFINAL].desc[i]);


	return 0;

error_out:
	rte_free(session->auth_key.data);
	rte_free(priv);
	return -1;
}

static int
dpaa2_sec_aead_init(struct rte_cryptodev *dev,
		    struct rte_crypto_sym_xform *xform,
		    dpaa2_sec_session *session)
{
	struct dpaa2_sec_aead_ctxt *ctxt = &session->ext_params.aead_ctxt;
	struct dpaa2_sec_dev_private *dev_priv = dev->data->dev_private;
	struct alginfo aeaddata;
	unsigned int bufsize, i;
	struct ctxt_priv *priv;
	struct sec_flow_context *flc;
	struct rte_crypto_aead_xform *aead_xform = &xform->aead;
	int err;

	PMD_INIT_FUNC_TRACE();

	/* Set IV parameters */
	session->iv.offset = aead_xform->iv.offset;
	session->iv.length = aead_xform->iv.length;
	session->ctxt_type = DPAA2_SEC_AEAD;

	/* For SEC AEAD only one descriptor is required */
	priv = (struct ctxt_priv *)rte_zmalloc(NULL,
			sizeof(struct ctxt_priv) + sizeof(struct sec_flc_desc),
			RTE_CACHE_LINE_SIZE);
	if (priv == NULL) {
		RTE_LOG(ERR, PMD, "No Memory for priv CTXT\n");
		return -1;
	}

	priv->fle_pool = dev_priv->fle_pool;
	flc = &priv->flc_desc[0].flc;

	session->aead_key.data = rte_zmalloc(NULL, aead_xform->key.length,
					       RTE_CACHE_LINE_SIZE);
	if (session->aead_key.data == NULL && aead_xform->key.length > 0) {
		RTE_LOG(ERR, PMD, "No Memory for aead key\n");
		rte_free(priv);
		return -1;
	}
	memcpy(session->aead_key.data, aead_xform->key.data,
	       aead_xform->key.length);

	session->digest_length = aead_xform->digest_length;
	session->aead_key.length = aead_xform->key.length;
	ctxt->auth_only_len = aead_xform->aad_length;

	aeaddata.key = (uint64_t)session->aead_key.data;
	aeaddata.keylen = session->aead_key.length;
	aeaddata.key_enc_flags = 0;
	aeaddata.key_type = RTA_DATA_IMM;

	switch (aead_xform->algo) {
	case RTE_CRYPTO_AEAD_AES_GCM:
		aeaddata.algtype = OP_ALG_ALGSEL_AES;
		aeaddata.algmode = OP_ALG_AAI_GCM;
		session->aead_alg = RTE_CRYPTO_AEAD_AES_GCM;
		break;
	case RTE_CRYPTO_AEAD_AES_CCM:
		RTE_LOG(ERR, PMD, "Crypto: Unsupported AEAD alg %u\n",
			aead_xform->algo);
		goto error_out;
	default:
		RTE_LOG(ERR, PMD, "Crypto: Undefined AEAD specified %u\n",
			aead_xform->algo);
		goto error_out;
	}
	session->dir = (aead_xform->op == RTE_CRYPTO_AEAD_OP_ENCRYPT) ?
				DIR_ENC : DIR_DEC;

	priv->flc_desc[0].desc[0] = aeaddata.keylen;
	err = rta_inline_query(IPSEC_AUTH_VAR_AES_DEC_BASE_DESC_LEN,
			       MIN_JOB_DESC_SIZE,
			       (unsigned int *)priv->flc_desc[0].desc,
			       &priv->flc_desc[0].desc[1], 1);

	if (err < 0) {
		PMD_DRV_LOG(ERR, "Crypto: Incorrect key lengths\n");
		goto error_out;
	}
	if (priv->flc_desc[0].desc[1] & 1) {
		aeaddata.key_type = RTA_DATA_IMM;
	} else {
		aeaddata.key = DPAA2_VADDR_TO_IOVA(aeaddata.key);
		aeaddata.key_type = RTA_DATA_PTR;
	}
	priv->flc_desc[0].desc[0] = 0;
	priv->flc_desc[0].desc[1] = 0;

	if (session->dir == DIR_ENC)
		bufsize = cnstr_shdsc_gcm_encap(
				priv->flc_desc[0].desc, 1, 0,
				&aeaddata, session->iv.length,
				session->digest_length);
	else
		bufsize = cnstr_shdsc_gcm_decap(
				priv->flc_desc[0].desc, 1, 0,
				&aeaddata, session->iv.length,
				session->digest_length);
	flc->word1_sdl = (uint8_t)bufsize;
	flc->word2_rflc_31_0 = lower_32_bits(
			(uint64_t)&(((struct dpaa2_sec_qp *)
			dev->data->queue_pairs[0])->rx_vq));
	flc->word3_rflc_63_32 = upper_32_bits(
			(uint64_t)&(((struct dpaa2_sec_qp *)
			dev->data->queue_pairs[0])->rx_vq));
	session->ctxt = priv;
	for (i = 0; i < bufsize; i++)
		PMD_DRV_LOG(DEBUG, "DESC[%d]:0x%x\n",
			    i, priv->flc_desc[0].desc[i]);

	return 0;

error_out:
	rte_free(session->aead_key.data);
	rte_free(priv);
	return -1;
}


static int
dpaa2_sec_aead_chain_init(struct rte_cryptodev *dev,
		    struct rte_crypto_sym_xform *xform,
		    dpaa2_sec_session *session)
{
	struct dpaa2_sec_aead_ctxt *ctxt = &session->ext_params.aead_ctxt;
	struct dpaa2_sec_dev_private *dev_priv = dev->data->dev_private;
	struct alginfo authdata, cipherdata;
	unsigned int bufsize, i;
	struct ctxt_priv *priv;
	struct sec_flow_context *flc;
	struct rte_crypto_cipher_xform *cipher_xform;
	struct rte_crypto_auth_xform *auth_xform;
	int err;

	PMD_INIT_FUNC_TRACE();

	if (session->ext_params.aead_ctxt.auth_cipher_text) {
		cipher_xform = &xform->cipher;
		auth_xform = &xform->next->auth;
		session->ctxt_type =
			(cipher_xform->op == RTE_CRYPTO_CIPHER_OP_ENCRYPT) ?
			DPAA2_SEC_CIPHER_HASH : DPAA2_SEC_HASH_CIPHER;
	} else {
		cipher_xform = &xform->next->cipher;
		auth_xform = &xform->auth;
		session->ctxt_type =
			(cipher_xform->op == RTE_CRYPTO_CIPHER_OP_ENCRYPT) ?
			DPAA2_SEC_HASH_CIPHER : DPAA2_SEC_CIPHER_HASH;
	}

	/* Set IV parameters */
	session->iv.offset = cipher_xform->iv.offset;
	session->iv.length = cipher_xform->iv.length;

	/* For SEC AEAD only one descriptor is required */
	priv = (struct ctxt_priv *)rte_zmalloc(NULL,
			sizeof(struct ctxt_priv) + sizeof(struct sec_flc_desc),
			RTE_CACHE_LINE_SIZE);
	if (priv == NULL) {
		RTE_LOG(ERR, PMD, "No Memory for priv CTXT\n");
		return -1;
	}

	priv->fle_pool = dev_priv->fle_pool;
	flc = &priv->flc_desc[0].flc;

	session->cipher_key.data = rte_zmalloc(NULL, cipher_xform->key.length,
					       RTE_CACHE_LINE_SIZE);
	if (session->cipher_key.data == NULL && cipher_xform->key.length > 0) {
		RTE_LOG(ERR, PMD, "No Memory for cipher key\n");
		rte_free(priv);
		return -1;
	}
	session->cipher_key.length = cipher_xform->key.length;
	session->auth_key.data = rte_zmalloc(NULL, auth_xform->key.length,
					     RTE_CACHE_LINE_SIZE);
	if (session->auth_key.data == NULL && auth_xform->key.length > 0) {
		RTE_LOG(ERR, PMD, "No Memory for auth key\n");
		rte_free(session->cipher_key.data);
		rte_free(priv);
		return -1;
	}
	session->auth_key.length = auth_xform->key.length;
	memcpy(session->cipher_key.data, cipher_xform->key.data,
	       cipher_xform->key.length);
	memcpy(session->auth_key.data, auth_xform->key.data,
	       auth_xform->key.length);

	authdata.key = (uint64_t)session->auth_key.data;
	authdata.keylen = session->auth_key.length;
	authdata.key_enc_flags = 0;
	authdata.key_type = RTA_DATA_IMM;

	session->digest_length = auth_xform->digest_length;

	switch (auth_xform->algo) {
	case RTE_CRYPTO_AUTH_SHA1_HMAC:
		authdata.algtype = OP_ALG_ALGSEL_SHA1;
		authdata.algmode = OP_ALG_AAI_HMAC;
		session->auth_alg = RTE_CRYPTO_AUTH_SHA1_HMAC;
		break;
	case RTE_CRYPTO_AUTH_MD5_HMAC:
		authdata.algtype = OP_ALG_ALGSEL_MD5;
		authdata.algmode = OP_ALG_AAI_HMAC;
		session->auth_alg = RTE_CRYPTO_AUTH_MD5_HMAC;
		break;
	case RTE_CRYPTO_AUTH_SHA224_HMAC:
		authdata.algtype = OP_ALG_ALGSEL_SHA224;
		authdata.algmode = OP_ALG_AAI_HMAC;
		session->auth_alg = RTE_CRYPTO_AUTH_SHA224_HMAC;
		break;
	case RTE_CRYPTO_AUTH_SHA256_HMAC:
		authdata.algtype = OP_ALG_ALGSEL_SHA256;
		authdata.algmode = OP_ALG_AAI_HMAC;
		session->auth_alg = RTE_CRYPTO_AUTH_SHA256_HMAC;
		break;
	case RTE_CRYPTO_AUTH_SHA384_HMAC:
		authdata.algtype = OP_ALG_ALGSEL_SHA384;
		authdata.algmode = OP_ALG_AAI_HMAC;
		session->auth_alg = RTE_CRYPTO_AUTH_SHA384_HMAC;
		break;
	case RTE_CRYPTO_AUTH_SHA512_HMAC:
		authdata.algtype = OP_ALG_ALGSEL_SHA512;
		authdata.algmode = OP_ALG_AAI_HMAC;
		session->auth_alg = RTE_CRYPTO_AUTH_SHA512_HMAC;
		break;
	case RTE_CRYPTO_AUTH_AES_XCBC_MAC:
	case RTE_CRYPTO_AUTH_SNOW3G_UIA2:
	case RTE_CRYPTO_AUTH_NULL:
	case RTE_CRYPTO_AUTH_SHA1:
	case RTE_CRYPTO_AUTH_SHA256:
	case RTE_CRYPTO_AUTH_SHA512:
	case RTE_CRYPTO_AUTH_SHA224:
	case RTE_CRYPTO_AUTH_SHA384:
	case RTE_CRYPTO_AUTH_MD5:
	case RTE_CRYPTO_AUTH_AES_GMAC:
	case RTE_CRYPTO_AUTH_KASUMI_F9:
	case RTE_CRYPTO_AUTH_AES_CMAC:
	case RTE_CRYPTO_AUTH_AES_CBC_MAC:
	case RTE_CRYPTO_AUTH_ZUC_EIA3:
		RTE_LOG(ERR, PMD, "Crypto: Unsupported auth alg %u\n",
			auth_xform->algo);
		goto error_out;
	default:
		RTE_LOG(ERR, PMD, "Crypto: Undefined Auth specified %u\n",
			auth_xform->algo);
		goto error_out;
	}
	cipherdata.key = (uint64_t)session->cipher_key.data;
	cipherdata.keylen = session->cipher_key.length;
	cipherdata.key_enc_flags = 0;
	cipherdata.key_type = RTA_DATA_IMM;

	switch (cipher_xform->algo) {
	case RTE_CRYPTO_CIPHER_AES_CBC:
		cipherdata.algtype = OP_ALG_ALGSEL_AES;
		cipherdata.algmode = OP_ALG_AAI_CBC;
		session->cipher_alg = RTE_CRYPTO_CIPHER_AES_CBC;
		break;
	case RTE_CRYPTO_CIPHER_3DES_CBC:
		cipherdata.algtype = OP_ALG_ALGSEL_3DES;
		cipherdata.algmode = OP_ALG_AAI_CBC;
		session->cipher_alg = RTE_CRYPTO_CIPHER_3DES_CBC;
		break;
	case RTE_CRYPTO_CIPHER_AES_CTR:
		cipherdata.algtype = OP_ALG_ALGSEL_AES;
		cipherdata.algmode = OP_ALG_AAI_CTR;
		session->cipher_alg = RTE_CRYPTO_CIPHER_AES_CTR;
		break;
	case RTE_CRYPTO_CIPHER_SNOW3G_UEA2:
	case RTE_CRYPTO_CIPHER_NULL:
	case RTE_CRYPTO_CIPHER_3DES_ECB:
	case RTE_CRYPTO_CIPHER_AES_ECB:
	case RTE_CRYPTO_CIPHER_KASUMI_F8:
		RTE_LOG(ERR, PMD, "Crypto: Unsupported Cipher alg %u\n",
			cipher_xform->algo);
		goto error_out;
	default:
		RTE_LOG(ERR, PMD, "Crypto: Undefined Cipher specified %u\n",
			cipher_xform->algo);
		goto error_out;
	}
	session->dir = (cipher_xform->op == RTE_CRYPTO_CIPHER_OP_ENCRYPT) ?
				DIR_ENC : DIR_DEC;

	priv->flc_desc[0].desc[0] = cipherdata.keylen;
	priv->flc_desc[0].desc[1] = authdata.keylen;
	err = rta_inline_query(IPSEC_AUTH_VAR_AES_DEC_BASE_DESC_LEN,
			       MIN_JOB_DESC_SIZE,
			       (unsigned int *)priv->flc_desc[0].desc,
			       &priv->flc_desc[0].desc[2], 2);

	if (err < 0) {
		PMD_DRV_LOG(ERR, "Crypto: Incorrect key lengths\n");
		goto error_out;
	}
	if (priv->flc_desc[0].desc[2] & 1) {
		cipherdata.key_type = RTA_DATA_IMM;
	} else {
		cipherdata.key = DPAA2_VADDR_TO_IOVA(cipherdata.key);
		cipherdata.key_type = RTA_DATA_PTR;
	}
	if (priv->flc_desc[0].desc[2] & (1 << 1)) {
		authdata.key_type = RTA_DATA_IMM;
	} else {
		authdata.key = DPAA2_VADDR_TO_IOVA(authdata.key);
		authdata.key_type = RTA_DATA_PTR;
	}
	priv->flc_desc[0].desc[0] = 0;
	priv->flc_desc[0].desc[1] = 0;
	priv->flc_desc[0].desc[2] = 0;

	if (session->ctxt_type == DPAA2_SEC_CIPHER_HASH) {
		bufsize = cnstr_shdsc_authenc(priv->flc_desc[0].desc, 1,
					      0, &cipherdata, &authdata,
					      session->iv.length,
					      ctxt->auth_only_len,
					      session->digest_length,
					      session->dir);
	} else {
		RTE_LOG(ERR, PMD, "Hash before cipher not supported\n");
		goto error_out;
	}

	flc->word1_sdl = (uint8_t)bufsize;
	flc->word2_rflc_31_0 = lower_32_bits(
			(uint64_t)&(((struct dpaa2_sec_qp *)
			dev->data->queue_pairs[0])->rx_vq));
	flc->word3_rflc_63_32 = upper_32_bits(
			(uint64_t)&(((struct dpaa2_sec_qp *)
			dev->data->queue_pairs[0])->rx_vq));
	session->ctxt = priv;
	for (i = 0; i < bufsize; i++)
		PMD_DRV_LOG(DEBUG, "DESC[%d]:0x%x\n",
			    i, priv->flc_desc[0].desc[i]);

	return 0;

error_out:
	rte_free(session->cipher_key.data);
	rte_free(session->auth_key.data);
	rte_free(priv);
	return -1;
}

static int
dpaa2_sec_set_session_parameters(struct rte_cryptodev *dev,
			    struct rte_crypto_sym_xform *xform,	void *sess)
{
	dpaa2_sec_session *session = sess;

	PMD_INIT_FUNC_TRACE();

	if (unlikely(sess == NULL)) {
		RTE_LOG(ERR, PMD, "invalid session struct\n");
		return -1;
	}

	/* Default IV length = 0 */
	session->iv.length = 0;

	/* Cipher Only */
	if (xform->type == RTE_CRYPTO_SYM_XFORM_CIPHER && xform->next == NULL) {
		session->ctxt_type = DPAA2_SEC_CIPHER;
		dpaa2_sec_cipher_init(dev, xform, session);

	/* Authentication Only */
	} else if (xform->type == RTE_CRYPTO_SYM_XFORM_AUTH &&
		   xform->next == NULL) {
		session->ctxt_type = DPAA2_SEC_AUTH;
		dpaa2_sec_auth_init(dev, xform, session);

	/* Cipher then Authenticate */
	} else if (xform->type == RTE_CRYPTO_SYM_XFORM_CIPHER &&
		   xform->next->type == RTE_CRYPTO_SYM_XFORM_AUTH) {
		session->ext_params.aead_ctxt.auth_cipher_text = true;
		dpaa2_sec_aead_chain_init(dev, xform, session);

	/* Authenticate then Cipher */
	} else if (xform->type == RTE_CRYPTO_SYM_XFORM_AUTH &&
		   xform->next->type == RTE_CRYPTO_SYM_XFORM_CIPHER) {
		session->ext_params.aead_ctxt.auth_cipher_text = false;
		dpaa2_sec_aead_chain_init(dev, xform, session);

	/* AEAD operation for AES-GCM kind of Algorithms */
	} else if (xform->type == RTE_CRYPTO_SYM_XFORM_AEAD &&
		   xform->next == NULL) {
		dpaa2_sec_aead_init(dev, xform, session);

	} else {
		RTE_LOG(ERR, PMD, "Invalid crypto type\n");
		return -EINVAL;
	}

	return 0;
}

static int
dpaa2_sec_set_ipsec_session(struct rte_cryptodev *dev,
			    struct rte_security_session_conf *conf,
			    void *sess)
{
	struct rte_security_ipsec_xform *ipsec_xform = &conf->ipsec;
	struct rte_crypto_auth_xform *auth_xform;
	struct rte_crypto_cipher_xform *cipher_xform;
	dpaa2_sec_session *session = (dpaa2_sec_session *)sess;
	struct ctxt_priv *priv;
	struct ipsec_encap_pdb encap_pdb;
	struct ipsec_decap_pdb decap_pdb;
	struct alginfo authdata, cipherdata;
	unsigned int bufsize;
	struct sec_flow_context *flc;

	PMD_INIT_FUNC_TRACE();

	if (ipsec_xform->direction == RTE_SECURITY_IPSEC_SA_DIR_EGRESS) {
		cipher_xform = &conf->crypto_xform->cipher;
		auth_xform = &conf->crypto_xform->next->auth;
	} else {
		auth_xform = &conf->crypto_xform->auth;
		cipher_xform = &conf->crypto_xform->next->cipher;
	}
	priv = (struct ctxt_priv *)rte_zmalloc(NULL,
				sizeof(struct ctxt_priv) +
				sizeof(struct sec_flc_desc),
				RTE_CACHE_LINE_SIZE);

	if (priv == NULL) {
		RTE_LOG(ERR, PMD, "\nNo memory for priv CTXT");
		return -ENOMEM;
	}

	flc = &priv->flc_desc[0].flc;

	session->ctxt_type = DPAA2_SEC_IPSEC;
	session->cipher_key.data = rte_zmalloc(NULL,
					       cipher_xform->key.length,
					       RTE_CACHE_LINE_SIZE);
	if (session->cipher_key.data == NULL &&
			cipher_xform->key.length > 0) {
		RTE_LOG(ERR, PMD, "No Memory for cipher key\n");
		rte_free(priv);
		return -ENOMEM;
	}

	session->cipher_key.length = cipher_xform->key.length;
	session->auth_key.data = rte_zmalloc(NULL,
					auth_xform->key.length,
					RTE_CACHE_LINE_SIZE);
	if (session->auth_key.data == NULL &&
			auth_xform->key.length > 0) {
		RTE_LOG(ERR, PMD, "No Memory for auth key\n");
		rte_free(session->cipher_key.data);
		rte_free(priv);
		return -ENOMEM;
	}
	session->auth_key.length = auth_xform->key.length;
	memcpy(session->cipher_key.data, cipher_xform->key.data,
			cipher_xform->key.length);
	memcpy(session->auth_key.data, auth_xform->key.data,
			auth_xform->key.length);

	authdata.key = (uint64_t)session->auth_key.data;
	authdata.keylen = session->auth_key.length;
	authdata.key_enc_flags = 0;
	authdata.key_type = RTA_DATA_IMM;
	switch (auth_xform->algo) {
	case RTE_CRYPTO_AUTH_SHA1_HMAC:
		authdata.algtype = OP_PCL_IPSEC_HMAC_SHA1_96;
		authdata.algmode = OP_ALG_AAI_HMAC;
		session->auth_alg = RTE_CRYPTO_AUTH_SHA1_HMAC;
		break;
	case RTE_CRYPTO_AUTH_MD5_HMAC:
		authdata.algtype = OP_PCL_IPSEC_HMAC_MD5_96;
		authdata.algmode = OP_ALG_AAI_HMAC;
		session->auth_alg = RTE_CRYPTO_AUTH_MD5_HMAC;
		break;
	case RTE_CRYPTO_AUTH_SHA256_HMAC:
		authdata.algtype = OP_PCL_IPSEC_HMAC_SHA2_256_128;
		authdata.algmode = OP_ALG_AAI_HMAC;
		session->auth_alg = RTE_CRYPTO_AUTH_SHA256_HMAC;
		break;
	case RTE_CRYPTO_AUTH_SHA384_HMAC:
		authdata.algtype = OP_PCL_IPSEC_HMAC_SHA2_384_192;
		authdata.algmode = OP_ALG_AAI_HMAC;
		session->auth_alg = RTE_CRYPTO_AUTH_SHA384_HMAC;
		break;
	case RTE_CRYPTO_AUTH_SHA512_HMAC:
		authdata.algtype = OP_PCL_IPSEC_HMAC_SHA2_512_256;
		authdata.algmode = OP_ALG_AAI_HMAC;
		session->auth_alg = RTE_CRYPTO_AUTH_SHA512_HMAC;
		break;
	case RTE_CRYPTO_AUTH_AES_CMAC:
		authdata.algtype = OP_PCL_IPSEC_AES_CMAC_96;
		session->auth_alg = RTE_CRYPTO_AUTH_AES_CMAC;
		break;
	case RTE_CRYPTO_AUTH_NULL:
		authdata.algtype = OP_PCL_IPSEC_HMAC_NULL;
		session->auth_alg = RTE_CRYPTO_AUTH_NULL;
		break;
	case RTE_CRYPTO_AUTH_SHA224_HMAC:
	case RTE_CRYPTO_AUTH_AES_XCBC_MAC:
	case RTE_CRYPTO_AUTH_SNOW3G_UIA2:
	case RTE_CRYPTO_AUTH_SHA1:
	case RTE_CRYPTO_AUTH_SHA256:
	case RTE_CRYPTO_AUTH_SHA512:
	case RTE_CRYPTO_AUTH_SHA224:
	case RTE_CRYPTO_AUTH_SHA384:
	case RTE_CRYPTO_AUTH_MD5:
	case RTE_CRYPTO_AUTH_AES_GMAC:
	case RTE_CRYPTO_AUTH_KASUMI_F9:
	case RTE_CRYPTO_AUTH_AES_CBC_MAC:
	case RTE_CRYPTO_AUTH_ZUC_EIA3:
		RTE_LOG(ERR, PMD, "Crypto: Unsupported auth alg %u\n",
			auth_xform->algo);
		goto out;
	default:
		RTE_LOG(ERR, PMD, "Crypto: Undefined Auth specified %u\n",
			auth_xform->algo);
		goto out;
	}
	cipherdata.key = (uint64_t)session->cipher_key.data;
	cipherdata.keylen = session->cipher_key.length;
	cipherdata.key_enc_flags = 0;
	cipherdata.key_type = RTA_DATA_IMM;

	switch (cipher_xform->algo) {
	case RTE_CRYPTO_CIPHER_AES_CBC:
		cipherdata.algtype = OP_PCL_IPSEC_AES_CBC;
		cipherdata.algmode = OP_ALG_AAI_CBC;
		session->cipher_alg = RTE_CRYPTO_CIPHER_AES_CBC;
		break;
	case RTE_CRYPTO_CIPHER_3DES_CBC:
		cipherdata.algtype = OP_PCL_IPSEC_3DES;
		cipherdata.algmode = OP_ALG_AAI_CBC;
		session->cipher_alg = RTE_CRYPTO_CIPHER_3DES_CBC;
		break;
	case RTE_CRYPTO_CIPHER_AES_CTR:
		cipherdata.algtype = OP_PCL_IPSEC_AES_CTR;
		cipherdata.algmode = OP_ALG_AAI_CTR;
		session->cipher_alg = RTE_CRYPTO_CIPHER_AES_CTR;
		break;
	case RTE_CRYPTO_CIPHER_NULL:
		cipherdata.algtype = OP_PCL_IPSEC_NULL;
		break;
	case RTE_CRYPTO_CIPHER_SNOW3G_UEA2:
	case RTE_CRYPTO_CIPHER_3DES_ECB:
	case RTE_CRYPTO_CIPHER_AES_ECB:
	case RTE_CRYPTO_CIPHER_KASUMI_F8:
		RTE_LOG(ERR, PMD, "Crypto: Unsupported Cipher alg %u\n",
			cipher_xform->algo);
		goto out;
	default:
		RTE_LOG(ERR, PMD, "Crypto: Undefined Cipher specified %u\n",
			cipher_xform->algo);
		goto out;
	}

	if (ipsec_xform->direction == RTE_SECURITY_IPSEC_SA_DIR_EGRESS) {
		struct ip ip4_hdr;

		flc->dhr = SEC_FLC_DHR_OUTBOUND;
		ip4_hdr.ip_v = IPVERSION;
		ip4_hdr.ip_hl = 5;
		ip4_hdr.ip_len = rte_cpu_to_be_16(sizeof(ip4_hdr));
		ip4_hdr.ip_tos = ipsec_xform->tunnel.ipv4.dscp;
		ip4_hdr.ip_id = 0;
		ip4_hdr.ip_off = 0;
		ip4_hdr.ip_ttl = ipsec_xform->tunnel.ipv4.ttl;
		ip4_hdr.ip_p = 0x32;
		ip4_hdr.ip_sum = 0;
		ip4_hdr.ip_src = ipsec_xform->tunnel.ipv4.src_ip;
		ip4_hdr.ip_dst = ipsec_xform->tunnel.ipv4.dst_ip;
		ip4_hdr.ip_sum = calc_chksum((uint16_t *)(void *)&ip4_hdr,
			sizeof(struct ip));

		/* For Sec Proto only one descriptor is required. */
		memset(&encap_pdb, 0, sizeof(struct ipsec_encap_pdb));
		encap_pdb.options = (IPVERSION << PDBNH_ESP_ENCAP_SHIFT) |
			PDBOPTS_ESP_OIHI_PDB_INL |
			PDBOPTS_ESP_IVSRC |
			PDBHMO_ESP_ENCAP_DTTL;
		encap_pdb.spi = ipsec_xform->spi;
		encap_pdb.ip_hdr_len = sizeof(struct ip);

		session->dir = DIR_ENC;
		bufsize = cnstr_shdsc_ipsec_new_encap(priv->flc_desc[0].desc,
				1, 0, &encap_pdb,
				(uint8_t *)&ip4_hdr,
				&cipherdata, &authdata);
	} else if (ipsec_xform->direction ==
			RTE_SECURITY_IPSEC_SA_DIR_INGRESS) {
		flc->dhr = SEC_FLC_DHR_INBOUND;
		memset(&decap_pdb, 0, sizeof(struct ipsec_decap_pdb));
		decap_pdb.options = sizeof(struct ip) << 16;
		session->dir = DIR_DEC;
		bufsize = cnstr_shdsc_ipsec_new_decap(priv->flc_desc[0].desc,
				1, 0, &decap_pdb, &cipherdata, &authdata);
	} else
		goto out;
	flc->word1_sdl = (uint8_t)bufsize;

	/* Enable the stashing control bit */
	DPAA2_SET_FLC_RSC(flc);
	flc->word2_rflc_31_0 = lower_32_bits(
			(uint64_t)&(((struct dpaa2_sec_qp *)
			dev->data->queue_pairs[0])->rx_vq) | 0x14);
	flc->word3_rflc_63_32 = upper_32_bits(
			(uint64_t)&(((struct dpaa2_sec_qp *)
			dev->data->queue_pairs[0])->rx_vq));

	/* Set EWS bit i.e. enable write-safe */
	DPAA2_SET_FLC_EWS(flc);
	/* Set BS = 1 i.e reuse input buffers as output buffers */
	DPAA2_SET_FLC_REUSE_BS(flc);
	/* Set FF = 10; reuse input buffers if they provide sufficient space */
	DPAA2_SET_FLC_REUSE_FF(flc);

	session->ctxt = priv;

	return 0;
out:
	rte_free(session->auth_key.data);
	rte_free(session->cipher_key.data);
	rte_free(priv);
	return -1;
}

static int
dpaa2_sec_security_session_create(void *dev,
				  struct rte_security_session_conf *conf,
				  struct rte_security_session *sess,
				  struct rte_mempool *mempool)
{
	void *sess_private_data;
	struct rte_cryptodev *cdev = (struct rte_cryptodev *)dev;
	int ret;

	if (rte_mempool_get(mempool, &sess_private_data)) {
		CDEV_LOG_ERR(
			"Couldn't get object from session mempool");
		return -ENOMEM;
	}

	switch (conf->protocol) {
	case RTE_SECURITY_PROTOCOL_IPSEC:
		ret = dpaa2_sec_set_ipsec_session(cdev, conf,
				sess_private_data);
		break;
	case RTE_SECURITY_PROTOCOL_MACSEC:
		return -ENOTSUP;
	default:
		return -EINVAL;
	}
	if (ret != 0) {
		PMD_DRV_LOG(ERR,
			"DPAA2 PMD: failed to configure session parameters");

		/* Return session to mempool */
		rte_mempool_put(mempool, sess_private_data);
		return ret;
	}

	set_sec_session_private_data(sess, sess_private_data);

	return ret;
}

/** Clear the memory of session so it doesn't leave key material behind */
static int
dpaa2_sec_security_session_destroy(void *dev __rte_unused,
		struct rte_security_session *sess)
{
	PMD_INIT_FUNC_TRACE();
	void *sess_priv = get_sec_session_private_data(sess);

	dpaa2_sec_session *s = (dpaa2_sec_session *)sess_priv;

	if (sess_priv) {
		struct rte_mempool *sess_mp = rte_mempool_from_obj(sess_priv);

		rte_free(s->ctxt);
		rte_free(s->cipher_key.data);
		rte_free(s->auth_key.data);
		memset(sess, 0, sizeof(dpaa2_sec_session));
		set_sec_session_private_data(sess, NULL);
		rte_mempool_put(sess_mp, sess_priv);
	}
	return 0;
}

static int
dpaa2_sec_session_configure(struct rte_cryptodev *dev,
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

	ret = dpaa2_sec_set_session_parameters(dev, xform, sess_private_data);
	if (ret != 0) {
		PMD_DRV_LOG(ERR, "DPAA2 PMD: failed to configure "
				"session parameters");

		/* Return session to mempool */
		rte_mempool_put(mempool, sess_private_data);
		return ret;
	}

	set_session_private_data(sess, dev->driver_id,
		sess_private_data);

	return 0;
}

/** Clear the memory of session so it doesn't leave key material behind */
static void
dpaa2_sec_session_clear(struct rte_cryptodev *dev,
		struct rte_cryptodev_sym_session *sess)
{
	PMD_INIT_FUNC_TRACE();
	uint8_t index = dev->driver_id;
	void *sess_priv = get_session_private_data(sess, index);
	dpaa2_sec_session *s = (dpaa2_sec_session *)sess_priv;

	if (sess_priv) {
		rte_free(s->ctxt);
		rte_free(s->cipher_key.data);
		rte_free(s->auth_key.data);
		memset(sess, 0, sizeof(dpaa2_sec_session));
		struct rte_mempool *sess_mp = rte_mempool_from_obj(sess_priv);
		set_session_private_data(sess, index, NULL);
		rte_mempool_put(sess_mp, sess_priv);
	}
}

static int
dpaa2_sec_dev_configure(struct rte_cryptodev *dev __rte_unused,
			struct rte_cryptodev_config *config __rte_unused)
{
	PMD_INIT_FUNC_TRACE();

	return 0;
}

static int
dpaa2_sec_dev_start(struct rte_cryptodev *dev)
{
	struct dpaa2_sec_dev_private *priv = dev->data->dev_private;
	struct fsl_mc_io *dpseci = (struct fsl_mc_io *)priv->hw;
	struct dpseci_attr attr;
	struct dpaa2_queue *dpaa2_q;
	struct dpaa2_sec_qp **qp = (struct dpaa2_sec_qp **)
					dev->data->queue_pairs;
	struct dpseci_rx_queue_attr rx_attr;
	struct dpseci_tx_queue_attr tx_attr;
	int ret, i;

	PMD_INIT_FUNC_TRACE();

	memset(&attr, 0, sizeof(struct dpseci_attr));

	ret = dpseci_enable(dpseci, CMD_PRI_LOW, priv->token);
	if (ret) {
		PMD_INIT_LOG(ERR, "DPSECI with HW_ID = %d ENABLE FAILED\n",
			     priv->hw_id);
		goto get_attr_failure;
	}
	ret = dpseci_get_attributes(dpseci, CMD_PRI_LOW, priv->token, &attr);
	if (ret) {
		PMD_INIT_LOG(ERR,
			     "DPSEC ATTRIBUTE READ FAILED, disabling DPSEC\n");
		goto get_attr_failure;
	}
	for (i = 0; i < attr.num_rx_queues && qp[i]; i++) {
		dpaa2_q = &qp[i]->rx_vq;
		dpseci_get_rx_queue(dpseci, CMD_PRI_LOW, priv->token, i,
				    &rx_attr);
		dpaa2_q->fqid = rx_attr.fqid;
		PMD_INIT_LOG(DEBUG, "rx_fqid: %d", dpaa2_q->fqid);
	}
	for (i = 0; i < attr.num_tx_queues && qp[i]; i++) {
		dpaa2_q = &qp[i]->tx_vq;
		dpseci_get_tx_queue(dpseci, CMD_PRI_LOW, priv->token, i,
				    &tx_attr);
		dpaa2_q->fqid = tx_attr.fqid;
		PMD_INIT_LOG(DEBUG, "tx_fqid: %d", dpaa2_q->fqid);
	}

	return 0;
get_attr_failure:
	dpseci_disable(dpseci, CMD_PRI_LOW, priv->token);
	return -1;
}

static void
dpaa2_sec_dev_stop(struct rte_cryptodev *dev)
{
	struct dpaa2_sec_dev_private *priv = dev->data->dev_private;
	struct fsl_mc_io *dpseci = (struct fsl_mc_io *)priv->hw;
	int ret;

	PMD_INIT_FUNC_TRACE();

	ret = dpseci_disable(dpseci, CMD_PRI_LOW, priv->token);
	if (ret) {
		PMD_INIT_LOG(ERR, "Failure in disabling dpseci %d device",
			     priv->hw_id);
		return;
	}

	ret = dpseci_reset(dpseci, CMD_PRI_LOW, priv->token);
	if (ret < 0) {
		PMD_INIT_LOG(ERR, "SEC Device cannot be reset:Error = %0x\n",
			     ret);
		return;
	}
}

static int
dpaa2_sec_dev_close(struct rte_cryptodev *dev)
{
	struct dpaa2_sec_dev_private *priv = dev->data->dev_private;
	struct fsl_mc_io *dpseci = (struct fsl_mc_io *)priv->hw;
	int ret;

	PMD_INIT_FUNC_TRACE();

	/* Function is reverse of dpaa2_sec_dev_init.
	 * It does the following:
	 * 1. Detach a DPSECI from attached resources i.e. buffer pools, dpbp_id
	 * 2. Close the DPSECI device
	 * 3. Free the allocated resources.
	 */

	/*Close the device at underlying layer*/
	ret = dpseci_close(dpseci, CMD_PRI_LOW, priv->token);
	if (ret) {
		PMD_INIT_LOG(ERR, "Failure closing dpseci device with"
			     " error code %d\n", ret);
		return -1;
	}

	/*Free the allocated memory for ethernet private data and dpseci*/
	priv->hw = NULL;
	rte_free(dpseci);

	return 0;
}

static void
dpaa2_sec_dev_infos_get(struct rte_cryptodev *dev,
			struct rte_cryptodev_info *info)
{
	struct dpaa2_sec_dev_private *internals = dev->data->dev_private;

	PMD_INIT_FUNC_TRACE();
	if (info != NULL) {
		info->max_nb_queue_pairs = internals->max_nb_queue_pairs;
		info->feature_flags = dev->feature_flags;
		info->capabilities = dpaa2_sec_capabilities;
		info->sym.max_nb_sessions = internals->max_nb_sessions;
		info->driver_id = cryptodev_driver_id;
	}
}

static
void dpaa2_sec_stats_get(struct rte_cryptodev *dev,
			 struct rte_cryptodev_stats *stats)
{
	struct dpaa2_sec_dev_private *priv = dev->data->dev_private;
	struct fsl_mc_io *dpseci = (struct fsl_mc_io *)priv->hw;
	struct dpseci_sec_counters counters = {0};
	struct dpaa2_sec_qp **qp = (struct dpaa2_sec_qp **)
					dev->data->queue_pairs;
	int ret, i;

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

		stats->enqueued_count += qp[i]->tx_vq.tx_pkts;
		stats->dequeued_count += qp[i]->rx_vq.rx_pkts;
		stats->enqueue_err_count += qp[i]->tx_vq.err_pkts;
		stats->dequeue_err_count += qp[i]->rx_vq.err_pkts;
	}

	ret = dpseci_get_sec_counters(dpseci, CMD_PRI_LOW, priv->token,
				      &counters);
	if (ret) {
		PMD_DRV_LOG(ERR, "dpseci_get_sec_counters failed\n");
	} else {
		PMD_DRV_LOG(INFO, "dpseci hw stats:"
			    "\n\tNumber of Requests Dequeued = %lu"
			    "\n\tNumber of Outbound Encrypt Requests = %lu"
			    "\n\tNumber of Inbound Decrypt Requests = %lu"
			    "\n\tNumber of Outbound Bytes Encrypted = %lu"
			    "\n\tNumber of Outbound Bytes Protected = %lu"
			    "\n\tNumber of Inbound Bytes Decrypted = %lu"
			    "\n\tNumber of Inbound Bytes Validated = %lu",
			    counters.dequeued_requests,
			    counters.ob_enc_requests,
			    counters.ib_dec_requests,
			    counters.ob_enc_bytes,
			    counters.ob_prot_bytes,
			    counters.ib_dec_bytes,
			    counters.ib_valid_bytes);
	}
}

static
void dpaa2_sec_stats_reset(struct rte_cryptodev *dev)
{
	int i;
	struct dpaa2_sec_qp **qp = (struct dpaa2_sec_qp **)
				   (dev->data->queue_pairs);

	PMD_INIT_FUNC_TRACE();

	for (i = 0; i < dev->data->nb_queue_pairs; i++) {
		if (qp[i] == NULL) {
			PMD_DRV_LOG(DEBUG, "Uninitialised queue pair");
			continue;
		}
		qp[i]->tx_vq.rx_pkts = 0;
		qp[i]->tx_vq.tx_pkts = 0;
		qp[i]->tx_vq.err_pkts = 0;
		qp[i]->rx_vq.rx_pkts = 0;
		qp[i]->rx_vq.tx_pkts = 0;
		qp[i]->rx_vq.err_pkts = 0;
	}
}

static struct rte_cryptodev_ops crypto_ops = {
	.dev_configure	      = dpaa2_sec_dev_configure,
	.dev_start	      = dpaa2_sec_dev_start,
	.dev_stop	      = dpaa2_sec_dev_stop,
	.dev_close	      = dpaa2_sec_dev_close,
	.dev_infos_get        = dpaa2_sec_dev_infos_get,
	.stats_get	      = dpaa2_sec_stats_get,
	.stats_reset	      = dpaa2_sec_stats_reset,
	.queue_pair_setup     = dpaa2_sec_queue_pair_setup,
	.queue_pair_release   = dpaa2_sec_queue_pair_release,
	.queue_pair_start     = dpaa2_sec_queue_pair_start,
	.queue_pair_stop      = dpaa2_sec_queue_pair_stop,
	.queue_pair_count     = dpaa2_sec_queue_pair_count,
	.session_get_size     = dpaa2_sec_session_get_size,
	.session_configure    = dpaa2_sec_session_configure,
	.session_clear        = dpaa2_sec_session_clear,
};

static const struct rte_security_capability *
dpaa2_sec_capabilities_get(void *device __rte_unused)
{
	return dpaa2_sec_security_cap;
}

struct rte_security_ops dpaa2_sec_security_ops = {
	.session_create = dpaa2_sec_security_session_create,
	.session_update = NULL,
	.session_stats_get = NULL,
	.session_destroy = dpaa2_sec_security_session_destroy,
	.set_pkt_metadata = NULL,
	.capabilities_get = dpaa2_sec_capabilities_get
};

static int
dpaa2_sec_uninit(const struct rte_cryptodev *dev)
{
	struct dpaa2_sec_dev_private *internals = dev->data->dev_private;

	rte_free(dev->security_ctx);

	rte_mempool_free(internals->fle_pool);

	PMD_INIT_LOG(INFO, "Closing DPAA2_SEC device %s on numa socket %u\n",
		     dev->data->name, rte_socket_id());

	return 0;
}

static int
dpaa2_sec_dev_init(struct rte_cryptodev *cryptodev)
{
	struct dpaa2_sec_dev_private *internals;
	struct rte_device *dev = cryptodev->device;
	struct rte_dpaa2_device *dpaa2_dev;
	struct rte_security_ctx *security_instance;
	struct fsl_mc_io *dpseci;
	uint16_t token;
	struct dpseci_attr attr;
	int retcode, hw_id;
	char str[20];

	PMD_INIT_FUNC_TRACE();
	dpaa2_dev = container_of(dev, struct rte_dpaa2_device, device);
	if (dpaa2_dev == NULL) {
		PMD_INIT_LOG(ERR, "dpaa2_device not found\n");
		return -1;
	}
	hw_id = dpaa2_dev->object_id;

	cryptodev->driver_id = cryptodev_driver_id;
	cryptodev->dev_ops = &crypto_ops;

	cryptodev->enqueue_burst = dpaa2_sec_enqueue_burst;
	cryptodev->dequeue_burst = dpaa2_sec_dequeue_burst;
	cryptodev->feature_flags = RTE_CRYPTODEV_FF_SYMMETRIC_CRYPTO |
			RTE_CRYPTODEV_FF_HW_ACCELERATED |
			RTE_CRYPTODEV_FF_SYM_OPERATION_CHAINING |
			RTE_CRYPTODEV_FF_SECURITY |
			RTE_CRYPTODEV_FF_MBUF_SCATTER_GATHER;

	internals = cryptodev->data->dev_private;
	internals->max_nb_sessions = RTE_DPAA2_SEC_PMD_MAX_NB_SESSIONS;

	/*
	 * For secondary processes, we don't initialise any further as primary
	 * has already done this work. Only check we don't need a different
	 * RX function
	 */
	if (rte_eal_process_type() != RTE_PROC_PRIMARY) {
		PMD_INIT_LOG(DEBUG, "Device already init by primary process");
		return 0;
	}

	/* Initialize security_ctx only for primary process*/
	security_instance = rte_malloc("rte_security_instances_ops",
				sizeof(struct rte_security_ctx), 0);
	if (security_instance == NULL)
		return -ENOMEM;
	security_instance->device = (void *)cryptodev;
	security_instance->ops = &dpaa2_sec_security_ops;
	security_instance->sess_cnt = 0;
	cryptodev->security_ctx = security_instance;

	/*Open the rte device via MC and save the handle for further use*/
	dpseci = (struct fsl_mc_io *)rte_calloc(NULL, 1,
				sizeof(struct fsl_mc_io), 0);
	if (!dpseci) {
		PMD_INIT_LOG(ERR,
			     "Error in allocating the memory for dpsec object");
		return -1;
	}
	dpseci->regs = rte_mcp_ptr_list[0];

	retcode = dpseci_open(dpseci, CMD_PRI_LOW, hw_id, &token);
	if (retcode != 0) {
		PMD_INIT_LOG(ERR, "Cannot open the dpsec device: Error = %x",
			     retcode);
		goto init_error;
	}
	retcode = dpseci_get_attributes(dpseci, CMD_PRI_LOW, token, &attr);
	if (retcode != 0) {
		PMD_INIT_LOG(ERR,
			     "Cannot get dpsec device attributed: Error = %x",
			     retcode);
		goto init_error;
	}
	sprintf(cryptodev->data->name, "dpsec-%u", hw_id);

	internals->max_nb_queue_pairs = attr.num_tx_queues;
	cryptodev->data->nb_queue_pairs = internals->max_nb_queue_pairs;
	internals->hw = dpseci;
	internals->token = token;

	sprintf(str, "fle_pool_%d", cryptodev->data->dev_id);
	internals->fle_pool = rte_mempool_create((const char *)str,
			FLE_POOL_NUM_BUFS,
			FLE_POOL_BUF_SIZE,
			FLE_POOL_CACHE_SIZE, 0,
			NULL, NULL, NULL, NULL,
			SOCKET_ID_ANY, 0);
	if (!internals->fle_pool) {
		RTE_LOG(ERR, PMD, "%s create failed\n", str);
		goto init_error;
	}

	PMD_INIT_LOG(DEBUG, "driver %s: created\n", cryptodev->data->name);
	return 0;

init_error:
	PMD_INIT_LOG(ERR, "driver %s: create failed\n", cryptodev->data->name);

	/* dpaa2_sec_uninit(crypto_dev_name); */
	return -EFAULT;
}

static int
cryptodev_dpaa2_sec_probe(struct rte_dpaa2_driver *dpaa2_drv,
			  struct rte_dpaa2_device *dpaa2_dev)
{
	struct rte_cryptodev *cryptodev;
	char cryptodev_name[RTE_CRYPTODEV_NAME_MAX_LEN];

	int retval;

	sprintf(cryptodev_name, "dpsec-%d", dpaa2_dev->object_id);

	cryptodev = rte_cryptodev_pmd_allocate(cryptodev_name, rte_socket_id());
	if (cryptodev == NULL)
		return -ENOMEM;

	if (rte_eal_process_type() == RTE_PROC_PRIMARY) {
		cryptodev->data->dev_private = rte_zmalloc_socket(
					"cryptodev private structure",
					sizeof(struct dpaa2_sec_dev_private),
					RTE_CACHE_LINE_SIZE,
					rte_socket_id());

		if (cryptodev->data->dev_private == NULL)
			rte_panic("Cannot allocate memzone for private "
					"device data");
	}

	dpaa2_dev->cryptodev = cryptodev;
	cryptodev->device = &dpaa2_dev->device;
	cryptodev->device->driver = &dpaa2_drv->driver;

	/* init user callbacks */
	TAILQ_INIT(&(cryptodev->link_intr_cbs));

	/* Invoke PMD device initialization function */
	retval = dpaa2_sec_dev_init(cryptodev);
	if (retval == 0)
		return 0;

	if (rte_eal_process_type() == RTE_PROC_PRIMARY)
		rte_free(cryptodev->data->dev_private);

	cryptodev->attached = RTE_CRYPTODEV_DETACHED;

	return -ENXIO;
}

static int
cryptodev_dpaa2_sec_remove(struct rte_dpaa2_device *dpaa2_dev)
{
	struct rte_cryptodev *cryptodev;
	int ret;

	cryptodev = dpaa2_dev->cryptodev;
	if (cryptodev == NULL)
		return -ENODEV;

	ret = dpaa2_sec_uninit(cryptodev);
	if (ret)
		return ret;

	return rte_cryptodev_pmd_destroy(cryptodev);
}

static struct rte_dpaa2_driver rte_dpaa2_sec_driver = {
	.drv_flags = RTE_DPAA2_DRV_IOVA_AS_VA,
	.drv_type = DPAA2_CRYPTO,
	.driver = {
		.name = "DPAA2 SEC PMD"
	},
	.probe = cryptodev_dpaa2_sec_probe,
	.remove = cryptodev_dpaa2_sec_remove,
};

static struct cryptodev_driver dpaa2_sec_crypto_drv;

RTE_PMD_REGISTER_DPAA2(CRYPTODEV_NAME_DPAA2_SEC_PMD, rte_dpaa2_sec_driver);
RTE_PMD_REGISTER_CRYPTO_DRIVER(dpaa2_sec_crypto_drv, rte_dpaa2_sec_driver,
		cryptodev_driver_id);
