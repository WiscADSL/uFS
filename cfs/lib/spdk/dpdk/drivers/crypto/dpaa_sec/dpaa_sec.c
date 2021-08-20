/* SPDX-License-Identifier: BSD-3-Clause
 *
 *   Copyright (c) 2016 Freescale Semiconductor, Inc. All rights reserved.
 *   Copyright 2017 NXP
 *
 */

#include <fcntl.h>
#include <unistd.h>
#include <sched.h>
#include <net/if.h>

#include <rte_byteorder.h>
#include <rte_common.h>
#include <rte_cryptodev_pmd.h>
#include <rte_crypto.h>
#include <rte_cryptodev.h>
#include <rte_security_driver.h>
#include <rte_cycles.h>
#include <rte_dev.h>
#include <rte_kvargs.h>
#include <rte_malloc.h>
#include <rte_mbuf.h>
#include <rte_memcpy.h>
#include <rte_string_fns.h>

#include <fsl_usd.h>
#include <fsl_qman.h>
#include <of.h>

/* RTA header files */
#include <hw/desc/common.h>
#include <hw/desc/algo.h>
#include <hw/desc/ipsec.h>

#include <rte_dpaa_bus.h>
#include <dpaa_sec.h>
#include <dpaa_sec_log.h>

enum rta_sec_era rta_sec_era;

static uint8_t cryptodev_driver_id;

static __thread struct rte_crypto_op **dpaa_sec_ops;
static __thread int dpaa_sec_op_nb;

static int
dpaa_sec_attach_sess_q(struct dpaa_sec_qp *qp, dpaa_sec_session *sess);

static inline void
dpaa_sec_op_ending(struct dpaa_sec_op_ctx *ctx)
{
	if (!ctx->fd_status) {
		ctx->op->status = RTE_CRYPTO_OP_STATUS_SUCCESS;
	} else {
		PMD_RX_LOG(ERR, "SEC return err: 0x%x", ctx->fd_status);
		ctx->op->status = RTE_CRYPTO_OP_STATUS_ERROR;
	}

	/* report op status to sym->op and then free the ctx memeory  */
	rte_mempool_put(ctx->ctx_pool, (void *)ctx);
}

static inline struct dpaa_sec_op_ctx *
dpaa_sec_alloc_ctx(dpaa_sec_session *ses)
{
	struct dpaa_sec_op_ctx *ctx;
	int retval;

	retval = rte_mempool_get(ses->ctx_pool, (void **)(&ctx));
	if (!ctx || retval) {
		PMD_TX_LOG(ERR, "Alloc sec descriptor failed!");
		return NULL;
	}
	/*
	 * Clear SG memory. There are 16 SG entries of 16 Bytes each.
	 * one call to dcbz_64() clear 64 bytes, hence calling it 4 times
	 * to clear all the SG entries. dpaa_sec_alloc_ctx() is called for
	 * each packet, memset is costlier than dcbz_64().
	 */
	dcbz_64(&ctx->job.sg[SG_CACHELINE_0]);
	dcbz_64(&ctx->job.sg[SG_CACHELINE_1]);
	dcbz_64(&ctx->job.sg[SG_CACHELINE_2]);
	dcbz_64(&ctx->job.sg[SG_CACHELINE_3]);

	ctx->ctx_pool = ses->ctx_pool;
	ctx->vtop_offset = (uint64_t) ctx
				- rte_mempool_virt2iova(ctx);

	return ctx;
}

static inline rte_iova_t
dpaa_mem_vtop(void *vaddr)
{
	const struct rte_memseg *memseg = rte_eal_get_physmem_layout();
	uint64_t vaddr_64, paddr;
	int i;

	vaddr_64 = (uint64_t)vaddr;
	for (i = 0; i < RTE_MAX_MEMSEG && memseg[i].addr_64 != 0; i++) {
		if (vaddr_64 >= memseg[i].addr_64 &&
		    vaddr_64 < memseg[i].addr_64 + memseg[i].len) {
			paddr = memseg[i].iova +
				(vaddr_64 - memseg[i].addr_64);

			return (rte_iova_t)paddr;
		}
	}
	return (rte_iova_t)(NULL);
}

/* virtual address conversin when mempool support is available for ctx */
static inline phys_addr_t
dpaa_mem_vtop_ctx(struct dpaa_sec_op_ctx *ctx, void *vaddr)
{
	return (uint64_t)vaddr - ctx->vtop_offset;
}

static inline void *
dpaa_mem_ptov(rte_iova_t paddr)
{
	const struct rte_memseg *memseg = rte_eal_get_physmem_layout();
	int i;

	for (i = 0; i < RTE_MAX_MEMSEG && memseg[i].addr_64 != 0; i++) {
		if (paddr >= memseg[i].iova &&
		    (char *)paddr < (char *)memseg[i].iova + memseg[i].len)
			return (void *)(memseg[i].addr_64 +
					(paddr - memseg[i].iova));
	}
	return NULL;
}

static void
ern_sec_fq_handler(struct qman_portal *qm __rte_unused,
		   struct qman_fq *fq,
		   const struct qm_mr_entry *msg)
{
	RTE_LOG_DP(ERR, PMD, "sec fq %d error, RC = %x, seqnum = %x\n",
		   fq->fqid, msg->ern.rc, msg->ern.seqnum);
}

/* initialize the queue with dest chan as caam chan so that
 * all the packets in this queue could be dispatched into caam
 */
static int
dpaa_sec_init_rx(struct qman_fq *fq_in, rte_iova_t hwdesc,
		 uint32_t fqid_out)
{
	struct qm_mcc_initfq fq_opts;
	uint32_t flags;
	int ret = -1;

	/* Clear FQ options */
	memset(&fq_opts, 0x00, sizeof(struct qm_mcc_initfq));

	flags = QMAN_INITFQ_FLAG_SCHED;
	fq_opts.we_mask = QM_INITFQ_WE_DESTWQ | QM_INITFQ_WE_CONTEXTA |
			  QM_INITFQ_WE_CONTEXTB;

	qm_fqd_context_a_set64(&fq_opts.fqd, hwdesc);
	fq_opts.fqd.context_b = fqid_out;
	fq_opts.fqd.dest.channel = qm_channel_caam;
	fq_opts.fqd.dest.wq = 0;

	fq_in->cb.ern  = ern_sec_fq_handler;

	PMD_INIT_LOG(DEBUG, "in-%x out-%x", fq_in->fqid, fqid_out);

	ret = qman_init_fq(fq_in, flags, &fq_opts);
	if (unlikely(ret != 0))
		PMD_INIT_LOG(ERR, "qman_init_fq failed %d", ret);

	return ret;
}

/* something is put into in_fq and caam put the crypto result into out_fq */
static enum qman_cb_dqrr_result
dqrr_out_fq_cb_rx(struct qman_portal *qm __always_unused,
		  struct qman_fq *fq __always_unused,
		  const struct qm_dqrr_entry *dqrr)
{
	const struct qm_fd *fd;
	struct dpaa_sec_job *job;
	struct dpaa_sec_op_ctx *ctx;

	if (dpaa_sec_op_nb >= DPAA_SEC_BURST)
		return qman_cb_dqrr_defer;

	if (!(dqrr->stat & QM_DQRR_STAT_FD_VALID))
		return qman_cb_dqrr_consume;

	fd = &dqrr->fd;
	/* sg is embedded in an op ctx,
	 * sg[0] is for output
	 * sg[1] for input
	 */
	job = dpaa_mem_ptov(qm_fd_addr_get64(fd));

	ctx = container_of(job, struct dpaa_sec_op_ctx, job);
	ctx->fd_status = fd->status;
	if (ctx->op->sess_type == RTE_CRYPTO_OP_SECURITY_SESSION) {
		struct qm_sg_entry *sg_out;
		uint32_t len;

		sg_out = &job->sg[0];
		hw_sg_to_cpu(sg_out);
		len = sg_out->length;
		ctx->op->sym->m_src->pkt_len = len;
		ctx->op->sym->m_src->data_len = len;
	}
	dpaa_sec_ops[dpaa_sec_op_nb++] = ctx->op;
	dpaa_sec_op_ending(ctx);

	return qman_cb_dqrr_consume;
}

/* caam result is put into this queue */
static int
dpaa_sec_init_tx(struct qman_fq *fq)
{
	int ret;
	struct qm_mcc_initfq opts;
	uint32_t flags;

	flags = QMAN_FQ_FLAG_NO_ENQUEUE | QMAN_FQ_FLAG_LOCKED |
		QMAN_FQ_FLAG_DYNAMIC_FQID;

	ret = qman_create_fq(0, flags, fq);
	if (unlikely(ret)) {
		PMD_INIT_LOG(ERR, "qman_create_fq failed");
		return ret;
	}

	memset(&opts, 0, sizeof(opts));
	opts.we_mask = QM_INITFQ_WE_DESTWQ | QM_INITFQ_WE_FQCTRL |
		       QM_INITFQ_WE_CONTEXTA | QM_INITFQ_WE_CONTEXTB;

	/* opts.fqd.dest.channel = dpaa_sec_pool_chan; */

	fq->cb.dqrr = dqrr_out_fq_cb_rx;
	fq->cb.ern  = ern_sec_fq_handler;

	ret = qman_init_fq(fq, 0, &opts);
	if (unlikely(ret)) {
		PMD_INIT_LOG(ERR, "unable to init caam source fq!");
		return ret;
	}

	return ret;
}

static inline int is_cipher_only(dpaa_sec_session *ses)
{
	return ((ses->cipher_alg != RTE_CRYPTO_CIPHER_NULL) &&
		(ses->auth_alg == RTE_CRYPTO_AUTH_NULL));
}

static inline int is_auth_only(dpaa_sec_session *ses)
{
	return ((ses->cipher_alg == RTE_CRYPTO_CIPHER_NULL) &&
		(ses->auth_alg != RTE_CRYPTO_AUTH_NULL));
}

static inline int is_aead(dpaa_sec_session *ses)
{
	return ((ses->cipher_alg == 0) &&
		(ses->auth_alg == 0) &&
		(ses->aead_alg != 0));
}

static inline int is_auth_cipher(dpaa_sec_session *ses)
{
	return ((ses->cipher_alg != RTE_CRYPTO_CIPHER_NULL) &&
		(ses->auth_alg != RTE_CRYPTO_AUTH_NULL) &&
		(ses->proto_alg != RTE_SECURITY_PROTOCOL_IPSEC));
}

static inline int is_proto_ipsec(dpaa_sec_session *ses)
{
	return (ses->proto_alg == RTE_SECURITY_PROTOCOL_IPSEC);
}

static inline int is_encode(dpaa_sec_session *ses)
{
	return ses->dir == DIR_ENC;
}

static inline int is_decode(dpaa_sec_session *ses)
{
	return ses->dir == DIR_DEC;
}

static inline void
caam_auth_alg(dpaa_sec_session *ses, struct alginfo *alginfo_a)
{
	switch (ses->auth_alg) {
	case RTE_CRYPTO_AUTH_NULL:
		ses->digest_length = 0;
		break;
	case RTE_CRYPTO_AUTH_MD5_HMAC:
		alginfo_a->algtype =
			(ses->proto_alg == RTE_SECURITY_PROTOCOL_IPSEC) ?
			OP_PCL_IPSEC_HMAC_MD5_96 : OP_ALG_ALGSEL_MD5;
		alginfo_a->algmode = OP_ALG_AAI_HMAC;
		break;
	case RTE_CRYPTO_AUTH_SHA1_HMAC:
		alginfo_a->algtype =
			(ses->proto_alg == RTE_SECURITY_PROTOCOL_IPSEC) ?
			OP_PCL_IPSEC_HMAC_SHA1_96 : OP_ALG_ALGSEL_SHA1;
		alginfo_a->algmode = OP_ALG_AAI_HMAC;
		break;
	case RTE_CRYPTO_AUTH_SHA224_HMAC:
		alginfo_a->algtype =
			(ses->proto_alg == RTE_SECURITY_PROTOCOL_IPSEC) ?
			OP_PCL_IPSEC_HMAC_SHA1_160 : OP_ALG_ALGSEL_SHA224;
		alginfo_a->algmode = OP_ALG_AAI_HMAC;
		break;
	case RTE_CRYPTO_AUTH_SHA256_HMAC:
		alginfo_a->algtype =
			(ses->proto_alg == RTE_SECURITY_PROTOCOL_IPSEC) ?
			OP_PCL_IPSEC_HMAC_SHA2_256_128 : OP_ALG_ALGSEL_SHA256;
		alginfo_a->algmode = OP_ALG_AAI_HMAC;
		break;
	case RTE_CRYPTO_AUTH_SHA384_HMAC:
		alginfo_a->algtype =
			(ses->proto_alg == RTE_SECURITY_PROTOCOL_IPSEC) ?
			OP_PCL_IPSEC_HMAC_SHA2_384_192 : OP_ALG_ALGSEL_SHA384;
		alginfo_a->algmode = OP_ALG_AAI_HMAC;
		break;
	case RTE_CRYPTO_AUTH_SHA512_HMAC:
		alginfo_a->algtype =
			(ses->proto_alg == RTE_SECURITY_PROTOCOL_IPSEC) ?
			OP_PCL_IPSEC_HMAC_SHA2_512_256 : OP_ALG_ALGSEL_SHA512;
		alginfo_a->algmode = OP_ALG_AAI_HMAC;
		break;
	default:
		PMD_INIT_LOG(ERR, "unsupported auth alg %u", ses->auth_alg);
	}
}

static inline void
caam_cipher_alg(dpaa_sec_session *ses, struct alginfo *alginfo_c)
{
	switch (ses->cipher_alg) {
	case RTE_CRYPTO_CIPHER_NULL:
		break;
	case RTE_CRYPTO_CIPHER_AES_CBC:
		alginfo_c->algtype =
			(ses->proto_alg == RTE_SECURITY_PROTOCOL_IPSEC) ?
			OP_PCL_IPSEC_AES_CBC : OP_ALG_ALGSEL_AES;
		alginfo_c->algmode = OP_ALG_AAI_CBC;
		break;
	case RTE_CRYPTO_CIPHER_3DES_CBC:
		alginfo_c->algtype =
			(ses->proto_alg == RTE_SECURITY_PROTOCOL_IPSEC) ?
			OP_PCL_IPSEC_3DES : OP_ALG_ALGSEL_3DES;
		alginfo_c->algmode = OP_ALG_AAI_CBC;
		break;
	case RTE_CRYPTO_CIPHER_AES_CTR:
		alginfo_c->algtype =
			(ses->proto_alg == RTE_SECURITY_PROTOCOL_IPSEC) ?
			OP_PCL_IPSEC_AES_CTR : OP_ALG_ALGSEL_AES;
		alginfo_c->algmode = OP_ALG_AAI_CTR;
		break;
	default:
		PMD_INIT_LOG(ERR, "unsupported cipher alg %d", ses->cipher_alg);
	}
}

static inline void
caam_aead_alg(dpaa_sec_session *ses, struct alginfo *alginfo)
{
	switch (ses->aead_alg) {
	case RTE_CRYPTO_AEAD_AES_GCM:
		alginfo->algtype = OP_ALG_ALGSEL_AES;
		alginfo->algmode = OP_ALG_AAI_GCM;
		break;
	default:
		PMD_INIT_LOG(ERR, "unsupported AEAD alg %d", ses->aead_alg);
	}
}


/* prepare command block of the session */
static int
dpaa_sec_prep_cdb(dpaa_sec_session *ses)
{
	struct alginfo alginfo_c = {0}, alginfo_a = {0}, alginfo = {0};
	uint32_t shared_desc_len = 0;
	struct sec_cdb *cdb = &ses->cdb;
	int err;
#if RTE_BYTE_ORDER == RTE_BIG_ENDIAN
	int swap = false;
#else
	int swap = true;
#endif

	memset(cdb, 0, sizeof(struct sec_cdb));

	if (is_cipher_only(ses)) {
		caam_cipher_alg(ses, &alginfo_c);
		if (alginfo_c.algtype == (unsigned int)DPAA_SEC_ALG_UNSUPPORT) {
			PMD_TX_LOG(ERR, "not supported cipher alg\n");
			return -ENOTSUP;
		}

		alginfo_c.key = (uint64_t)ses->cipher_key.data;
		alginfo_c.keylen = ses->cipher_key.length;
		alginfo_c.key_enc_flags = 0;
		alginfo_c.key_type = RTA_DATA_IMM;

		shared_desc_len = cnstr_shdsc_blkcipher(
						cdb->sh_desc, true,
						swap, &alginfo_c,
						NULL,
						ses->iv.length,
						ses->dir);
	} else if (is_auth_only(ses)) {
		caam_auth_alg(ses, &alginfo_a);
		if (alginfo_a.algtype == (unsigned int)DPAA_SEC_ALG_UNSUPPORT) {
			PMD_TX_LOG(ERR, "not supported auth alg\n");
			return -ENOTSUP;
		}

		alginfo_a.key = (uint64_t)ses->auth_key.data;
		alginfo_a.keylen = ses->auth_key.length;
		alginfo_a.key_enc_flags = 0;
		alginfo_a.key_type = RTA_DATA_IMM;

		shared_desc_len = cnstr_shdsc_hmac(cdb->sh_desc, true,
						   swap, &alginfo_a,
						   !ses->dir,
						   ses->digest_length);
	} else if (is_aead(ses)) {
		caam_aead_alg(ses, &alginfo);
		if (alginfo.algtype == (unsigned int)DPAA_SEC_ALG_UNSUPPORT) {
			PMD_TX_LOG(ERR, "not supported aead alg\n");
			return -ENOTSUP;
		}
		alginfo.key = (uint64_t)ses->aead_key.data;
		alginfo.keylen = ses->aead_key.length;
		alginfo.key_enc_flags = 0;
		alginfo.key_type = RTA_DATA_IMM;

		if (ses->dir == DIR_ENC)
			shared_desc_len = cnstr_shdsc_gcm_encap(
					cdb->sh_desc, true, swap,
					&alginfo,
					ses->iv.length,
					ses->digest_length);
		else
			shared_desc_len = cnstr_shdsc_gcm_decap(
					cdb->sh_desc, true, swap,
					&alginfo,
					ses->iv.length,
					ses->digest_length);
	} else {
		caam_cipher_alg(ses, &alginfo_c);
		if (alginfo_c.algtype == (unsigned int)DPAA_SEC_ALG_UNSUPPORT) {
			PMD_TX_LOG(ERR, "not supported cipher alg\n");
			return -ENOTSUP;
		}

		alginfo_c.key = (uint64_t)ses->cipher_key.data;
		alginfo_c.keylen = ses->cipher_key.length;
		alginfo_c.key_enc_flags = 0;
		alginfo_c.key_type = RTA_DATA_IMM;

		caam_auth_alg(ses, &alginfo_a);
		if (alginfo_a.algtype == (unsigned int)DPAA_SEC_ALG_UNSUPPORT) {
			PMD_TX_LOG(ERR, "not supported auth alg\n");
			return -ENOTSUP;
		}

		alginfo_a.key = (uint64_t)ses->auth_key.data;
		alginfo_a.keylen = ses->auth_key.length;
		alginfo_a.key_enc_flags = 0;
		alginfo_a.key_type = RTA_DATA_IMM;

		cdb->sh_desc[0] = alginfo_c.keylen;
		cdb->sh_desc[1] = alginfo_a.keylen;
		err = rta_inline_query(IPSEC_AUTH_VAR_AES_DEC_BASE_DESC_LEN,
				       MIN_JOB_DESC_SIZE,
				       (unsigned int *)cdb->sh_desc,
				       &cdb->sh_desc[2], 2);

		if (err < 0) {
			PMD_TX_LOG(ERR, "Crypto: Incorrect key lengths");
			return err;
		}
		if (cdb->sh_desc[2] & 1)
			alginfo_c.key_type = RTA_DATA_IMM;
		else {
			alginfo_c.key = (uint64_t)dpaa_mem_vtop(
							(void *)alginfo_c.key);
			alginfo_c.key_type = RTA_DATA_PTR;
		}
		if (cdb->sh_desc[2] & (1<<1))
			alginfo_a.key_type = RTA_DATA_IMM;
		else {
			alginfo_a.key = (uint64_t)dpaa_mem_vtop(
							(void *)alginfo_a.key);
			alginfo_a.key_type = RTA_DATA_PTR;
		}
		cdb->sh_desc[0] = 0;
		cdb->sh_desc[1] = 0;
		cdb->sh_desc[2] = 0;
		if (is_proto_ipsec(ses)) {
			if (ses->dir == DIR_ENC) {
				shared_desc_len = cnstr_shdsc_ipsec_new_encap(
						cdb->sh_desc,
						true, swap, &ses->encap_pdb,
						(uint8_t *)&ses->ip4_hdr,
						&alginfo_c, &alginfo_a);
			} else if (ses->dir == DIR_DEC) {
				shared_desc_len = cnstr_shdsc_ipsec_new_decap(
						cdb->sh_desc,
						true, swap, &ses->decap_pdb,
						&alginfo_c, &alginfo_a);
			}
		} else {
			/* Auth_only_len is set as 0 here and it will be
			 * overwritten in fd for each packet.
			 */
			shared_desc_len = cnstr_shdsc_authenc(cdb->sh_desc,
					true, swap, &alginfo_c, &alginfo_a,
					ses->iv.length, 0,
					ses->digest_length, ses->dir);
		}
	}
	cdb->sh_hdr.hi.field.idlen = shared_desc_len;
	cdb->sh_hdr.hi.word = rte_cpu_to_be_32(cdb->sh_hdr.hi.word);
	cdb->sh_hdr.lo.word = rte_cpu_to_be_32(cdb->sh_hdr.lo.word);

	return 0;
}

/* qp is lockless, should be accessed by only one thread */
static int
dpaa_sec_deq(struct dpaa_sec_qp *qp, struct rte_crypto_op **ops, int nb_ops)
{
	struct qman_fq *fq;
	unsigned int pkts = 0;
	int ret;
	struct qm_dqrr_entry *dq;

	fq = &qp->outq;
	ret = qman_set_vdq(fq, (nb_ops > DPAA_MAX_DEQUEUE_NUM_FRAMES) ?
				DPAA_MAX_DEQUEUE_NUM_FRAMES : nb_ops);
	if (ret)
		return 0;

	do {
		const struct qm_fd *fd;
		struct dpaa_sec_job *job;
		struct dpaa_sec_op_ctx *ctx;
		struct rte_crypto_op *op;

		dq = qman_dequeue(fq);
		if (!dq)
			continue;

		fd = &dq->fd;
		/* sg is embedded in an op ctx,
		 * sg[0] is for output
		 * sg[1] for input
		 */
		job = dpaa_mem_ptov(qm_fd_addr_get64(fd));

		ctx = container_of(job, struct dpaa_sec_op_ctx, job);
		ctx->fd_status = fd->status;
		op = ctx->op;
		if (op->sess_type == RTE_CRYPTO_OP_SECURITY_SESSION) {
			struct qm_sg_entry *sg_out;
			uint32_t len;

			sg_out = &job->sg[0];
			hw_sg_to_cpu(sg_out);
			len = sg_out->length;
			op->sym->m_src->pkt_len = len;
			op->sym->m_src->data_len = len;
		}
		if (!ctx->fd_status) {
			op->status = RTE_CRYPTO_OP_STATUS_SUCCESS;
		} else {
			printf("\nSEC return err: 0x%x", ctx->fd_status);
			op->status = RTE_CRYPTO_OP_STATUS_ERROR;
		}
		ops[pkts++] = op;

		/* report op status to sym->op and then free the ctx memeory */
		rte_mempool_put(ctx->ctx_pool, (void *)ctx);

		qman_dqrr_consume(fq, dq);
	} while (fq->flags & QMAN_FQ_STATE_VDQCR);

	return pkts;
}

static inline struct dpaa_sec_job *
build_auth_only_sg(struct rte_crypto_op *op, dpaa_sec_session *ses)
{
	struct rte_crypto_sym_op *sym = op->sym;
	struct rte_mbuf *mbuf = sym->m_src;
	struct dpaa_sec_job *cf;
	struct dpaa_sec_op_ctx *ctx;
	struct qm_sg_entry *sg, *out_sg, *in_sg;
	phys_addr_t start_addr;
	uint8_t *old_digest, extra_segs;

	if (is_decode(ses))
		extra_segs = 3;
	else
		extra_segs = 2;

	if ((mbuf->nb_segs + extra_segs) > MAX_SG_ENTRIES) {
		PMD_TX_LOG(ERR, "Auth: Max sec segs supported is %d\n",
								MAX_SG_ENTRIES);
		return NULL;
	}
	ctx = dpaa_sec_alloc_ctx(ses);
	if (!ctx)
		return NULL;

	cf = &ctx->job;
	ctx->op = op;
	old_digest = ctx->digest;

	/* output */
	out_sg = &cf->sg[0];
	qm_sg_entry_set64(out_sg, sym->auth.digest.phys_addr);
	out_sg->length = ses->digest_length;
	cpu_to_hw_sg(out_sg);

	/* input */
	in_sg = &cf->sg[1];
	/* need to extend the input to a compound frame */
	in_sg->extension = 1;
	in_sg->final = 1;
	in_sg->length = sym->auth.data.length;
	qm_sg_entry_set64(in_sg, dpaa_mem_vtop_ctx(ctx, &cf->sg[2]));

	/* 1st seg */
	sg = in_sg + 1;
	qm_sg_entry_set64(sg, rte_pktmbuf_mtophys(mbuf));
	sg->length = mbuf->data_len - sym->auth.data.offset;
	sg->offset = sym->auth.data.offset;

	/* Successive segs */
	mbuf = mbuf->next;
	while (mbuf) {
		cpu_to_hw_sg(sg);
		sg++;
		qm_sg_entry_set64(sg, rte_pktmbuf_mtophys(mbuf));
		sg->length = mbuf->data_len;
		mbuf = mbuf->next;
	}

	if (is_decode(ses)) {
		/* Digest verification case */
		cpu_to_hw_sg(sg);
		sg++;
		rte_memcpy(old_digest, sym->auth.digest.data,
				ses->digest_length);
		start_addr = dpaa_mem_vtop_ctx(ctx, old_digest);
		qm_sg_entry_set64(sg, start_addr);
		sg->length = ses->digest_length;
		in_sg->length += ses->digest_length;
	} else {
		/* Digest calculation case */
		sg->length -= ses->digest_length;
	}
	sg->final = 1;
	cpu_to_hw_sg(sg);
	cpu_to_hw_sg(in_sg);

	return cf;
}

/**
 * packet looks like:
 *		|<----data_len------->|
 *    |ip_header|ah_header|icv|payload|
 *              ^
 *		|
 *	   mbuf->pkt.data
 */
static inline struct dpaa_sec_job *
build_auth_only(struct rte_crypto_op *op, dpaa_sec_session *ses)
{
	struct rte_crypto_sym_op *sym = op->sym;
	struct rte_mbuf *mbuf = sym->m_src;
	struct dpaa_sec_job *cf;
	struct dpaa_sec_op_ctx *ctx;
	struct qm_sg_entry *sg;
	rte_iova_t start_addr;
	uint8_t *old_digest;

	ctx = dpaa_sec_alloc_ctx(ses);
	if (!ctx)
		return NULL;

	cf = &ctx->job;
	ctx->op = op;
	old_digest = ctx->digest;

	start_addr = rte_pktmbuf_iova(mbuf);
	/* output */
	sg = &cf->sg[0];
	qm_sg_entry_set64(sg, sym->auth.digest.phys_addr);
	sg->length = ses->digest_length;
	cpu_to_hw_sg(sg);

	/* input */
	sg = &cf->sg[1];
	if (is_decode(ses)) {
		/* need to extend the input to a compound frame */
		sg->extension = 1;
		qm_sg_entry_set64(sg, dpaa_mem_vtop_ctx(ctx, &cf->sg[2]));
		sg->length = sym->auth.data.length + ses->digest_length;
		sg->final = 1;
		cpu_to_hw_sg(sg);

		sg = &cf->sg[2];
		/* hash result or digest, save digest first */
		rte_memcpy(old_digest, sym->auth.digest.data,
			   ses->digest_length);
		qm_sg_entry_set64(sg, start_addr + sym->auth.data.offset);
		sg->length = sym->auth.data.length;
		cpu_to_hw_sg(sg);

		/* let's check digest by hw */
		start_addr = dpaa_mem_vtop_ctx(ctx, old_digest);
		sg++;
		qm_sg_entry_set64(sg, start_addr);
		sg->length = ses->digest_length;
		sg->final = 1;
		cpu_to_hw_sg(sg);
	} else {
		qm_sg_entry_set64(sg, start_addr + sym->auth.data.offset);
		sg->length = sym->auth.data.length;
		sg->final = 1;
		cpu_to_hw_sg(sg);
	}

	return cf;
}

static inline struct dpaa_sec_job *
build_cipher_only_sg(struct rte_crypto_op *op, dpaa_sec_session *ses)
{
	struct rte_crypto_sym_op *sym = op->sym;
	struct dpaa_sec_job *cf;
	struct dpaa_sec_op_ctx *ctx;
	struct qm_sg_entry *sg, *out_sg, *in_sg;
	struct rte_mbuf *mbuf;
	uint8_t req_segs;
	uint8_t *IV_ptr = rte_crypto_op_ctod_offset(op, uint8_t *,
			ses->iv.offset);

	if (sym->m_dst) {
		mbuf = sym->m_dst;
		req_segs = mbuf->nb_segs + sym->m_src->nb_segs + 3;
	} else {
		mbuf = sym->m_src;
		req_segs = mbuf->nb_segs * 2 + 3;
	}

	if (req_segs > MAX_SG_ENTRIES) {
		PMD_TX_LOG(ERR, "Cipher: Max sec segs supported is %d\n",
								MAX_SG_ENTRIES);
		return NULL;
	}

	ctx = dpaa_sec_alloc_ctx(ses);
	if (!ctx)
		return NULL;

	cf = &ctx->job;
	ctx->op = op;

	/* output */
	out_sg = &cf->sg[0];
	out_sg->extension = 1;
	out_sg->length = sym->cipher.data.length;
	qm_sg_entry_set64(out_sg, dpaa_mem_vtop_ctx(ctx, &cf->sg[2]));
	cpu_to_hw_sg(out_sg);

	/* 1st seg */
	sg = &cf->sg[2];
	qm_sg_entry_set64(sg, rte_pktmbuf_mtophys(mbuf));
	sg->length = mbuf->data_len - sym->cipher.data.offset;
	sg->offset = sym->cipher.data.offset;

	/* Successive segs */
	mbuf = mbuf->next;
	while (mbuf) {
		cpu_to_hw_sg(sg);
		sg++;
		qm_sg_entry_set64(sg, rte_pktmbuf_mtophys(mbuf));
		sg->length = mbuf->data_len;
		mbuf = mbuf->next;
	}
	sg->final = 1;
	cpu_to_hw_sg(sg);

	/* input */
	mbuf = sym->m_src;
	in_sg = &cf->sg[1];
	in_sg->extension = 1;
	in_sg->final = 1;
	in_sg->length = sym->cipher.data.length + ses->iv.length;

	sg++;
	qm_sg_entry_set64(in_sg, dpaa_mem_vtop_ctx(ctx, sg));
	cpu_to_hw_sg(in_sg);

	/* IV */
	qm_sg_entry_set64(sg, dpaa_mem_vtop(IV_ptr));
	sg->length = ses->iv.length;
	cpu_to_hw_sg(sg);

	/* 1st seg */
	sg++;
	qm_sg_entry_set64(sg, rte_pktmbuf_mtophys(mbuf));
	sg->length = mbuf->data_len - sym->cipher.data.offset;
	sg->offset = sym->cipher.data.offset;

	/* Successive segs */
	mbuf = mbuf->next;
	while (mbuf) {
		cpu_to_hw_sg(sg);
		sg++;
		qm_sg_entry_set64(sg, rte_pktmbuf_mtophys(mbuf));
		sg->length = mbuf->data_len;
		mbuf = mbuf->next;
	}
	sg->final = 1;
	cpu_to_hw_sg(sg);

	return cf;
}

static inline struct dpaa_sec_job *
build_cipher_only(struct rte_crypto_op *op, dpaa_sec_session *ses)
{
	struct rte_crypto_sym_op *sym = op->sym;
	struct dpaa_sec_job *cf;
	struct dpaa_sec_op_ctx *ctx;
	struct qm_sg_entry *sg;
	rte_iova_t src_start_addr, dst_start_addr;
	uint8_t *IV_ptr = rte_crypto_op_ctod_offset(op, uint8_t *,
			ses->iv.offset);

	ctx = dpaa_sec_alloc_ctx(ses);
	if (!ctx)
		return NULL;

	cf = &ctx->job;
	ctx->op = op;

	src_start_addr = rte_pktmbuf_iova(sym->m_src);

	if (sym->m_dst)
		dst_start_addr = rte_pktmbuf_iova(sym->m_dst);
	else
		dst_start_addr = src_start_addr;

	/* output */
	sg = &cf->sg[0];
	qm_sg_entry_set64(sg, dst_start_addr + sym->cipher.data.offset);
	sg->length = sym->cipher.data.length + ses->iv.length;
	cpu_to_hw_sg(sg);

	/* input */
	sg = &cf->sg[1];

	/* need to extend the input to a compound frame */
	sg->extension = 1;
	sg->final = 1;
	sg->length = sym->cipher.data.length + ses->iv.length;
	qm_sg_entry_set64(sg, dpaa_mem_vtop_ctx(ctx, &cf->sg[2]));
	cpu_to_hw_sg(sg);

	sg = &cf->sg[2];
	qm_sg_entry_set64(sg, dpaa_mem_vtop(IV_ptr));
	sg->length = ses->iv.length;
	cpu_to_hw_sg(sg);

	sg++;
	qm_sg_entry_set64(sg, src_start_addr + sym->cipher.data.offset);
	sg->length = sym->cipher.data.length;
	sg->final = 1;
	cpu_to_hw_sg(sg);

	return cf;
}

static inline struct dpaa_sec_job *
build_cipher_auth_gcm_sg(struct rte_crypto_op *op, dpaa_sec_session *ses)
{
	struct rte_crypto_sym_op *sym = op->sym;
	struct dpaa_sec_job *cf;
	struct dpaa_sec_op_ctx *ctx;
	struct qm_sg_entry *sg, *out_sg, *in_sg;
	struct rte_mbuf *mbuf;
	uint8_t req_segs;
	uint8_t *IV_ptr = rte_crypto_op_ctod_offset(op, uint8_t *,
			ses->iv.offset);

	if (sym->m_dst) {
		mbuf = sym->m_dst;
		req_segs = mbuf->nb_segs + sym->m_src->nb_segs + 4;
	} else {
		mbuf = sym->m_src;
		req_segs = mbuf->nb_segs * 2 + 4;
	}

	if (ses->auth_only_len)
		req_segs++;

	if (req_segs > MAX_SG_ENTRIES) {
		PMD_TX_LOG(ERR, "AEAD: Max sec segs supported is %d\n",
				MAX_SG_ENTRIES);
		return NULL;
	}

	ctx = dpaa_sec_alloc_ctx(ses);
	if (!ctx)
		return NULL;

	cf = &ctx->job;
	ctx->op = op;

	rte_prefetch0(cf->sg);

	/* output */
	out_sg = &cf->sg[0];
	out_sg->extension = 1;
	if (is_encode(ses))
		out_sg->length = sym->aead.data.length + ses->auth_only_len
						+ ses->digest_length;
	else
		out_sg->length = sym->aead.data.length + ses->auth_only_len;

	/* output sg entries */
	sg = &cf->sg[2];
	qm_sg_entry_set64(out_sg, dpaa_mem_vtop_ctx(ctx, sg));
	cpu_to_hw_sg(out_sg);

	/* 1st seg */
	qm_sg_entry_set64(sg, rte_pktmbuf_mtophys(mbuf));
	sg->length = mbuf->data_len - sym->aead.data.offset +
					ses->auth_only_len;
	sg->offset = sym->aead.data.offset - ses->auth_only_len;

	/* Successive segs */
	mbuf = mbuf->next;
	while (mbuf) {
		cpu_to_hw_sg(sg);
		sg++;
		qm_sg_entry_set64(sg, rte_pktmbuf_mtophys(mbuf));
		sg->length = mbuf->data_len;
		mbuf = mbuf->next;
	}
	sg->length -= ses->digest_length;

	if (is_encode(ses)) {
		cpu_to_hw_sg(sg);
		/* set auth output */
		sg++;
		qm_sg_entry_set64(sg, sym->aead.digest.phys_addr);
		sg->length = ses->digest_length;
	}
	sg->final = 1;
	cpu_to_hw_sg(sg);

	/* input */
	mbuf = sym->m_src;
	in_sg = &cf->sg[1];
	in_sg->extension = 1;
	in_sg->final = 1;
	if (is_encode(ses))
		in_sg->length = ses->iv.length + sym->aead.data.length
							+ ses->auth_only_len;
	else
		in_sg->length = ses->iv.length + sym->aead.data.length
				+ ses->auth_only_len + ses->digest_length;

	/* input sg entries */
	sg++;
	qm_sg_entry_set64(in_sg, dpaa_mem_vtop_ctx(ctx, sg));
	cpu_to_hw_sg(in_sg);

	/* 1st seg IV */
	qm_sg_entry_set64(sg, dpaa_mem_vtop(IV_ptr));
	sg->length = ses->iv.length;
	cpu_to_hw_sg(sg);

	/* 2nd seg auth only */
	if (ses->auth_only_len) {
		sg++;
		qm_sg_entry_set64(sg, dpaa_mem_vtop(sym->aead.aad.data));
		sg->length = ses->auth_only_len;
		cpu_to_hw_sg(sg);
	}

	/* 3rd seg */
	sg++;
	qm_sg_entry_set64(sg, rte_pktmbuf_mtophys(mbuf));
	sg->length = mbuf->data_len - sym->aead.data.offset;
	sg->offset = sym->aead.data.offset;

	/* Successive segs */
	mbuf = mbuf->next;
	while (mbuf) {
		cpu_to_hw_sg(sg);
		sg++;
		qm_sg_entry_set64(sg, rte_pktmbuf_mtophys(mbuf));
		sg->length = mbuf->data_len;
		mbuf = mbuf->next;
	}

	if (is_decode(ses)) {
		cpu_to_hw_sg(sg);
		sg++;
		memcpy(ctx->digest, sym->aead.digest.data,
			ses->digest_length);
		qm_sg_entry_set64(sg, dpaa_mem_vtop_ctx(ctx, ctx->digest));
		sg->length = ses->digest_length;
	}
	sg->final = 1;
	cpu_to_hw_sg(sg);

	return cf;
}

static inline struct dpaa_sec_job *
build_cipher_auth_gcm(struct rte_crypto_op *op, dpaa_sec_session *ses)
{
	struct rte_crypto_sym_op *sym = op->sym;
	struct dpaa_sec_job *cf;
	struct dpaa_sec_op_ctx *ctx;
	struct qm_sg_entry *sg;
	uint32_t length = 0;
	rte_iova_t src_start_addr, dst_start_addr;
	uint8_t *IV_ptr = rte_crypto_op_ctod_offset(op, uint8_t *,
			ses->iv.offset);

	src_start_addr = sym->m_src->buf_iova + sym->m_src->data_off;

	if (sym->m_dst)
		dst_start_addr = sym->m_dst->buf_iova + sym->m_dst->data_off;
	else
		dst_start_addr = src_start_addr;

	ctx = dpaa_sec_alloc_ctx(ses);
	if (!ctx)
		return NULL;

	cf = &ctx->job;
	ctx->op = op;

	/* input */
	rte_prefetch0(cf->sg);
	sg = &cf->sg[2];
	qm_sg_entry_set64(&cf->sg[1], dpaa_mem_vtop_ctx(ctx, sg));
	if (is_encode(ses)) {
		qm_sg_entry_set64(sg, dpaa_mem_vtop(IV_ptr));
		sg->length = ses->iv.length;
		length += sg->length;
		cpu_to_hw_sg(sg);

		sg++;
		if (ses->auth_only_len) {
			qm_sg_entry_set64(sg,
					  dpaa_mem_vtop(sym->aead.aad.data));
			sg->length = ses->auth_only_len;
			length += sg->length;
			cpu_to_hw_sg(sg);
			sg++;
		}
		qm_sg_entry_set64(sg, src_start_addr + sym->aead.data.offset);
		sg->length = sym->aead.data.length;
		length += sg->length;
		sg->final = 1;
		cpu_to_hw_sg(sg);
	} else {
		qm_sg_entry_set64(sg, dpaa_mem_vtop(IV_ptr));
		sg->length = ses->iv.length;
		length += sg->length;
		cpu_to_hw_sg(sg);

		sg++;
		if (ses->auth_only_len) {
			qm_sg_entry_set64(sg,
					  dpaa_mem_vtop(sym->aead.aad.data));
			sg->length = ses->auth_only_len;
			length += sg->length;
			cpu_to_hw_sg(sg);
			sg++;
		}
		qm_sg_entry_set64(sg, src_start_addr + sym->aead.data.offset);
		sg->length = sym->aead.data.length;
		length += sg->length;
		cpu_to_hw_sg(sg);

		memcpy(ctx->digest, sym->aead.digest.data,
		       ses->digest_length);
		sg++;

		qm_sg_entry_set64(sg, dpaa_mem_vtop_ctx(ctx, ctx->digest));
		sg->length = ses->digest_length;
		length += sg->length;
		sg->final = 1;
		cpu_to_hw_sg(sg);
	}
	/* input compound frame */
	cf->sg[1].length = length;
	cf->sg[1].extension = 1;
	cf->sg[1].final = 1;
	cpu_to_hw_sg(&cf->sg[1]);

	/* output */
	sg++;
	qm_sg_entry_set64(&cf->sg[0], dpaa_mem_vtop_ctx(ctx, sg));
	qm_sg_entry_set64(sg,
		dst_start_addr + sym->aead.data.offset - ses->auth_only_len);
	sg->length = sym->aead.data.length + ses->auth_only_len;
	length = sg->length;
	if (is_encode(ses)) {
		cpu_to_hw_sg(sg);
		/* set auth output */
		sg++;
		qm_sg_entry_set64(sg, sym->aead.digest.phys_addr);
		sg->length = ses->digest_length;
		length += sg->length;
	}
	sg->final = 1;
	cpu_to_hw_sg(sg);

	/* output compound frame */
	cf->sg[0].length = length;
	cf->sg[0].extension = 1;
	cpu_to_hw_sg(&cf->sg[0]);

	return cf;
}

static inline struct dpaa_sec_job *
build_cipher_auth_sg(struct rte_crypto_op *op, dpaa_sec_session *ses)
{
	struct rte_crypto_sym_op *sym = op->sym;
	struct dpaa_sec_job *cf;
	struct dpaa_sec_op_ctx *ctx;
	struct qm_sg_entry *sg, *out_sg, *in_sg;
	struct rte_mbuf *mbuf;
	uint8_t req_segs;
	uint8_t *IV_ptr = rte_crypto_op_ctod_offset(op, uint8_t *,
			ses->iv.offset);

	if (sym->m_dst) {
		mbuf = sym->m_dst;
		req_segs = mbuf->nb_segs + sym->m_src->nb_segs + 4;
	} else {
		mbuf = sym->m_src;
		req_segs = mbuf->nb_segs * 2 + 4;
	}

	if (req_segs > MAX_SG_ENTRIES) {
		PMD_TX_LOG(ERR, "Cipher-Auth: Max sec segs supported is %d\n",
				MAX_SG_ENTRIES);
		return NULL;
	}

	ctx = dpaa_sec_alloc_ctx(ses);
	if (!ctx)
		return NULL;

	cf = &ctx->job;
	ctx->op = op;

	rte_prefetch0(cf->sg);

	/* output */
	out_sg = &cf->sg[0];
	out_sg->extension = 1;
	if (is_encode(ses))
		out_sg->length = sym->auth.data.length + ses->digest_length;
	else
		out_sg->length = sym->auth.data.length;

	/* output sg entries */
	sg = &cf->sg[2];
	qm_sg_entry_set64(out_sg, dpaa_mem_vtop_ctx(ctx, sg));
	cpu_to_hw_sg(out_sg);

	/* 1st seg */
	qm_sg_entry_set64(sg, rte_pktmbuf_mtophys(mbuf));
	sg->length = mbuf->data_len - sym->auth.data.offset;
	sg->offset = sym->auth.data.offset;

	/* Successive segs */
	mbuf = mbuf->next;
	while (mbuf) {
		cpu_to_hw_sg(sg);
		sg++;
		qm_sg_entry_set64(sg, rte_pktmbuf_mtophys(mbuf));
		sg->length = mbuf->data_len;
		mbuf = mbuf->next;
	}
	sg->length -= ses->digest_length;

	if (is_encode(ses)) {
		cpu_to_hw_sg(sg);
		/* set auth output */
		sg++;
		qm_sg_entry_set64(sg, sym->auth.digest.phys_addr);
		sg->length = ses->digest_length;
	}
	sg->final = 1;
	cpu_to_hw_sg(sg);

	/* input */
	mbuf = sym->m_src;
	in_sg = &cf->sg[1];
	in_sg->extension = 1;
	in_sg->final = 1;
	if (is_encode(ses))
		in_sg->length = ses->iv.length + sym->auth.data.length;
	else
		in_sg->length = ses->iv.length + sym->auth.data.length
						+ ses->digest_length;

	/* input sg entries */
	sg++;
	qm_sg_entry_set64(in_sg, dpaa_mem_vtop_ctx(ctx, sg));
	cpu_to_hw_sg(in_sg);

	/* 1st seg IV */
	qm_sg_entry_set64(sg, dpaa_mem_vtop(IV_ptr));
	sg->length = ses->iv.length;
	cpu_to_hw_sg(sg);

	/* 2nd seg */
	sg++;
	qm_sg_entry_set64(sg, rte_pktmbuf_mtophys(mbuf));
	sg->length = mbuf->data_len - sym->auth.data.offset;
	sg->offset = sym->auth.data.offset;

	/* Successive segs */
	mbuf = mbuf->next;
	while (mbuf) {
		cpu_to_hw_sg(sg);
		sg++;
		qm_sg_entry_set64(sg, rte_pktmbuf_mtophys(mbuf));
		sg->length = mbuf->data_len;
		mbuf = mbuf->next;
	}

	sg->length -= ses->digest_length;
	if (is_decode(ses)) {
		cpu_to_hw_sg(sg);
		sg++;
		memcpy(ctx->digest, sym->auth.digest.data,
			ses->digest_length);
		qm_sg_entry_set64(sg, dpaa_mem_vtop_ctx(ctx, ctx->digest));
		sg->length = ses->digest_length;
	}
	sg->final = 1;
	cpu_to_hw_sg(sg);

	return cf;
}

static inline struct dpaa_sec_job *
build_cipher_auth(struct rte_crypto_op *op, dpaa_sec_session *ses)
{
	struct rte_crypto_sym_op *sym = op->sym;
	struct dpaa_sec_job *cf;
	struct dpaa_sec_op_ctx *ctx;
	struct qm_sg_entry *sg;
	rte_iova_t src_start_addr, dst_start_addr;
	uint32_t length = 0;
	uint8_t *IV_ptr = rte_crypto_op_ctod_offset(op, uint8_t *,
			ses->iv.offset);

	src_start_addr = sym->m_src->buf_iova + sym->m_src->data_off;
	if (sym->m_dst)
		dst_start_addr = sym->m_dst->buf_iova + sym->m_dst->data_off;
	else
		dst_start_addr = src_start_addr;

	ctx = dpaa_sec_alloc_ctx(ses);
	if (!ctx)
		return NULL;

	cf = &ctx->job;
	ctx->op = op;

	/* input */
	rte_prefetch0(cf->sg);
	sg = &cf->sg[2];
	qm_sg_entry_set64(&cf->sg[1], dpaa_mem_vtop_ctx(ctx, sg));
	if (is_encode(ses)) {
		qm_sg_entry_set64(sg, dpaa_mem_vtop(IV_ptr));
		sg->length = ses->iv.length;
		length += sg->length;
		cpu_to_hw_sg(sg);

		sg++;
		qm_sg_entry_set64(sg, src_start_addr + sym->auth.data.offset);
		sg->length = sym->auth.data.length;
		length += sg->length;
		sg->final = 1;
		cpu_to_hw_sg(sg);
	} else {
		qm_sg_entry_set64(sg, dpaa_mem_vtop(IV_ptr));
		sg->length = ses->iv.length;
		length += sg->length;
		cpu_to_hw_sg(sg);

		sg++;

		qm_sg_entry_set64(sg, src_start_addr + sym->auth.data.offset);
		sg->length = sym->auth.data.length;
		length += sg->length;
		cpu_to_hw_sg(sg);

		memcpy(ctx->digest, sym->auth.digest.data,
		       ses->digest_length);
		sg++;

		qm_sg_entry_set64(sg, dpaa_mem_vtop_ctx(ctx, ctx->digest));
		sg->length = ses->digest_length;
		length += sg->length;
		sg->final = 1;
		cpu_to_hw_sg(sg);
	}
	/* input compound frame */
	cf->sg[1].length = length;
	cf->sg[1].extension = 1;
	cf->sg[1].final = 1;
	cpu_to_hw_sg(&cf->sg[1]);

	/* output */
	sg++;
	qm_sg_entry_set64(&cf->sg[0], dpaa_mem_vtop_ctx(ctx, sg));
	qm_sg_entry_set64(sg, dst_start_addr + sym->cipher.data.offset);
	sg->length = sym->cipher.data.length;
	length = sg->length;
	if (is_encode(ses)) {
		cpu_to_hw_sg(sg);
		/* set auth output */
		sg++;
		qm_sg_entry_set64(sg, sym->auth.digest.phys_addr);
		sg->length = ses->digest_length;
		length += sg->length;
	}
	sg->final = 1;
	cpu_to_hw_sg(sg);

	/* output compound frame */
	cf->sg[0].length = length;
	cf->sg[0].extension = 1;
	cpu_to_hw_sg(&cf->sg[0]);

	return cf;
}

static inline struct dpaa_sec_job *
build_proto(struct rte_crypto_op *op, dpaa_sec_session *ses)
{
	struct rte_crypto_sym_op *sym = op->sym;
	struct dpaa_sec_job *cf;
	struct dpaa_sec_op_ctx *ctx;
	struct qm_sg_entry *sg;
	phys_addr_t src_start_addr, dst_start_addr;

	ctx = dpaa_sec_alloc_ctx(ses);
	if (!ctx)
		return NULL;
	cf = &ctx->job;
	ctx->op = op;

	src_start_addr = rte_pktmbuf_mtophys(sym->m_src);

	if (sym->m_dst)
		dst_start_addr = rte_pktmbuf_mtophys(sym->m_dst);
	else
		dst_start_addr = src_start_addr;

	/* input */
	sg = &cf->sg[1];
	qm_sg_entry_set64(sg, src_start_addr);
	sg->length = sym->m_src->pkt_len;
	sg->final = 1;
	cpu_to_hw_sg(sg);

	sym->m_src->packet_type &= ~RTE_PTYPE_L4_MASK;
	/* output */
	sg = &cf->sg[0];
	qm_sg_entry_set64(sg, dst_start_addr);
	sg->length = sym->m_src->buf_len - sym->m_src->data_off;
	cpu_to_hw_sg(sg);

	return cf;
}

static uint16_t
dpaa_sec_enqueue_burst(void *qp, struct rte_crypto_op **ops,
		       uint16_t nb_ops)
{
	/* Function to transmit the frames to given device and queuepair */
	uint32_t loop;
	struct dpaa_sec_qp *dpaa_qp = (struct dpaa_sec_qp *)qp;
	uint16_t num_tx = 0;
	struct qm_fd fds[DPAA_SEC_BURST], *fd;
	uint32_t frames_to_send;
	struct rte_crypto_op *op;
	struct dpaa_sec_job *cf;
	dpaa_sec_session *ses;
	struct dpaa_sec_op_ctx *ctx;
	uint32_t auth_only_len;
	struct qman_fq *inq[DPAA_SEC_BURST];

	while (nb_ops) {
		frames_to_send = (nb_ops > DPAA_SEC_BURST) ?
				DPAA_SEC_BURST : nb_ops;
		for (loop = 0; loop < frames_to_send; loop++) {
			op = *(ops++);
			switch (op->sess_type) {
			case RTE_CRYPTO_OP_WITH_SESSION:
				ses = (dpaa_sec_session *)
					get_session_private_data(
							op->sym->session,
							cryptodev_driver_id);
				break;
			case RTE_CRYPTO_OP_SECURITY_SESSION:
				ses = (dpaa_sec_session *)
					get_sec_session_private_data(
							op->sym->sec_session);
				break;
			default:
				PMD_TX_LOG(ERR,
					"sessionless crypto op not supported");
				frames_to_send = loop;
				nb_ops = loop;
				goto send_pkts;
			}
			if (unlikely(!ses->qp || ses->qp != qp)) {
				PMD_INIT_LOG(DEBUG, "sess->qp - %p qp %p",
						ses->qp, qp);
				if (dpaa_sec_attach_sess_q(qp, ses)) {
					frames_to_send = loop;
					nb_ops = loop;
					goto send_pkts;
				}
			}

			auth_only_len = op->sym->auth.data.length -
						op->sym->cipher.data.length;
			if (rte_pktmbuf_is_contiguous(op->sym->m_src)) {
				if (is_auth_only(ses)) {
					cf = build_auth_only(op, ses);
				} else if (is_cipher_only(ses)) {
					cf = build_cipher_only(op, ses);
				} else if (is_aead(ses)) {
					cf = build_cipher_auth_gcm(op, ses);
					auth_only_len = ses->auth_only_len;
				} else if (is_auth_cipher(ses)) {
					cf = build_cipher_auth(op, ses);
				} else if (is_proto_ipsec(ses)) {
					cf = build_proto(op, ses);
				} else {
					PMD_TX_LOG(ERR, "not supported sec op");
					frames_to_send = loop;
					nb_ops = loop;
					goto send_pkts;
				}
			} else {
				if (is_auth_only(ses)) {
					cf = build_auth_only_sg(op, ses);
				} else if (is_cipher_only(ses)) {
					cf = build_cipher_only_sg(op, ses);
				} else if (is_aead(ses)) {
					cf = build_cipher_auth_gcm_sg(op, ses);
					auth_only_len = ses->auth_only_len;
				} else if (is_auth_cipher(ses)) {
					cf = build_cipher_auth_sg(op, ses);
				} else {
					PMD_TX_LOG(ERR, "not supported sec op");
					frames_to_send = loop;
					nb_ops = loop;
					goto send_pkts;
				}
			}
			if (unlikely(!cf)) {
				frames_to_send = loop;
				nb_ops = loop;
				goto send_pkts;
			}

			fd = &fds[loop];
			inq[loop] = ses->inq;
			fd->opaque_addr = 0;
			fd->cmd = 0;
			ctx = container_of(cf, struct dpaa_sec_op_ctx, job);
			qm_fd_addr_set64(fd, dpaa_mem_vtop_ctx(ctx, cf->sg));
			fd->_format1 = qm_fd_compound;
			fd->length29 = 2 * sizeof(struct qm_sg_entry);
			/* Auth_only_len is set as 0 in descriptor and it is
			 * overwritten here in the fd.cmd which will update
			 * the DPOVRD reg.
			 */
			if (auth_only_len)
				fd->cmd = 0x80000000 | auth_only_len;

		}
send_pkts:
		loop = 0;
		while (loop < frames_to_send) {
			loop += qman_enqueue_multi_fq(&inq[loop], &fds[loop],
					frames_to_send - loop);
		}
		nb_ops -= frames_to_send;
		num_tx += frames_to_send;
	}

	dpaa_qp->tx_pkts += num_tx;
	dpaa_qp->tx_errs += nb_ops - num_tx;

	return num_tx;
}

static uint16_t
dpaa_sec_dequeue_burst(void *qp, struct rte_crypto_op **ops,
		       uint16_t nb_ops)
{
	uint16_t num_rx;
	struct dpaa_sec_qp *dpaa_qp = (struct dpaa_sec_qp *)qp;

	num_rx = dpaa_sec_deq(dpaa_qp, ops, nb_ops);

	dpaa_qp->rx_pkts += num_rx;
	dpaa_qp->rx_errs += nb_ops - num_rx;

	PMD_RX_LOG(DEBUG, "SEC Received %d Packets\n", num_rx);

	return num_rx;
}

/** Release queue pair */
static int
dpaa_sec_queue_pair_release(struct rte_cryptodev *dev,
			    uint16_t qp_id)
{
	struct dpaa_sec_dev_private *internals;
	struct dpaa_sec_qp *qp = NULL;

	PMD_INIT_FUNC_TRACE();

	PMD_INIT_LOG(DEBUG, "dev =%p, queue =%d", dev, qp_id);

	internals = dev->data->dev_private;
	if (qp_id >= internals->max_nb_queue_pairs) {
		PMD_INIT_LOG(ERR, "Max supported qpid %d",
			     internals->max_nb_queue_pairs);
		return -EINVAL;
	}

	qp = &internals->qps[qp_id];
	qp->internals = NULL;
	dev->data->queue_pairs[qp_id] = NULL;

	return 0;
}

/** Setup a queue pair */
static int
dpaa_sec_queue_pair_setup(struct rte_cryptodev *dev, uint16_t qp_id,
		__rte_unused const struct rte_cryptodev_qp_conf *qp_conf,
		__rte_unused int socket_id,
		__rte_unused struct rte_mempool *session_pool)
{
	struct dpaa_sec_dev_private *internals;
	struct dpaa_sec_qp *qp = NULL;

	PMD_INIT_LOG(DEBUG, "dev =%p, queue =%d, conf =%p",
		     dev, qp_id, qp_conf);

	internals = dev->data->dev_private;
	if (qp_id >= internals->max_nb_queue_pairs) {
		PMD_INIT_LOG(ERR, "Max supported qpid %d",
			     internals->max_nb_queue_pairs);
		return -EINVAL;
	}

	qp = &internals->qps[qp_id];
	qp->internals = internals;
	dev->data->queue_pairs[qp_id] = qp;

	return 0;
}

/** Start queue pair */
static int
dpaa_sec_queue_pair_start(__rte_unused struct rte_cryptodev *dev,
			  __rte_unused uint16_t queue_pair_id)
{
	PMD_INIT_FUNC_TRACE();

	return 0;
}

/** Stop queue pair */
static int
dpaa_sec_queue_pair_stop(__rte_unused struct rte_cryptodev *dev,
			 __rte_unused uint16_t queue_pair_id)
{
	PMD_INIT_FUNC_TRACE();

	return 0;
}

/** Return the number of allocated queue pairs */
static uint32_t
dpaa_sec_queue_pair_count(struct rte_cryptodev *dev)
{
	PMD_INIT_FUNC_TRACE();

	return dev->data->nb_queue_pairs;
}

/** Returns the size of session structure */
static unsigned int
dpaa_sec_session_get_size(struct rte_cryptodev *dev __rte_unused)
{
	PMD_INIT_FUNC_TRACE();

	return sizeof(dpaa_sec_session);
}

static int
dpaa_sec_cipher_init(struct rte_cryptodev *dev __rte_unused,
		     struct rte_crypto_sym_xform *xform,
		     dpaa_sec_session *session)
{
	session->cipher_alg = xform->cipher.algo;
	session->iv.length = xform->cipher.iv.length;
	session->iv.offset = xform->cipher.iv.offset;
	session->cipher_key.data = rte_zmalloc(NULL, xform->cipher.key.length,
					       RTE_CACHE_LINE_SIZE);
	if (session->cipher_key.data == NULL && xform->cipher.key.length > 0) {
		PMD_INIT_LOG(ERR, "No Memory for cipher key\n");
		return -ENOMEM;
	}
	session->cipher_key.length = xform->cipher.key.length;

	memcpy(session->cipher_key.data, xform->cipher.key.data,
	       xform->cipher.key.length);
	session->dir = (xform->cipher.op == RTE_CRYPTO_CIPHER_OP_ENCRYPT) ?
			DIR_ENC : DIR_DEC;

	return 0;
}

static int
dpaa_sec_auth_init(struct rte_cryptodev *dev __rte_unused,
		   struct rte_crypto_sym_xform *xform,
		   dpaa_sec_session *session)
{
	session->auth_alg = xform->auth.algo;
	session->auth_key.data = rte_zmalloc(NULL, xform->auth.key.length,
					     RTE_CACHE_LINE_SIZE);
	if (session->auth_key.data == NULL && xform->auth.key.length > 0) {
		PMD_INIT_LOG(ERR, "No Memory for auth key\n");
		return -ENOMEM;
	}
	session->auth_key.length = xform->auth.key.length;
	session->digest_length = xform->auth.digest_length;

	memcpy(session->auth_key.data, xform->auth.key.data,
	       xform->auth.key.length);
	session->dir = (xform->auth.op == RTE_CRYPTO_AUTH_OP_GENERATE) ?
			DIR_ENC : DIR_DEC;

	return 0;
}

static int
dpaa_sec_aead_init(struct rte_cryptodev *dev __rte_unused,
		   struct rte_crypto_sym_xform *xform,
		   dpaa_sec_session *session)
{
	session->aead_alg = xform->aead.algo;
	session->iv.length = xform->aead.iv.length;
	session->iv.offset = xform->aead.iv.offset;
	session->auth_only_len = xform->aead.aad_length;
	session->aead_key.data = rte_zmalloc(NULL, xform->aead.key.length,
					     RTE_CACHE_LINE_SIZE);
	if (session->aead_key.data == NULL && xform->aead.key.length > 0) {
		PMD_INIT_LOG(ERR, "No Memory for aead key\n");
		return -ENOMEM;
	}
	session->aead_key.length = xform->aead.key.length;
	session->digest_length = xform->aead.digest_length;

	memcpy(session->aead_key.data, xform->aead.key.data,
	       xform->aead.key.length);
	session->dir = (xform->aead.op == RTE_CRYPTO_AEAD_OP_ENCRYPT) ?
			DIR_ENC : DIR_DEC;

	return 0;
}

static struct qman_fq *
dpaa_sec_attach_rxq(struct dpaa_sec_dev_private *qi)
{
	unsigned int i;

	for (i = 0; i < qi->max_nb_sessions; i++) {
		if (qi->inq_attach[i] == 0) {
			qi->inq_attach[i] = 1;
			return &qi->inq[i];
		}
	}
	PMD_DRV_LOG(ERR, "All ses session in use %x", qi->max_nb_sessions);

	return NULL;
}

static int
dpaa_sec_detach_rxq(struct dpaa_sec_dev_private *qi, struct qman_fq *fq)
{
	unsigned int i;

	for (i = 0; i < qi->max_nb_sessions; i++) {
		if (&qi->inq[i] == fq) {
			qman_retire_fq(fq, NULL);
			qman_oos_fq(fq);
			qi->inq_attach[i] = 0;
			return 0;
		}
	}
	return -1;
}

static int
dpaa_sec_attach_sess_q(struct dpaa_sec_qp *qp, dpaa_sec_session *sess)
{
	int ret;

	sess->qp = qp;
	ret = dpaa_sec_prep_cdb(sess);
	if (ret) {
		PMD_DRV_LOG(ERR, "Unable to prepare sec cdb");
		return -1;
	}

	ret = dpaa_sec_init_rx(sess->inq, dpaa_mem_vtop(&sess->cdb),
			       qman_fq_fqid(&qp->outq));
	if (ret)
		PMD_DRV_LOG(ERR, "Unable to init sec queue");

	return ret;
}

static int
dpaa_sec_qp_attach_sess(struct rte_cryptodev *dev __rte_unused,
			uint16_t qp_id __rte_unused,
			void *ses __rte_unused)
{
	PMD_INIT_FUNC_TRACE();
	return 0;
}

static int
dpaa_sec_qp_detach_sess(struct rte_cryptodev *dev,
			uint16_t qp_id  __rte_unused,
			void *ses)
{
	dpaa_sec_session *sess = ses;
	struct dpaa_sec_dev_private *qi = dev->data->dev_private;

	PMD_INIT_FUNC_TRACE();

	if (sess->inq)
		dpaa_sec_detach_rxq(qi, sess->inq);
	sess->inq = NULL;

	sess->qp = NULL;

	return 0;
}

static int
dpaa_sec_set_session_parameters(struct rte_cryptodev *dev,
			    struct rte_crypto_sym_xform *xform,	void *sess)
{
	struct dpaa_sec_dev_private *internals = dev->data->dev_private;
	dpaa_sec_session *session = sess;

	PMD_INIT_FUNC_TRACE();

	if (unlikely(sess == NULL)) {
		RTE_LOG(ERR, PMD, "invalid session struct\n");
		return -EINVAL;
	}

	/* Default IV length = 0 */
	session->iv.length = 0;

	/* Cipher Only */
	if (xform->type == RTE_CRYPTO_SYM_XFORM_CIPHER && xform->next == NULL) {
		session->auth_alg = RTE_CRYPTO_AUTH_NULL;
		dpaa_sec_cipher_init(dev, xform, session);

	/* Authentication Only */
	} else if (xform->type == RTE_CRYPTO_SYM_XFORM_AUTH &&
		   xform->next == NULL) {
		session->cipher_alg = RTE_CRYPTO_CIPHER_NULL;
		dpaa_sec_auth_init(dev, xform, session);

	/* Cipher then Authenticate */
	} else if (xform->type == RTE_CRYPTO_SYM_XFORM_CIPHER &&
		   xform->next->type == RTE_CRYPTO_SYM_XFORM_AUTH) {
		if (xform->cipher.op == RTE_CRYPTO_CIPHER_OP_ENCRYPT) {
			dpaa_sec_cipher_init(dev, xform, session);
			dpaa_sec_auth_init(dev, xform->next, session);
		} else {
			PMD_DRV_LOG(ERR, "Not supported: Auth then Cipher");
			return -EINVAL;
		}

	/* Authenticate then Cipher */
	} else if (xform->type == RTE_CRYPTO_SYM_XFORM_AUTH &&
		   xform->next->type == RTE_CRYPTO_SYM_XFORM_CIPHER) {
		if (xform->next->cipher.op == RTE_CRYPTO_CIPHER_OP_DECRYPT) {
			dpaa_sec_auth_init(dev, xform, session);
			dpaa_sec_cipher_init(dev, xform->next, session);
		} else {
			PMD_DRV_LOG(ERR, "Not supported: Auth then Cipher");
			return -EINVAL;
		}

	/* AEAD operation for AES-GCM kind of Algorithms */
	} else if (xform->type == RTE_CRYPTO_SYM_XFORM_AEAD &&
		   xform->next == NULL) {
		dpaa_sec_aead_init(dev, xform, session);

	} else {
		PMD_DRV_LOG(ERR, "Invalid crypto type");
		return -EINVAL;
	}
	session->ctx_pool = internals->ctx_pool;
	session->inq = dpaa_sec_attach_rxq(internals);
	if (session->inq == NULL) {
		PMD_DRV_LOG(ERR, "unable to attach sec queue");
		goto err1;
	}

	return 0;

err1:
	rte_free(session->cipher_key.data);
	rte_free(session->auth_key.data);
	memset(session, 0, sizeof(dpaa_sec_session));

	return -EINVAL;
}

static int
dpaa_sec_session_configure(struct rte_cryptodev *dev,
		struct rte_crypto_sym_xform *xform,
		struct rte_cryptodev_sym_session *sess,
		struct rte_mempool *mempool)
{
	void *sess_private_data;
	int ret;

	PMD_INIT_FUNC_TRACE();

	if (rte_mempool_get(mempool, &sess_private_data)) {
		CDEV_LOG_ERR(
			"Couldn't get object from session mempool");
		return -ENOMEM;
	}

	ret = dpaa_sec_set_session_parameters(dev, xform, sess_private_data);
	if (ret != 0) {
		PMD_DRV_LOG(ERR, "DPAA PMD: failed to configure "
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
dpaa_sec_session_clear(struct rte_cryptodev *dev,
		struct rte_cryptodev_sym_session *sess)
{
	struct dpaa_sec_dev_private *qi = dev->data->dev_private;
	uint8_t index = dev->driver_id;
	void *sess_priv = get_session_private_data(sess, index);

	PMD_INIT_FUNC_TRACE();

	dpaa_sec_session *s = (dpaa_sec_session *)sess_priv;

	if (sess_priv) {
		struct rte_mempool *sess_mp = rte_mempool_from_obj(sess_priv);

		if (s->inq)
			dpaa_sec_detach_rxq(qi, s->inq);
		rte_free(s->cipher_key.data);
		rte_free(s->auth_key.data);
		memset(s, 0, sizeof(dpaa_sec_session));
		set_session_private_data(sess, index, NULL);
		rte_mempool_put(sess_mp, sess_priv);
	}
}

static int
dpaa_sec_set_ipsec_session(__rte_unused struct rte_cryptodev *dev,
			   struct rte_security_session_conf *conf,
			   void *sess)
{
	struct dpaa_sec_dev_private *internals = dev->data->dev_private;
	struct rte_security_ipsec_xform *ipsec_xform = &conf->ipsec;
	struct rte_crypto_auth_xform *auth_xform;
	struct rte_crypto_cipher_xform *cipher_xform;
	dpaa_sec_session *session = (dpaa_sec_session *)sess;

	PMD_INIT_FUNC_TRACE();

	if (ipsec_xform->direction == RTE_SECURITY_IPSEC_SA_DIR_EGRESS) {
		cipher_xform = &conf->crypto_xform->cipher;
		auth_xform = &conf->crypto_xform->next->auth;
	} else {
		auth_xform = &conf->crypto_xform->auth;
		cipher_xform = &conf->crypto_xform->next->cipher;
	}
	session->proto_alg = conf->protocol;
	session->cipher_key.data = rte_zmalloc(NULL,
					       cipher_xform->key.length,
					       RTE_CACHE_LINE_SIZE);
	if (session->cipher_key.data == NULL &&
			cipher_xform->key.length > 0) {
		RTE_LOG(ERR, PMD, "No Memory for cipher key\n");
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
		return -ENOMEM;
	}
	session->auth_key.length = auth_xform->key.length;
	memcpy(session->cipher_key.data, cipher_xform->key.data,
			cipher_xform->key.length);
	memcpy(session->auth_key.data, auth_xform->key.data,
			auth_xform->key.length);

	switch (auth_xform->algo) {
	case RTE_CRYPTO_AUTH_SHA1_HMAC:
		session->auth_alg = RTE_CRYPTO_AUTH_SHA1_HMAC;
		break;
	case RTE_CRYPTO_AUTH_MD5_HMAC:
		session->auth_alg = RTE_CRYPTO_AUTH_MD5_HMAC;
		break;
	case RTE_CRYPTO_AUTH_SHA256_HMAC:
		session->auth_alg = RTE_CRYPTO_AUTH_SHA256_HMAC;
		break;
	case RTE_CRYPTO_AUTH_SHA384_HMAC:
		session->auth_alg = RTE_CRYPTO_AUTH_SHA384_HMAC;
		break;
	case RTE_CRYPTO_AUTH_SHA512_HMAC:
		session->auth_alg = RTE_CRYPTO_AUTH_SHA512_HMAC;
		break;
	case RTE_CRYPTO_AUTH_AES_CMAC:
		session->auth_alg = RTE_CRYPTO_AUTH_AES_CMAC;
		break;
	case RTE_CRYPTO_AUTH_NULL:
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

	switch (cipher_xform->algo) {
	case RTE_CRYPTO_CIPHER_AES_CBC:
		session->cipher_alg = RTE_CRYPTO_CIPHER_AES_CBC;
		break;
	case RTE_CRYPTO_CIPHER_3DES_CBC:
		session->cipher_alg = RTE_CRYPTO_CIPHER_3DES_CBC;
		break;
	case RTE_CRYPTO_CIPHER_AES_CTR:
		session->cipher_alg = RTE_CRYPTO_CIPHER_AES_CTR;
		break;
	case RTE_CRYPTO_CIPHER_NULL:
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
		memset(&session->encap_pdb, 0, sizeof(struct ipsec_encap_pdb) +
				sizeof(session->ip4_hdr));
		session->ip4_hdr.ip_v = IPVERSION;
		session->ip4_hdr.ip_hl = 5;
		session->ip4_hdr.ip_len = rte_cpu_to_be_16(
						sizeof(session->ip4_hdr));
		session->ip4_hdr.ip_tos = ipsec_xform->tunnel.ipv4.dscp;
		session->ip4_hdr.ip_id = 0;
		session->ip4_hdr.ip_off = 0;
		session->ip4_hdr.ip_ttl = ipsec_xform->tunnel.ipv4.ttl;
		session->ip4_hdr.ip_p = (ipsec_xform->proto ==
				RTE_SECURITY_IPSEC_SA_PROTO_ESP) ? IPPROTO_ESP
				: IPPROTO_AH;
		session->ip4_hdr.ip_sum = 0;
		session->ip4_hdr.ip_src = ipsec_xform->tunnel.ipv4.src_ip;
		session->ip4_hdr.ip_dst = ipsec_xform->tunnel.ipv4.dst_ip;
		session->ip4_hdr.ip_sum = calc_chksum((uint16_t *)
						(void *)&session->ip4_hdr,
						sizeof(struct ip));

		session->encap_pdb.options =
			(IPVERSION << PDBNH_ESP_ENCAP_SHIFT) |
			PDBOPTS_ESP_OIHI_PDB_INL |
			PDBOPTS_ESP_IVSRC |
			PDBHMO_ESP_ENCAP_DTTL;
		session->encap_pdb.spi = ipsec_xform->spi;
		session->encap_pdb.ip_hdr_len = sizeof(struct ip);

		session->dir = DIR_ENC;
	} else if (ipsec_xform->direction ==
			RTE_SECURITY_IPSEC_SA_DIR_INGRESS) {
		memset(&session->decap_pdb, 0, sizeof(struct ipsec_decap_pdb));
		session->decap_pdb.options = sizeof(struct ip) << 16;
		session->dir = DIR_DEC;
	} else
		goto out;
	session->ctx_pool = internals->ctx_pool;
	session->inq = dpaa_sec_attach_rxq(internals);
	if (session->inq == NULL) {
		PMD_DRV_LOG(ERR, "unable to attach sec queue");
		goto out;
	}


	return 0;
out:
	rte_free(session->auth_key.data);
	rte_free(session->cipher_key.data);
	memset(session, 0, sizeof(dpaa_sec_session));
	return -1;
}

static int
dpaa_sec_security_session_create(void *dev,
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
		ret = dpaa_sec_set_ipsec_session(cdev, conf,
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
dpaa_sec_security_session_destroy(void *dev __rte_unused,
		struct rte_security_session *sess)
{
	PMD_INIT_FUNC_TRACE();
	void *sess_priv = get_sec_session_private_data(sess);

	dpaa_sec_session *s = (dpaa_sec_session *)sess_priv;

	if (sess_priv) {
		struct rte_mempool *sess_mp = rte_mempool_from_obj(sess_priv);

		rte_free(s->cipher_key.data);
		rte_free(s->auth_key.data);
		memset(sess, 0, sizeof(dpaa_sec_session));
		set_sec_session_private_data(sess, NULL);
		rte_mempool_put(sess_mp, sess_priv);
	}
	return 0;
}


static int
dpaa_sec_dev_configure(struct rte_cryptodev *dev __rte_unused,
		       struct rte_cryptodev_config *config __rte_unused)
{
	PMD_INIT_FUNC_TRACE();

	return 0;
}

static int
dpaa_sec_dev_start(struct rte_cryptodev *dev __rte_unused)
{
	PMD_INIT_FUNC_TRACE();
	return 0;
}

static void
dpaa_sec_dev_stop(struct rte_cryptodev *dev __rte_unused)
{
	PMD_INIT_FUNC_TRACE();
}

static int
dpaa_sec_dev_close(struct rte_cryptodev *dev __rte_unused)
{
	PMD_INIT_FUNC_TRACE();
	return 0;
}

static void
dpaa_sec_dev_infos_get(struct rte_cryptodev *dev,
		       struct rte_cryptodev_info *info)
{
	struct dpaa_sec_dev_private *internals = dev->data->dev_private;

	PMD_INIT_FUNC_TRACE();
	if (info != NULL) {
		info->max_nb_queue_pairs = internals->max_nb_queue_pairs;
		info->feature_flags = dev->feature_flags;
		info->capabilities = dpaa_sec_capabilities;
		info->sym.max_nb_sessions = internals->max_nb_sessions;
		info->sym.max_nb_sessions_per_qp =
			RTE_DPAA_SEC_PMD_MAX_NB_SESSIONS /
			RTE_DPAA_MAX_NB_SEC_QPS;
		info->driver_id = cryptodev_driver_id;
	}
}

static struct rte_cryptodev_ops crypto_ops = {
	.dev_configure	      = dpaa_sec_dev_configure,
	.dev_start	      = dpaa_sec_dev_start,
	.dev_stop	      = dpaa_sec_dev_stop,
	.dev_close	      = dpaa_sec_dev_close,
	.dev_infos_get        = dpaa_sec_dev_infos_get,
	.queue_pair_setup     = dpaa_sec_queue_pair_setup,
	.queue_pair_release   = dpaa_sec_queue_pair_release,
	.queue_pair_start     = dpaa_sec_queue_pair_start,
	.queue_pair_stop      = dpaa_sec_queue_pair_stop,
	.queue_pair_count     = dpaa_sec_queue_pair_count,
	.session_get_size     = dpaa_sec_session_get_size,
	.session_configure    = dpaa_sec_session_configure,
	.session_clear        = dpaa_sec_session_clear,
	.qp_attach_session    = dpaa_sec_qp_attach_sess,
	.qp_detach_session    = dpaa_sec_qp_detach_sess,
};

static const struct rte_security_capability *
dpaa_sec_capabilities_get(void *device __rte_unused)
{
	return dpaa_sec_security_cap;
}

struct rte_security_ops dpaa_sec_security_ops = {
	.session_create = dpaa_sec_security_session_create,
	.session_update = NULL,
	.session_stats_get = NULL,
	.session_destroy = dpaa_sec_security_session_destroy,
	.set_pkt_metadata = NULL,
	.capabilities_get = dpaa_sec_capabilities_get
};

static int
dpaa_sec_uninit(struct rte_cryptodev *dev)
{
	struct dpaa_sec_dev_private *internals = dev->data->dev_private;

	if (dev == NULL)
		return -ENODEV;

	rte_free(dev->security_ctx);

	rte_mempool_free(internals->ctx_pool);
	rte_free(internals);

	PMD_INIT_LOG(INFO, "Closing DPAA_SEC device %s on numa socket %u\n",
		     dev->data->name, rte_socket_id());

	return 0;
}

static int
dpaa_sec_dev_init(struct rte_cryptodev *cryptodev)
{
	struct dpaa_sec_dev_private *internals;
	struct rte_security_ctx *security_instance;
	struct dpaa_sec_qp *qp;
	uint32_t i, flags;
	int ret;
	char str[20];

	PMD_INIT_FUNC_TRACE();

	cryptodev->driver_id = cryptodev_driver_id;
	cryptodev->dev_ops = &crypto_ops;

	cryptodev->enqueue_burst = dpaa_sec_enqueue_burst;
	cryptodev->dequeue_burst = dpaa_sec_dequeue_burst;
	cryptodev->feature_flags = RTE_CRYPTODEV_FF_SYMMETRIC_CRYPTO |
			RTE_CRYPTODEV_FF_HW_ACCELERATED |
			RTE_CRYPTODEV_FF_SYM_OPERATION_CHAINING |
			RTE_CRYPTODEV_FF_SECURITY |
			RTE_CRYPTODEV_FF_MBUF_SCATTER_GATHER;

	internals = cryptodev->data->dev_private;
	internals->max_nb_queue_pairs = RTE_DPAA_MAX_NB_SEC_QPS;
	internals->max_nb_sessions = RTE_DPAA_SEC_PMD_MAX_NB_SESSIONS;

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
	security_instance->ops = &dpaa_sec_security_ops;
	security_instance->sess_cnt = 0;
	cryptodev->security_ctx = security_instance;

	for (i = 0; i < internals->max_nb_queue_pairs; i++) {
		/* init qman fq for queue pair */
		qp = &internals->qps[i];
		ret = dpaa_sec_init_tx(&qp->outq);
		if (ret) {
			PMD_INIT_LOG(ERR, "config tx of queue pair  %d", i);
			goto init_error;
		}
	}

	flags = QMAN_FQ_FLAG_LOCKED | QMAN_FQ_FLAG_DYNAMIC_FQID |
		QMAN_FQ_FLAG_TO_DCPORTAL;
	for (i = 0; i < internals->max_nb_sessions; i++) {
		/* create rx qman fq for sessions*/
		ret = qman_create_fq(0, flags, &internals->inq[i]);
		if (unlikely(ret != 0)) {
			PMD_INIT_LOG(ERR, "sec qman_create_fq failed");
			goto init_error;
		}
	}

	sprintf(str, "ctx_pool_%d", cryptodev->data->dev_id);
	internals->ctx_pool = rte_mempool_create((const char *)str,
			CTX_POOL_NUM_BUFS,
			CTX_POOL_BUF_SIZE,
			CTX_POOL_CACHE_SIZE, 0,
			NULL, NULL, NULL, NULL,
			SOCKET_ID_ANY, 0);
	if (!internals->ctx_pool) {
		RTE_LOG(ERR, PMD, "%s create failed\n", str);
		goto init_error;
	}

	PMD_INIT_LOG(DEBUG, "driver %s: created\n", cryptodev->data->name);
	return 0;

init_error:
	PMD_INIT_LOG(ERR, "driver %s: create failed\n", cryptodev->data->name);

	dpaa_sec_uninit(cryptodev);
	return -EFAULT;
}

static int
cryptodev_dpaa_sec_probe(struct rte_dpaa_driver *dpaa_drv,
				struct rte_dpaa_device *dpaa_dev)
{
	struct rte_cryptodev *cryptodev;
	char cryptodev_name[RTE_CRYPTODEV_NAME_MAX_LEN];

	int retval;

	sprintf(cryptodev_name, "dpaa_sec-%d", dpaa_dev->id.dev_id);

	cryptodev = rte_cryptodev_pmd_allocate(cryptodev_name, rte_socket_id());
	if (cryptodev == NULL)
		return -ENOMEM;

	if (rte_eal_process_type() == RTE_PROC_PRIMARY) {
		cryptodev->data->dev_private = rte_zmalloc_socket(
					"cryptodev private structure",
					sizeof(struct dpaa_sec_dev_private),
					RTE_CACHE_LINE_SIZE,
					rte_socket_id());

		if (cryptodev->data->dev_private == NULL)
			rte_panic("Cannot allocate memzone for private "
					"device data");
	}

	dpaa_dev->crypto_dev = cryptodev;
	cryptodev->device = &dpaa_dev->device;
	cryptodev->device->driver = &dpaa_drv->driver;

	/* init user callbacks */
	TAILQ_INIT(&(cryptodev->link_intr_cbs));

	/* if sec device version is not configured */
	if (!rta_get_sec_era()) {
		const struct device_node *caam_node;

		for_each_compatible_node(caam_node, NULL, "fsl,sec-v4.0") {
			const uint32_t *prop = of_get_property(caam_node,
					"fsl,sec-era",
					NULL);
			if (prop) {
				rta_set_sec_era(
					INTL_SEC_ERA(rte_cpu_to_be_32(*prop)));
				break;
			}
		}
	}

	/* Invoke PMD device initialization function */
	retval = dpaa_sec_dev_init(cryptodev);
	if (retval == 0)
		return 0;

	/* In case of error, cleanup is done */
	if (rte_eal_process_type() == RTE_PROC_PRIMARY)
		rte_free(cryptodev->data->dev_private);

	rte_cryptodev_pmd_release_device(cryptodev);

	return -ENXIO;
}

static int
cryptodev_dpaa_sec_remove(struct rte_dpaa_device *dpaa_dev)
{
	struct rte_cryptodev *cryptodev;
	int ret;

	cryptodev = dpaa_dev->crypto_dev;
	if (cryptodev == NULL)
		return -ENODEV;

	ret = dpaa_sec_uninit(cryptodev);
	if (ret)
		return ret;

	return rte_cryptodev_pmd_destroy(cryptodev);
}

static struct rte_dpaa_driver rte_dpaa_sec_driver = {
	.drv_type = FSL_DPAA_CRYPTO,
	.driver = {
		.name = "DPAA SEC PMD"
	},
	.probe = cryptodev_dpaa_sec_probe,
	.remove = cryptodev_dpaa_sec_remove,
};

static struct cryptodev_driver dpaa_sec_crypto_drv;

RTE_PMD_REGISTER_DPAA(CRYPTODEV_NAME_DPAA_SEC_PMD, rte_dpaa_sec_driver);
RTE_PMD_REGISTER_CRYPTO_DRIVER(dpaa_sec_crypto_drv, rte_dpaa_sec_driver,
		cryptodev_driver_id);
