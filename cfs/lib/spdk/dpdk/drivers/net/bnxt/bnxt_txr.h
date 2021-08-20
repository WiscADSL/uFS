/*-
 *   BSD LICENSE
 *
 *   Copyright(c) Broadcom Limited.
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
 *     * Neither the name of Broadcom Corporation nor the names of its
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

#ifndef _BNXT_TXR_H_
#define _BNXT_TXR_H_

#include <rte_io.h>

#define MAX_TX_RINGS	16
#define BNXT_TX_PUSH_THRESH 92

#define B_TX_DB(db, prod)	rte_write32((DB_KEY_TX | (prod)), db)

struct bnxt_tx_ring_info {
	uint16_t		tx_prod;
	uint16_t		tx_cons;
	void			*tx_doorbell;

	struct tx_bd_long	*tx_desc_ring;
	struct bnxt_sw_tx_bd	*tx_buf_ring;

	rte_iova_t		tx_desc_mapping;

#define BNXT_DEV_STATE_CLOSING	0x1
	uint32_t		dev_state;

	struct bnxt_ring	*tx_ring_struct;
};

struct bnxt_sw_tx_bd {
	struct rte_mbuf		*mbuf; /* mbuf associated with TX descriptor */
	uint8_t			is_gso;
	unsigned short		nr_bds;
};

void bnxt_free_tx_rings(struct bnxt *bp);
int bnxt_init_one_tx_ring(struct bnxt_tx_queue *txq);
int bnxt_init_tx_ring_struct(struct bnxt_tx_queue *txq, unsigned int socket_id);
uint16_t bnxt_xmit_pkts(void *tx_queue, struct rte_mbuf **tx_pkts,
			       uint16_t nb_pkts);
int bnxt_tx_queue_start(struct rte_eth_dev *dev, uint16_t tx_queue_id);
int bnxt_tx_queue_stop(struct rte_eth_dev *dev, uint16_t tx_queue_id);

#define PKT_TX_OIP_IIP_TCP_UDP_CKSUM	(PKT_TX_TCP_CKSUM | PKT_TX_UDP_CKSUM | \
					PKT_TX_IP_CKSUM | PKT_TX_OUTER_IP_CKSUM)
#define PKT_TX_IIP_TCP_UDP_CKSUM	(PKT_TX_TCP_CKSUM | PKT_TX_UDP_CKSUM | \
					PKT_TX_IP_CKSUM)
#define PKT_TX_OIP_TCP_UDP_CKSUM	(PKT_TX_TCP_CKSUM | PKT_TX_UDP_CKSUM | \
					PKT_TX_OUTER_IP_CKSUM)
#define PKT_TX_OIP_IIP_CKSUM		(PKT_TX_IP_CKSUM |	\
					 PKT_TX_OUTER_IP_CKSUM)
#define PKT_TX_TCP_UDP_CKSUM		(PKT_TX_TCP_CKSUM | PKT_TX_UDP_CKSUM)


#define TX_BD_FLG_TIP_IP_TCP_UDP_CHKSUM	(TX_BD_LONG_LFLAGS_TCP_UDP_CHKSUM | \
					TX_BD_LONG_LFLAGS_T_IP_CHKSUM | \
					TX_BD_LONG_LFLAGS_IP_CHKSUM)
#define TX_BD_FLG_IP_TCP_UDP_CHKSUM	(TX_BD_LONG_LFLAGS_TCP_UDP_CHKSUM | \
					TX_BD_LONG_LFLAGS_IP_CHKSUM)
#define TX_BD_FLG_TIP_IP_CHKSUM		(TX_BD_LONG_LFLAGS_T_IP_CHKSUM | \
					TX_BD_LONG_LFLAGS_IP_CHKSUM)
#define TX_BD_FLG_TIP_TCP_UDP_CHKSUM	(TX_BD_LONG_LFLAGS_TCP_UDP_CHKSUM | \
					TX_BD_LONG_LFLAGS_T_IP_CHKSUM)

#endif
