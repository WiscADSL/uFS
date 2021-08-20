/*
 * Copyright (c) 2014, 2015 Netronome Systems, Inc.
 * All rights reserved.
 *
 * Small portions derived from code Copyright(c) 2010-2015 Intel Corporation.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *  this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *  notice, this list of conditions and the following disclaimer in the
 *  documentation and/or other materials provided with the distribution
 *
 * 3. Neither the name of the copyright holder nor the names of its
 *  contributors may be used to endorse or promote products derived from this
 *  software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * vim:shiftwidth=8:noexpandtab
 *
 * @file dpdk/pmd/nfp_net.c
 *
 * Netronome vNIC DPDK Poll-Mode Driver: Main entry point
 */

#include <rte_byteorder.h>
#include <rte_common.h>
#include <rte_log.h>
#include <rte_debug.h>
#include <rte_ethdev_driver.h>
#include <rte_ethdev_pci.h>
#include <rte_dev.h>
#include <rte_ether.h>
#include <rte_malloc.h>
#include <rte_memzone.h>
#include <rte_mempool.h>
#include <rte_version.h>
#include <rte_string_fns.h>
#include <rte_alarm.h>
#include <rte_spinlock.h>

#include "nfp_nfpu.h"
#include "nfp_net_pmd.h"
#include "nfp_net_logs.h"
#include "nfp_net_ctrl.h"

/* Prototypes */
static void nfp_net_close(struct rte_eth_dev *dev);
static int nfp_net_configure(struct rte_eth_dev *dev);
static void nfp_net_dev_interrupt_handler(void *param);
static void nfp_net_dev_interrupt_delayed_handler(void *param);
static int nfp_net_dev_mtu_set(struct rte_eth_dev *dev, uint16_t mtu);
static void nfp_net_infos_get(struct rte_eth_dev *dev,
			      struct rte_eth_dev_info *dev_info);
static int nfp_net_init(struct rte_eth_dev *eth_dev);
static int nfp_net_link_update(struct rte_eth_dev *dev, int wait_to_complete);
static void nfp_net_promisc_enable(struct rte_eth_dev *dev);
static void nfp_net_promisc_disable(struct rte_eth_dev *dev);
static int nfp_net_rx_fill_freelist(struct nfp_net_rxq *rxq);
static uint32_t nfp_net_rx_queue_count(struct rte_eth_dev *dev,
				       uint16_t queue_idx);
static uint16_t nfp_net_recv_pkts(void *rx_queue, struct rte_mbuf **rx_pkts,
				  uint16_t nb_pkts);
static void nfp_net_rx_queue_release(void *rxq);
static int nfp_net_rx_queue_setup(struct rte_eth_dev *dev, uint16_t queue_idx,
				  uint16_t nb_desc, unsigned int socket_id,
				  const struct rte_eth_rxconf *rx_conf,
				  struct rte_mempool *mp);
static int nfp_net_tx_free_bufs(struct nfp_net_txq *txq);
static void nfp_net_tx_queue_release(void *txq);
static int nfp_net_tx_queue_setup(struct rte_eth_dev *dev, uint16_t queue_idx,
				  uint16_t nb_desc, unsigned int socket_id,
				  const struct rte_eth_txconf *tx_conf);
static int nfp_net_start(struct rte_eth_dev *dev);
static int nfp_net_stats_get(struct rte_eth_dev *dev,
			      struct rte_eth_stats *stats);
static void nfp_net_stats_reset(struct rte_eth_dev *dev);
static void nfp_net_stop(struct rte_eth_dev *dev);
static uint16_t nfp_net_xmit_pkts(void *tx_queue, struct rte_mbuf **tx_pkts,
				  uint16_t nb_pkts);

static int nfp_net_rss_config_default(struct rte_eth_dev *dev);
static int nfp_net_rss_hash_update(struct rte_eth_dev *dev,
				   struct rte_eth_rss_conf *rss_conf);
static int nfp_net_rss_reta_write(struct rte_eth_dev *dev,
		    struct rte_eth_rss_reta_entry64 *reta_conf,
		    uint16_t reta_size);
static int nfp_net_rss_hash_write(struct rte_eth_dev *dev,
			struct rte_eth_rss_conf *rss_conf);

/*
 * The offset of the queue controller queues in the PCIe Target. These
 * happen to be at the same offset on the NFP6000 and the NFP3200 so
 * we use a single macro here.
 */
#define NFP_PCIE_QUEUE(_q)	(0x800 * ((_q) & 0xff))

/* Maximum value which can be added to a queue with one transaction */
#define NFP_QCP_MAX_ADD	0x7f

#define RTE_MBUF_DMA_ADDR_DEFAULT(mb) \
	(uint64_t)((mb)->buf_iova + RTE_PKTMBUF_HEADROOM)

/* nfp_qcp_ptr - Read or Write Pointer of a queue */
enum nfp_qcp_ptr {
	NFP_QCP_READ_PTR = 0,
	NFP_QCP_WRITE_PTR
};

/*
 * nfp_qcp_ptr_add - Add the value to the selected pointer of a queue
 * @q: Base address for queue structure
 * @ptr: Add to the Read or Write pointer
 * @val: Value to add to the queue pointer
 *
 * If @val is greater than @NFP_QCP_MAX_ADD multiple writes are performed.
 */
static inline void
nfp_qcp_ptr_add(uint8_t *q, enum nfp_qcp_ptr ptr, uint32_t val)
{
	uint32_t off;

	if (ptr == NFP_QCP_READ_PTR)
		off = NFP_QCP_QUEUE_ADD_RPTR;
	else
		off = NFP_QCP_QUEUE_ADD_WPTR;

	while (val > NFP_QCP_MAX_ADD) {
		nn_writel(rte_cpu_to_le_32(NFP_QCP_MAX_ADD), q + off);
		val -= NFP_QCP_MAX_ADD;
	}

	nn_writel(rte_cpu_to_le_32(val), q + off);
}

/*
 * nfp_qcp_read - Read the current Read/Write pointer value for a queue
 * @q:  Base address for queue structure
 * @ptr: Read or Write pointer
 */
static inline uint32_t
nfp_qcp_read(uint8_t *q, enum nfp_qcp_ptr ptr)
{
	uint32_t off;
	uint32_t val;

	if (ptr == NFP_QCP_READ_PTR)
		off = NFP_QCP_QUEUE_STS_LO;
	else
		off = NFP_QCP_QUEUE_STS_HI;

	val = rte_cpu_to_le_32(nn_readl(q + off));

	if (ptr == NFP_QCP_READ_PTR)
		return val & NFP_QCP_QUEUE_STS_LO_READPTR_mask;
	else
		return val & NFP_QCP_QUEUE_STS_HI_WRITEPTR_mask;
}

/*
 * Functions to read/write from/to Config BAR
 * Performs any endian conversion necessary.
 */
static inline uint8_t
nn_cfg_readb(struct nfp_net_hw *hw, int off)
{
	return nn_readb(hw->ctrl_bar + off);
}

static inline void
nn_cfg_writeb(struct nfp_net_hw *hw, int off, uint8_t val)
{
	nn_writeb(val, hw->ctrl_bar + off);
}

static inline uint32_t
nn_cfg_readl(struct nfp_net_hw *hw, int off)
{
	return rte_le_to_cpu_32(nn_readl(hw->ctrl_bar + off));
}

static inline void
nn_cfg_writel(struct nfp_net_hw *hw, int off, uint32_t val)
{
	nn_writel(rte_cpu_to_le_32(val), hw->ctrl_bar + off);
}

static inline uint64_t
nn_cfg_readq(struct nfp_net_hw *hw, int off)
{
	return rte_le_to_cpu_64(nn_readq(hw->ctrl_bar + off));
}

static inline void
nn_cfg_writeq(struct nfp_net_hw *hw, int off, uint64_t val)
{
	nn_writeq(rte_cpu_to_le_64(val), hw->ctrl_bar + off);
}

/*
 * Atomically reads link status information from global structure rte_eth_dev.
 *
 * @param dev
 *   - Pointer to the structure rte_eth_dev to read from.
 *   - Pointer to the buffer to be saved with the link status.
 *
 * @return
 *   - On success, zero.
 *   - On failure, negative value.
 */
static inline int
nfp_net_dev_atomic_read_link_status(struct rte_eth_dev *dev,
				    struct rte_eth_link *link)
{
	struct rte_eth_link *dst = link;
	struct rte_eth_link *src = &dev->data->dev_link;

	if (rte_atomic64_cmpset((uint64_t *)dst, *(uint64_t *)dst,
				*(uint64_t *)src) == 0)
		return -1;

	return 0;
}

/*
 * Atomically writes the link status information into global
 * structure rte_eth_dev.
 *
 * @param dev
 *   - Pointer to the structure rte_eth_dev to read from.
 *   - Pointer to the buffer to be saved with the link status.
 *
 * @return
 *   - On success, zero.
 *   - On failure, negative value.
 */
static inline int
nfp_net_dev_atomic_write_link_status(struct rte_eth_dev *dev,
				     struct rte_eth_link *link)
{
	struct rte_eth_link *dst = &dev->data->dev_link;
	struct rte_eth_link *src = link;

	if (rte_atomic64_cmpset((uint64_t *)dst, *(uint64_t *)dst,
				*(uint64_t *)src) == 0)
		return -1;

	return 0;
}

static void
nfp_net_rx_queue_release_mbufs(struct nfp_net_rxq *rxq)
{
	unsigned i;

	if (rxq->rxbufs == NULL)
		return;

	for (i = 0; i < rxq->rx_count; i++) {
		if (rxq->rxbufs[i].mbuf) {
			rte_pktmbuf_free_seg(rxq->rxbufs[i].mbuf);
			rxq->rxbufs[i].mbuf = NULL;
		}
	}
}

static void
nfp_net_rx_queue_release(void *rx_queue)
{
	struct nfp_net_rxq *rxq = rx_queue;

	if (rxq) {
		nfp_net_rx_queue_release_mbufs(rxq);
		rte_free(rxq->rxbufs);
		rte_free(rxq);
	}
}

static void
nfp_net_reset_rx_queue(struct nfp_net_rxq *rxq)
{
	nfp_net_rx_queue_release_mbufs(rxq);
	rxq->rd_p = 0;
	rxq->nb_rx_hold = 0;
}

static void
nfp_net_tx_queue_release_mbufs(struct nfp_net_txq *txq)
{
	unsigned i;

	if (txq->txbufs == NULL)
		return;

	for (i = 0; i < txq->tx_count; i++) {
		if (txq->txbufs[i].mbuf) {
			rte_pktmbuf_free(txq->txbufs[i].mbuf);
			txq->txbufs[i].mbuf = NULL;
		}
	}
}

static void
nfp_net_tx_queue_release(void *tx_queue)
{
	struct nfp_net_txq *txq = tx_queue;

	if (txq) {
		nfp_net_tx_queue_release_mbufs(txq);
		rte_free(txq->txbufs);
		rte_free(txq);
	}
}

static void
nfp_net_reset_tx_queue(struct nfp_net_txq *txq)
{
	nfp_net_tx_queue_release_mbufs(txq);
	txq->wr_p = 0;
	txq->rd_p = 0;
}

static int
__nfp_net_reconfig(struct nfp_net_hw *hw, uint32_t update)
{
	int cnt;
	uint32_t new;
	struct timespec wait;

	PMD_DRV_LOG(DEBUG, "Writing to the configuration queue (%p)...\n",
		    hw->qcp_cfg);

	if (hw->qcp_cfg == NULL)
		rte_panic("Bad configuration queue pointer\n");

	nfp_qcp_ptr_add(hw->qcp_cfg, NFP_QCP_WRITE_PTR, 1);

	wait.tv_sec = 0;
	wait.tv_nsec = 1000000;

	PMD_DRV_LOG(DEBUG, "Polling for update ack...\n");

	/* Poll update field, waiting for NFP to ack the config */
	for (cnt = 0; ; cnt++) {
		new = nn_cfg_readl(hw, NFP_NET_CFG_UPDATE);
		if (new == 0)
			break;
		if (new & NFP_NET_CFG_UPDATE_ERR) {
			PMD_INIT_LOG(ERR, "Reconfig error: 0x%08x", new);
			return -1;
		}
		if (cnt >= NFP_NET_POLL_TIMEOUT) {
			PMD_INIT_LOG(ERR, "Reconfig timeout for 0x%08x after"
					  " %dms", update, cnt);
			rte_panic("Exiting\n");
		}
		nanosleep(&wait, 0); /* waiting for a 1ms */
	}
	PMD_DRV_LOG(DEBUG, "Ack DONE\n");
	return 0;
}

/*
 * Reconfigure the NIC
 * @nn:    device to reconfigure
 * @ctrl:    The value for the ctrl field in the BAR config
 * @update:  The value for the update field in the BAR config
 *
 * Write the update word to the BAR and ping the reconfig queue. Then poll
 * until the firmware has acknowledged the update by zeroing the update word.
 */
static int
nfp_net_reconfig(struct nfp_net_hw *hw, uint32_t ctrl, uint32_t update)
{
	uint32_t err;

	PMD_DRV_LOG(DEBUG, "nfp_net_reconfig: ctrl=%08x update=%08x\n",
		    ctrl, update);

	rte_spinlock_lock(&hw->reconfig_lock);

	nn_cfg_writel(hw, NFP_NET_CFG_CTRL, ctrl);
	nn_cfg_writel(hw, NFP_NET_CFG_UPDATE, update);

	rte_wmb();

	err = __nfp_net_reconfig(hw, update);

	rte_spinlock_unlock(&hw->reconfig_lock);

	if (!err)
		return 0;

	/*
	 * Reconfig errors imply situations where they can be handled.
	 * Otherwise, rte_panic is called inside __nfp_net_reconfig
	 */
	PMD_INIT_LOG(ERR, "Error nfp_net reconfig for ctrl: %x update: %x",
		     ctrl, update);
	return -EIO;
}

/*
 * Configure an Ethernet device. This function must be invoked first
 * before any other function in the Ethernet API. This function can
 * also be re-invoked when a device is in the stopped state.
 */
static int
nfp_net_configure(struct rte_eth_dev *dev)
{
	struct rte_eth_conf *dev_conf;
	struct rte_eth_rxmode *rxmode;
	struct rte_eth_txmode *txmode;
	uint32_t new_ctrl = 0;
	uint32_t update = 0;
	struct nfp_net_hw *hw;

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);

	/*
	 * A DPDK app sends info about how many queues to use and how
	 * those queues need to be configured. This is used by the
	 * DPDK core and it makes sure no more queues than those
	 * advertised by the driver are requested. This function is
	 * called after that internal process
	 */

	PMD_INIT_LOG(DEBUG, "Configure");

	dev_conf = &dev->data->dev_conf;
	rxmode = &dev_conf->rxmode;
	txmode = &dev_conf->txmode;

	/* Checking TX mode */
	if (txmode->mq_mode) {
		PMD_INIT_LOG(INFO, "TX mq_mode DCB and VMDq not supported");
		return -EINVAL;
	}

	/* Checking RX mode */
	if (rxmode->mq_mode & ETH_MQ_RX_RSS) {
		if (hw->cap & NFP_NET_CFG_CTRL_RSS) {
			update = NFP_NET_CFG_UPDATE_RSS;
			new_ctrl = NFP_NET_CFG_CTRL_RSS;
		} else {
			PMD_INIT_LOG(INFO, "RSS not supported");
			return -EINVAL;
		}
	}

	if (rxmode->split_hdr_size) {
		PMD_INIT_LOG(INFO, "rxmode does not support split header");
		return -EINVAL;
	}

	if (rxmode->hw_ip_checksum) {
		if (hw->cap & NFP_NET_CFG_CTRL_RXCSUM) {
			new_ctrl |= NFP_NET_CFG_CTRL_RXCSUM;
		} else {
			PMD_INIT_LOG(INFO, "RXCSUM not supported");
			return -EINVAL;
		}
	}

	if (rxmode->hw_vlan_filter) {
		PMD_INIT_LOG(INFO, "VLAN filter not supported");
		return -EINVAL;
	}

	if (rxmode->hw_vlan_strip) {
		if (hw->cap & NFP_NET_CFG_CTRL_RXVLAN) {
			new_ctrl |= NFP_NET_CFG_CTRL_RXVLAN;
		} else {
			PMD_INIT_LOG(INFO, "hw vlan strip not supported");
			return -EINVAL;
		}
	}

	if (rxmode->hw_vlan_extend) {
		PMD_INIT_LOG(INFO, "VLAN extended not supported");
		return -EINVAL;
	}

	if (rxmode->jumbo_frame)
		hw->mtu = rxmode->max_rx_pkt_len;

	if (!rxmode->hw_strip_crc)
		PMD_INIT_LOG(INFO, "HW does strip CRC and it is not configurable");

	if (rxmode->enable_scatter) {
		PMD_INIT_LOG(INFO, "Scatter not supported");
		return -EINVAL;
	}

	/* If next capabilities are supported, configure them by default */

	/* VLAN insertion */
	if (hw->cap & NFP_NET_CFG_CTRL_TXVLAN)
		new_ctrl |= NFP_NET_CFG_CTRL_TXVLAN;

	/* L2 broadcast */
	if (hw->cap & NFP_NET_CFG_CTRL_L2BC)
		new_ctrl |= NFP_NET_CFG_CTRL_L2BC;

	/* L2 multicast */
	if (hw->cap & NFP_NET_CFG_CTRL_L2MC)
		new_ctrl |= NFP_NET_CFG_CTRL_L2MC;

	/* TX checksum offload */
	if (hw->cap & NFP_NET_CFG_CTRL_TXCSUM)
		new_ctrl |= NFP_NET_CFG_CTRL_TXCSUM;

	/* LSO offload */
	if (hw->cap & NFP_NET_CFG_CTRL_LSO)
		new_ctrl |= NFP_NET_CFG_CTRL_LSO;

	/* RX gather */
	if (hw->cap & NFP_NET_CFG_CTRL_GATHER)
		new_ctrl |= NFP_NET_CFG_CTRL_GATHER;

	if (!new_ctrl)
		return 0;

	update |= NFP_NET_CFG_UPDATE_GEN;

	nn_cfg_writel(hw, NFP_NET_CFG_CTRL, new_ctrl);
	if (nfp_net_reconfig(hw, new_ctrl, update) < 0)
		return -EIO;

	hw->ctrl = new_ctrl;

	return 0;
}

static void
nfp_net_enable_queues(struct rte_eth_dev *dev)
{
	struct nfp_net_hw *hw;
	uint64_t enabled_queues = 0;
	int i;

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);

	/* Enabling the required TX queues in the device */
	for (i = 0; i < dev->data->nb_tx_queues; i++)
		enabled_queues |= (1 << i);

	nn_cfg_writeq(hw, NFP_NET_CFG_TXRS_ENABLE, enabled_queues);

	enabled_queues = 0;

	/* Enabling the required RX queues in the device */
	for (i = 0; i < dev->data->nb_rx_queues; i++)
		enabled_queues |= (1 << i);

	nn_cfg_writeq(hw, NFP_NET_CFG_RXRS_ENABLE, enabled_queues);
}

static void
nfp_net_disable_queues(struct rte_eth_dev *dev)
{
	struct nfp_net_hw *hw;
	uint32_t new_ctrl, update = 0;

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);

	nn_cfg_writeq(hw, NFP_NET_CFG_TXRS_ENABLE, 0);
	nn_cfg_writeq(hw, NFP_NET_CFG_RXRS_ENABLE, 0);

	new_ctrl = hw->ctrl & ~NFP_NET_CFG_CTRL_ENABLE;
	update = NFP_NET_CFG_UPDATE_GEN | NFP_NET_CFG_UPDATE_RING |
		 NFP_NET_CFG_UPDATE_MSIX;

	if (hw->cap & NFP_NET_CFG_CTRL_RINGCFG)
		new_ctrl &= ~NFP_NET_CFG_CTRL_RINGCFG;

	/* If an error when reconfig we avoid to change hw state */
	if (nfp_net_reconfig(hw, new_ctrl, update) < 0)
		return;

	hw->ctrl = new_ctrl;
}

static int
nfp_net_rx_freelist_setup(struct rte_eth_dev *dev)
{
	int i;

	for (i = 0; i < dev->data->nb_rx_queues; i++) {
		if (nfp_net_rx_fill_freelist(dev->data->rx_queues[i]) < 0)
			return -1;
	}
	return 0;
}

static void
nfp_net_params_setup(struct nfp_net_hw *hw)
{
	nn_cfg_writel(hw, NFP_NET_CFG_MTU, hw->mtu);
	nn_cfg_writel(hw, NFP_NET_CFG_FLBUFSZ, hw->flbufsz);
}

static void
nfp_net_cfg_queue_setup(struct nfp_net_hw *hw)
{
	hw->qcp_cfg = hw->tx_bar + NFP_QCP_QUEUE_ADDR_SZ;
}

#define ETH_ADDR_LEN	6

static void
nfp_eth_copy_mac_reverse(uint8_t *dst, const uint8_t *src)
{
	int i;

	for (i = 0; i < ETH_ADDR_LEN; i++)
		dst[ETH_ADDR_LEN - i - 1] = src[i];
}

static int
nfp_net_pf_read_mac(struct nfp_net_hw *hw, int port)
{
	union eth_table_entry *entry;
	int idx, i;

	idx = port;
	entry = hw->eth_table;

	/* Reading NFP ethernet table obtained before */
	for (i = 0; i < NSP_ETH_MAX_COUNT; i++) {
		if (!(entry->port & NSP_ETH_PORT_LANES_MASK)) {
			/* port not in use */
			entry++;
			continue;
		}
		if (idx == 0)
			break;
		idx--;
		entry++;
	}

	if (i == NSP_ETH_MAX_COUNT)
		return -EINVAL;

	/*
	 * hw points to port0 private data. We need hw now pointing to
	 * right port.
	 */
	hw += port;
	nfp_eth_copy_mac_reverse((uint8_t *)&hw->mac_addr,
				 (uint8_t *)&entry->mac_addr);

	return 0;
}

static void
nfp_net_vf_read_mac(struct nfp_net_hw *hw)
{
	uint32_t tmp;

	tmp = rte_be_to_cpu_32(nn_cfg_readl(hw, NFP_NET_CFG_MACADDR));
	memcpy(&hw->mac_addr[0], &tmp, sizeof(struct ether_addr));

	tmp = rte_be_to_cpu_32(nn_cfg_readl(hw, NFP_NET_CFG_MACADDR + 4));
	memcpy(&hw->mac_addr[4], &tmp, 2);
}

static void
nfp_net_write_mac(struct nfp_net_hw *hw, uint8_t *mac)
{
	uint32_t mac0 = *(uint32_t *)mac;
	uint16_t mac1;

	nn_writel(rte_cpu_to_be_32(mac0), hw->ctrl_bar + NFP_NET_CFG_MACADDR);

	mac += 4;
	mac1 = *(uint16_t *)mac;
	nn_writew(rte_cpu_to_be_16(mac1),
		  hw->ctrl_bar + NFP_NET_CFG_MACADDR + 6);
}

static int
nfp_configure_rx_interrupt(struct rte_eth_dev *dev,
			   struct rte_intr_handle *intr_handle)
{
	struct nfp_net_hw *hw;
	int i;

	if (!intr_handle->intr_vec) {
		intr_handle->intr_vec =
			rte_zmalloc("intr_vec",
				    dev->data->nb_rx_queues * sizeof(int), 0);
		if (!intr_handle->intr_vec) {
			PMD_INIT_LOG(ERR, "Failed to allocate %d rx_queues"
				     " intr_vec", dev->data->nb_rx_queues);
			return -ENOMEM;
		}
	}

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);

	if (intr_handle->type == RTE_INTR_HANDLE_UIO) {
		PMD_INIT_LOG(INFO, "VF: enabling RX interrupt with UIO");
		/* UIO just supports one queue and no LSC*/
		nn_cfg_writeb(hw, NFP_NET_CFG_RXR_VEC(0), 0);
		intr_handle->intr_vec[0] = 0;
	} else {
		PMD_INIT_LOG(INFO, "VF: enabling RX interrupt with VFIO");
		for (i = 0; i < dev->data->nb_rx_queues; i++) {
			/*
			 * The first msix vector is reserved for non
			 * efd interrupts
			*/
			nn_cfg_writeb(hw, NFP_NET_CFG_RXR_VEC(i), i + 1);
			intr_handle->intr_vec[i] = i + 1;
			PMD_INIT_LOG(DEBUG, "intr_vec[%d]= %d\n", i,
					    intr_handle->intr_vec[i]);
		}
	}

	/* Avoiding TX interrupts */
	hw->ctrl |= NFP_NET_CFG_CTRL_MSIX_TX_OFF;
	return 0;
}

static int
nfp_net_start(struct rte_eth_dev *dev)
{
	struct rte_pci_device *pci_dev = RTE_ETH_DEV_TO_PCI(dev);
	struct rte_intr_handle *intr_handle = &pci_dev->intr_handle;
	struct rte_eth_conf *dev_conf;
	struct rte_eth_rxmode *rxmode;
	uint32_t new_ctrl, update = 0;
	struct nfp_net_hw *hw;
	uint32_t intr_vector;
	int ret;

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);

	PMD_INIT_LOG(DEBUG, "Start");

	/* Disabling queues just in case... */
	nfp_net_disable_queues(dev);

	/* Writing configuration parameters in the device */
	nfp_net_params_setup(hw);

	/* Enabling the required queues in the device */
	nfp_net_enable_queues(dev);

	/* check and configure queue intr-vector mapping */
	if (dev->data->dev_conf.intr_conf.rxq != 0) {
		if (hw->pf_multiport_enabled) {
			PMD_INIT_LOG(ERR, "PMD rx interrupt is not supported "
					  "with NFP multiport PF");
				return -EINVAL;
		}
		if (intr_handle->type == RTE_INTR_HANDLE_UIO) {
			/*
			 * Better not to share LSC with RX interrupts.
			 * Unregistering LSC interrupt handler
			 */
			rte_intr_callback_unregister(&pci_dev->intr_handle,
				nfp_net_dev_interrupt_handler, (void *)dev);

			if (dev->data->nb_rx_queues > 1) {
				PMD_INIT_LOG(ERR, "PMD rx interrupt only "
					     "supports 1 queue with UIO");
				return -EIO;
			}
		}
		intr_vector = dev->data->nb_rx_queues;
		if (rte_intr_efd_enable(intr_handle, intr_vector))
			return -1;

		nfp_configure_rx_interrupt(dev, intr_handle);
		update = NFP_NET_CFG_UPDATE_MSIX;
	}

	rte_intr_enable(intr_handle);

	dev_conf = &dev->data->dev_conf;
	rxmode = &dev_conf->rxmode;

	/* Checking RX mode */
	if (rxmode->mq_mode & ETH_MQ_RX_RSS) {
		if (hw->cap & NFP_NET_CFG_CTRL_RSS) {
			if (!nfp_net_rss_config_default(dev))
				update |= NFP_NET_CFG_UPDATE_RSS;
		} else {
			PMD_INIT_LOG(INFO, "RSS not supported");
			return -EINVAL;
		}
	}
	/* Enable device */
	new_ctrl = hw->ctrl | NFP_NET_CFG_CTRL_ENABLE;

	update |= NFP_NET_CFG_UPDATE_GEN | NFP_NET_CFG_UPDATE_RING;

	if (hw->cap & NFP_NET_CFG_CTRL_RINGCFG)
		new_ctrl |= NFP_NET_CFG_CTRL_RINGCFG;

	nn_cfg_writel(hw, NFP_NET_CFG_CTRL, new_ctrl);
	if (nfp_net_reconfig(hw, new_ctrl, update) < 0)
		return -EIO;

	/*
	 * Allocating rte mbuffs for configured rx queues.
	 * This requires queues being enabled before
	 */
	if (nfp_net_rx_freelist_setup(dev) < 0) {
		ret = -ENOMEM;
		goto error;
	}

	if (hw->is_pf)
		/* Configure the physical port up */
		nfp_nsp_eth_config(hw->nspu_desc, hw->pf_port_idx, 1);

	hw->ctrl = new_ctrl;

	return 0;

error:
	/*
	 * An error returned by this function should mean the app
	 * exiting and then the system releasing all the memory
	 * allocated even memory coming from hugepages.
	 *
	 * The device could be enabled at this point with some queues
	 * ready for getting packets. This is true if the call to
	 * nfp_net_rx_freelist_setup() succeeds for some queues but
	 * fails for subsequent queues.
	 *
	 * This should make the app exiting but better if we tell the
	 * device first.
	 */
	nfp_net_disable_queues(dev);

	return ret;
}

/* Stop device: disable rx and tx functions to allow for reconfiguring. */
static void
nfp_net_stop(struct rte_eth_dev *dev)
{
	int i;
	struct nfp_net_hw *hw;

	PMD_INIT_LOG(DEBUG, "Stop");

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);

	nfp_net_disable_queues(dev);

	/* Clear queues */
	for (i = 0; i < dev->data->nb_tx_queues; i++) {
		nfp_net_reset_tx_queue(
			(struct nfp_net_txq *)dev->data->tx_queues[i]);
	}

	for (i = 0; i < dev->data->nb_rx_queues; i++) {
		nfp_net_reset_rx_queue(
			(struct nfp_net_rxq *)dev->data->rx_queues[i]);
	}

	if (hw->is_pf)
		/* Configure the physical port down */
		nfp_nsp_eth_config(hw->nspu_desc, hw->pf_port_idx, 0);
}

/* Reset and stop device. The device can not be restarted. */
static void
nfp_net_close(struct rte_eth_dev *dev)
{
	struct nfp_net_hw *hw;
	struct rte_pci_device *pci_dev;
	int i;

	PMD_INIT_LOG(DEBUG, "Close");

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);
	pci_dev = RTE_ETH_DEV_TO_PCI(dev);

	/*
	 * We assume that the DPDK application is stopping all the
	 * threads/queues before calling the device close function.
	 */

	nfp_net_disable_queues(dev);

	/* Clear queues */
	for (i = 0; i < dev->data->nb_tx_queues; i++) {
		nfp_net_reset_tx_queue(
			(struct nfp_net_txq *)dev->data->tx_queues[i]);
	}

	for (i = 0; i < dev->data->nb_rx_queues; i++) {
		nfp_net_reset_rx_queue(
			(struct nfp_net_rxq *)dev->data->rx_queues[i]);
	}

	rte_intr_disable(&pci_dev->intr_handle);
	nn_cfg_writeb(hw, NFP_NET_CFG_LSC, 0xff);

	/* unregister callback func from eal lib */
	rte_intr_callback_unregister(&pci_dev->intr_handle,
				     nfp_net_dev_interrupt_handler,
				     (void *)dev);

	/*
	 * The ixgbe PMD driver disables the pcie master on the
	 * device. The i40e does not...
	 */
}

static void
nfp_net_promisc_enable(struct rte_eth_dev *dev)
{
	uint32_t new_ctrl, update = 0;
	struct nfp_net_hw *hw;

	PMD_DRV_LOG(DEBUG, "Promiscuous mode enable\n");

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);

	if (!(hw->cap & NFP_NET_CFG_CTRL_PROMISC)) {
		PMD_INIT_LOG(INFO, "Promiscuous mode not supported");
		return;
	}

	if (hw->ctrl & NFP_NET_CFG_CTRL_PROMISC) {
		PMD_DRV_LOG(INFO, "Promiscuous mode already enabled\n");
		return;
	}

	new_ctrl = hw->ctrl | NFP_NET_CFG_CTRL_PROMISC;
	update = NFP_NET_CFG_UPDATE_GEN;

	/*
	 * DPDK sets promiscuous mode on just after this call assuming
	 * it can not fail ...
	 */
	if (nfp_net_reconfig(hw, new_ctrl, update) < 0)
		return;

	hw->ctrl = new_ctrl;
}

static void
nfp_net_promisc_disable(struct rte_eth_dev *dev)
{
	uint32_t new_ctrl, update = 0;
	struct nfp_net_hw *hw;

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);

	if ((hw->ctrl & NFP_NET_CFG_CTRL_PROMISC) == 0) {
		PMD_DRV_LOG(INFO, "Promiscuous mode already disabled\n");
		return;
	}

	new_ctrl = hw->ctrl & ~NFP_NET_CFG_CTRL_PROMISC;
	update = NFP_NET_CFG_UPDATE_GEN;

	/*
	 * DPDK sets promiscuous mode off just before this call
	 * assuming it can not fail ...
	 */
	if (nfp_net_reconfig(hw, new_ctrl, update) < 0)
		return;

	hw->ctrl = new_ctrl;
}

/*
 * return 0 means link status changed, -1 means not changed
 *
 * Wait to complete is needed as it can take up to 9 seconds to get the Link
 * status.
 */
static int
nfp_net_link_update(struct rte_eth_dev *dev, __rte_unused int wait_to_complete)
{
	struct nfp_net_hw *hw;
	struct rte_eth_link link, old;
	uint32_t nn_link_status;

	static const uint32_t ls_to_ethtool[] = {
		[NFP_NET_CFG_STS_LINK_RATE_UNSUPPORTED] = ETH_SPEED_NUM_NONE,
		[NFP_NET_CFG_STS_LINK_RATE_UNKNOWN]     = ETH_SPEED_NUM_NONE,
		[NFP_NET_CFG_STS_LINK_RATE_1G]          = ETH_SPEED_NUM_1G,
		[NFP_NET_CFG_STS_LINK_RATE_10G]         = ETH_SPEED_NUM_10G,
		[NFP_NET_CFG_STS_LINK_RATE_25G]         = ETH_SPEED_NUM_25G,
		[NFP_NET_CFG_STS_LINK_RATE_40G]         = ETH_SPEED_NUM_40G,
		[NFP_NET_CFG_STS_LINK_RATE_50G]         = ETH_SPEED_NUM_50G,
		[NFP_NET_CFG_STS_LINK_RATE_100G]        = ETH_SPEED_NUM_100G,
	};

	PMD_DRV_LOG(DEBUG, "Link update\n");

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);

	memset(&old, 0, sizeof(old));
	nfp_net_dev_atomic_read_link_status(dev, &old);

	nn_link_status = nn_cfg_readl(hw, NFP_NET_CFG_STS);

	memset(&link, 0, sizeof(struct rte_eth_link));

	if (nn_link_status & NFP_NET_CFG_STS_LINK)
		link.link_status = ETH_LINK_UP;

	link.link_duplex = ETH_LINK_FULL_DUPLEX;

	nn_link_status = (nn_link_status >> NFP_NET_CFG_STS_LINK_RATE_SHIFT) &
			 NFP_NET_CFG_STS_LINK_RATE_MASK;

	if (nn_link_status >= RTE_DIM(ls_to_ethtool))
		link.link_speed = ETH_SPEED_NUM_NONE;
	else
		link.link_speed = ls_to_ethtool[nn_link_status];

	if (old.link_status != link.link_status) {
		nfp_net_dev_atomic_write_link_status(dev, &link);
		if (link.link_status)
			PMD_DRV_LOG(INFO, "NIC Link is Up\n");
		else
			PMD_DRV_LOG(INFO, "NIC Link is Down\n");
		return 0;
	}

	return -1;
}

static int
nfp_net_stats_get(struct rte_eth_dev *dev, struct rte_eth_stats *stats)
{
	int i;
	struct nfp_net_hw *hw;
	struct rte_eth_stats nfp_dev_stats;

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);

	/* RTE_ETHDEV_QUEUE_STAT_CNTRS default value is 16 */

	memset(&nfp_dev_stats, 0, sizeof(nfp_dev_stats));

	/* reading per RX ring stats */
	for (i = 0; i < dev->data->nb_rx_queues; i++) {
		if (i == RTE_ETHDEV_QUEUE_STAT_CNTRS)
			break;

		nfp_dev_stats.q_ipackets[i] =
			nn_cfg_readq(hw, NFP_NET_CFG_RXR_STATS(i));

		nfp_dev_stats.q_ipackets[i] -=
			hw->eth_stats_base.q_ipackets[i];

		nfp_dev_stats.q_ibytes[i] =
			nn_cfg_readq(hw, NFP_NET_CFG_RXR_STATS(i) + 0x8);

		nfp_dev_stats.q_ibytes[i] -=
			hw->eth_stats_base.q_ibytes[i];
	}

	/* reading per TX ring stats */
	for (i = 0; i < dev->data->nb_tx_queues; i++) {
		if (i == RTE_ETHDEV_QUEUE_STAT_CNTRS)
			break;

		nfp_dev_stats.q_opackets[i] =
			nn_cfg_readq(hw, NFP_NET_CFG_TXR_STATS(i));

		nfp_dev_stats.q_opackets[i] -=
			hw->eth_stats_base.q_opackets[i];

		nfp_dev_stats.q_obytes[i] =
			nn_cfg_readq(hw, NFP_NET_CFG_TXR_STATS(i) + 0x8);

		nfp_dev_stats.q_obytes[i] -=
			hw->eth_stats_base.q_obytes[i];
	}

	nfp_dev_stats.ipackets =
		nn_cfg_readq(hw, NFP_NET_CFG_STATS_RX_FRAMES);

	nfp_dev_stats.ipackets -= hw->eth_stats_base.ipackets;

	nfp_dev_stats.ibytes =
		nn_cfg_readq(hw, NFP_NET_CFG_STATS_RX_OCTETS);

	nfp_dev_stats.ibytes -= hw->eth_stats_base.ibytes;

	nfp_dev_stats.opackets =
		nn_cfg_readq(hw, NFP_NET_CFG_STATS_TX_FRAMES);

	nfp_dev_stats.opackets -= hw->eth_stats_base.opackets;

	nfp_dev_stats.obytes =
		nn_cfg_readq(hw, NFP_NET_CFG_STATS_TX_OCTETS);

	nfp_dev_stats.obytes -= hw->eth_stats_base.obytes;

	/* reading general device stats */
	nfp_dev_stats.ierrors =
		nn_cfg_readq(hw, NFP_NET_CFG_STATS_RX_ERRORS);

	nfp_dev_stats.ierrors -= hw->eth_stats_base.ierrors;

	nfp_dev_stats.oerrors =
		nn_cfg_readq(hw, NFP_NET_CFG_STATS_TX_ERRORS);

	nfp_dev_stats.oerrors -= hw->eth_stats_base.oerrors;

	/* RX ring mbuf allocation failures */
	nfp_dev_stats.rx_nombuf = dev->data->rx_mbuf_alloc_failed;

	nfp_dev_stats.imissed =
		nn_cfg_readq(hw, NFP_NET_CFG_STATS_RX_DISCARDS);

	nfp_dev_stats.imissed -= hw->eth_stats_base.imissed;

	if (stats) {
		memcpy(stats, &nfp_dev_stats, sizeof(*stats));
		return 0;
	}
	return -EINVAL;
}

static void
nfp_net_stats_reset(struct rte_eth_dev *dev)
{
	int i;
	struct nfp_net_hw *hw;

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);

	/*
	 * hw->eth_stats_base records the per counter starting point.
	 * Lets update it now
	 */

	/* reading per RX ring stats */
	for (i = 0; i < dev->data->nb_rx_queues; i++) {
		if (i == RTE_ETHDEV_QUEUE_STAT_CNTRS)
			break;

		hw->eth_stats_base.q_ipackets[i] =
			nn_cfg_readq(hw, NFP_NET_CFG_RXR_STATS(i));

		hw->eth_stats_base.q_ibytes[i] =
			nn_cfg_readq(hw, NFP_NET_CFG_RXR_STATS(i) + 0x8);
	}

	/* reading per TX ring stats */
	for (i = 0; i < dev->data->nb_tx_queues; i++) {
		if (i == RTE_ETHDEV_QUEUE_STAT_CNTRS)
			break;

		hw->eth_stats_base.q_opackets[i] =
			nn_cfg_readq(hw, NFP_NET_CFG_TXR_STATS(i));

		hw->eth_stats_base.q_obytes[i] =
			nn_cfg_readq(hw, NFP_NET_CFG_TXR_STATS(i) + 0x8);
	}

	hw->eth_stats_base.ipackets =
		nn_cfg_readq(hw, NFP_NET_CFG_STATS_RX_FRAMES);

	hw->eth_stats_base.ibytes =
		nn_cfg_readq(hw, NFP_NET_CFG_STATS_RX_OCTETS);

	hw->eth_stats_base.opackets =
		nn_cfg_readq(hw, NFP_NET_CFG_STATS_TX_FRAMES);

	hw->eth_stats_base.obytes =
		nn_cfg_readq(hw, NFP_NET_CFG_STATS_TX_OCTETS);

	/* reading general device stats */
	hw->eth_stats_base.ierrors =
		nn_cfg_readq(hw, NFP_NET_CFG_STATS_RX_ERRORS);

	hw->eth_stats_base.oerrors =
		nn_cfg_readq(hw, NFP_NET_CFG_STATS_TX_ERRORS);

	/* RX ring mbuf allocation failures */
	dev->data->rx_mbuf_alloc_failed = 0;

	hw->eth_stats_base.imissed =
		nn_cfg_readq(hw, NFP_NET_CFG_STATS_RX_DISCARDS);
}

static void
nfp_net_infos_get(struct rte_eth_dev *dev, struct rte_eth_dev_info *dev_info)
{
	struct nfp_net_hw *hw;

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);

	dev_info->pci_dev = RTE_ETH_DEV_TO_PCI(dev);
	dev_info->max_rx_queues = (uint16_t)hw->max_rx_queues;
	dev_info->max_tx_queues = (uint16_t)hw->max_tx_queues;
	dev_info->min_rx_bufsize = ETHER_MIN_MTU;
	dev_info->max_rx_pktlen = hw->max_mtu;
	/* Next should change when PF support is implemented */
	dev_info->max_mac_addrs = 1;

	if (hw->cap & NFP_NET_CFG_CTRL_RXVLAN)
		dev_info->rx_offload_capa = DEV_RX_OFFLOAD_VLAN_STRIP;

	if (hw->cap & NFP_NET_CFG_CTRL_RXCSUM)
		dev_info->rx_offload_capa |= DEV_RX_OFFLOAD_IPV4_CKSUM |
					     DEV_RX_OFFLOAD_UDP_CKSUM |
					     DEV_RX_OFFLOAD_TCP_CKSUM;

	if (hw->cap & NFP_NET_CFG_CTRL_TXVLAN)
		dev_info->tx_offload_capa = DEV_TX_OFFLOAD_VLAN_INSERT;

	if (hw->cap & NFP_NET_CFG_CTRL_TXCSUM)
		dev_info->tx_offload_capa |= DEV_TX_OFFLOAD_IPV4_CKSUM |
					     DEV_TX_OFFLOAD_UDP_CKSUM |
					     DEV_TX_OFFLOAD_TCP_CKSUM;

	dev_info->default_rxconf = (struct rte_eth_rxconf) {
		.rx_thresh = {
			.pthresh = DEFAULT_RX_PTHRESH,
			.hthresh = DEFAULT_RX_HTHRESH,
			.wthresh = DEFAULT_RX_WTHRESH,
		},
		.rx_free_thresh = DEFAULT_RX_FREE_THRESH,
		.rx_drop_en = 0,
	};

	dev_info->default_txconf = (struct rte_eth_txconf) {
		.tx_thresh = {
			.pthresh = DEFAULT_TX_PTHRESH,
			.hthresh = DEFAULT_TX_HTHRESH,
			.wthresh = DEFAULT_TX_WTHRESH,
		},
		.tx_free_thresh = DEFAULT_TX_FREE_THRESH,
		.tx_rs_thresh = DEFAULT_TX_RSBIT_THRESH,
		.txq_flags = ETH_TXQ_FLAGS_NOMULTSEGS |
			     ETH_TXQ_FLAGS_NOOFFLOADS,
	};

	dev_info->flow_type_rss_offloads = ETH_RSS_NONFRAG_IPV4_TCP |
					   ETH_RSS_NONFRAG_IPV4_UDP |
					   ETH_RSS_NONFRAG_IPV6_TCP |
					   ETH_RSS_NONFRAG_IPV6_UDP;

	dev_info->reta_size = NFP_NET_CFG_RSS_ITBL_SZ;
	dev_info->hash_key_size = NFP_NET_CFG_RSS_KEY_SZ;

	dev_info->speed_capa = ETH_SPEED_NUM_1G | ETH_LINK_SPEED_10G |
			       ETH_SPEED_NUM_25G | ETH_SPEED_NUM_40G |
			       ETH_SPEED_NUM_50G | ETH_LINK_SPEED_100G;

	if (hw->cap & NFP_NET_CFG_CTRL_LSO)
		dev_info->tx_offload_capa |= DEV_TX_OFFLOAD_TCP_TSO;
}

static const uint32_t *
nfp_net_supported_ptypes_get(struct rte_eth_dev *dev)
{
	static const uint32_t ptypes[] = {
		/* refers to nfp_net_set_hash() */
		RTE_PTYPE_INNER_L3_IPV4,
		RTE_PTYPE_INNER_L3_IPV6,
		RTE_PTYPE_INNER_L3_IPV6_EXT,
		RTE_PTYPE_INNER_L4_MASK,
		RTE_PTYPE_UNKNOWN
	};

	if (dev->rx_pkt_burst == nfp_net_recv_pkts)
		return ptypes;
	return NULL;
}

static uint32_t
nfp_net_rx_queue_count(struct rte_eth_dev *dev, uint16_t queue_idx)
{
	struct nfp_net_rxq *rxq;
	struct nfp_net_rx_desc *rxds;
	uint32_t idx;
	uint32_t count;

	rxq = (struct nfp_net_rxq *)dev->data->rx_queues[queue_idx];

	idx = rxq->rd_p;

	count = 0;

	/*
	 * Other PMDs are just checking the DD bit in intervals of 4
	 * descriptors and counting all four if the first has the DD
	 * bit on. Of course, this is not accurate but can be good for
	 * performance. But ideally that should be done in descriptors
	 * chunks belonging to the same cache line
	 */

	while (count < rxq->rx_count) {
		rxds = &rxq->rxds[idx];
		if ((rxds->rxd.meta_len_dd & PCIE_DESC_RX_DD) == 0)
			break;

		count++;
		idx++;

		/* Wrapping? */
		if ((idx) == rxq->rx_count)
			idx = 0;
	}

	return count;
}

static int
nfp_rx_queue_intr_enable(struct rte_eth_dev *dev, uint16_t queue_id)
{
	struct rte_pci_device *pci_dev;
	struct nfp_net_hw *hw;
	int base = 0;

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);
	pci_dev = RTE_ETH_DEV_TO_PCI(dev);

	if (pci_dev->intr_handle.type != RTE_INTR_HANDLE_UIO)
		base = 1;

	/* Make sure all updates are written before un-masking */
	rte_wmb();
	nn_cfg_writeb(hw, NFP_NET_CFG_ICR(base + queue_id),
		      NFP_NET_CFG_ICR_UNMASKED);
	return 0;
}

static int
nfp_rx_queue_intr_disable(struct rte_eth_dev *dev, uint16_t queue_id)
{
	struct rte_pci_device *pci_dev;
	struct nfp_net_hw *hw;
	int base = 0;

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);
	pci_dev = RTE_ETH_DEV_TO_PCI(dev);

	if (pci_dev->intr_handle.type != RTE_INTR_HANDLE_UIO)
		base = 1;

	/* Make sure all updates are written before un-masking */
	rte_wmb();
	nn_cfg_writeb(hw, NFP_NET_CFG_ICR(base + queue_id), 0x1);
	return 0;
}

static void
nfp_net_dev_link_status_print(struct rte_eth_dev *dev)
{
	struct rte_pci_device *pci_dev = RTE_ETH_DEV_TO_PCI(dev);
	struct rte_eth_link link;

	memset(&link, 0, sizeof(link));
	nfp_net_dev_atomic_read_link_status(dev, &link);
	if (link.link_status)
		RTE_LOG(INFO, PMD, "Port %d: Link Up - speed %u Mbps - %s\n",
			dev->data->port_id, link.link_speed,
			link.link_duplex == ETH_LINK_FULL_DUPLEX
			? "full-duplex" : "half-duplex");
	else
		RTE_LOG(INFO, PMD, " Port %d: Link Down\n",
			dev->data->port_id);

	RTE_LOG(INFO, PMD, "PCI Address: %04d:%02d:%02d:%d\n",
		pci_dev->addr.domain, pci_dev->addr.bus,
		pci_dev->addr.devid, pci_dev->addr.function);
}

/* Interrupt configuration and handling */

/*
 * nfp_net_irq_unmask - Unmask an interrupt
 *
 * If MSI-X auto-masking is enabled clear the mask bit, otherwise
 * clear the ICR for the entry.
 */
static void
nfp_net_irq_unmask(struct rte_eth_dev *dev)
{
	struct nfp_net_hw *hw;
	struct rte_pci_device *pci_dev;

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);
	pci_dev = RTE_ETH_DEV_TO_PCI(dev);

	if (hw->ctrl & NFP_NET_CFG_CTRL_MSIXAUTO) {
		/* If MSI-X auto-masking is used, clear the entry */
		rte_wmb();
		rte_intr_enable(&pci_dev->intr_handle);
	} else {
		/* Make sure all updates are written before un-masking */
		rte_wmb();
		nn_cfg_writeb(hw, NFP_NET_CFG_ICR(NFP_NET_IRQ_LSC_IDX),
			      NFP_NET_CFG_ICR_UNMASKED);
	}
}

static void
nfp_net_dev_interrupt_handler(void *param)
{
	int64_t timeout;
	struct rte_eth_link link;
	struct rte_eth_dev *dev = (struct rte_eth_dev *)param;

	PMD_DRV_LOG(DEBUG, "We got a LSC interrupt!!!\n");

	/* get the link status */
	memset(&link, 0, sizeof(link));
	nfp_net_dev_atomic_read_link_status(dev, &link);

	nfp_net_link_update(dev, 0);

	/* likely to up */
	if (!link.link_status) {
		/* handle it 1 sec later, wait it being stable */
		timeout = NFP_NET_LINK_UP_CHECK_TIMEOUT;
		/* likely to down */
	} else {
		/* handle it 4 sec later, wait it being stable */
		timeout = NFP_NET_LINK_DOWN_CHECK_TIMEOUT;
	}

	if (rte_eal_alarm_set(timeout * 1000,
			      nfp_net_dev_interrupt_delayed_handler,
			      (void *)dev) < 0) {
		RTE_LOG(ERR, PMD, "Error setting alarm");
		/* Unmasking */
		nfp_net_irq_unmask(dev);
	}
}

/*
 * Interrupt handler which shall be registered for alarm callback for delayed
 * handling specific interrupt to wait for the stable nic state. As the NIC
 * interrupt state is not stable for nfp after link is just down, it needs
 * to wait 4 seconds to get the stable status.
 *
 * @param handle   Pointer to interrupt handle.
 * @param param    The address of parameter (struct rte_eth_dev *)
 *
 * @return  void
 */
static void
nfp_net_dev_interrupt_delayed_handler(void *param)
{
	struct rte_eth_dev *dev = (struct rte_eth_dev *)param;

	nfp_net_link_update(dev, 0);
	_rte_eth_dev_callback_process(dev, RTE_ETH_EVENT_INTR_LSC, NULL);

	nfp_net_dev_link_status_print(dev);

	/* Unmasking */
	nfp_net_irq_unmask(dev);
}

static int
nfp_net_dev_mtu_set(struct rte_eth_dev *dev, uint16_t mtu)
{
	struct nfp_net_hw *hw;

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);

	/* check that mtu is within the allowed range */
	if ((mtu < ETHER_MIN_MTU) || ((uint32_t)mtu > hw->max_mtu))
		return -EINVAL;

	/* mtu setting is forbidden if port is started */
	if (dev->data->dev_started) {
		PMD_DRV_LOG(ERR, "port %d must be stopped before configuration",
			    dev->data->port_id);
		return -EBUSY;
	}

	/* switch to jumbo mode if needed */
	if ((uint32_t)mtu > ETHER_MAX_LEN)
		dev->data->dev_conf.rxmode.jumbo_frame = 1;
	else
		dev->data->dev_conf.rxmode.jumbo_frame = 0;

	/* update max frame size */
	dev->data->dev_conf.rxmode.max_rx_pkt_len = (uint32_t)mtu;

	/* writing to configuration space */
	nn_cfg_writel(hw, NFP_NET_CFG_MTU, (uint32_t)mtu);

	hw->mtu = mtu;

	return 0;
}

static int
nfp_net_rx_queue_setup(struct rte_eth_dev *dev,
		       uint16_t queue_idx, uint16_t nb_desc,
		       unsigned int socket_id,
		       const struct rte_eth_rxconf *rx_conf,
		       struct rte_mempool *mp)
{
	const struct rte_memzone *tz;
	struct nfp_net_rxq *rxq;
	struct nfp_net_hw *hw;

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);

	PMD_INIT_FUNC_TRACE();

	/* Validating number of descriptors */
	if (((nb_desc * sizeof(struct nfp_net_rx_desc)) % 128) != 0 ||
	    (nb_desc > NFP_NET_MAX_RX_DESC) ||
	    (nb_desc < NFP_NET_MIN_RX_DESC)) {
		RTE_LOG(ERR, PMD, "Wrong nb_desc value\n");
		return -EINVAL;
	}

	/*
	 * Free memory prior to re-allocation if needed. This is the case after
	 * calling nfp_net_stop
	 */
	if (dev->data->rx_queues[queue_idx]) {
		nfp_net_rx_queue_release(dev->data->rx_queues[queue_idx]);
		dev->data->rx_queues[queue_idx] = NULL;
	}

	/* Allocating rx queue data structure */
	rxq = rte_zmalloc_socket("ethdev RX queue", sizeof(struct nfp_net_rxq),
				 RTE_CACHE_LINE_SIZE, socket_id);
	if (rxq == NULL)
		return -ENOMEM;

	/* Hw queues mapping based on firmware confifguration */
	rxq->qidx = queue_idx;
	rxq->fl_qcidx = queue_idx * hw->stride_rx;
	rxq->rx_qcidx = rxq->fl_qcidx + (hw->stride_rx - 1);
	rxq->qcp_fl = hw->rx_bar + NFP_QCP_QUEUE_OFF(rxq->fl_qcidx);
	rxq->qcp_rx = hw->rx_bar + NFP_QCP_QUEUE_OFF(rxq->rx_qcidx);

	/*
	 * Tracking mbuf size for detecting a potential mbuf overflow due to
	 * RX offset
	 */
	rxq->mem_pool = mp;
	rxq->mbuf_size = rxq->mem_pool->elt_size;
	rxq->mbuf_size -= (sizeof(struct rte_mbuf) + RTE_PKTMBUF_HEADROOM);
	hw->flbufsz = rxq->mbuf_size;

	rxq->rx_count = nb_desc;
	rxq->port_id = dev->data->port_id;
	rxq->rx_free_thresh = rx_conf->rx_free_thresh;
	rxq->crc_len = (uint8_t) ((dev->data->dev_conf.rxmode.hw_strip_crc) ? 0
				  : ETHER_CRC_LEN);
	rxq->drop_en = rx_conf->rx_drop_en;

	/*
	 * Allocate RX ring hardware descriptors. A memzone large enough to
	 * handle the maximum ring size is allocated in order to allow for
	 * resizing in later calls to the queue setup function.
	 */
	tz = rte_eth_dma_zone_reserve(dev, "rx_ring", queue_idx,
				   sizeof(struct nfp_net_rx_desc) *
				   NFP_NET_MAX_RX_DESC, NFP_MEMZONE_ALIGN,
				   socket_id);

	if (tz == NULL) {
		RTE_LOG(ERR, PMD, "Error allocatig rx dma\n");
		nfp_net_rx_queue_release(rxq);
		return -ENOMEM;
	}

	/* Saving physical and virtual addresses for the RX ring */
	rxq->dma = (uint64_t)tz->iova;
	rxq->rxds = (struct nfp_net_rx_desc *)tz->addr;

	/* mbuf pointers array for referencing mbufs linked to RX descriptors */
	rxq->rxbufs = rte_zmalloc_socket("rxq->rxbufs",
					 sizeof(*rxq->rxbufs) * nb_desc,
					 RTE_CACHE_LINE_SIZE, socket_id);
	if (rxq->rxbufs == NULL) {
		nfp_net_rx_queue_release(rxq);
		return -ENOMEM;
	}

	PMD_RX_LOG(DEBUG, "rxbufs=%p hw_ring=%p dma_addr=0x%" PRIx64 "\n",
		   rxq->rxbufs, rxq->rxds, (unsigned long int)rxq->dma);

	nfp_net_reset_rx_queue(rxq);

	dev->data->rx_queues[queue_idx] = rxq;
	rxq->hw = hw;

	/*
	 * Telling the HW about the physical address of the RX ring and number
	 * of descriptors in log2 format
	 */
	nn_cfg_writeq(hw, NFP_NET_CFG_RXR_ADDR(queue_idx), rxq->dma);
	nn_cfg_writeb(hw, NFP_NET_CFG_RXR_SZ(queue_idx), rte_log2_u32(nb_desc));

	return 0;
}

static int
nfp_net_rx_fill_freelist(struct nfp_net_rxq *rxq)
{
	struct nfp_net_rx_buff *rxe = rxq->rxbufs;
	uint64_t dma_addr;
	unsigned i;

	PMD_RX_LOG(DEBUG, "nfp_net_rx_fill_freelist for %u descriptors\n",
		   rxq->rx_count);

	for (i = 0; i < rxq->rx_count; i++) {
		struct nfp_net_rx_desc *rxd;
		struct rte_mbuf *mbuf = rte_pktmbuf_alloc(rxq->mem_pool);

		if (mbuf == NULL) {
			RTE_LOG(ERR, PMD, "RX mbuf alloc failed queue_id=%u\n",
				(unsigned)rxq->qidx);
			return -ENOMEM;
		}

		dma_addr = rte_cpu_to_le_64(RTE_MBUF_DMA_ADDR_DEFAULT(mbuf));

		rxd = &rxq->rxds[i];
		rxd->fld.dd = 0;
		rxd->fld.dma_addr_hi = (dma_addr >> 32) & 0xff;
		rxd->fld.dma_addr_lo = dma_addr & 0xffffffff;
		rxe[i].mbuf = mbuf;
		PMD_RX_LOG(DEBUG, "[%d]: %" PRIx64 "\n", i, dma_addr);
	}

	/* Make sure all writes are flushed before telling the hardware */
	rte_wmb();

	/* Not advertising the whole ring as the firmware gets confused if so */
	PMD_RX_LOG(DEBUG, "Increment FL write pointer in %u\n",
		   rxq->rx_count - 1);

	nfp_qcp_ptr_add(rxq->qcp_fl, NFP_QCP_WRITE_PTR, rxq->rx_count - 1);

	return 0;
}

static int
nfp_net_tx_queue_setup(struct rte_eth_dev *dev, uint16_t queue_idx,
		       uint16_t nb_desc, unsigned int socket_id,
		       const struct rte_eth_txconf *tx_conf)
{
	const struct rte_memzone *tz;
	struct nfp_net_txq *txq;
	uint16_t tx_free_thresh;
	struct nfp_net_hw *hw;

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);

	PMD_INIT_FUNC_TRACE();

	/* Validating number of descriptors */
	if (((nb_desc * sizeof(struct nfp_net_tx_desc)) % 128) != 0 ||
	    (nb_desc > NFP_NET_MAX_TX_DESC) ||
	    (nb_desc < NFP_NET_MIN_TX_DESC)) {
		RTE_LOG(ERR, PMD, "Wrong nb_desc value\n");
		return -EINVAL;
	}

	tx_free_thresh = (uint16_t)((tx_conf->tx_free_thresh) ?
				    tx_conf->tx_free_thresh :
				    DEFAULT_TX_FREE_THRESH);

	if (tx_free_thresh > (nb_desc)) {
		RTE_LOG(ERR, PMD,
			"tx_free_thresh must be less than the number of TX "
			"descriptors. (tx_free_thresh=%u port=%d "
			"queue=%d)\n", (unsigned int)tx_free_thresh,
			dev->data->port_id, (int)queue_idx);
		return -(EINVAL);
	}

	/*
	 * Free memory prior to re-allocation if needed. This is the case after
	 * calling nfp_net_stop
	 */
	if (dev->data->tx_queues[queue_idx]) {
		PMD_TX_LOG(DEBUG, "Freeing memory prior to re-allocation %d\n",
			   queue_idx);
		nfp_net_tx_queue_release(dev->data->tx_queues[queue_idx]);
		dev->data->tx_queues[queue_idx] = NULL;
	}

	/* Allocating tx queue data structure */
	txq = rte_zmalloc_socket("ethdev TX queue", sizeof(struct nfp_net_txq),
				 RTE_CACHE_LINE_SIZE, socket_id);
	if (txq == NULL) {
		RTE_LOG(ERR, PMD, "Error allocating tx dma\n");
		return -ENOMEM;
	}

	/*
	 * Allocate TX ring hardware descriptors. A memzone large enough to
	 * handle the maximum ring size is allocated in order to allow for
	 * resizing in later calls to the queue setup function.
	 */
	tz = rte_eth_dma_zone_reserve(dev, "tx_ring", queue_idx,
				   sizeof(struct nfp_net_tx_desc) *
				   NFP_NET_MAX_TX_DESC, NFP_MEMZONE_ALIGN,
				   socket_id);
	if (tz == NULL) {
		RTE_LOG(ERR, PMD, "Error allocating tx dma\n");
		nfp_net_tx_queue_release(txq);
		return -ENOMEM;
	}

	txq->tx_count = nb_desc;
	txq->tx_free_thresh = tx_free_thresh;
	txq->tx_pthresh = tx_conf->tx_thresh.pthresh;
	txq->tx_hthresh = tx_conf->tx_thresh.hthresh;
	txq->tx_wthresh = tx_conf->tx_thresh.wthresh;

	/* queue mapping based on firmware configuration */
	txq->qidx = queue_idx;
	txq->tx_qcidx = queue_idx * hw->stride_tx;
	txq->qcp_q = hw->tx_bar + NFP_QCP_QUEUE_OFF(txq->tx_qcidx);

	txq->port_id = dev->data->port_id;
	txq->txq_flags = tx_conf->txq_flags;

	/* Saving physical and virtual addresses for the TX ring */
	txq->dma = (uint64_t)tz->iova;
	txq->txds = (struct nfp_net_tx_desc *)tz->addr;

	/* mbuf pointers array for referencing mbufs linked to TX descriptors */
	txq->txbufs = rte_zmalloc_socket("txq->txbufs",
					 sizeof(*txq->txbufs) * nb_desc,
					 RTE_CACHE_LINE_SIZE, socket_id);
	if (txq->txbufs == NULL) {
		nfp_net_tx_queue_release(txq);
		return -ENOMEM;
	}
	PMD_TX_LOG(DEBUG, "txbufs=%p hw_ring=%p dma_addr=0x%" PRIx64 "\n",
		   txq->txbufs, txq->txds, (unsigned long int)txq->dma);

	nfp_net_reset_tx_queue(txq);

	dev->data->tx_queues[queue_idx] = txq;
	txq->hw = hw;

	/*
	 * Telling the HW about the physical address of the TX ring and number
	 * of descriptors in log2 format
	 */
	nn_cfg_writeq(hw, NFP_NET_CFG_TXR_ADDR(queue_idx), txq->dma);
	nn_cfg_writeb(hw, NFP_NET_CFG_TXR_SZ(queue_idx), rte_log2_u32(nb_desc));

	return 0;
}

/* nfp_net_tx_tso - Set TX descriptor for TSO */
static inline void
nfp_net_tx_tso(struct nfp_net_txq *txq, struct nfp_net_tx_desc *txd,
	       struct rte_mbuf *mb)
{
	uint64_t ol_flags;
	struct nfp_net_hw *hw = txq->hw;

	if (!(hw->cap & NFP_NET_CFG_CTRL_LSO))
		goto clean_txd;

	ol_flags = mb->ol_flags;

	if (!(ol_flags & PKT_TX_TCP_SEG))
		goto clean_txd;

	txd->l4_offset = mb->l2_len + mb->l3_len + mb->l4_len;
	txd->lso = rte_cpu_to_le_16(mb->tso_segsz);
	txd->flags = PCIE_DESC_TX_LSO;
	return;

clean_txd:
	txd->flags = 0;
	txd->l4_offset = 0;
	txd->lso = 0;
}

/* nfp_net_tx_cksum - Set TX CSUM offload flags in TX descriptor */
static inline void
nfp_net_tx_cksum(struct nfp_net_txq *txq, struct nfp_net_tx_desc *txd,
		 struct rte_mbuf *mb)
{
	uint64_t ol_flags;
	struct nfp_net_hw *hw = txq->hw;

	if (!(hw->cap & NFP_NET_CFG_CTRL_TXCSUM))
		return;

	ol_flags = mb->ol_flags;

	/* IPv6 does not need checksum */
	if (ol_flags & PKT_TX_IP_CKSUM)
		txd->flags |= PCIE_DESC_TX_IP4_CSUM;

	switch (ol_flags & PKT_TX_L4_MASK) {
	case PKT_TX_UDP_CKSUM:
		txd->flags |= PCIE_DESC_TX_UDP_CSUM;
		break;
	case PKT_TX_TCP_CKSUM:
		txd->flags |= PCIE_DESC_TX_TCP_CSUM;
		break;
	}

	if (ol_flags & (PKT_TX_IP_CKSUM | PKT_TX_L4_MASK))
		txd->flags |= PCIE_DESC_TX_CSUM;
}

/* nfp_net_rx_cksum - set mbuf checksum flags based on RX descriptor flags */
static inline void
nfp_net_rx_cksum(struct nfp_net_rxq *rxq, struct nfp_net_rx_desc *rxd,
		 struct rte_mbuf *mb)
{
	struct nfp_net_hw *hw = rxq->hw;

	if (!(hw->ctrl & NFP_NET_CFG_CTRL_RXCSUM))
		return;

	/* If IPv4 and IP checksum error, fail */
	if ((rxd->rxd.flags & PCIE_DESC_RX_IP4_CSUM) &&
	    !(rxd->rxd.flags & PCIE_DESC_RX_IP4_CSUM_OK))
		mb->ol_flags |= PKT_RX_IP_CKSUM_BAD;

	/* If neither UDP nor TCP return */
	if (!(rxd->rxd.flags & PCIE_DESC_RX_TCP_CSUM) &&
	    !(rxd->rxd.flags & PCIE_DESC_RX_UDP_CSUM))
		return;

	if ((rxd->rxd.flags & PCIE_DESC_RX_TCP_CSUM) &&
	    !(rxd->rxd.flags & PCIE_DESC_RX_TCP_CSUM_OK))
		mb->ol_flags |= PKT_RX_L4_CKSUM_BAD;

	if ((rxd->rxd.flags & PCIE_DESC_RX_UDP_CSUM) &&
	    !(rxd->rxd.flags & PCIE_DESC_RX_UDP_CSUM_OK))
		mb->ol_flags |= PKT_RX_L4_CKSUM_BAD;
}

#define NFP_HASH_OFFSET      ((uint8_t *)mbuf->buf_addr + mbuf->data_off - 4)
#define NFP_HASH_TYPE_OFFSET ((uint8_t *)mbuf->buf_addr + mbuf->data_off - 8)

#define NFP_DESC_META_LEN(d) (d->rxd.meta_len_dd & PCIE_DESC_RX_META_LEN_MASK)

/*
 * nfp_net_set_hash - Set mbuf hash data
 *
 * The RSS hash and hash-type are pre-pended to the packet data.
 * Extract and decode it and set the mbuf fields.
 */
static inline void
nfp_net_set_hash(struct nfp_net_rxq *rxq, struct nfp_net_rx_desc *rxd,
		 struct rte_mbuf *mbuf)
{
	struct nfp_net_hw *hw = rxq->hw;
	uint8_t *meta_offset;
	uint32_t meta_info;
	uint32_t hash = 0;
	uint32_t hash_type = 0;

	if (!(hw->ctrl & NFP_NET_CFG_CTRL_RSS))
		return;

	if (NFD_CFG_MAJOR_VERSION_of(hw->ver) <= 3) {
		if (!(rxd->rxd.flags & PCIE_DESC_RX_RSS))
			return;

		hash = rte_be_to_cpu_32(*(uint32_t *)NFP_HASH_OFFSET);
		hash_type = rte_be_to_cpu_32(*(uint32_t *)NFP_HASH_TYPE_OFFSET);

	} else if (NFP_DESC_META_LEN(rxd)) {
		/*
		 * new metadata api:
		 * <----  32 bit  ----->
		 * m    field type word
		 * e     data field #2
		 * t     data field #1
		 * a     data field #0
		 * ====================
		 *    packet data
		 *
		 * Field type word contains up to 8 4bit field types
		 * A 4bit field type refers to a data field word
		 * A data field word can have several 4bit field types
		 */
		meta_offset = rte_pktmbuf_mtod(mbuf, uint8_t *);
		meta_offset -= NFP_DESC_META_LEN(rxd);
		meta_info = rte_be_to_cpu_32(*(uint32_t *)meta_offset);
		meta_offset += 4;
		/* NFP PMD just supports metadata for hashing */
		switch (meta_info & NFP_NET_META_FIELD_MASK) {
		case NFP_NET_META_HASH:
			/* next field type is about the hash type */
			meta_info >>= NFP_NET_META_FIELD_SIZE;
			/* hash value is in the data field */
			hash = rte_be_to_cpu_32(*(uint32_t *)meta_offset);
			hash_type = meta_info & NFP_NET_META_FIELD_MASK;
			break;
		default:
			/* Unsupported metadata can be a performance issue */
			return;
		}
	} else {
		return;
	}

	mbuf->hash.rss = hash;
	mbuf->ol_flags |= PKT_RX_RSS_HASH;

	switch (hash_type) {
	case NFP_NET_RSS_IPV4:
		mbuf->packet_type |= RTE_PTYPE_INNER_L3_IPV4;
		break;
	case NFP_NET_RSS_IPV6:
		mbuf->packet_type |= RTE_PTYPE_INNER_L3_IPV6;
		break;
	case NFP_NET_RSS_IPV6_EX:
		mbuf->packet_type |= RTE_PTYPE_INNER_L3_IPV6_EXT;
		break;
	default:
		mbuf->packet_type |= RTE_PTYPE_INNER_L4_MASK;
	}
}

static inline void
nfp_net_mbuf_alloc_failed(struct nfp_net_rxq *rxq)
{
	rte_eth_devices[rxq->port_id].data->rx_mbuf_alloc_failed++;
}

#define NFP_DESC_META_LEN(d) (d->rxd.meta_len_dd & PCIE_DESC_RX_META_LEN_MASK)

/*
 * RX path design:
 *
 * There are some decissions to take:
 * 1) How to check DD RX descriptors bit
 * 2) How and when to allocate new mbufs
 *
 * Current implementation checks just one single DD bit each loop. As each
 * descriptor is 8 bytes, it is likely a good idea to check descriptors in
 * a single cache line instead. Tests with this change have not shown any
 * performance improvement but it requires further investigation. For example,
 * depending on which descriptor is next, the number of descriptors could be
 * less than 8 for just checking those in the same cache line. This implies
 * extra work which could be counterproductive by itself. Indeed, last firmware
 * changes are just doing this: writing several descriptors with the DD bit
 * for saving PCIe bandwidth and DMA operations from the NFP.
 *
 * Mbuf allocation is done when a new packet is received. Then the descriptor
 * is automatically linked with the new mbuf and the old one is given to the
 * user. The main drawback with this design is mbuf allocation is heavier than
 * using bulk allocations allowed by DPDK with rte_mempool_get_bulk. From the
 * cache point of view it does not seem allocating the mbuf early on as we are
 * doing now have any benefit at all. Again, tests with this change have not
 * shown any improvement. Also, rte_mempool_get_bulk returns all or nothing
 * so looking at the implications of this type of allocation should be studied
 * deeply
 */

static uint16_t
nfp_net_recv_pkts(void *rx_queue, struct rte_mbuf **rx_pkts, uint16_t nb_pkts)
{
	struct nfp_net_rxq *rxq;
	struct nfp_net_rx_desc *rxds;
	struct nfp_net_rx_buff *rxb;
	struct nfp_net_hw *hw;
	struct rte_mbuf *mb;
	struct rte_mbuf *new_mb;
	uint16_t nb_hold;
	uint64_t dma_addr;
	int avail;

	rxq = rx_queue;
	if (unlikely(rxq == NULL)) {
		/*
		 * DPDK just checks the queue is lower than max queues
		 * enabled. But the queue needs to be configured
		 */
		RTE_LOG_DP(ERR, PMD, "RX Bad queue\n");
		return -EINVAL;
	}

	hw = rxq->hw;
	avail = 0;
	nb_hold = 0;

	while (avail < nb_pkts) {
		rxb = &rxq->rxbufs[rxq->rd_p];
		if (unlikely(rxb == NULL)) {
			RTE_LOG_DP(ERR, PMD, "rxb does not exist!\n");
			break;
		}

		/*
		 * Memory barrier to ensure that we won't do other
		 * reads before the DD bit.
		 */
		rte_rmb();

		rxds = &rxq->rxds[rxq->rd_p];
		if ((rxds->rxd.meta_len_dd & PCIE_DESC_RX_DD) == 0)
			break;

		/*
		 * We got a packet. Let's alloc a new mbuff for refilling the
		 * free descriptor ring as soon as possible
		 */
		new_mb = rte_pktmbuf_alloc(rxq->mem_pool);
		if (unlikely(new_mb == NULL)) {
			RTE_LOG_DP(DEBUG, PMD,
			"RX mbuf alloc failed port_id=%u queue_id=%u\n",
				rxq->port_id, (unsigned int)rxq->qidx);
			nfp_net_mbuf_alloc_failed(rxq);
			break;
		}

		nb_hold++;

		/*
		 * Grab the mbuff and refill the descriptor with the
		 * previously allocated mbuff
		 */
		mb = rxb->mbuf;
		rxb->mbuf = new_mb;

		PMD_RX_LOG(DEBUG, "Packet len: %u, mbuf_size: %u\n",
			   rxds->rxd.data_len, rxq->mbuf_size);

		/* Size of this segment */
		mb->data_len = rxds->rxd.data_len - NFP_DESC_META_LEN(rxds);
		/* Size of the whole packet. We just support 1 segment */
		mb->pkt_len = rxds->rxd.data_len - NFP_DESC_META_LEN(rxds);

		if (unlikely((mb->data_len + hw->rx_offset) >
			     rxq->mbuf_size)) {
			/*
			 * This should not happen and the user has the
			 * responsibility of avoiding it. But we have
			 * to give some info about the error
			 */
			RTE_LOG_DP(ERR, PMD,
				"mbuf overflow likely due to the RX offset.\n"
				"\t\tYour mbuf size should have extra space for"
				" RX offset=%u bytes.\n"
				"\t\tCurrently you just have %u bytes available"
				" but the received packet is %u bytes long",
				hw->rx_offset,
				rxq->mbuf_size - hw->rx_offset,
				mb->data_len);
			return -EINVAL;
		}

		/* Filling the received mbuff with packet info */
		if (hw->rx_offset)
			mb->data_off = RTE_PKTMBUF_HEADROOM + hw->rx_offset;
		else
			mb->data_off = RTE_PKTMBUF_HEADROOM +
				       NFP_DESC_META_LEN(rxds);

		/* No scatter mode supported */
		mb->nb_segs = 1;
		mb->next = NULL;

		/* Checking the RSS flag */
		nfp_net_set_hash(rxq, rxds, mb);

		/* Checking the checksum flag */
		nfp_net_rx_cksum(rxq, rxds, mb);

		if ((rxds->rxd.flags & PCIE_DESC_RX_VLAN) &&
		    (hw->ctrl & NFP_NET_CFG_CTRL_RXVLAN)) {
			mb->vlan_tci = rte_cpu_to_le_32(rxds->rxd.vlan);
			mb->ol_flags |= PKT_RX_VLAN | PKT_RX_VLAN_STRIPPED;
		}

		/* Adding the mbuff to the mbuff array passed by the app */
		rx_pkts[avail++] = mb;

		/* Now resetting and updating the descriptor */
		rxds->vals[0] = 0;
		rxds->vals[1] = 0;
		dma_addr = rte_cpu_to_le_64(RTE_MBUF_DMA_ADDR_DEFAULT(new_mb));
		rxds->fld.dd = 0;
		rxds->fld.dma_addr_hi = (dma_addr >> 32) & 0xff;
		rxds->fld.dma_addr_lo = dma_addr & 0xffffffff;

		rxq->rd_p++;
		if (unlikely(rxq->rd_p == rxq->rx_count)) /* wrapping?*/
			rxq->rd_p = 0;
	}

	if (nb_hold == 0)
		return nb_hold;

	PMD_RX_LOG(DEBUG, "RX  port_id=%u queue_id=%u, %d packets received\n",
		   rxq->port_id, (unsigned int)rxq->qidx, nb_hold);

	nb_hold += rxq->nb_rx_hold;

	/*
	 * FL descriptors needs to be written before incrementing the
	 * FL queue WR pointer
	 */
	rte_wmb();
	if (nb_hold > rxq->rx_free_thresh) {
		PMD_RX_LOG(DEBUG, "port=%u queue=%u nb_hold=%u avail=%u\n",
			   rxq->port_id, (unsigned int)rxq->qidx,
			   (unsigned)nb_hold, (unsigned)avail);
		nfp_qcp_ptr_add(rxq->qcp_fl, NFP_QCP_WRITE_PTR, nb_hold);
		nb_hold = 0;
	}
	rxq->nb_rx_hold = nb_hold;

	return avail;
}

/*
 * nfp_net_tx_free_bufs - Check for descriptors with a complete
 * status
 * @txq: TX queue to work with
 * Returns number of descriptors freed
 */
int
nfp_net_tx_free_bufs(struct nfp_net_txq *txq)
{
	uint32_t qcp_rd_p;
	int todo;

	PMD_TX_LOG(DEBUG, "queue %u. Check for descriptor with a complete"
		   " status\n", txq->qidx);

	/* Work out how many packets have been sent */
	qcp_rd_p = nfp_qcp_read(txq->qcp_q, NFP_QCP_READ_PTR);

	if (qcp_rd_p == txq->rd_p) {
		PMD_TX_LOG(DEBUG, "queue %u: It seems harrier is not sending "
			   "packets (%u, %u)\n", txq->qidx,
			   qcp_rd_p, txq->rd_p);
		return 0;
	}

	if (qcp_rd_p > txq->rd_p)
		todo = qcp_rd_p - txq->rd_p;
	else
		todo = qcp_rd_p + txq->tx_count - txq->rd_p;

	PMD_TX_LOG(DEBUG, "qcp_rd_p %u, txq->rd_p: %u, qcp->rd_p: %u\n",
		   qcp_rd_p, txq->rd_p, txq->rd_p);

	if (todo == 0)
		return todo;

	txq->rd_p += todo;
	if (unlikely(txq->rd_p >= txq->tx_count))
		txq->rd_p -= txq->tx_count;

	return todo;
}

/* Leaving always free descriptors for avoiding wrapping confusion */
static inline
uint32_t nfp_free_tx_desc(struct nfp_net_txq *txq)
{
	if (txq->wr_p >= txq->rd_p)
		return txq->tx_count - (txq->wr_p - txq->rd_p) - 8;
	else
		return txq->rd_p - txq->wr_p - 8;
}

/*
 * nfp_net_txq_full - Check if the TX queue free descriptors
 * is below tx_free_threshold
 *
 * @txq: TX queue to check
 *
 * This function uses the host copy* of read/write pointers
 */
static inline
uint32_t nfp_net_txq_full(struct nfp_net_txq *txq)
{
	return (nfp_free_tx_desc(txq) < txq->tx_free_thresh);
}

static uint16_t
nfp_net_xmit_pkts(void *tx_queue, struct rte_mbuf **tx_pkts, uint16_t nb_pkts)
{
	struct nfp_net_txq *txq;
	struct nfp_net_hw *hw;
	struct nfp_net_tx_desc *txds, txd;
	struct rte_mbuf *pkt;
	uint64_t dma_addr;
	int pkt_size, dma_size;
	uint16_t free_descs, issued_descs;
	struct rte_mbuf **lmbuf;
	int i;

	txq = tx_queue;
	hw = txq->hw;
	txds = &txq->txds[txq->wr_p];

	PMD_TX_LOG(DEBUG, "working for queue %u at pos %d and %u packets\n",
		   txq->qidx, txq->wr_p, nb_pkts);

	if ((nfp_free_tx_desc(txq) < nb_pkts) || (nfp_net_txq_full(txq)))
		nfp_net_tx_free_bufs(txq);

	free_descs = (uint16_t)nfp_free_tx_desc(txq);
	if (unlikely(free_descs == 0))
		return 0;

	pkt = *tx_pkts;

	i = 0;
	issued_descs = 0;
	PMD_TX_LOG(DEBUG, "queue: %u. Sending %u packets\n",
		   txq->qidx, nb_pkts);
	/* Sending packets */
	while ((i < nb_pkts) && free_descs) {
		/* Grabbing the mbuf linked to the current descriptor */
		lmbuf = &txq->txbufs[txq->wr_p].mbuf;
		/* Warming the cache for releasing the mbuf later on */
		RTE_MBUF_PREFETCH_TO_FREE(*lmbuf);

		pkt = *(tx_pkts + i);

		if (unlikely((pkt->nb_segs > 1) &&
			     !(hw->cap & NFP_NET_CFG_CTRL_GATHER))) {
			PMD_INIT_LOG(INFO, "NFP_NET_CFG_CTRL_GATHER not set");
			rte_panic("Multisegment packet unsupported\n");
		}

		/* Checking if we have enough descriptors */
		if (unlikely(pkt->nb_segs > free_descs))
			goto xmit_end;

		/*
		 * Checksum and VLAN flags just in the first descriptor for a
		 * multisegment packet, but TSO info needs to be in all of them.
		 */
		txd.data_len = pkt->pkt_len;
		nfp_net_tx_tso(txq, &txd, pkt);
		nfp_net_tx_cksum(txq, &txd, pkt);

		if ((pkt->ol_flags & PKT_TX_VLAN_PKT) &&
		    (hw->cap & NFP_NET_CFG_CTRL_TXVLAN)) {
			txd.flags |= PCIE_DESC_TX_VLAN;
			txd.vlan = pkt->vlan_tci;
		}

		/*
		 * mbuf data_len is the data in one segment and pkt_len data
		 * in the whole packet. When the packet is just one segment,
		 * then data_len = pkt_len
		 */
		pkt_size = pkt->pkt_len;

		while (pkt) {
			/* Copying TSO, VLAN and cksum info */
			*txds = txd;

			/* Releasing mbuf used by this descriptor previously*/
			if (*lmbuf)
				rte_pktmbuf_free_seg(*lmbuf);

			/*
			 * Linking mbuf with descriptor for being released
			 * next time descriptor is used
			 */
			*lmbuf = pkt;

			dma_size = pkt->data_len;
			dma_addr = rte_mbuf_data_iova(pkt);
			PMD_TX_LOG(DEBUG, "Working with mbuf at dma address:"
				   "%" PRIx64 "\n", dma_addr);

			/* Filling descriptors fields */
			txds->dma_len = dma_size;
			txds->data_len = txd.data_len;
			txds->dma_addr_hi = (dma_addr >> 32) & 0xff;
			txds->dma_addr_lo = (dma_addr & 0xffffffff);
			ASSERT(free_descs > 0);
			free_descs--;

			txq->wr_p++;
			if (unlikely(txq->wr_p == txq->tx_count)) /* wrapping?*/
				txq->wr_p = 0;

			pkt_size -= dma_size;
			if (!pkt_size)
				/* End of packet */
				txds->offset_eop |= PCIE_DESC_TX_EOP;
			else
				txds->offset_eop &= PCIE_DESC_TX_OFFSET_MASK;

			pkt = pkt->next;
			/* Referencing next free TX descriptor */
			txds = &txq->txds[txq->wr_p];
			lmbuf = &txq->txbufs[txq->wr_p].mbuf;
			issued_descs++;
		}
		i++;
	}

xmit_end:
	/* Increment write pointers. Force memory write before we let HW know */
	rte_wmb();
	nfp_qcp_ptr_add(txq->qcp_q, NFP_QCP_WRITE_PTR, issued_descs);

	return i;
}

static int
nfp_net_vlan_offload_set(struct rte_eth_dev *dev, int mask)
{
	uint32_t new_ctrl, update;
	struct nfp_net_hw *hw;
	int ret;

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);
	new_ctrl = 0;

	if ((mask & ETH_VLAN_FILTER_OFFLOAD) ||
	    (mask & ETH_VLAN_EXTEND_OFFLOAD))
		RTE_LOG(INFO, PMD, "No support for ETH_VLAN_FILTER_OFFLOAD or"
			" ETH_VLAN_EXTEND_OFFLOAD");

	/* Enable vlan strip if it is not configured yet */
	if ((mask & ETH_VLAN_STRIP_OFFLOAD) &&
	    !(hw->ctrl & NFP_NET_CFG_CTRL_RXVLAN))
		new_ctrl = hw->ctrl | NFP_NET_CFG_CTRL_RXVLAN;

	/* Disable vlan strip just if it is configured */
	if (!(mask & ETH_VLAN_STRIP_OFFLOAD) &&
	    (hw->ctrl & NFP_NET_CFG_CTRL_RXVLAN))
		new_ctrl = hw->ctrl & ~NFP_NET_CFG_CTRL_RXVLAN;

	if (new_ctrl == 0)
		return 0;

	update = NFP_NET_CFG_UPDATE_GEN;

	ret = nfp_net_reconfig(hw, new_ctrl, update);
	if (!ret)
		hw->ctrl = new_ctrl;

	return ret;
}

static int
nfp_net_rss_reta_write(struct rte_eth_dev *dev,
		    struct rte_eth_rss_reta_entry64 *reta_conf,
		    uint16_t reta_size)
{
	uint32_t reta, mask;
	int i, j;
	int idx, shift;
	struct nfp_net_hw *hw =
		NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);

	if (reta_size != NFP_NET_CFG_RSS_ITBL_SZ) {
		RTE_LOG(ERR, PMD, "The size of hash lookup table configured "
			"(%d) doesn't match the number hardware can supported "
			"(%d)\n", reta_size, NFP_NET_CFG_RSS_ITBL_SZ);
		return -EINVAL;
	}

	/*
	 * Update Redirection Table. There are 128 8bit-entries which can be
	 * manage as 32 32bit-entries
	 */
	for (i = 0; i < reta_size; i += 4) {
		/* Handling 4 RSS entries per loop */
		idx = i / RTE_RETA_GROUP_SIZE;
		shift = i % RTE_RETA_GROUP_SIZE;
		mask = (uint8_t)((reta_conf[idx].mask >> shift) & 0xF);

		if (!mask)
			continue;

		reta = 0;
		/* If all 4 entries were set, don't need read RETA register */
		if (mask != 0xF)
			reta = nn_cfg_readl(hw, NFP_NET_CFG_RSS_ITBL + i);

		for (j = 0; j < 4; j++) {
			if (!(mask & (0x1 << j)))
				continue;
			if (mask != 0xF)
				/* Clearing the entry bits */
				reta &= ~(0xFF << (8 * j));
			reta |= reta_conf[idx].reta[shift + j] << (8 * j);
		}
		nn_cfg_writel(hw, NFP_NET_CFG_RSS_ITBL + (idx * 64) + shift,
			      reta);
	}
	return 0;
}

/* Update Redirection Table(RETA) of Receive Side Scaling of Ethernet device */
static int
nfp_net_reta_update(struct rte_eth_dev *dev,
		    struct rte_eth_rss_reta_entry64 *reta_conf,
		    uint16_t reta_size)
{
	struct nfp_net_hw *hw =
		NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);
	uint32_t update;
	int ret;

	if (!(hw->ctrl & NFP_NET_CFG_CTRL_RSS))
		return -EINVAL;

	ret = nfp_net_rss_reta_write(dev, reta_conf, reta_size);
	if (ret != 0)
		return ret;

	update = NFP_NET_CFG_UPDATE_RSS;

	if (nfp_net_reconfig(hw, hw->ctrl, update) < 0)
		return -EIO;

	return 0;
}

 /* Query Redirection Table(RETA) of Receive Side Scaling of Ethernet device. */
static int
nfp_net_reta_query(struct rte_eth_dev *dev,
		   struct rte_eth_rss_reta_entry64 *reta_conf,
		   uint16_t reta_size)
{
	uint8_t i, j, mask;
	int idx, shift;
	uint32_t reta;
	struct nfp_net_hw *hw;

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);

	if (!(hw->ctrl & NFP_NET_CFG_CTRL_RSS))
		return -EINVAL;

	if (reta_size != NFP_NET_CFG_RSS_ITBL_SZ) {
		RTE_LOG(ERR, PMD, "The size of hash lookup table configured "
			"(%d) doesn't match the number hardware can supported "
			"(%d)\n", reta_size, NFP_NET_CFG_RSS_ITBL_SZ);
		return -EINVAL;
	}

	/*
	 * Reading Redirection Table. There are 128 8bit-entries which can be
	 * manage as 32 32bit-entries
	 */
	for (i = 0; i < reta_size; i += 4) {
		/* Handling 4 RSS entries per loop */
		idx = i / RTE_RETA_GROUP_SIZE;
		shift = i % RTE_RETA_GROUP_SIZE;
		mask = (uint8_t)((reta_conf[idx].mask >> shift) & 0xF);

		if (!mask)
			continue;

		reta = nn_cfg_readl(hw, NFP_NET_CFG_RSS_ITBL + (idx * 64) +
				    shift);
		for (j = 0; j < 4; j++) {
			if (!(mask & (0x1 << j)))
				continue;
			reta_conf->reta[shift + j] =
				(uint8_t)((reta >> (8 * j)) & 0xF);
		}
	}
	return 0;
}

static int
nfp_net_rss_hash_write(struct rte_eth_dev *dev,
			struct rte_eth_rss_conf *rss_conf)
{
	struct nfp_net_hw *hw;
	uint64_t rss_hf;
	uint32_t cfg_rss_ctrl = 0;
	uint8_t key;
	int i;

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);

	/* Writing the key byte a byte */
	for (i = 0; i < rss_conf->rss_key_len; i++) {
		memcpy(&key, &rss_conf->rss_key[i], 1);
		nn_cfg_writeb(hw, NFP_NET_CFG_RSS_KEY + i, key);
	}

	rss_hf = rss_conf->rss_hf;

	if (rss_hf & ETH_RSS_IPV4)
		cfg_rss_ctrl |= NFP_NET_CFG_RSS_IPV4 |
				NFP_NET_CFG_RSS_IPV4_TCP |
				NFP_NET_CFG_RSS_IPV4_UDP;

	if (rss_hf & ETH_RSS_IPV6)
		cfg_rss_ctrl |= NFP_NET_CFG_RSS_IPV6 |
				NFP_NET_CFG_RSS_IPV6_TCP |
				NFP_NET_CFG_RSS_IPV6_UDP;

	cfg_rss_ctrl |= NFP_NET_CFG_RSS_MASK;
	cfg_rss_ctrl |= NFP_NET_CFG_RSS_TOEPLITZ;

	/* configuring where to apply the RSS hash */
	nn_cfg_writel(hw, NFP_NET_CFG_RSS_CTRL, cfg_rss_ctrl);

	/* Writing the key size */
	nn_cfg_writeb(hw, NFP_NET_CFG_RSS_KEY_SZ, rss_conf->rss_key_len);

	return 0;
}

static int
nfp_net_rss_hash_update(struct rte_eth_dev *dev,
			struct rte_eth_rss_conf *rss_conf)
{
	uint32_t update;
	uint64_t rss_hf;
	struct nfp_net_hw *hw;

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);

	rss_hf = rss_conf->rss_hf;

	/* Checking if RSS is enabled */
	if (!(hw->ctrl & NFP_NET_CFG_CTRL_RSS)) {
		if (rss_hf != 0) { /* Enable RSS? */
			RTE_LOG(ERR, PMD, "RSS unsupported\n");
			return -EINVAL;
		}
		return 0; /* Nothing to do */
	}

	if (rss_conf->rss_key_len > NFP_NET_CFG_RSS_KEY_SZ) {
		RTE_LOG(ERR, PMD, "hash key too long\n");
		return -EINVAL;
	}

	nfp_net_rss_hash_write(dev, rss_conf);

	update = NFP_NET_CFG_UPDATE_RSS;

	if (nfp_net_reconfig(hw, hw->ctrl, update) < 0)
		return -EIO;

	return 0;
}

static int
nfp_net_rss_hash_conf_get(struct rte_eth_dev *dev,
			  struct rte_eth_rss_conf *rss_conf)
{
	uint64_t rss_hf;
	uint32_t cfg_rss_ctrl;
	uint8_t key;
	int i;
	struct nfp_net_hw *hw;

	hw = NFP_NET_DEV_PRIVATE_TO_HW(dev->data->dev_private);

	if (!(hw->ctrl & NFP_NET_CFG_CTRL_RSS))
		return -EINVAL;

	rss_hf = rss_conf->rss_hf;
	cfg_rss_ctrl = nn_cfg_readl(hw, NFP_NET_CFG_RSS_CTRL);

	if (cfg_rss_ctrl & NFP_NET_CFG_RSS_IPV4)
		rss_hf |= ETH_RSS_NONFRAG_IPV4_TCP | ETH_RSS_NONFRAG_IPV4_UDP;

	if (cfg_rss_ctrl & NFP_NET_CFG_RSS_IPV4_TCP)
		rss_hf |= ETH_RSS_NONFRAG_IPV4_TCP;

	if (cfg_rss_ctrl & NFP_NET_CFG_RSS_IPV6_TCP)
		rss_hf |= ETH_RSS_NONFRAG_IPV6_TCP;

	if (cfg_rss_ctrl & NFP_NET_CFG_RSS_IPV4_UDP)
		rss_hf |= ETH_RSS_NONFRAG_IPV4_UDP;

	if (cfg_rss_ctrl & NFP_NET_CFG_RSS_IPV6_UDP)
		rss_hf |= ETH_RSS_NONFRAG_IPV6_UDP;

	if (cfg_rss_ctrl & NFP_NET_CFG_RSS_IPV6)
		rss_hf |= ETH_RSS_NONFRAG_IPV4_UDP | ETH_RSS_NONFRAG_IPV6_UDP;

	/* Reading the key size */
	rss_conf->rss_key_len = nn_cfg_readl(hw, NFP_NET_CFG_RSS_KEY_SZ);

	/* Reading the key byte a byte */
	for (i = 0; i < rss_conf->rss_key_len; i++) {
		key = nn_cfg_readb(hw, NFP_NET_CFG_RSS_KEY + i);
		memcpy(&rss_conf->rss_key[i], &key, 1);
	}

	return 0;
}

static int
nfp_net_rss_config_default(struct rte_eth_dev *dev)
{
	struct rte_eth_conf *dev_conf;
	struct rte_eth_rss_conf rss_conf;
	struct rte_eth_rss_reta_entry64 nfp_reta_conf[2];
	uint16_t rx_queues = dev->data->nb_rx_queues;
	uint16_t queue;
	int i, j, ret;

	RTE_LOG(INFO, PMD, "setting default RSS conf for %u queues\n",
		rx_queues);

	nfp_reta_conf[0].mask = ~0x0;
	nfp_reta_conf[1].mask = ~0x0;

	queue = 0;
	for (i = 0; i < 0x40; i += 8) {
		for (j = i; j < (i + 8); j++) {
			nfp_reta_conf[0].reta[j] = queue;
			nfp_reta_conf[1].reta[j] = queue++;
			queue %= rx_queues;
		}
	}
	ret = nfp_net_rss_reta_write(dev, nfp_reta_conf, 0x80);
	if (ret != 0)
		return ret;

	dev_conf = &dev->data->dev_conf;
	if (!dev_conf) {
		RTE_LOG(INFO, PMD, "wrong rss conf");
		return -EINVAL;
	}
	rss_conf = dev_conf->rx_adv_conf.rss_conf;

	ret = nfp_net_rss_hash_write(dev, &rss_conf);

	return ret;
}


/* Initialise and register driver with DPDK Application */
static const struct eth_dev_ops nfp_net_eth_dev_ops = {
	.dev_configure		= nfp_net_configure,
	.dev_start		= nfp_net_start,
	.dev_stop		= nfp_net_stop,
	.dev_close		= nfp_net_close,
	.promiscuous_enable	= nfp_net_promisc_enable,
	.promiscuous_disable	= nfp_net_promisc_disable,
	.link_update		= nfp_net_link_update,
	.stats_get		= nfp_net_stats_get,
	.stats_reset		= nfp_net_stats_reset,
	.dev_infos_get		= nfp_net_infos_get,
	.dev_supported_ptypes_get = nfp_net_supported_ptypes_get,
	.mtu_set		= nfp_net_dev_mtu_set,
	.vlan_offload_set	= nfp_net_vlan_offload_set,
	.reta_update		= nfp_net_reta_update,
	.reta_query		= nfp_net_reta_query,
	.rss_hash_update	= nfp_net_rss_hash_update,
	.rss_hash_conf_get	= nfp_net_rss_hash_conf_get,
	.rx_queue_setup		= nfp_net_rx_queue_setup,
	.rx_queue_release	= nfp_net_rx_queue_release,
	.rx_queue_count		= nfp_net_rx_queue_count,
	.tx_queue_setup		= nfp_net_tx_queue_setup,
	.tx_queue_release	= nfp_net_tx_queue_release,
	.rx_queue_intr_enable   = nfp_rx_queue_intr_enable,
	.rx_queue_intr_disable  = nfp_rx_queue_intr_disable,
};

/*
 * All eth_dev created got its private data, but before nfp_net_init, that
 * private data is referencing private data for all the PF ports. This is due
 * to how the vNIC bars are mapped based on first port, so all ports need info
 * about port 0 private data. Inside nfp_net_init the private data pointer is
 * changed to the right address for each port once the bars have been mapped.
 *
 * This functions helps to find out which port and therefore which offset
 * inside the private data array to use.
 */
static int
get_pf_port_number(char *name)
{
	char *pf_str = name;
	int size = 0;

	while ((*pf_str != '_') && (*pf_str != '\0') && (size++ < 30))
		pf_str++;

	if (size == 30)
		/*
		 * This should not happen at all and it would mean major
		 * implementation fault.
		 */
		rte_panic("nfp_net: problem with pf device name\n");

	/* Expecting _portX with X within [0,7] */
	pf_str += 5;

	return (int)strtol(pf_str, NULL, 10);
}

static int
nfp_net_init(struct rte_eth_dev *eth_dev)
{
	struct rte_pci_device *pci_dev;
	struct nfp_net_hw *hw, *hwport0;

	uint64_t tx_bar_off = 0, rx_bar_off = 0;
	uint32_t start_q;
	int stride = 4;

	nspu_desc_t *nspu_desc = NULL;
	uint64_t bar_offset;
	int port = 0;

	PMD_INIT_FUNC_TRACE();

	pci_dev = RTE_ETH_DEV_TO_PCI(eth_dev);

	if ((pci_dev->id.device_id == PCI_DEVICE_ID_NFP4000_PF_NIC) ||
	    (pci_dev->id.device_id == PCI_DEVICE_ID_NFP6000_PF_NIC)) {
		port = get_pf_port_number(eth_dev->data->name);
		if (port < 0 || port > 7) {
			RTE_LOG(ERR, PMD, "Port value is wrong\n");
			return -ENODEV;
		}

		PMD_INIT_LOG(DEBUG, "Working with PF port value %d\n", port);

		/* This points to port 0 private data */
		hwport0 = NFP_NET_DEV_PRIVATE_TO_HW(eth_dev->data->dev_private);

		/* This points to the specific port private data */
		hw = &hwport0[port];
		hw->pf_port_idx = port;
	} else {
		hw = NFP_NET_DEV_PRIVATE_TO_HW(eth_dev->data->dev_private);
		hwport0 = 0;
	}

	eth_dev->dev_ops = &nfp_net_eth_dev_ops;
	eth_dev->rx_pkt_burst = &nfp_net_recv_pkts;
	eth_dev->tx_pkt_burst = &nfp_net_xmit_pkts;

	/* For secondary processes, the primary has done all the work */
	if (rte_eal_process_type() != RTE_PROC_PRIMARY)
		return 0;

	rte_eth_copy_pci_info(eth_dev, pci_dev);

	hw->device_id = pci_dev->id.device_id;
	hw->vendor_id = pci_dev->id.vendor_id;
	hw->subsystem_device_id = pci_dev->id.subsystem_device_id;
	hw->subsystem_vendor_id = pci_dev->id.subsystem_vendor_id;

	PMD_INIT_LOG(DEBUG, "nfp_net: device (%u:%u) %u:%u:%u:%u",
		     pci_dev->id.vendor_id, pci_dev->id.device_id,
		     pci_dev->addr.domain, pci_dev->addr.bus,
		     pci_dev->addr.devid, pci_dev->addr.function);

	hw->ctrl_bar = (uint8_t *)pci_dev->mem_resource[0].addr;
	if (hw->ctrl_bar == NULL) {
		RTE_LOG(ERR, PMD,
			"hw->ctrl_bar is NULL. BAR0 not configured\n");
		return -ENODEV;
	}

	if (hw->is_pf && port == 0) {
		nspu_desc = hw->nspu_desc;

		if (nfp_nsp_map_ctrl_bar(nspu_desc, &bar_offset) != 0) {
			/*
			 * A firmware should be there after PF probe so this
			 * should not happen.
			 */
			RTE_LOG(ERR, PMD, "PF BAR symbol resolution failed\n");
			return -ENODEV;
		}

		/* vNIC PF control BAR is a subset of PF PCI device BAR */
		hw->ctrl_bar += bar_offset;
		PMD_INIT_LOG(DEBUG, "ctrl bar: %p\n", hw->ctrl_bar);
	}

	if (port > 0) {
		if (!hwport0->ctrl_bar)
			return -ENODEV;

		/* address based on port0 offset */
		hw->ctrl_bar = hwport0->ctrl_bar +
			       (port * NFP_PF_CSR_SLICE_SIZE);
	}

	PMD_INIT_LOG(DEBUG, "ctrl bar: %p\n", hw->ctrl_bar);

	hw->max_rx_queues = nn_cfg_readl(hw, NFP_NET_CFG_MAX_RXRINGS);
	hw->max_tx_queues = nn_cfg_readl(hw, NFP_NET_CFG_MAX_TXRINGS);

	/* Work out where in the BAR the queues start. */
	switch (pci_dev->id.device_id) {
	case PCI_DEVICE_ID_NFP4000_PF_NIC:
	case PCI_DEVICE_ID_NFP6000_PF_NIC:
	case PCI_DEVICE_ID_NFP6000_VF_NIC:
		start_q = nn_cfg_readl(hw, NFP_NET_CFG_START_TXQ);
		tx_bar_off = NFP_PCIE_QUEUE(start_q);
		start_q = nn_cfg_readl(hw, NFP_NET_CFG_START_RXQ);
		rx_bar_off = NFP_PCIE_QUEUE(start_q);
		break;
	default:
		RTE_LOG(ERR, PMD, "nfp_net: no device ID matching\n");
		return -ENODEV;
	}

	PMD_INIT_LOG(DEBUG, "tx_bar_off: 0x%" PRIx64 "\n", tx_bar_off);
	PMD_INIT_LOG(DEBUG, "rx_bar_off: 0x%" PRIx64 "\n", rx_bar_off);

	if (hw->is_pf && port == 0) {
		/* configure access to tx/rx vNIC BARs */
		nfp_nsp_map_queues_bar(nspu_desc, &bar_offset);
		PMD_INIT_LOG(DEBUG, "tx/rx bar_offset: %" PRIx64 "\n",
				    bar_offset);
		hwport0->hw_queues = (uint8_t *)pci_dev->mem_resource[0].addr;

		/* vNIC PF tx/rx BARs are a subset of PF PCI device */
		hwport0->hw_queues += bar_offset;

		/* Lets seize the chance to read eth table from hw */
		if (nfp_nsp_eth_read_table(nspu_desc, &hw->eth_table))
			return -ENODEV;
	}

	if (hw->is_pf) {
		hw->tx_bar = hwport0->hw_queues + tx_bar_off;
		hw->rx_bar = hwport0->hw_queues + rx_bar_off;
		eth_dev->data->dev_private = hw;
	} else {
		hw->tx_bar = (uint8_t *)pci_dev->mem_resource[2].addr +
			     tx_bar_off;
		hw->rx_bar = (uint8_t *)pci_dev->mem_resource[2].addr +
			     rx_bar_off;
	}

	PMD_INIT_LOG(DEBUG, "ctrl_bar: %p, tx_bar: %p, rx_bar: %p",
		     hw->ctrl_bar, hw->tx_bar, hw->rx_bar);

	nfp_net_cfg_queue_setup(hw);

	/* Get some of the read-only fields from the config BAR */
	hw->ver = nn_cfg_readl(hw, NFP_NET_CFG_VERSION);
	hw->cap = nn_cfg_readl(hw, NFP_NET_CFG_CAP);
	hw->max_mtu = nn_cfg_readl(hw, NFP_NET_CFG_MAX_MTU);
	hw->mtu = ETHER_MTU;

	if (NFD_CFG_MAJOR_VERSION_of(hw->ver) < 2)
		hw->rx_offset = NFP_NET_RX_OFFSET;
	else
		hw->rx_offset = nn_cfg_readl(hw, NFP_NET_CFG_RX_OFFSET_ADDR);

	PMD_INIT_LOG(INFO, "VER: %#x, Maximum supported MTU: %d",
		     hw->ver, hw->max_mtu);
	PMD_INIT_LOG(INFO, "CAP: %#x, %s%s%s%s%s%s%s%s%s%s%s", hw->cap,
		     hw->cap & NFP_NET_CFG_CTRL_PROMISC ? "PROMISC " : "",
		     hw->cap & NFP_NET_CFG_CTRL_L2BC    ? "L2BCFILT " : "",
		     hw->cap & NFP_NET_CFG_CTRL_L2MC    ? "L2MCFILT " : "",
		     hw->cap & NFP_NET_CFG_CTRL_RXCSUM  ? "RXCSUM "  : "",
		     hw->cap & NFP_NET_CFG_CTRL_TXCSUM  ? "TXCSUM "  : "",
		     hw->cap & NFP_NET_CFG_CTRL_RXVLAN  ? "RXVLAN "  : "",
		     hw->cap & NFP_NET_CFG_CTRL_TXVLAN  ? "TXVLAN "  : "",
		     hw->cap & NFP_NET_CFG_CTRL_SCATTER ? "SCATTER " : "",
		     hw->cap & NFP_NET_CFG_CTRL_GATHER  ? "GATHER "  : "",
		     hw->cap & NFP_NET_CFG_CTRL_LSO     ? "TSO "     : "",
		     hw->cap & NFP_NET_CFG_CTRL_RSS     ? "RSS "     : "");

	hw->ctrl = 0;

	hw->stride_rx = stride;
	hw->stride_tx = stride;

	PMD_INIT_LOG(INFO, "max_rx_queues: %u, max_tx_queues: %u",
		     hw->max_rx_queues, hw->max_tx_queues);

	/* Initializing spinlock for reconfigs */
	rte_spinlock_init(&hw->reconfig_lock);

	/* Allocating memory for mac addr */
	eth_dev->data->mac_addrs = rte_zmalloc("mac_addr", ETHER_ADDR_LEN, 0);
	if (eth_dev->data->mac_addrs == NULL) {
		PMD_INIT_LOG(ERR, "Failed to space for MAC address");
		return -ENOMEM;
	}

	if (hw->is_pf) {
		nfp_net_pf_read_mac(hwport0, port);
		nfp_net_write_mac(hw, (uint8_t *)&hw->mac_addr);
	} else {
		nfp_net_vf_read_mac(hw);
	}

	if (!is_valid_assigned_ether_addr((struct ether_addr *)&hw->mac_addr)) {
		/* Using random mac addresses for VFs */
		eth_random_addr(&hw->mac_addr[0]);
		nfp_net_write_mac(hw, (uint8_t *)&hw->mac_addr);
	}

	/* Copying mac address to DPDK eth_dev struct */
	ether_addr_copy((struct ether_addr *)hw->mac_addr,
			&eth_dev->data->mac_addrs[0]);

	PMD_INIT_LOG(INFO, "port %d VendorID=0x%x DeviceID=0x%x "
		     "mac=%02x:%02x:%02x:%02x:%02x:%02x",
		     eth_dev->data->port_id, pci_dev->id.vendor_id,
		     pci_dev->id.device_id,
		     hw->mac_addr[0], hw->mac_addr[1], hw->mac_addr[2],
		     hw->mac_addr[3], hw->mac_addr[4], hw->mac_addr[5]);

	/* Registering LSC interrupt handler */
	rte_intr_callback_register(&pci_dev->intr_handle,
				   nfp_net_dev_interrupt_handler,
				   (void *)eth_dev);

	/* Telling the firmware about the LSC interrupt entry */
	nn_cfg_writeb(hw, NFP_NET_CFG_LSC, NFP_NET_IRQ_LSC_IDX);

	/* Recording current stats counters values */
	nfp_net_stats_reset(eth_dev);

	return 0;
}

static int
nfp_pf_create_dev(struct rte_pci_device *dev, int port, int ports,
		  nfpu_desc_t *nfpu_desc, void **priv)
{
	struct rte_eth_dev *eth_dev;
	struct nfp_net_hw *hw;
	char *port_name;
	int ret;

	port_name = rte_zmalloc("nfp_pf_port_name", 100, 0);
	if (!port_name)
		return -ENOMEM;

	if (ports > 1)
		sprintf(port_name, "%s_port%d", dev->device.name, port);
	else
		sprintf(port_name, "%s", dev->device.name);

	eth_dev = rte_eth_dev_allocate(port_name);
	if (!eth_dev)
		return -ENOMEM;

	if (port == 0) {
		*priv = rte_zmalloc(port_name,
				    sizeof(struct nfp_net_adapter) * ports,
				    RTE_CACHE_LINE_SIZE);
		if (!*priv) {
			rte_eth_dev_release_port(eth_dev);
			return -ENOMEM;
		}
	}

	eth_dev->data->dev_private = *priv;

	/*
	 * dev_private pointing to port0 dev_private because we need
	 * to configure vNIC bars based on port0 at nfp_net_init.
	 * Then dev_private is adjusted per port.
	 */
	hw = (struct nfp_net_hw *)(eth_dev->data->dev_private) + port;
	hw->nspu_desc = nfpu_desc->nspu;
	hw->nfpu_desc = nfpu_desc;
	hw->is_pf = 1;
	if (ports > 1)
		hw->pf_multiport_enabled = 1;

	eth_dev->device = &dev->device;
	rte_eth_copy_pci_info(eth_dev, dev);

	ret = nfp_net_init(eth_dev);

	if (ret)
		rte_eth_dev_release_port(eth_dev);

	rte_free(port_name);

	return ret;
}

static int nfp_pf_pci_probe(struct rte_pci_driver *pci_drv __rte_unused,
			    struct rte_pci_device *dev)
{
	nfpu_desc_t *nfpu_desc;
	nspu_desc_t *nspu_desc;
	uint64_t offset_symbol;
	uint8_t *bar_offset;
	int major, minor;
	int total_ports;
	void *priv = 0;
	int ret = -ENODEV;
	int i;

	if (!dev)
		return ret;

	nfpu_desc = rte_malloc("nfp nfpu", sizeof(nfpu_desc_t), 0);
	if (!nfpu_desc)
		return -ENOMEM;

	if (nfpu_open(dev, nfpu_desc, 0) < 0) {
		RTE_LOG(ERR, PMD,
			"nfpu_open failed\n");
		goto nfpu_error;
	}

	nspu_desc = nfpu_desc->nspu;


	/* Check NSP ABI version */
	if (nfp_nsp_get_abi_version(nspu_desc, &major, &minor) < 0) {
		RTE_LOG(INFO, PMD, "NFP NSP not present\n");
		goto error;
	}
	PMD_INIT_LOG(INFO, "nspu ABI version: %d.%d\n", major, minor);

	if ((major == 0) && (minor < 20)) {
		RTE_LOG(INFO, PMD, "NFP NSP ABI version too old. Required 0.20 or higher\n");
		goto error;
	}

	ret = nfp_nsp_fw_setup(nspu_desc, "nfd_cfg_pf0_num_ports",
			       &offset_symbol);
	if (ret)
		goto error;

	bar_offset = (uint8_t *)dev->mem_resource[0].addr;
	bar_offset += offset_symbol;
	total_ports = (uint32_t)*bar_offset;
	PMD_INIT_LOG(INFO, "Total pf ports: %d\n", total_ports);

	if (total_ports <= 0 || total_ports > 8) {
		RTE_LOG(ERR, PMD, "nfd_cfg_pf0_num_ports symbol with wrong value");
		ret = -ENODEV;
		goto error;
	}

	for (i = 0; i < total_ports; i++) {
		ret = nfp_pf_create_dev(dev, i, total_ports, nfpu_desc, &priv);
		if (ret)
			goto error;
	}

	return 0;

error:
	nfpu_close(nfpu_desc);
nfpu_error:
	rte_free(nfpu_desc);

	return ret;
}

int nfp_logtype_init;
int nfp_logtype_driver;

static const struct rte_pci_id pci_id_nfp_pf_net_map[] = {
	{
		RTE_PCI_DEVICE(PCI_VENDOR_ID_NETRONOME,
			       PCI_DEVICE_ID_NFP4000_PF_NIC)
	},
	{
		RTE_PCI_DEVICE(PCI_VENDOR_ID_NETRONOME,
			       PCI_DEVICE_ID_NFP6000_PF_NIC)
	},
	{
		.vendor_id = 0,
	},
};

static const struct rte_pci_id pci_id_nfp_vf_net_map[] = {
	{
		RTE_PCI_DEVICE(PCI_VENDOR_ID_NETRONOME,
			       PCI_DEVICE_ID_NFP6000_VF_NIC)
	},
	{
		.vendor_id = 0,
	},
};

static int eth_nfp_pci_probe(struct rte_pci_driver *pci_drv __rte_unused,
	struct rte_pci_device *pci_dev)
{
	return rte_eth_dev_pci_generic_probe(pci_dev,
		sizeof(struct nfp_net_adapter), nfp_net_init);
}

static int eth_nfp_pci_remove(struct rte_pci_device *pci_dev)
{
	struct rte_eth_dev *eth_dev;
	struct nfp_net_hw *hw, *hwport0;
	int port = 0;

	eth_dev = rte_eth_dev_allocated(pci_dev->device.name);
	if ((pci_dev->id.device_id == PCI_DEVICE_ID_NFP4000_PF_NIC) ||
	    (pci_dev->id.device_id == PCI_DEVICE_ID_NFP6000_PF_NIC)) {
		port = get_pf_port_number(eth_dev->data->name);
		hwport0 = NFP_NET_DEV_PRIVATE_TO_HW(eth_dev->data->dev_private);
		hw = &hwport0[port];
	} else {
		hw = NFP_NET_DEV_PRIVATE_TO_HW(eth_dev->data->dev_private);
	}
	/* hotplug is not possible with multiport PF */
	if (hw->pf_multiport_enabled)
		return -ENOTSUP;
	return rte_eth_dev_pci_generic_remove(pci_dev, NULL);
}

static struct rte_pci_driver rte_nfp_net_pf_pmd = {
	.id_table = pci_id_nfp_pf_net_map,
	.drv_flags = RTE_PCI_DRV_NEED_MAPPING | RTE_PCI_DRV_INTR_LSC,
	.probe = nfp_pf_pci_probe,
	.remove = eth_nfp_pci_remove,
};

static struct rte_pci_driver rte_nfp_net_vf_pmd = {
	.id_table = pci_id_nfp_vf_net_map,
	.drv_flags = RTE_PCI_DRV_NEED_MAPPING | RTE_PCI_DRV_INTR_LSC,
	.probe = eth_nfp_pci_probe,
	.remove = eth_nfp_pci_remove,
};

RTE_PMD_REGISTER_PCI(net_nfp_pf, rte_nfp_net_pf_pmd);
RTE_PMD_REGISTER_PCI(net_nfp_vf, rte_nfp_net_vf_pmd);
RTE_PMD_REGISTER_PCI_TABLE(net_nfp_pf, pci_id_nfp_pf_net_map);
RTE_PMD_REGISTER_PCI_TABLE(net_nfp_vf, pci_id_nfp_vf_net_map);
RTE_PMD_REGISTER_KMOD_DEP(net_nfp_pf, "* igb_uio | uio_pci_generic | vfio");
RTE_PMD_REGISTER_KMOD_DEP(net_nfp_vf, "* igb_uio | uio_pci_generic | vfio");

RTE_INIT(nfp_init_log);
static void
nfp_init_log(void)
{
	nfp_logtype_init = rte_log_register("pmd.net.nfp.init");
	if (nfp_logtype_init >= 0)
		rte_log_set_level(nfp_logtype_init, RTE_LOG_NOTICE);
	nfp_logtype_driver = rte_log_register("pmd.net.nfp.driver");
	if (nfp_logtype_driver >= 0)
		rte_log_set_level(nfp_logtype_driver, RTE_LOG_NOTICE);
}
/*
 * Local variables:
 * c-file-style: "Linux"
 * indent-tabs-mode: t
 * End:
 */
