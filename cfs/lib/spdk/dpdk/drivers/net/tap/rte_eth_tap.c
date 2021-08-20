/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2016-2017 Intel Corporation
 */

#include <rte_atomic.h>
#include <rte_branch_prediction.h>
#include <rte_byteorder.h>
#include <rte_common.h>
#include <rte_mbuf.h>
#include <rte_ethdev_driver.h>
#include <rte_ethdev_vdev.h>
#include <rte_malloc.h>
#include <rte_bus_vdev.h>
#include <rte_kvargs.h>
#include <rte_net.h>
#include <rte_debug.h>
#include <rte_ip.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/utsname.h>
#include <sys/mman.h>
#include <errno.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/uio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <net/if.h>
#include <linux/if_tun.h>
#include <linux/if_ether.h>
#include <fcntl.h>

#include <rte_eth_tap.h>
#include <tap_flow.h>
#include <tap_netlink.h>
#include <tap_tcmsgs.h>

/* Linux based path to the TUN device */
#define TUN_TAP_DEV_PATH        "/dev/net/tun"
#define DEFAULT_TAP_NAME        "dtap"

#define ETH_TAP_IFACE_ARG       "iface"
#define ETH_TAP_REMOTE_ARG      "remote"
#define ETH_TAP_MAC_ARG         "mac"
#define ETH_TAP_MAC_FIXED       "fixed"

static struct rte_vdev_driver pmd_tap_drv;

static const char *valid_arguments[] = {
	ETH_TAP_IFACE_ARG,
	ETH_TAP_REMOTE_ARG,
	ETH_TAP_MAC_ARG,
	NULL
};

static int tap_unit;

static volatile uint32_t tap_trigger;	/* Rx trigger */

static struct rte_eth_link pmd_link = {
	.link_speed = ETH_SPEED_NUM_10G,
	.link_duplex = ETH_LINK_FULL_DUPLEX,
	.link_status = ETH_LINK_DOWN,
	.link_autoneg = ETH_LINK_AUTONEG
};

static void
tap_trigger_cb(int sig __rte_unused)
{
	/* Valid trigger values are nonzero */
	tap_trigger = (tap_trigger + 1) | 0x80000000;
}

/* Specifies on what netdevices the ioctl should be applied */
enum ioctl_mode {
	LOCAL_AND_REMOTE,
	LOCAL_ONLY,
	REMOTE_ONLY,
};

static int tap_intr_handle_set(struct rte_eth_dev *dev, int set);

/* Tun/Tap allocation routine
 *
 * name is the number of the interface to use, unless NULL to take the host
 * supplied name.
 */
static int
tun_alloc(struct pmd_internals *pmd)
{
	struct ifreq ifr;
#ifdef IFF_MULTI_QUEUE
	unsigned int features;
#endif
	int fd;

	memset(&ifr, 0, sizeof(struct ifreq));

	/*
	 * Do not set IFF_NO_PI as packet information header will be needed
	 * to check if a received packet has been truncated.
	 */
	ifr.ifr_flags = IFF_TAP;
	snprintf(ifr.ifr_name, IFNAMSIZ, "%s", pmd->name);

	RTE_LOG(DEBUG, PMD, "ifr_name '%s'\n", ifr.ifr_name);

	fd = open(TUN_TAP_DEV_PATH, O_RDWR);
	if (fd < 0) {
		RTE_LOG(ERR, PMD, "Unable to create TAP interface\n");
		goto error;
	}

#ifdef IFF_MULTI_QUEUE
	/* Grab the TUN features to verify we can work multi-queue */
	if (ioctl(fd, TUNGETFEATURES, &features) < 0) {
		RTE_LOG(ERR, PMD, "TAP unable to get TUN/TAP features\n");
		goto error;
	}
	RTE_LOG(DEBUG, PMD, "  TAP Features %08x\n", features);

	if (features & IFF_MULTI_QUEUE) {
		RTE_LOG(DEBUG, PMD, "  Multi-queue support for %d queues\n",
			RTE_PMD_TAP_MAX_QUEUES);
		ifr.ifr_flags |= IFF_MULTI_QUEUE;
	} else
#endif
	{
		ifr.ifr_flags |= IFF_ONE_QUEUE;
		RTE_LOG(DEBUG, PMD, "  Single queue only support\n");
	}

	/* Set the TUN/TAP configuration and set the name if needed */
	if (ioctl(fd, TUNSETIFF, (void *)&ifr) < 0) {
		RTE_LOG(WARNING, PMD,
			"Unable to set TUNSETIFF for %s\n",
			ifr.ifr_name);
		perror("TUNSETIFF");
		goto error;
	}

	/* Always set the file descriptor to non-blocking */
	if (fcntl(fd, F_SETFL, O_NONBLOCK) < 0) {
		RTE_LOG(WARNING, PMD,
			"Unable to set %s to nonblocking\n",
			ifr.ifr_name);
		perror("F_SETFL, NONBLOCK");
		goto error;
	}

	/* Set up trigger to optimize empty Rx bursts */
	errno = 0;
	do {
		struct sigaction sa;
		int flags = fcntl(fd, F_GETFL);

		if (flags == -1 || sigaction(SIGIO, NULL, &sa) == -1)
			break;
		if (sa.sa_handler != tap_trigger_cb) {
			/*
			 * Make sure SIGIO is not already taken. This is done
			 * as late as possible to leave the application a
			 * chance to set up its own signal handler first.
			 */
			if (sa.sa_handler != SIG_IGN &&
			    sa.sa_handler != SIG_DFL) {
				errno = EBUSY;
				break;
			}
			sa = (struct sigaction){
				.sa_flags = SA_RESTART,
				.sa_handler = tap_trigger_cb,
			};
			if (sigaction(SIGIO, &sa, NULL) == -1)
				break;
		}
		/* Enable SIGIO on file descriptor */
		fcntl(fd, F_SETFL, flags | O_ASYNC);
		fcntl(fd, F_SETOWN, getpid());
	} while (0);
	if (errno) {
		/* Disable trigger globally in case of error */
		tap_trigger = 0;
		RTE_LOG(WARNING, PMD, "Rx trigger disabled: %s\n",
			strerror(errno));
	}

	return fd;

error:
	if (fd > 0)
		close(fd);
	return -1;
}

static void
tap_verify_csum(struct rte_mbuf *mbuf)
{
	uint32_t l2 = mbuf->packet_type & RTE_PTYPE_L2_MASK;
	uint32_t l3 = mbuf->packet_type & RTE_PTYPE_L3_MASK;
	uint32_t l4 = mbuf->packet_type & RTE_PTYPE_L4_MASK;
	unsigned int l2_len = sizeof(struct ether_hdr);
	unsigned int l3_len;
	uint16_t cksum = 0;
	void *l3_hdr;
	void *l4_hdr;

	if (l2 == RTE_PTYPE_L2_ETHER_VLAN)
		l2_len += 4;
	else if (l2 == RTE_PTYPE_L2_ETHER_QINQ)
		l2_len += 8;
	/* Don't verify checksum for packets with discontinuous L2 header */
	if (unlikely(l2_len + sizeof(struct ipv4_hdr) >
		     rte_pktmbuf_data_len(mbuf)))
		return;
	l3_hdr = rte_pktmbuf_mtod_offset(mbuf, void *, l2_len);
	if (l3 == RTE_PTYPE_L3_IPV4 || l3 == RTE_PTYPE_L3_IPV4_EXT) {
		struct ipv4_hdr *iph = l3_hdr;

		/* ihl contains the number of 4-byte words in the header */
		l3_len = 4 * (iph->version_ihl & 0xf);
		if (unlikely(l2_len + l3_len > rte_pktmbuf_data_len(mbuf)))
			return;

		cksum = ~rte_raw_cksum(iph, l3_len);
		mbuf->ol_flags |= cksum ?
			PKT_RX_IP_CKSUM_BAD :
			PKT_RX_IP_CKSUM_GOOD;
	} else if (l3 == RTE_PTYPE_L3_IPV6) {
		l3_len = sizeof(struct ipv6_hdr);
	} else {
		/* IPv6 extensions are not supported */
		return;
	}
	if (l4 == RTE_PTYPE_L4_UDP || l4 == RTE_PTYPE_L4_TCP) {
		l4_hdr = rte_pktmbuf_mtod_offset(mbuf, void *, l2_len + l3_len);
		/* Don't verify checksum for multi-segment packets. */
		if (mbuf->nb_segs > 1)
			return;
		if (l3 == RTE_PTYPE_L3_IPV4)
			cksum = ~rte_ipv4_udptcp_cksum(l3_hdr, l4_hdr);
		else if (l3 == RTE_PTYPE_L3_IPV6)
			cksum = ~rte_ipv6_udptcp_cksum(l3_hdr, l4_hdr);
		mbuf->ol_flags |= cksum ?
			PKT_RX_L4_CKSUM_BAD :
			PKT_RX_L4_CKSUM_GOOD;
	}
}

static uint64_t
tap_rx_offload_get_port_capa(void)
{
	/*
	 * In order to support legacy apps,
	 * report capabilities also as port capabilities.
	 */
	return DEV_RX_OFFLOAD_SCATTER |
	       DEV_RX_OFFLOAD_IPV4_CKSUM |
	       DEV_RX_OFFLOAD_UDP_CKSUM |
	       DEV_RX_OFFLOAD_TCP_CKSUM |
	       DEV_RX_OFFLOAD_CRC_STRIP;
}

static uint64_t
tap_rx_offload_get_queue_capa(void)
{
	return DEV_RX_OFFLOAD_SCATTER |
	       DEV_RX_OFFLOAD_IPV4_CKSUM |
	       DEV_RX_OFFLOAD_UDP_CKSUM |
	       DEV_RX_OFFLOAD_TCP_CKSUM |
	       DEV_RX_OFFLOAD_CRC_STRIP;
}

static bool
tap_rxq_are_offloads_valid(struct rte_eth_dev *dev, uint64_t offloads)
{
	uint64_t port_offloads = dev->data->dev_conf.rxmode.offloads;
	uint64_t queue_supp_offloads = tap_rx_offload_get_queue_capa();
	uint64_t port_supp_offloads = tap_rx_offload_get_port_capa();

	if ((offloads & (queue_supp_offloads | port_supp_offloads)) !=
	    offloads)
		return false;
	if ((port_offloads ^ offloads) & port_supp_offloads)
		return false;
	return true;
}

/* Callback to handle the rx burst of packets to the correct interface and
 * file descriptor(s) in a multi-queue setup.
 */
static uint16_t
pmd_rx_burst(void *queue, struct rte_mbuf **bufs, uint16_t nb_pkts)
{
	struct rx_queue *rxq = queue;
	uint16_t num_rx;
	unsigned long num_rx_bytes = 0;
	uint32_t trigger = tap_trigger;

	if (trigger == rxq->trigger_seen)
		return 0;
	if (trigger)
		rxq->trigger_seen = trigger;
	rte_compiler_barrier();
	for (num_rx = 0; num_rx < nb_pkts; ) {
		struct rte_mbuf *mbuf = rxq->pool;
		struct rte_mbuf *seg = NULL;
		struct rte_mbuf *new_tail = NULL;
		uint16_t data_off = rte_pktmbuf_headroom(mbuf);
		int len;

		len = readv(rxq->fd, *rxq->iovecs,
			    1 +
			    (rxq->rxmode->offloads & DEV_RX_OFFLOAD_SCATTER ?
			     rxq->nb_rx_desc : 1));
		if (len < (int)sizeof(struct tun_pi))
			break;

		/* Packet couldn't fit in the provided mbuf */
		if (unlikely(rxq->pi.flags & TUN_PKT_STRIP)) {
			rxq->stats.ierrors++;
			continue;
		}

		len -= sizeof(struct tun_pi);

		mbuf->pkt_len = len;
		mbuf->port = rxq->in_port;
		while (1) {
			struct rte_mbuf *buf = rte_pktmbuf_alloc(rxq->mp);

			if (unlikely(!buf)) {
				rxq->stats.rx_nombuf++;
				/* No new buf has been allocated: do nothing */
				if (!new_tail || !seg)
					goto end;

				seg->next = NULL;
				rte_pktmbuf_free(mbuf);

				goto end;
			}
			seg = seg ? seg->next : mbuf;
			if (rxq->pool == mbuf)
				rxq->pool = buf;
			if (new_tail)
				new_tail->next = buf;
			new_tail = buf;
			new_tail->next = seg->next;

			/* iovecs[0] is reserved for packet info (pi) */
			(*rxq->iovecs)[mbuf->nb_segs].iov_len =
				buf->buf_len - data_off;
			(*rxq->iovecs)[mbuf->nb_segs].iov_base =
				(char *)buf->buf_addr + data_off;

			seg->data_len = RTE_MIN(seg->buf_len - data_off, len);
			seg->data_off = data_off;

			len -= seg->data_len;
			if (len <= 0)
				break;
			mbuf->nb_segs++;
			/* First segment has headroom, not the others */
			data_off = 0;
		}
		seg->next = NULL;
		mbuf->packet_type = rte_net_get_ptype(mbuf, NULL,
						      RTE_PTYPE_ALL_MASK);
		if (rxq->rxmode->offloads & DEV_RX_OFFLOAD_CHECKSUM)
			tap_verify_csum(mbuf);

		/* account for the receive frame */
		bufs[num_rx++] = mbuf;
		num_rx_bytes += mbuf->pkt_len;
	}
end:
	rxq->stats.ipackets += num_rx;
	rxq->stats.ibytes += num_rx_bytes;

	return num_rx;
}

static uint64_t
tap_tx_offload_get_port_capa(void)
{
	/*
	 * In order to support legacy apps,
	 * report capabilities also as port capabilities.
	 */
	return DEV_TX_OFFLOAD_MULTI_SEGS |
	       DEV_TX_OFFLOAD_IPV4_CKSUM |
	       DEV_TX_OFFLOAD_UDP_CKSUM |
	       DEV_TX_OFFLOAD_TCP_CKSUM;
}

static uint64_t
tap_tx_offload_get_queue_capa(void)
{
	return DEV_TX_OFFLOAD_MULTI_SEGS |
	       DEV_TX_OFFLOAD_IPV4_CKSUM |
	       DEV_TX_OFFLOAD_UDP_CKSUM |
	       DEV_TX_OFFLOAD_TCP_CKSUM;
}

static bool
tap_txq_are_offloads_valid(struct rte_eth_dev *dev, uint64_t offloads)
{
	uint64_t port_offloads = dev->data->dev_conf.txmode.offloads;
	uint64_t queue_supp_offloads = tap_tx_offload_get_queue_capa();
	uint64_t port_supp_offloads = tap_tx_offload_get_port_capa();

	if ((offloads & (queue_supp_offloads | port_supp_offloads)) !=
	    offloads)
		return false;
	/* Verify we have no conflict with port offloads */
	if ((port_offloads ^ offloads) & port_supp_offloads)
		return false;
	return true;
}

static void
tap_tx_offload(char *packet, uint64_t ol_flags, unsigned int l2_len,
	       unsigned int l3_len)
{
	void *l3_hdr = packet + l2_len;

	if (ol_flags & (PKT_TX_IP_CKSUM | PKT_TX_IPV4)) {
		struct ipv4_hdr *iph = l3_hdr;
		uint16_t cksum;

		iph->hdr_checksum = 0;
		cksum = rte_raw_cksum(iph, l3_len);
		iph->hdr_checksum = (cksum == 0xffff) ? cksum : ~cksum;
	}
	if (ol_flags & PKT_TX_L4_MASK) {
		uint16_t l4_len;
		uint32_t cksum;
		uint16_t *l4_cksum;
		void *l4_hdr;

		l4_hdr = packet + l2_len + l3_len;
		if ((ol_flags & PKT_TX_L4_MASK) == PKT_TX_UDP_CKSUM)
			l4_cksum = &((struct udp_hdr *)l4_hdr)->dgram_cksum;
		else if ((ol_flags & PKT_TX_L4_MASK) == PKT_TX_TCP_CKSUM)
			l4_cksum = &((struct tcp_hdr *)l4_hdr)->cksum;
		else
			return;
		*l4_cksum = 0;
		if (ol_flags & PKT_TX_IPV4) {
			struct ipv4_hdr *iph = l3_hdr;

			l4_len = rte_be_to_cpu_16(iph->total_length) - l3_len;
			cksum = rte_ipv4_phdr_cksum(l3_hdr, 0);
		} else {
			struct ipv6_hdr *ip6h = l3_hdr;

			/* payload_len does not include ext headers */
			l4_len = rte_be_to_cpu_16(ip6h->payload_len) -
				l3_len + sizeof(struct ipv6_hdr);
			cksum = rte_ipv6_phdr_cksum(l3_hdr, 0);
		}
		cksum += rte_raw_cksum(l4_hdr, l4_len);
		cksum = ((cksum & 0xffff0000) >> 16) + (cksum & 0xffff);
		cksum = (~cksum) & 0xffff;
		if (cksum == 0)
			cksum = 0xffff;
		*l4_cksum = cksum;
	}
}

/* Callback to handle sending packets from the tap interface
 */
static uint16_t
pmd_tx_burst(void *queue, struct rte_mbuf **bufs, uint16_t nb_pkts)
{
	struct tx_queue *txq = queue;
	uint16_t num_tx = 0;
	unsigned long num_tx_bytes = 0;
	uint32_t max_size;
	int i;

	if (unlikely(nb_pkts == 0))
		return 0;

	max_size = *txq->mtu + (ETHER_HDR_LEN + ETHER_CRC_LEN + 4);
	for (i = 0; i < nb_pkts; i++) {
		struct rte_mbuf *mbuf = bufs[num_tx];
		struct iovec iovecs[mbuf->nb_segs + 1];
		struct tun_pi pi = { .flags = 0 };
		struct rte_mbuf *seg = mbuf;
		char m_copy[mbuf->data_len];
		int n;
		int j;

		/* stats.errs will be incremented */
		if (rte_pktmbuf_pkt_len(mbuf) > max_size)
			break;

		iovecs[0].iov_base = &pi;
		iovecs[0].iov_len = sizeof(pi);
		for (j = 1; j <= mbuf->nb_segs; j++) {
			iovecs[j].iov_len = rte_pktmbuf_data_len(seg);
			iovecs[j].iov_base =
				rte_pktmbuf_mtod(seg, void *);
			seg = seg->next;
		}
		if (txq->csum &&
		    ((mbuf->ol_flags & (PKT_TX_IP_CKSUM | PKT_TX_IPV4) ||
		     (mbuf->ol_flags & PKT_TX_L4_MASK) == PKT_TX_UDP_CKSUM ||
		     (mbuf->ol_flags & PKT_TX_L4_MASK) == PKT_TX_TCP_CKSUM))) {
			/* Support only packets with all data in the same seg */
			if (mbuf->nb_segs > 1)
				break;
			/* To change checksums, work on a copy of data. */
			rte_memcpy(m_copy, rte_pktmbuf_mtod(mbuf, void *),
				   rte_pktmbuf_data_len(mbuf));
			tap_tx_offload(m_copy, mbuf->ol_flags,
				       mbuf->l2_len, mbuf->l3_len);
			iovecs[1].iov_base = m_copy;
		}
		/* copy the tx frame data */
		n = writev(txq->fd, iovecs, mbuf->nb_segs + 1);
		if (n <= 0)
			break;

		num_tx++;
		num_tx_bytes += mbuf->pkt_len;
		rte_pktmbuf_free(mbuf);
	}

	txq->stats.opackets += num_tx;
	txq->stats.errs += nb_pkts - num_tx;
	txq->stats.obytes += num_tx_bytes;

	return num_tx;
}

static const char *
tap_ioctl_req2str(unsigned long request)
{
	switch (request) {
	case SIOCSIFFLAGS:
		return "SIOCSIFFLAGS";
	case SIOCGIFFLAGS:
		return "SIOCGIFFLAGS";
	case SIOCGIFHWADDR:
		return "SIOCGIFHWADDR";
	case SIOCSIFHWADDR:
		return "SIOCSIFHWADDR";
	case SIOCSIFMTU:
		return "SIOCSIFMTU";
	}
	return "UNKNOWN";
}

static int
tap_ioctl(struct pmd_internals *pmd, unsigned long request,
	  struct ifreq *ifr, int set, enum ioctl_mode mode)
{
	short req_flags = ifr->ifr_flags;
	int remote = pmd->remote_if_index &&
		(mode == REMOTE_ONLY || mode == LOCAL_AND_REMOTE);

	if (!pmd->remote_if_index && mode == REMOTE_ONLY)
		return 0;
	/*
	 * If there is a remote netdevice, apply ioctl on it, then apply it on
	 * the tap netdevice.
	 */
apply:
	if (remote)
		snprintf(ifr->ifr_name, IFNAMSIZ, "%s", pmd->remote_iface);
	else if (mode == LOCAL_ONLY || mode == LOCAL_AND_REMOTE)
		snprintf(ifr->ifr_name, IFNAMSIZ, "%s", pmd->name);
	switch (request) {
	case SIOCSIFFLAGS:
		/* fetch current flags to leave other flags untouched */
		if (ioctl(pmd->ioctl_sock, SIOCGIFFLAGS, ifr) < 0)
			goto error;
		if (set)
			ifr->ifr_flags |= req_flags;
		else
			ifr->ifr_flags &= ~req_flags;
		break;
	case SIOCGIFFLAGS:
	case SIOCGIFHWADDR:
	case SIOCSIFHWADDR:
	case SIOCSIFMTU:
		break;
	default:
		RTE_ASSERT(!"unsupported request type: must not happen");
	}
	if (ioctl(pmd->ioctl_sock, request, ifr) < 0)
		goto error;
	if (remote-- && mode == LOCAL_AND_REMOTE)
		goto apply;
	return 0;

error:
	RTE_LOG(DEBUG, PMD, "%s: %s(%s) failed: %s(%d)\n", ifr->ifr_name,
		__func__, tap_ioctl_req2str(request), strerror(errno), errno);
	return -errno;
}

static int
tap_link_set_down(struct rte_eth_dev *dev)
{
	struct pmd_internals *pmd = dev->data->dev_private;
	struct ifreq ifr = { .ifr_flags = IFF_UP };

	dev->data->dev_link.link_status = ETH_LINK_DOWN;
	return tap_ioctl(pmd, SIOCSIFFLAGS, &ifr, 0, LOCAL_ONLY);
}

static int
tap_link_set_up(struct rte_eth_dev *dev)
{
	struct pmd_internals *pmd = dev->data->dev_private;
	struct ifreq ifr = { .ifr_flags = IFF_UP };

	dev->data->dev_link.link_status = ETH_LINK_UP;
	return tap_ioctl(pmd, SIOCSIFFLAGS, &ifr, 1, LOCAL_AND_REMOTE);
}

static int
tap_dev_start(struct rte_eth_dev *dev)
{
	int err;

	err = tap_intr_handle_set(dev, 1);
	if (err)
		return err;
	return tap_link_set_up(dev);
}

/* This function gets called when the current port gets stopped.
 */
static void
tap_dev_stop(struct rte_eth_dev *dev)
{
	tap_intr_handle_set(dev, 0);
	tap_link_set_down(dev);
}

static int
tap_dev_configure(struct rte_eth_dev *dev)
{
	uint64_t supp_tx_offloads = tap_tx_offload_get_port_capa();
	uint64_t tx_offloads = dev->data->dev_conf.txmode.offloads;

	if ((tx_offloads & supp_tx_offloads) != tx_offloads) {
		rte_errno = ENOTSUP;
		RTE_LOG(ERR, PMD,
			"Some Tx offloads are not supported "
			"requested 0x%" PRIx64 " supported 0x%" PRIx64 "\n",
			tx_offloads, supp_tx_offloads);
		return -rte_errno;
	}
	if (dev->data->nb_rx_queues > RTE_PMD_TAP_MAX_QUEUES) {
		RTE_LOG(ERR, PMD,
			"%s: number of rx queues %d exceeds max num of queues %d\n",
			dev->device->name,
			dev->data->nb_rx_queues,
			RTE_PMD_TAP_MAX_QUEUES);
		return -1;
	}
	if (dev->data->nb_tx_queues > RTE_PMD_TAP_MAX_QUEUES) {
		RTE_LOG(ERR, PMD,
			"%s: number of tx queues %d exceeds max num of queues %d\n",
			dev->device->name,
			dev->data->nb_tx_queues,
			RTE_PMD_TAP_MAX_QUEUES);
		return -1;
	}

	RTE_LOG(INFO, PMD, "%s: %p: TX configured queues number: %u\n",
	     dev->device->name, (void *)dev, dev->data->nb_tx_queues);

	RTE_LOG(INFO, PMD, "%s: %p: RX configured queues number: %u\n",
	     dev->device->name, (void *)dev, dev->data->nb_rx_queues);

	return 0;
}

static uint32_t
tap_dev_speed_capa(void)
{
	uint32_t speed = pmd_link.link_speed;
	uint32_t capa = 0;

	if (speed >= ETH_SPEED_NUM_10M)
		capa |= ETH_LINK_SPEED_10M;
	if (speed >= ETH_SPEED_NUM_100M)
		capa |= ETH_LINK_SPEED_100M;
	if (speed >= ETH_SPEED_NUM_1G)
		capa |= ETH_LINK_SPEED_1G;
	if (speed >= ETH_SPEED_NUM_5G)
		capa |= ETH_LINK_SPEED_2_5G;
	if (speed >= ETH_SPEED_NUM_5G)
		capa |= ETH_LINK_SPEED_5G;
	if (speed >= ETH_SPEED_NUM_10G)
		capa |= ETH_LINK_SPEED_10G;
	if (speed >= ETH_SPEED_NUM_20G)
		capa |= ETH_LINK_SPEED_20G;
	if (speed >= ETH_SPEED_NUM_25G)
		capa |= ETH_LINK_SPEED_25G;
	if (speed >= ETH_SPEED_NUM_40G)
		capa |= ETH_LINK_SPEED_40G;
	if (speed >= ETH_SPEED_NUM_50G)
		capa |= ETH_LINK_SPEED_50G;
	if (speed >= ETH_SPEED_NUM_56G)
		capa |= ETH_LINK_SPEED_56G;
	if (speed >= ETH_SPEED_NUM_100G)
		capa |= ETH_LINK_SPEED_100G;

	return capa;
}

static void
tap_dev_info(struct rte_eth_dev *dev, struct rte_eth_dev_info *dev_info)
{
	struct pmd_internals *internals = dev->data->dev_private;

	dev_info->if_index = internals->if_index;
	dev_info->max_mac_addrs = 1;
	dev_info->max_rx_pktlen = (uint32_t)ETHER_MAX_VLAN_FRAME_LEN;
	dev_info->max_rx_queues = RTE_PMD_TAP_MAX_QUEUES;
	dev_info->max_tx_queues = RTE_PMD_TAP_MAX_QUEUES;
	dev_info->min_rx_bufsize = 0;
	dev_info->pci_dev = NULL;
	dev_info->speed_capa = tap_dev_speed_capa();
	dev_info->rx_queue_offload_capa = tap_rx_offload_get_queue_capa();
	dev_info->rx_offload_capa = tap_rx_offload_get_port_capa() |
				    dev_info->rx_queue_offload_capa;
	dev_info->tx_queue_offload_capa = tap_tx_offload_get_queue_capa();
	dev_info->tx_offload_capa = tap_tx_offload_get_port_capa() |
				    dev_info->tx_queue_offload_capa;
}

static int
tap_stats_get(struct rte_eth_dev *dev, struct rte_eth_stats *tap_stats)
{
	unsigned int i, imax;
	unsigned long rx_total = 0, tx_total = 0, tx_err_total = 0;
	unsigned long rx_bytes_total = 0, tx_bytes_total = 0;
	unsigned long rx_nombuf = 0, ierrors = 0;
	const struct pmd_internals *pmd = dev->data->dev_private;

	/* rx queue statistics */
	imax = (dev->data->nb_rx_queues < RTE_ETHDEV_QUEUE_STAT_CNTRS) ?
		dev->data->nb_rx_queues : RTE_ETHDEV_QUEUE_STAT_CNTRS;
	for (i = 0; i < imax; i++) {
		tap_stats->q_ipackets[i] = pmd->rxq[i].stats.ipackets;
		tap_stats->q_ibytes[i] = pmd->rxq[i].stats.ibytes;
		rx_total += tap_stats->q_ipackets[i];
		rx_bytes_total += tap_stats->q_ibytes[i];
		rx_nombuf += pmd->rxq[i].stats.rx_nombuf;
		ierrors += pmd->rxq[i].stats.ierrors;
	}

	/* tx queue statistics */
	imax = (dev->data->nb_tx_queues < RTE_ETHDEV_QUEUE_STAT_CNTRS) ?
		dev->data->nb_tx_queues : RTE_ETHDEV_QUEUE_STAT_CNTRS;

	for (i = 0; i < imax; i++) {
		tap_stats->q_opackets[i] = pmd->txq[i].stats.opackets;
		tap_stats->q_errors[i] = pmd->txq[i].stats.errs;
		tap_stats->q_obytes[i] = pmd->txq[i].stats.obytes;
		tx_total += tap_stats->q_opackets[i];
		tx_err_total += tap_stats->q_errors[i];
		tx_bytes_total += tap_stats->q_obytes[i];
	}

	tap_stats->ipackets = rx_total;
	tap_stats->ibytes = rx_bytes_total;
	tap_stats->ierrors = ierrors;
	tap_stats->rx_nombuf = rx_nombuf;
	tap_stats->opackets = tx_total;
	tap_stats->oerrors = tx_err_total;
	tap_stats->obytes = tx_bytes_total;
	return 0;
}

static void
tap_stats_reset(struct rte_eth_dev *dev)
{
	int i;
	struct pmd_internals *pmd = dev->data->dev_private;

	for (i = 0; i < RTE_PMD_TAP_MAX_QUEUES; i++) {
		pmd->rxq[i].stats.ipackets = 0;
		pmd->rxq[i].stats.ibytes = 0;
		pmd->rxq[i].stats.ierrors = 0;
		pmd->rxq[i].stats.rx_nombuf = 0;

		pmd->txq[i].stats.opackets = 0;
		pmd->txq[i].stats.errs = 0;
		pmd->txq[i].stats.obytes = 0;
	}
}

static void
tap_dev_close(struct rte_eth_dev *dev)
{
	int i;
	struct pmd_internals *internals = dev->data->dev_private;

	tap_link_set_down(dev);
	tap_flow_flush(dev, NULL);
	tap_flow_implicit_flush(internals, NULL);

	for (i = 0; i < RTE_PMD_TAP_MAX_QUEUES; i++) {
		if (internals->rxq[i].fd != -1) {
			close(internals->rxq[i].fd);
			internals->rxq[i].fd = -1;
		}
		if (internals->txq[i].fd != -1) {
			close(internals->txq[i].fd);
			internals->txq[i].fd = -1;
		}
	}

	if (internals->remote_if_index) {
		/* Restore initial remote state */
		ioctl(internals->ioctl_sock, SIOCSIFFLAGS,
				&internals->remote_initial_flags);
	}
}

static void
tap_rx_queue_release(void *queue)
{
	struct rx_queue *rxq = queue;

	if (rxq && (rxq->fd > 0)) {
		close(rxq->fd);
		rxq->fd = -1;
		rte_pktmbuf_free(rxq->pool);
		rte_free(rxq->iovecs);
		rxq->pool = NULL;
		rxq->iovecs = NULL;
	}
}

static void
tap_tx_queue_release(void *queue)
{
	struct tx_queue *txq = queue;

	if (txq && (txq->fd > 0)) {
		close(txq->fd);
		txq->fd = -1;
	}
}

static int
tap_link_update(struct rte_eth_dev *dev, int wait_to_complete __rte_unused)
{
	struct rte_eth_link *dev_link = &dev->data->dev_link;
	struct pmd_internals *pmd = dev->data->dev_private;
	struct ifreq ifr = { .ifr_flags = 0 };

	if (pmd->remote_if_index) {
		tap_ioctl(pmd, SIOCGIFFLAGS, &ifr, 0, REMOTE_ONLY);
		if (!(ifr.ifr_flags & IFF_UP) ||
		    !(ifr.ifr_flags & IFF_RUNNING)) {
			dev_link->link_status = ETH_LINK_DOWN;
			return 0;
		}
	}
	tap_ioctl(pmd, SIOCGIFFLAGS, &ifr, 0, LOCAL_ONLY);
	dev_link->link_status =
		((ifr.ifr_flags & IFF_UP) && (ifr.ifr_flags & IFF_RUNNING) ?
		 ETH_LINK_UP :
		 ETH_LINK_DOWN);
	return 0;
}

static void
tap_promisc_enable(struct rte_eth_dev *dev)
{
	struct pmd_internals *pmd = dev->data->dev_private;
	struct ifreq ifr = { .ifr_flags = IFF_PROMISC };

	dev->data->promiscuous = 1;
	tap_ioctl(pmd, SIOCSIFFLAGS, &ifr, 1, LOCAL_AND_REMOTE);
	if (pmd->remote_if_index && !pmd->flow_isolate)
		tap_flow_implicit_create(pmd, TAP_REMOTE_PROMISC);
}

static void
tap_promisc_disable(struct rte_eth_dev *dev)
{
	struct pmd_internals *pmd = dev->data->dev_private;
	struct ifreq ifr = { .ifr_flags = IFF_PROMISC };

	dev->data->promiscuous = 0;
	tap_ioctl(pmd, SIOCSIFFLAGS, &ifr, 0, LOCAL_AND_REMOTE);
	if (pmd->remote_if_index && !pmd->flow_isolate)
		tap_flow_implicit_destroy(pmd, TAP_REMOTE_PROMISC);
}

static void
tap_allmulti_enable(struct rte_eth_dev *dev)
{
	struct pmd_internals *pmd = dev->data->dev_private;
	struct ifreq ifr = { .ifr_flags = IFF_ALLMULTI };

	dev->data->all_multicast = 1;
	tap_ioctl(pmd, SIOCSIFFLAGS, &ifr, 1, LOCAL_AND_REMOTE);
	if (pmd->remote_if_index && !pmd->flow_isolate)
		tap_flow_implicit_create(pmd, TAP_REMOTE_ALLMULTI);
}

static void
tap_allmulti_disable(struct rte_eth_dev *dev)
{
	struct pmd_internals *pmd = dev->data->dev_private;
	struct ifreq ifr = { .ifr_flags = IFF_ALLMULTI };

	dev->data->all_multicast = 0;
	tap_ioctl(pmd, SIOCSIFFLAGS, &ifr, 0, LOCAL_AND_REMOTE);
	if (pmd->remote_if_index && !pmd->flow_isolate)
		tap_flow_implicit_destroy(pmd, TAP_REMOTE_ALLMULTI);
}

static void
tap_mac_set(struct rte_eth_dev *dev, struct ether_addr *mac_addr)
{
	struct pmd_internals *pmd = dev->data->dev_private;
	enum ioctl_mode mode = LOCAL_ONLY;
	struct ifreq ifr;

	if (is_zero_ether_addr(mac_addr)) {
		RTE_LOG(ERR, PMD, "%s: can't set an empty MAC address\n",
			dev->device->name);
		return;
	}
	/* Check the actual current MAC address on the tap netdevice */
	if (tap_ioctl(pmd, SIOCGIFHWADDR, &ifr, 0, LOCAL_ONLY) < 0)
		return;
	if (is_same_ether_addr((struct ether_addr *)&ifr.ifr_hwaddr.sa_data,
			       mac_addr))
		return;
	/* Check the current MAC address on the remote */
	if (tap_ioctl(pmd, SIOCGIFHWADDR, &ifr, 0, REMOTE_ONLY) < 0)
		return;
	if (!is_same_ether_addr((struct ether_addr *)&ifr.ifr_hwaddr.sa_data,
			       mac_addr))
		mode = LOCAL_AND_REMOTE;
	ifr.ifr_hwaddr.sa_family = AF_LOCAL;
	rte_memcpy(ifr.ifr_hwaddr.sa_data, mac_addr, ETHER_ADDR_LEN);
	if (tap_ioctl(pmd, SIOCSIFHWADDR, &ifr, 1, mode) < 0)
		return;
	rte_memcpy(&pmd->eth_addr, mac_addr, ETHER_ADDR_LEN);
	if (pmd->remote_if_index && !pmd->flow_isolate) {
		/* Replace MAC redirection rule after a MAC change */
		if (tap_flow_implicit_destroy(pmd, TAP_REMOTE_LOCAL_MAC) < 0) {
			RTE_LOG(ERR, PMD,
				"%s: Couldn't delete MAC redirection rule\n",
				dev->device->name);
			return;
		}
		if (tap_flow_implicit_create(pmd, TAP_REMOTE_LOCAL_MAC) < 0)
			RTE_LOG(ERR, PMD,
				"%s: Couldn't add MAC redirection rule\n",
				dev->device->name);
	}
}

static int
tap_setup_queue(struct rte_eth_dev *dev,
		struct pmd_internals *internals,
		uint16_t qid,
		int is_rx)
{
	int *fd;
	int *other_fd;
	const char *dir;
	struct pmd_internals *pmd = dev->data->dev_private;
	struct rx_queue *rx = &internals->rxq[qid];
	struct tx_queue *tx = &internals->txq[qid];

	if (is_rx) {
		fd = &rx->fd;
		other_fd = &tx->fd;
		dir = "rx";
	} else {
		fd = &tx->fd;
		other_fd = &rx->fd;
		dir = "tx";
	}
	if (*fd != -1) {
		/* fd for this queue already exists */
		RTE_LOG(DEBUG, PMD, "%s: fd %d for %s queue qid %d exists\n",
			pmd->name, *fd, dir, qid);
	} else if (*other_fd != -1) {
		/* Only other_fd exists. dup it */
		*fd = dup(*other_fd);
		if (*fd < 0) {
			*fd = -1;
			RTE_LOG(ERR, PMD, "%s: dup() failed.\n",
				pmd->name);
			return -1;
		}
		RTE_LOG(DEBUG, PMD, "%s: dup fd %d for %s queue qid %d (%d)\n",
			pmd->name, *other_fd, dir, qid, *fd);
	} else {
		/* Both RX and TX fds do not exist (equal -1). Create fd */
		*fd = tun_alloc(pmd);
		if (*fd < 0) {
			*fd = -1; /* restore original value */
			RTE_LOG(ERR, PMD, "%s: tun_alloc() failed.\n",
				pmd->name);
			return -1;
		}
		RTE_LOG(DEBUG, PMD, "%s: add %s queue for qid %d fd %d\n",
			pmd->name, dir, qid, *fd);
	}

	tx->mtu = &dev->data->mtu;
	rx->rxmode = &dev->data->dev_conf.rxmode;

	return *fd;
}

static int
tap_rx_queue_setup(struct rte_eth_dev *dev,
		   uint16_t rx_queue_id,
		   uint16_t nb_rx_desc,
		   unsigned int socket_id,
		   const struct rte_eth_rxconf *rx_conf __rte_unused,
		   struct rte_mempool *mp)
{
	struct pmd_internals *internals = dev->data->dev_private;
	struct rx_queue *rxq = &internals->rxq[rx_queue_id];
	struct rte_mbuf **tmp = &rxq->pool;
	long iov_max = sysconf(_SC_IOV_MAX);
	uint16_t nb_desc = RTE_MIN(nb_rx_desc, iov_max - 1);
	struct iovec (*iovecs)[nb_desc + 1];
	int data_off = RTE_PKTMBUF_HEADROOM;
	int ret = 0;
	int fd;
	int i;

	if (rx_queue_id >= dev->data->nb_rx_queues || !mp) {
		RTE_LOG(WARNING, PMD,
			"nb_rx_queues %d too small or mempool NULL\n",
			dev->data->nb_rx_queues);
		return -1;
	}

	/* Verify application offloads are valid for our port and queue. */
	if (!tap_rxq_are_offloads_valid(dev, rx_conf->offloads)) {
		rte_errno = ENOTSUP;
		RTE_LOG(ERR, PMD,
			"%p: Rx queue offloads 0x%" PRIx64
			" don't match port offloads 0x%" PRIx64
			" or supported offloads 0x%" PRIx64 "\n",
			(void *)dev, rx_conf->offloads,
			dev->data->dev_conf.rxmode.offloads,
			(tap_rx_offload_get_port_capa() |
			 tap_rx_offload_get_queue_capa()));
		return -rte_errno;
	}
	rxq->mp = mp;
	rxq->trigger_seen = 1; /* force initial burst */
	rxq->in_port = dev->data->port_id;
	rxq->nb_rx_desc = nb_desc;
	iovecs = rte_zmalloc_socket(dev->device->name, sizeof(*iovecs), 0,
				    socket_id);
	if (!iovecs) {
		RTE_LOG(WARNING, PMD,
			"%s: Couldn't allocate %d RX descriptors\n",
			dev->device->name, nb_desc);
		return -ENOMEM;
	}
	rxq->iovecs = iovecs;

	dev->data->rx_queues[rx_queue_id] = rxq;
	fd = tap_setup_queue(dev, internals, rx_queue_id, 1);
	if (fd == -1) {
		ret = fd;
		goto error;
	}

	(*rxq->iovecs)[0].iov_len = sizeof(struct tun_pi);
	(*rxq->iovecs)[0].iov_base = &rxq->pi;

	for (i = 1; i <= nb_desc; i++) {
		*tmp = rte_pktmbuf_alloc(rxq->mp);
		if (!*tmp) {
			RTE_LOG(WARNING, PMD,
				"%s: couldn't allocate memory for queue %d\n",
				dev->device->name, rx_queue_id);
			ret = -ENOMEM;
			goto error;
		}
		(*rxq->iovecs)[i].iov_len = (*tmp)->buf_len - data_off;
		(*rxq->iovecs)[i].iov_base =
			(char *)(*tmp)->buf_addr + data_off;
		data_off = 0;
		tmp = &(*tmp)->next;
	}

	RTE_LOG(DEBUG, PMD, "  RX TAP device name %s, qid %d on fd %d\n",
		internals->name, rx_queue_id, internals->rxq[rx_queue_id].fd);

	return 0;

error:
	rte_pktmbuf_free(rxq->pool);
	rxq->pool = NULL;
	rte_free(rxq->iovecs);
	rxq->iovecs = NULL;
	return ret;
}

static int
tap_tx_queue_setup(struct rte_eth_dev *dev,
		   uint16_t tx_queue_id,
		   uint16_t nb_tx_desc __rte_unused,
		   unsigned int socket_id __rte_unused,
		   const struct rte_eth_txconf *tx_conf)
{
	struct pmd_internals *internals = dev->data->dev_private;
	struct tx_queue *txq;
	int ret;

	if (tx_queue_id >= dev->data->nb_tx_queues)
		return -1;
	dev->data->tx_queues[tx_queue_id] = &internals->txq[tx_queue_id];
	txq = dev->data->tx_queues[tx_queue_id];
	/*
	 * Don't verify port offloads for application which
	 * use the old API.
	 */
	if (tx_conf != NULL &&
	    !!(tx_conf->txq_flags & ETH_TXQ_FLAGS_IGNORE)) {
		if (tap_txq_are_offloads_valid(dev, tx_conf->offloads)) {
			txq->csum = !!(tx_conf->offloads &
					(DEV_TX_OFFLOAD_IPV4_CKSUM |
					 DEV_TX_OFFLOAD_UDP_CKSUM |
					 DEV_TX_OFFLOAD_TCP_CKSUM));
		} else {
			rte_errno = ENOTSUP;
			RTE_LOG(ERR, PMD,
				"%p: Tx queue offloads 0x%" PRIx64
				" don't match port offloads 0x%" PRIx64
				" or supported offloads 0x%" PRIx64,
				(void *)dev, tx_conf->offloads,
				dev->data->dev_conf.txmode.offloads,
				tap_tx_offload_get_port_capa());
			return -rte_errno;
		}
	}
	ret = tap_setup_queue(dev, internals, tx_queue_id, 0);
	if (ret == -1)
		return -1;
	RTE_LOG(DEBUG, PMD,
		"  TX TAP device name %s, qid %d on fd %d csum %s\n",
		internals->name, tx_queue_id, internals->txq[tx_queue_id].fd,
		txq->csum ? "on" : "off");

	return 0;
}

static int
tap_mtu_set(struct rte_eth_dev *dev, uint16_t mtu)
{
	struct pmd_internals *pmd = dev->data->dev_private;
	struct ifreq ifr = { .ifr_mtu = mtu };
	int err = 0;

	err = tap_ioctl(pmd, SIOCSIFMTU, &ifr, 1, LOCAL_AND_REMOTE);
	if (!err)
		dev->data->mtu = mtu;

	return err;
}

static int
tap_set_mc_addr_list(struct rte_eth_dev *dev __rte_unused,
		     struct ether_addr *mc_addr_set __rte_unused,
		     uint32_t nb_mc_addr __rte_unused)
{
	/*
	 * Nothing to do actually: the tap has no filtering whatsoever, every
	 * packet is received.
	 */
	return 0;
}

static int
tap_nl_msg_handler(struct nlmsghdr *nh, void *arg)
{
	struct rte_eth_dev *dev = arg;
	struct pmd_internals *pmd = dev->data->dev_private;
	struct ifinfomsg *info = NLMSG_DATA(nh);

	if (nh->nlmsg_type != RTM_NEWLINK ||
	    (info->ifi_index != pmd->if_index &&
	     info->ifi_index != pmd->remote_if_index))
		return 0;
	return tap_link_update(dev, 0);
}

static void
tap_dev_intr_handler(void *cb_arg)
{
	struct rte_eth_dev *dev = cb_arg;
	struct pmd_internals *pmd = dev->data->dev_private;

	tap_nl_recv(pmd->intr_handle.fd, tap_nl_msg_handler, dev);
}

static int
tap_lsc_intr_handle_set(struct rte_eth_dev *dev, int set)
{
	struct pmd_internals *pmd = dev->data->dev_private;

	/* In any case, disable interrupt if the conf is no longer there. */
	if (!dev->data->dev_conf.intr_conf.lsc) {
		if (pmd->intr_handle.fd != -1) {
			tap_nl_final(pmd->intr_handle.fd);
			rte_intr_callback_unregister(&pmd->intr_handle,
				tap_dev_intr_handler, dev);
		}
		return 0;
	}
	if (set) {
		pmd->intr_handle.fd = tap_nl_init(RTMGRP_LINK);
		if (unlikely(pmd->intr_handle.fd == -1))
			return -EBADF;
		return rte_intr_callback_register(
			&pmd->intr_handle, tap_dev_intr_handler, dev);
	}
	tap_nl_final(pmd->intr_handle.fd);
	return rte_intr_callback_unregister(&pmd->intr_handle,
					    tap_dev_intr_handler, dev);
}

static int
tap_intr_handle_set(struct rte_eth_dev *dev, int set)
{
	int err;

	err = tap_lsc_intr_handle_set(dev, set);
	if (err)
		return err;
	err = tap_rx_intr_vec_set(dev, set);
	if (err && set)
		tap_lsc_intr_handle_set(dev, 0);
	return err;
}

static const uint32_t*
tap_dev_supported_ptypes_get(struct rte_eth_dev *dev __rte_unused)
{
	static const uint32_t ptypes[] = {
		RTE_PTYPE_INNER_L2_ETHER,
		RTE_PTYPE_INNER_L2_ETHER_VLAN,
		RTE_PTYPE_INNER_L2_ETHER_QINQ,
		RTE_PTYPE_INNER_L3_IPV4,
		RTE_PTYPE_INNER_L3_IPV4_EXT,
		RTE_PTYPE_INNER_L3_IPV6,
		RTE_PTYPE_INNER_L3_IPV6_EXT,
		RTE_PTYPE_INNER_L4_FRAG,
		RTE_PTYPE_INNER_L4_UDP,
		RTE_PTYPE_INNER_L4_TCP,
		RTE_PTYPE_INNER_L4_SCTP,
		RTE_PTYPE_L2_ETHER,
		RTE_PTYPE_L2_ETHER_VLAN,
		RTE_PTYPE_L2_ETHER_QINQ,
		RTE_PTYPE_L3_IPV4,
		RTE_PTYPE_L3_IPV4_EXT,
		RTE_PTYPE_L3_IPV6_EXT,
		RTE_PTYPE_L3_IPV6,
		RTE_PTYPE_L4_FRAG,
		RTE_PTYPE_L4_UDP,
		RTE_PTYPE_L4_TCP,
		RTE_PTYPE_L4_SCTP,
	};

	return ptypes;
}

static int
tap_flow_ctrl_get(struct rte_eth_dev *dev __rte_unused,
		  struct rte_eth_fc_conf *fc_conf)
{
	fc_conf->mode = RTE_FC_NONE;
	return 0;
}

static int
tap_flow_ctrl_set(struct rte_eth_dev *dev __rte_unused,
		  struct rte_eth_fc_conf *fc_conf)
{
	if (fc_conf->mode != RTE_FC_NONE)
		return -ENOTSUP;
	return 0;
}

static const struct eth_dev_ops ops = {
	.dev_start              = tap_dev_start,
	.dev_stop               = tap_dev_stop,
	.dev_close              = tap_dev_close,
	.dev_configure          = tap_dev_configure,
	.dev_infos_get          = tap_dev_info,
	.rx_queue_setup         = tap_rx_queue_setup,
	.tx_queue_setup         = tap_tx_queue_setup,
	.rx_queue_release       = tap_rx_queue_release,
	.tx_queue_release       = tap_tx_queue_release,
	.flow_ctrl_get          = tap_flow_ctrl_get,
	.flow_ctrl_set          = tap_flow_ctrl_set,
	.link_update            = tap_link_update,
	.dev_set_link_up        = tap_link_set_up,
	.dev_set_link_down      = tap_link_set_down,
	.promiscuous_enable     = tap_promisc_enable,
	.promiscuous_disable    = tap_promisc_disable,
	.allmulticast_enable    = tap_allmulti_enable,
	.allmulticast_disable   = tap_allmulti_disable,
	.mac_addr_set           = tap_mac_set,
	.mtu_set                = tap_mtu_set,
	.set_mc_addr_list       = tap_set_mc_addr_list,
	.stats_get              = tap_stats_get,
	.stats_reset            = tap_stats_reset,
	.dev_supported_ptypes_get = tap_dev_supported_ptypes_get,
	.filter_ctrl            = tap_dev_filter_ctrl,
};

static int
eth_dev_tap_create(struct rte_vdev_device *vdev, char *tap_name,
		   char *remote_iface, int fixed_mac_type)
{
	int numa_node = rte_socket_id();
	struct rte_eth_dev *dev;
	struct pmd_internals *pmd;
	struct rte_eth_dev_data *data;
	struct ifreq ifr;
	int i;

	RTE_LOG(DEBUG, PMD, "  TAP device on numa %u\n", rte_socket_id());

	data = rte_zmalloc_socket(tap_name, sizeof(*data), 0, numa_node);
	if (!data) {
		RTE_LOG(ERR, PMD, "TAP Failed to allocate data\n");
		goto error_exit_nodev;
	}

	dev = rte_eth_vdev_allocate(vdev, sizeof(*pmd));
	if (!dev) {
		RTE_LOG(ERR, PMD, "TAP Unable to allocate device struct\n");
		goto error_exit_nodev;
	}

	pmd = dev->data->dev_private;
	pmd->dev = dev;
	snprintf(pmd->name, sizeof(pmd->name), "%s", tap_name);

	pmd->ioctl_sock = socket(AF_INET, SOCK_DGRAM, 0);
	if (pmd->ioctl_sock == -1) {
		RTE_LOG(ERR, PMD,
			"TAP Unable to get a socket for management: %s\n",
			strerror(errno));
		goto error_exit;
	}

	/* Setup some default values */
	rte_memcpy(data, dev->data, sizeof(*data));
	data->dev_private = pmd;
	data->dev_flags = RTE_ETH_DEV_INTR_LSC;
	data->numa_node = numa_node;

	data->dev_link = pmd_link;
	data->mac_addrs = &pmd->eth_addr;
	/* Set the number of RX and TX queues */
	data->nb_rx_queues = 0;
	data->nb_tx_queues = 0;

	dev->data = data;
	dev->dev_ops = &ops;
	dev->rx_pkt_burst = pmd_rx_burst;
	dev->tx_pkt_burst = pmd_tx_burst;

	pmd->intr_handle.type = RTE_INTR_HANDLE_EXT;
	pmd->intr_handle.fd = -1;
	dev->intr_handle = &pmd->intr_handle;

	/* Presetup the fds to -1 as being not valid */
	for (i = 0; i < RTE_PMD_TAP_MAX_QUEUES; i++) {
		pmd->rxq[i].fd = -1;
		pmd->txq[i].fd = -1;
	}

	if (fixed_mac_type) {
		/* fixed mac = 00:64:74:61:70:<iface_idx> */
		static int iface_idx;
		char mac[ETHER_ADDR_LEN] = "\0dtap";

		mac[ETHER_ADDR_LEN - 1] = iface_idx++;
		rte_memcpy(&pmd->eth_addr, mac, ETHER_ADDR_LEN);
	} else {
		eth_random_addr((uint8_t *)&pmd->eth_addr);
	}

	/* Immediately create the netdevice (this will create the 1st queue). */
	/* rx queue */
	if (tap_setup_queue(dev, pmd, 0, 1) == -1)
		goto error_exit;
	/* tx queue */
	if (tap_setup_queue(dev, pmd, 0, 0) == -1)
		goto error_exit;

	ifr.ifr_mtu = dev->data->mtu;
	if (tap_ioctl(pmd, SIOCSIFMTU, &ifr, 1, LOCAL_AND_REMOTE) < 0)
		goto error_exit;

	memset(&ifr, 0, sizeof(struct ifreq));
	ifr.ifr_hwaddr.sa_family = AF_LOCAL;
	rte_memcpy(ifr.ifr_hwaddr.sa_data, &pmd->eth_addr, ETHER_ADDR_LEN);
	if (tap_ioctl(pmd, SIOCSIFHWADDR, &ifr, 0, LOCAL_ONLY) < 0)
		goto error_exit;

	/*
	 * Set up everything related to rte_flow:
	 * - netlink socket
	 * - tap / remote if_index
	 * - mandatory QDISCs
	 * - rte_flow actual/implicit lists
	 * - implicit rules
	 */
	pmd->nlsk_fd = tap_nl_init(0);
	if (pmd->nlsk_fd == -1) {
		RTE_LOG(WARNING, PMD, "%s: failed to create netlink socket.\n",
			pmd->name);
		goto disable_rte_flow;
	}
	pmd->if_index = if_nametoindex(pmd->name);
	if (!pmd->if_index) {
		RTE_LOG(ERR, PMD, "%s: failed to get if_index.\n", pmd->name);
		goto disable_rte_flow;
	}
	if (qdisc_create_multiq(pmd->nlsk_fd, pmd->if_index) < 0) {
		RTE_LOG(ERR, PMD, "%s: failed to create multiq qdisc.\n",
			pmd->name);
		goto disable_rte_flow;
	}
	if (qdisc_create_ingress(pmd->nlsk_fd, pmd->if_index) < 0) {
		RTE_LOG(ERR, PMD, "%s: failed to create ingress qdisc.\n",
			pmd->name);
		goto disable_rte_flow;
	}
	LIST_INIT(&pmd->flows);

	if (strlen(remote_iface)) {
		pmd->remote_if_index = if_nametoindex(remote_iface);
		if (!pmd->remote_if_index) {
			RTE_LOG(ERR, PMD, "%s: failed to get %s if_index.\n",
				pmd->name, remote_iface);
			goto error_remote;
		}
		snprintf(pmd->remote_iface, RTE_ETH_NAME_MAX_LEN,
			 "%s", remote_iface);

		/* Save state of remote device */
		tap_ioctl(pmd, SIOCGIFFLAGS, &pmd->remote_initial_flags, 0, REMOTE_ONLY);

		/* Replicate remote MAC address */
		if (tap_ioctl(pmd, SIOCGIFHWADDR, &ifr, 0, REMOTE_ONLY) < 0) {
			RTE_LOG(ERR, PMD, "%s: failed to get %s MAC address.\n",
				pmd->name, pmd->remote_iface);
			goto error_remote;
		}
		rte_memcpy(&pmd->eth_addr, ifr.ifr_hwaddr.sa_data,
			   ETHER_ADDR_LEN);
		/* The desired MAC is already in ifreq after SIOCGIFHWADDR. */
		if (tap_ioctl(pmd, SIOCSIFHWADDR, &ifr, 0, LOCAL_ONLY) < 0) {
			RTE_LOG(ERR, PMD, "%s: failed to get %s MAC address.\n",
				pmd->name, remote_iface);
			goto error_remote;
		}

		/*
		 * Flush usually returns negative value because it tries to
		 * delete every QDISC (and on a running device, one QDISC at
		 * least is needed). Ignore negative return value.
		 */
		qdisc_flush(pmd->nlsk_fd, pmd->remote_if_index);
		if (qdisc_create_ingress(pmd->nlsk_fd,
					 pmd->remote_if_index) < 0) {
			RTE_LOG(ERR, PMD, "%s: failed to create ingress qdisc.\n",
				pmd->remote_iface);
			goto error_remote;
		}
		LIST_INIT(&pmd->implicit_flows);
		if (tap_flow_implicit_create(pmd, TAP_REMOTE_TX) < 0 ||
		    tap_flow_implicit_create(pmd, TAP_REMOTE_LOCAL_MAC) < 0 ||
		    tap_flow_implicit_create(pmd, TAP_REMOTE_BROADCAST) < 0 ||
		    tap_flow_implicit_create(pmd, TAP_REMOTE_BROADCASTV6) < 0) {
			RTE_LOG(ERR, PMD,
				"%s: failed to create implicit rules.\n",
				pmd->name);
			goto error_remote;
		}
	}

	return 0;

disable_rte_flow:
	RTE_LOG(ERR, PMD, " Disabling rte flow support: %s(%d)\n",
		strerror(errno), errno);
	if (strlen(remote_iface)) {
		RTE_LOG(ERR, PMD, "Remote feature requires flow support.\n");
		goto error_exit;
	}
	return 0;

error_remote:
	RTE_LOG(ERR, PMD, " Can't set up remote feature: %s(%d)\n",
		strerror(errno), errno);
	tap_flow_implicit_flush(pmd, NULL);

error_exit:
	if (pmd->ioctl_sock > 0)
		close(pmd->ioctl_sock);
	rte_eth_dev_release_port(dev);

error_exit_nodev:
	RTE_LOG(ERR, PMD, "TAP Unable to initialize %s\n",
		rte_vdev_device_name(vdev));

	rte_free(data);
	return -EINVAL;
}

static int
set_interface_name(const char *key __rte_unused,
		   const char *value,
		   void *extra_args)
{
	char *name = (char *)extra_args;

	if (value)
		snprintf(name, RTE_ETH_NAME_MAX_LEN - 1, "%s", value);
	else
		snprintf(name, RTE_ETH_NAME_MAX_LEN - 1, "%s%d",
			 DEFAULT_TAP_NAME, (tap_unit - 1));

	return 0;
}

static int
set_remote_iface(const char *key __rte_unused,
		 const char *value,
		 void *extra_args)
{
	char *name = (char *)extra_args;

	if (value)
		snprintf(name, RTE_ETH_NAME_MAX_LEN, "%s", value);

	return 0;
}

static int
set_mac_type(const char *key __rte_unused,
	     const char *value,
	     void *extra_args)
{
	if (value &&
	    !strncasecmp(ETH_TAP_MAC_FIXED, value, strlen(ETH_TAP_MAC_FIXED)))
		*(int *)extra_args = 1;
	return 0;
}

/* Open a TAP interface device.
 */
static int
rte_pmd_tap_probe(struct rte_vdev_device *dev)
{
	const char *name, *params;
	int ret;
	struct rte_kvargs *kvlist = NULL;
	int speed;
	char tap_name[RTE_ETH_NAME_MAX_LEN];
	char remote_iface[RTE_ETH_NAME_MAX_LEN];
	int fixed_mac_type = 0;

	name = rte_vdev_device_name(dev);
	params = rte_vdev_device_args(dev);

	speed = ETH_SPEED_NUM_10G;
	snprintf(tap_name, sizeof(tap_name), "%s%d",
		 DEFAULT_TAP_NAME, tap_unit++);
	memset(remote_iface, 0, RTE_ETH_NAME_MAX_LEN);

	if (params && (params[0] != '\0')) {
		RTE_LOG(DEBUG, PMD, "parameters (%s)\n", params);

		kvlist = rte_kvargs_parse(params, valid_arguments);
		if (kvlist) {
			if (rte_kvargs_count(kvlist, ETH_TAP_IFACE_ARG) == 1) {
				ret = rte_kvargs_process(kvlist,
							 ETH_TAP_IFACE_ARG,
							 &set_interface_name,
							 tap_name);
				if (ret == -1)
					goto leave;
			}

			if (rte_kvargs_count(kvlist, ETH_TAP_REMOTE_ARG) == 1) {
				ret = rte_kvargs_process(kvlist,
							 ETH_TAP_REMOTE_ARG,
							 &set_remote_iface,
							 remote_iface);
				if (ret == -1)
					goto leave;
			}

			if (rte_kvargs_count(kvlist, ETH_TAP_MAC_ARG) == 1) {
				ret = rte_kvargs_process(kvlist,
							 ETH_TAP_MAC_ARG,
							 &set_mac_type,
							 &fixed_mac_type);
				if (ret == -1)
					goto leave;
			}
		}
	}
	pmd_link.link_speed = speed;

	RTE_LOG(NOTICE, PMD, "Initializing pmd_tap for %s as %s\n",
		name, tap_name);

	ret = eth_dev_tap_create(dev, tap_name, remote_iface, fixed_mac_type);

leave:
	if (ret == -1) {
		RTE_LOG(ERR, PMD, "Failed to create pmd for %s as %s\n",
			name, tap_name);
		tap_unit--;		/* Restore the unit number */
	}
	rte_kvargs_free(kvlist);

	return ret;
}

/* detach a TAP device.
 */
static int
rte_pmd_tap_remove(struct rte_vdev_device *dev)
{
	struct rte_eth_dev *eth_dev = NULL;
	struct pmd_internals *internals;
	int i;

	RTE_LOG(DEBUG, PMD, "Closing TUN/TAP Ethernet device on numa %u\n",
		rte_socket_id());

	/* find the ethdev entry */
	eth_dev = rte_eth_dev_allocated(rte_vdev_device_name(dev));
	if (!eth_dev)
		return 0;

	internals = eth_dev->data->dev_private;
	if (internals->nlsk_fd) {
		tap_flow_flush(eth_dev, NULL);
		tap_flow_implicit_flush(internals, NULL);
		tap_nl_final(internals->nlsk_fd);
	}
	for (i = 0; i < RTE_PMD_TAP_MAX_QUEUES; i++) {
		if (internals->rxq[i].fd != -1) {
			close(internals->rxq[i].fd);
			internals->rxq[i].fd = -1;
		}
		if (internals->txq[i].fd != -1) {
			close(internals->txq[i].fd);
			internals->txq[i].fd = -1;
		}
	}

	close(internals->ioctl_sock);
	rte_free(eth_dev->data->dev_private);
	rte_free(eth_dev->data);

	rte_eth_dev_release_port(eth_dev);

	return 0;
}

static struct rte_vdev_driver pmd_tap_drv = {
	.probe = rte_pmd_tap_probe,
	.remove = rte_pmd_tap_remove,
};
RTE_PMD_REGISTER_VDEV(net_tap, pmd_tap_drv);
RTE_PMD_REGISTER_ALIAS(net_tap, eth_tap);
RTE_PMD_REGISTER_PARAM_STRING(net_tap,
			      ETH_TAP_IFACE_ARG "=<string> "
			      ETH_TAP_MAC_ARG "=" ETH_TAP_MAC_FIXED " "
			      ETH_TAP_REMOTE_ARG "=<string>");
