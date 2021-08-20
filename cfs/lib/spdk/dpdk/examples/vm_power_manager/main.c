/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <signal.h>
#include <errno.h>

#include <sys/queue.h>

#include <rte_common.h>
#include <rte_eal.h>
#include <rte_launch.h>
#include <rte_log.h>
#include <rte_per_lcore.h>
#include <rte_lcore.h>
#include <rte_ethdev.h>
#include <getopt.h>
#include <rte_cycles.h>
#include <rte_debug.h>

#include "channel_manager.h"
#include "channel_monitor.h"
#include "power_manager.h"
#include "vm_power_cli.h"
#include <rte_pmd_ixgbe.h>
#include <rte_pmd_i40e.h>
#include <rte_pmd_bnxt.h>

#define RX_RING_SIZE 1024
#define TX_RING_SIZE 1024

#define NUM_MBUFS 8191
#define MBUF_CACHE_SIZE 250
#define BURST_SIZE 32

static uint32_t enabled_port_mask;
static volatile bool force_quit;

/****************/
static const struct rte_eth_conf port_conf_default = {
	.rxmode = {
		.max_rx_pkt_len = ETHER_MAX_LEN,
		.ignore_offload_bitfield = 1,
	},
};

static inline int
port_init(uint16_t port, struct rte_mempool *mbuf_pool)
{
	struct rte_eth_conf port_conf = port_conf_default;
	const uint16_t rx_rings = 1, tx_rings = 1;
	int retval;
	uint16_t q;
	struct rte_eth_dev_info dev_info;
	struct rte_eth_txconf txq_conf;

	if (port >= rte_eth_dev_count())
		return -1;

	rte_eth_dev_info_get(port, &dev_info);
	if (dev_info.tx_offload_capa & DEV_TX_OFFLOAD_MBUF_FAST_FREE)
		port_conf.txmode.offloads |=
			DEV_TX_OFFLOAD_MBUF_FAST_FREE;

	/* Configure the Ethernet device. */
	retval = rte_eth_dev_configure(port, rx_rings, tx_rings, &port_conf);
	if (retval != 0)
		return retval;

	/* Allocate and set up 1 RX queue per Ethernet port. */
	for (q = 0; q < rx_rings; q++) {
		retval = rte_eth_rx_queue_setup(port, q, RX_RING_SIZE,
				rte_eth_dev_socket_id(port), NULL, mbuf_pool);
		if (retval < 0)
			return retval;
	}

	txq_conf = dev_info.default_txconf;
	txq_conf.txq_flags = ETH_TXQ_FLAGS_IGNORE;
	txq_conf.offloads = port_conf.txmode.offloads;
	/* Allocate and set up 1 TX queue per Ethernet port. */
	for (q = 0; q < tx_rings; q++) {
		retval = rte_eth_tx_queue_setup(port, q, TX_RING_SIZE,
				rte_eth_dev_socket_id(port), &txq_conf);
		if (retval < 0)
			return retval;
	}

	/* Start the Ethernet port. */
	retval = rte_eth_dev_start(port);
	if (retval < 0)
		return retval;

	/* Display the port MAC address. */
	struct ether_addr addr;
	rte_eth_macaddr_get(port, &addr);
	printf("Port %u MAC: %02" PRIx8 " %02" PRIx8 " %02" PRIx8
			   " %02" PRIx8 " %02" PRIx8 " %02" PRIx8 "\n",
			(unsigned int)port,
			addr.addr_bytes[0], addr.addr_bytes[1],
			addr.addr_bytes[2], addr.addr_bytes[3],
			addr.addr_bytes[4], addr.addr_bytes[5]);

	/* Enable RX in promiscuous mode for the Ethernet device. */
	rte_eth_promiscuous_enable(port);


	return 0;
}

static int
parse_portmask(const char *portmask)
{
	char *end = NULL;
	unsigned long pm;

	/* parse hexadecimal string */
	pm = strtoul(portmask, &end, 16);
	if ((portmask[0] == '\0') || (end == NULL) || (*end != '\0'))
		return -1;

	if (pm == 0)
		return -1;

	return pm;
}
/* Parse the argument given in the command line of the application */
static int
parse_args(int argc, char **argv)
{
	int opt, ret;
	char **argvopt;
	int option_index;
	char *prgname = argv[0];
	static struct option lgopts[] = {
		{ "mac-updating", no_argument, 0, 1},
		{ "no-mac-updating", no_argument, 0, 0},
		{NULL, 0, 0, 0}
	};
	argvopt = argv;

	while ((opt = getopt_long(argc, argvopt, "p:q:T:",
				  lgopts, &option_index)) != EOF) {

		switch (opt) {
		/* portmask */
		case 'p':
			enabled_port_mask = parse_portmask(optarg);
			if (enabled_port_mask == 0) {
				printf("invalid portmask\n");
				return -1;
			}
			break;
		/* long options */
		case 0:
			break;

		default:
			return -1;
		}
	}

	if (optind >= 0)
		argv[optind-1] = prgname;

	ret = optind-1;
	optind = 0; /* reset getopt lib */
	return ret;
}

static void
check_all_ports_link_status(uint16_t port_num, uint32_t port_mask)
{
#define CHECK_INTERVAL 100 /* 100ms */
#define MAX_CHECK_TIME 90 /* 9s (90 * 100ms) in total */
	uint16_t portid, count, all_ports_up, print_flag = 0;
	struct rte_eth_link link;

	printf("\nChecking link status");
	fflush(stdout);
	for (count = 0; count <= MAX_CHECK_TIME; count++) {
		if (force_quit)
			return;
		all_ports_up = 1;
		for (portid = 0; portid < port_num; portid++) {
			if (force_quit)
				return;
			if ((port_mask & (1 << portid)) == 0)
				continue;
			memset(&link, 0, sizeof(link));
			rte_eth_link_get_nowait(portid, &link);
			/* print link status if flag set */
			if (print_flag == 1) {
				if (link.link_status)
					printf("Port %d Link Up - speed %u "
						"Mbps - %s\n", (uint16_t)portid,
						(unsigned int)link.link_speed,
				(link.link_duplex == ETH_LINK_FULL_DUPLEX) ?
					("full-duplex") : ("half-duplex\n"));
				else
					printf("Port %d Link Down\n",
						(uint16_t)portid);
				continue;
			}
		       /* clear all_ports_up flag if any link down */
			if (link.link_status == ETH_LINK_DOWN) {
				all_ports_up = 0;
				break;
			}
		}
		/* after finally printing all link status, get out */
		if (print_flag == 1)
			break;

		if (all_ports_up == 0) {
			printf(".");
			fflush(stdout);
			rte_delay_ms(CHECK_INTERVAL);
		}

		/* set the print_flag if all ports up or timeout */
		if (all_ports_up == 1 || count == (MAX_CHECK_TIME - 1)) {
			print_flag = 1;
			printf("done\n");
		}
	}
}
static int
run_monitor(__attribute__((unused)) void *arg)
{
	if (channel_monitor_init() < 0) {
		printf("Unable to initialize channel monitor\n");
		return -1;
	}
	run_channel_monitor();
	return 0;
}

static void
sig_handler(int signo)
{
	printf("Received signal %d, exiting...\n", signo);
	channel_monitor_exit();
	channel_manager_exit();
	power_manager_exit();

}

int
main(int argc, char **argv)
{
	int ret;
	unsigned lcore_id;
	unsigned int nb_ports;
	struct rte_mempool *mbuf_pool;
	uint16_t portid;


	ret = rte_eal_init(argc, argv);
	if (ret < 0)
		rte_panic("Cannot init EAL\n");

	signal(SIGINT, sig_handler);
	signal(SIGTERM, sig_handler);

	argc -= ret;
	argv += ret;

	/* parse application arguments (after the EAL ones) */
	ret = parse_args(argc, argv);
	if (ret < 0)
		rte_exit(EXIT_FAILURE, "Invalid arguments\n");

	nb_ports = rte_eth_dev_count();

	mbuf_pool = rte_pktmbuf_pool_create("MBUF_POOL", NUM_MBUFS * nb_ports,
		MBUF_CACHE_SIZE, 0, RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());

	if (mbuf_pool == NULL)
		rte_exit(EXIT_FAILURE, "Cannot create mbuf pool\n");

	/* Initialize ports. */
	for (portid = 0; portid < nb_ports; portid++) {
		struct ether_addr eth;
		int w, j;
		int ret;

		if ((enabled_port_mask & (1 << portid)) == 0)
			continue;

		eth.addr_bytes[0] = 0xe0;
		eth.addr_bytes[1] = 0xe0;
		eth.addr_bytes[2] = 0xe0;
		eth.addr_bytes[3] = 0xe0;
		eth.addr_bytes[4] = portid + 0xf0;

		if (port_init(portid, mbuf_pool) != 0)
			rte_exit(EXIT_FAILURE, "Cannot init port %"PRIu8 "\n",
					portid);

		for (w = 0; w < MAX_VFS; w++) {
			eth.addr_bytes[5] = w + 0xf0;

			ret = rte_pmd_ixgbe_set_vf_mac_addr(portid,
						w, &eth);
			if (ret == -ENOTSUP)
				ret = rte_pmd_i40e_set_vf_mac_addr(portid,
						w, &eth);
			if (ret == -ENOTSUP)
				ret = rte_pmd_bnxt_set_vf_mac_addr(portid,
						w, &eth);

			switch (ret) {
			case 0:
				printf("Port %d VF %d MAC: ",
						portid, w);
				for (j = 0; j < 6; j++) {
					printf("%02x", eth.addr_bytes[j]);
					if (j < 5)
						printf(":");
				}
				printf("\n");
				break;
			}
		}
	}

	lcore_id = rte_get_next_lcore(-1, 1, 0);
	if (lcore_id == RTE_MAX_LCORE) {
		RTE_LOG(ERR, EAL, "A minimum of two cores are required to run "
				"application\n");
		return 0;
	}

	check_all_ports_link_status(nb_ports, enabled_port_mask);
	rte_eal_remote_launch(run_monitor, NULL, lcore_id);

	if (power_manager_init() < 0) {
		printf("Unable to initialize power manager\n");
		return -1;
	}
	if (channel_manager_init(CHANNEL_MGR_DEFAULT_HV_PATH) < 0) {
		printf("Unable to initialize channel manager\n");
		return -1;
	}
	run_cli(NULL);

	rte_eal_mp_wait_lcore();
	return 0;
}
