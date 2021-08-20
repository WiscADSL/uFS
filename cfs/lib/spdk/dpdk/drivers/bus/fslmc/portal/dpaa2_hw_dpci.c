/* SPDX-License-Identifier: BSD-3-Clause
 *
 *   Copyright 2017 NXP
 *
 */

#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>

#include <rte_malloc.h>
#include <rte_memcpy.h>
#include <rte_string_fns.h>
#include <rte_cycles.h>
#include <rte_kvargs.h>
#include <rte_dev.h>
#include <rte_ethdev_driver.h>

#include <fslmc_logs.h>
#include <rte_fslmc.h>
#include <mc/fsl_dpci.h>
#include "portal/dpaa2_hw_pvt.h"
#include "portal/dpaa2_hw_dpio.h"

TAILQ_HEAD(dpci_dev_list, dpaa2_dpci_dev);
static struct dpci_dev_list dpci_dev_list
	= TAILQ_HEAD_INITIALIZER(dpci_dev_list); /*!< DPCI device list */

static int
rte_dpaa2_create_dpci_device(int vdev_fd __rte_unused,
			     struct vfio_device_info *obj_info __rte_unused,
			     int dpci_id)
{
	struct dpaa2_dpci_dev *dpci_node;
	struct dpci_attr attr;
	struct dpci_rx_queue_cfg rx_queue_cfg;
	struct dpci_rx_queue_attr rx_attr;
	int ret, i;

	/* Allocate DPAA2 dpci handle */
	dpci_node = rte_malloc(NULL, sizeof(struct dpaa2_dpci_dev), 0);
	if (!dpci_node) {
		PMD_INIT_LOG(ERR, "Memory allocation failed for DPCI Device");
		return -1;
	}

	/* Open the dpci object */
	dpci_node->dpci.regs = rte_mcp_ptr_list[MC_PORTAL_INDEX];
	ret = dpci_open(&dpci_node->dpci,
			CMD_PRI_LOW, dpci_id, &dpci_node->token);
	if (ret) {
		PMD_INIT_LOG(ERR, "Resource alloc failure with err code: %d",
			     ret);
		rte_free(dpci_node);
		return -1;
	}

	/* Get the device attributes */
	ret = dpci_get_attributes(&dpci_node->dpci,
				  CMD_PRI_LOW, dpci_node->token, &attr);
	if (ret != 0) {
		PMD_INIT_LOG(ERR, "Reading device failed with err code: %d",
			     ret);
		rte_free(dpci_node);
		return -1;
	}

	/* Set up the Rx Queue */
	memset(&rx_queue_cfg, 0, sizeof(struct dpci_rx_queue_cfg));
	ret = dpci_set_rx_queue(&dpci_node->dpci,
				CMD_PRI_LOW,
				dpci_node->token,
				0, &rx_queue_cfg);
	if (ret) {
		PMD_INIT_LOG(ERR, "Setting Rx queue failed with err code: %d",
			     ret);
		rte_free(dpci_node);
		return -1;
	}

	/* Enable the device */
	ret = dpci_enable(&dpci_node->dpci,
			  CMD_PRI_LOW, dpci_node->token);
	if (ret != 0) {
		PMD_INIT_LOG(ERR, "Enabling device failed with err code: %d",
			     ret);
		rte_free(dpci_node);
		return -1;
	}

	for (i = 0; i < DPAA2_DPCI_MAX_QUEUES; i++) {
		/* Get the Rx FQID's */
		ret = dpci_get_rx_queue(&dpci_node->dpci,
					CMD_PRI_LOW,
					dpci_node->token, i,
					&rx_attr);
		if (ret != 0) {
			PMD_INIT_LOG(ERR,
				     "Reading device failed with err code: %d",
				ret);
			rte_free(dpci_node);
			return -1;
		}

		dpci_node->queue[i].fqid = rx_attr.fqid;
	}

	dpci_node->dpci_id = dpci_id;
	rte_atomic16_init(&dpci_node->in_use);

	TAILQ_INSERT_TAIL(&dpci_dev_list, dpci_node, next);

	RTE_LOG(DEBUG, PMD, "DPAA2: Added [dpci.%d]\n", dpci_id);

	return 0;
}

struct dpaa2_dpci_dev *rte_dpaa2_alloc_dpci_dev(void)
{
	struct dpaa2_dpci_dev *dpci_dev = NULL;

	/* Get DPCI dev handle from list using index */
	TAILQ_FOREACH(dpci_dev, &dpci_dev_list, next) {
		if (dpci_dev && rte_atomic16_test_and_set(&dpci_dev->in_use))
			break;
	}

	return dpci_dev;
}

void rte_dpaa2_free_dpci_dev(struct dpaa2_dpci_dev *dpci)
{
	struct dpaa2_dpci_dev *dpci_dev = NULL;

	/* Match DPCI handle and mark it free */
	TAILQ_FOREACH(dpci_dev, &dpci_dev_list, next) {
		if (dpci_dev == dpci) {
			rte_atomic16_dec(&dpci_dev->in_use);
			return;
		}
	}
}

static struct rte_dpaa2_object rte_dpaa2_dpci_obj = {
	.dev_type = DPAA2_CI,
	.create = rte_dpaa2_create_dpci_device,
};

RTE_PMD_REGISTER_DPAA2_OBJECT(dpci, rte_dpaa2_dpci_obj);
