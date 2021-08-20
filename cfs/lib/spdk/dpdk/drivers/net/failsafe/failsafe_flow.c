/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright 2017 6WIND S.A.
 * Copyright 2017 Mellanox.
 */

#include <sys/queue.h>

#include <rte_malloc.h>
#include <rte_tailq.h>
#include <rte_flow.h>
#include <rte_flow_driver.h>

#include "failsafe_private.h"

static struct rte_flow *
fs_flow_allocate(const struct rte_flow_attr *attr,
		 const struct rte_flow_item *items,
		 const struct rte_flow_action *actions)
{
	struct rte_flow *flow;
	size_t fdsz;

	fdsz = rte_flow_copy(NULL, 0, attr, items, actions);
	flow = rte_zmalloc(NULL,
			   sizeof(struct rte_flow) + fdsz,
			   RTE_CACHE_LINE_SIZE);
	if (flow == NULL) {
		ERROR("Could not allocate new flow");
		return NULL;
	}
	flow->fd = (void *)((uintptr_t)flow + sizeof(*flow));
	if (rte_flow_copy(flow->fd, fdsz, attr, items, actions) != fdsz) {
		ERROR("Failed to copy flow description");
		rte_free(flow);
		return NULL;
	}
	return flow;
}

static void
fs_flow_release(struct rte_flow **flow)
{
	rte_free(*flow);
	*flow = NULL;
}

static int
fs_flow_validate(struct rte_eth_dev *dev,
		 const struct rte_flow_attr *attr,
		 const struct rte_flow_item patterns[],
		 const struct rte_flow_action actions[],
		 struct rte_flow_error *error)
{
	struct sub_device *sdev;
	uint8_t i;
	int ret;

	fs_lock(dev, 0);
	FOREACH_SUBDEV_STATE(sdev, i, dev, DEV_ACTIVE) {
		DEBUG("Calling rte_flow_validate on sub_device %d", i);
		ret = rte_flow_validate(PORT_ID(sdev),
				attr, patterns, actions, error);
		if ((ret = fs_err(sdev, ret))) {
			ERROR("Operation rte_flow_validate failed for sub_device %d"
			      " with error %d", i, ret);
			fs_unlock(dev, 0);
			return ret;
		}
	}
	fs_unlock(dev, 0);
	return 0;
}

static struct rte_flow *
fs_flow_create(struct rte_eth_dev *dev,
	       const struct rte_flow_attr *attr,
	       const struct rte_flow_item patterns[],
	       const struct rte_flow_action actions[],
	       struct rte_flow_error *error)
{
	struct sub_device *sdev;
	struct rte_flow *flow;
	uint8_t i;

	fs_lock(dev, 0);
	flow = fs_flow_allocate(attr, patterns, actions);
	FOREACH_SUBDEV_STATE(sdev, i, dev, DEV_ACTIVE) {
		flow->flows[i] = rte_flow_create(PORT_ID(sdev),
				attr, patterns, actions, error);
		if (flow->flows[i] == NULL && fs_err(sdev, -rte_errno)) {
			ERROR("Failed to create flow on sub_device %d",
				i);
			goto err;
		}
	}
	TAILQ_INSERT_TAIL(&PRIV(dev)->flow_list, flow, next);
	fs_unlock(dev, 0);
	return flow;
err:
	FOREACH_SUBDEV(sdev, i, dev) {
		if (flow->flows[i] != NULL)
			rte_flow_destroy(PORT_ID(sdev),
				flow->flows[i], error);
	}
	fs_flow_release(&flow);
	fs_unlock(dev, 0);
	return NULL;
}

static int
fs_flow_destroy(struct rte_eth_dev *dev,
		struct rte_flow *flow,
		struct rte_flow_error *error)
{
	struct sub_device *sdev;
	uint8_t i;
	int ret;

	if (flow == NULL) {
		ERROR("Invalid flow");
		return -EINVAL;
	}
	ret = 0;
	fs_lock(dev, 0);
	FOREACH_SUBDEV_STATE(sdev, i, dev, DEV_ACTIVE) {
		int local_ret;

		if (flow->flows[i] == NULL)
			continue;
		local_ret = rte_flow_destroy(PORT_ID(sdev),
				flow->flows[i], error);
		if ((local_ret = fs_err(sdev, local_ret))) {
			ERROR("Failed to destroy flow on sub_device %d: %d",
					i, local_ret);
			if (ret == 0)
				ret = local_ret;
		}
	}
	TAILQ_REMOVE(&PRIV(dev)->flow_list, flow, next);
	fs_flow_release(&flow);
	fs_unlock(dev, 0);
	return ret;
}

static int
fs_flow_flush(struct rte_eth_dev *dev,
	      struct rte_flow_error *error)
{
	struct sub_device *sdev;
	struct rte_flow *flow;
	void *tmp;
	uint8_t i;
	int ret;

	fs_lock(dev, 0);
	FOREACH_SUBDEV_STATE(sdev, i, dev, DEV_ACTIVE) {
		DEBUG("Calling rte_flow_flush on sub_device %d", i);
		ret = rte_flow_flush(PORT_ID(sdev), error);
		if ((ret = fs_err(sdev, ret))) {
			ERROR("Operation rte_flow_flush failed for sub_device %d"
			      " with error %d", i, ret);
			fs_unlock(dev, 0);
			return ret;
		}
	}
	TAILQ_FOREACH_SAFE(flow, &PRIV(dev)->flow_list, next, tmp) {
		TAILQ_REMOVE(&PRIV(dev)->flow_list, flow, next);
		fs_flow_release(&flow);
	}
	fs_unlock(dev, 0);
	return 0;
}

static int
fs_flow_query(struct rte_eth_dev *dev,
	      struct rte_flow *flow,
	      enum rte_flow_action_type type,
	      void *arg,
	      struct rte_flow_error *error)
{
	struct sub_device *sdev;

	fs_lock(dev, 0);
	sdev = TX_SUBDEV(dev);
	if (sdev != NULL) {
		int ret = rte_flow_query(PORT_ID(sdev),
					 flow->flows[SUB_ID(sdev)],
					 type, arg, error);

		if ((ret = fs_err(sdev, ret))) {
			fs_unlock(dev, 0);
			return ret;
		}
	}
	fs_unlock(dev, 0);
	WARN("No active sub_device to query about its flow");
	return -1;
}

static int
fs_flow_isolate(struct rte_eth_dev *dev,
		int set,
		struct rte_flow_error *error)
{
	struct sub_device *sdev;
	uint8_t i;
	int ret;

	fs_lock(dev, 0);
	FOREACH_SUBDEV(sdev, i, dev) {
		if (sdev->state < DEV_PROBED)
			continue;
		DEBUG("Calling rte_flow_isolate on sub_device %d", i);
		if (PRIV(dev)->flow_isolated != sdev->flow_isolated)
			WARN("flow isolation mode of sub_device %d in incoherent state.",
				i);
		ret = rte_flow_isolate(PORT_ID(sdev), set, error);
		if ((ret = fs_err(sdev, ret))) {
			ERROR("Operation rte_flow_isolate failed for sub_device %d"
			      " with error %d", i, ret);
			fs_unlock(dev, 0);
			return ret;
		}
		sdev->flow_isolated = set;
	}
	PRIV(dev)->flow_isolated = set;
	fs_unlock(dev, 0);
	return 0;
}

const struct rte_flow_ops fs_flow_ops = {
	.validate = fs_flow_validate,
	.create = fs_flow_create,
	.destroy = fs_flow_destroy,
	.flush = fs_flow_flush,
	.query = fs_flow_query,
	.isolate = fs_flow_isolate,
};
