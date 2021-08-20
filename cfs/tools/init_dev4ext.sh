#! /bin/bash

# run as root

set -e

if [ "$#" -lt 1 ]
then
    echo "Usage: ./init_dev4ext.sh <devName> [nj]"
    exit
fi

DEV_NAME=$1
IF_NJ=$2

source ./bench_common.sh
echo "$KFS_MOUNT_PATH"

mkfs -F -t ext4 "$DEV_NAME"

if [ -z "$IF_NJ" ]
then
    echo "Journal enabled"
else
    echo "Journal disabled"
    tune2fs -O ^has_journal ${DEV_NAME}
fi

mount "${DEV_NAME}" "${KFS_MOUNT_PATH}"
# verify the journal
tune2fs -l "${DEV_NAME}" | grep journal

mkdir -p "${KFS_MOUNT_PATH}"/bench/
rm -rf "${KFS_MOUNT_PATH}"/bench/*

