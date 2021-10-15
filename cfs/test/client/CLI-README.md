# uFS's CLI

CLI is used as a simple tool to examine and test uFS, which requires to manually start uServer.

Here is the basic exmple:

```
# mkfs thus the device is in uFS's on-disk format
$ cd cfs/build/test/fsproc/
$ sudo ./testRWFsUtil mkfs

# Run uFS and command line tool
## Terminal 1 -- server
$ cd cfs/build/
$ cp ../test/sample_config/*.conf .
$ sudo su           # the driver requires root permission
$ ./fsMain 1 1 1 /tmp/cfs_exit spdk_dev.conf 1 fsp.conf

## Terminal 2 -- client
$ cd cfs/build/test/client
$ ./testAppCli 1
### now its inside uFS cli
> help              # will show how to use the cli tool (like ls, open, read etc.)
### simply try this and see what's the outcome
> lisdir /
> quit              # exit cli

# - Kill uFS server
## In *Terminal 1*
$ ctrl+c
## Or try this command and wait a bit to let uFS gracefully exit
$ touch /tmp/cfs_exit
```
