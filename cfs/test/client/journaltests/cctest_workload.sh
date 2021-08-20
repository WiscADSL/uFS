#run after starting fsp with 4 workers
#sudo rm -                                                                     \
    rf / tmp / readyFile / tmp /                                               \
        cfs_exit&& sudo READY_FILE_NAME = / tmp / readyFile./ fsMain 4 4 1,    \
                        11, 22,                                                \
                        33 / tmp / cfs_exit ~ /.config / fsp / spdk.conf 1, 2, \
                        3, 4 ~ /.config / fsp / fsp.conf
for
  i in { 0..3 }
do
sudo./ journaltests / cctest_workload - a $i - n 4 - w $i 2 > &1 >
    app_$i.log& done wait
