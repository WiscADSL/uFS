set(TEST_BATCH_SCRIPTS
    delete-mkfs-tests.txt
    test-simple-create-subdir.txt
    test-rand-macro.txt
    create-lots-of-dirs.txt
    create-lots-of-files.txt
    create-file-2-extent.txt
    create-file-3-extent.txt
    test-fs-rw-tc1-0.txt
    test-fs-rw-tc1-1.txt
    test-fs-rw-tc1-2.txt
    test-fs-ccread-tc0-orig.txt
    test-fs-ccread-tc0.txt
    unlink-tc1-0.txt
    unlink-tc1-1.txt
    unlink-tc1-2.txt)

foreach(TEST_BATCH_SCRIPT ${TEST_BATCH_SCRIPTS})
  configure_file(${TEST_BATCH_SCRIPT} ${TEST_BATCH_SCRIPT} COPYONLY)
endforeach(TEST_BATCH_SCRIPT)
