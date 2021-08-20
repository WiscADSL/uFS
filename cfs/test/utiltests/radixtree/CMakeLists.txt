set(RADIX_TESTS
    erase
    find
    greedy_match
    insert
    iterator
    longest_match
    prefix_match
)

foreach(RADIX_TEST ${RADIX_TESTS})
    add_executable(radixtree_utest_${RADIX_TEST} common.hpp test_radix_tree_${RADIX_TEST}.cpp)
    target_include_directories(radixtree_utest_${RADIX_TEST} PRIVATE .)
    target_link_libraries(radixtree_utest_${RADIX_TEST} gtest pthread)
endforeach(RADIX_TEST)

