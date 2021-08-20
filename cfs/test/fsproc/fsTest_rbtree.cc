
#include "rbtree.h"
#include "gtest/gtest.h"

namespace {

#define INDENT_STEP 4

void print_tree_helper(rbtree_node n, int indent) {
  int i;
  if (n == NULL) {
    fputs("<empty tree>", stdout);
    return;
  }
  if (n->right != NULL) {
    print_tree_helper(n->right, indent + INDENT_STEP);
  }
  for (i = 0; i < indent; i++)
    fputs(" ", stdout);
  if (n->color == BLACK)
    printf("%lu\n", (unsigned long)n->key);
  else
    printf("<%lu>\n", (unsigned long)n->key);
  if (n->left != NULL) {
    print_tree_helper(n->left, indent + INDENT_STEP);
  }
}

void print_tree(rbtree t) {
  print_tree_helper(t->root, 0);
  puts("");
}

int compare_int(node n, void *leftp, void *rightp) {
  int left = (int)((size_t)leftp);
  ;
  int right = (int)((size_t)rightp);
  ;
  fprintf(stdout, "compare_int() left:%d right:%d\n", left, right);
  if (left < right)
    return -1;
  else if (left > right)
    return 1;
  else {
    assert(left == right);
    return 0;
  }
}

TEST(RBTree_Int, T1) {
  int i;
  rbtree t = rbtree_create();
  print_tree(t);

  for (i = 0; i < 5000; i++) {
    // for (i = 0; i < 5; i++) {
    int x = rand() % 10000;
    int y = rand() % 10000;
    // print_tree(t);
    printf("Inserting %d -> %d\n\n", x, y);
    rbtree_insert(t, reinterpret_cast<void *>(x), reinterpret_cast<void *>(y),
                  reinterpret_cast<compare_func>(compare_int));
    // assert(rbtree_lookup(t, (void *)x, compare_int) == (void *)y);
    void *val = rbtree_lookup(t, reinterpret_cast<void *>(x),
                              reinterpret_cast<compare_func>(compare_int));
    long intptr = reinterpret_cast<intptr_t>(val);
    printf("lookup ret:%ld\n", intptr);
    EXPECT_EQ(intptr, y);
  }
  for (i = 0; i < 60000; i++) {
    // for (i = 0; i < 6; i++) {
    int x = rand() % 10000;
    // print_tree(t);
    printf("Deleting key %d\n\n", x);
    rbtree_delete(t, reinterpret_cast<void *>(x),
                  reinterpret_cast<compare_func>(compare_int));
  }
}

} // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
