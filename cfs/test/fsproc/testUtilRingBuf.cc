
#include "util/util_buf_ring.h"
#include <assert.h>
#include <cstring>

#define RING_SIZE 64
#define MAX_BULK 16

int test_ring_basic() {
  struct util_ring *r;
  unsigned count = RING_SIZE;
  long memsize = util_ring_get_memsize(count);
  r = (struct util_ring *)malloc(memsize);
  util_ring_init(r, "test_ring_basic", count, 0);
  fprintf(stderr, "ring mem size:%ld\n", memsize);
  fprintf(stderr, "************* ring initialized ****************\n");

  void **src = NULL, **cur_src = NULL, **dst = NULL, **cur_dst = NULL;
  int ret;
  unsigned i, num_elems;

  /* alloc dummy object pointers */
  src = (void **)malloc(RING_SIZE * 2 * sizeof(void *));
  if (src == NULL)
    goto fail;

  for (i = 0; i < RING_SIZE * 2; i++) {
    src[i] = (void *)(unsigned long)i;
  }
  cur_src = src;

  /* alloc some room for copied objects */
  dst = (void **)malloc(RING_SIZE * 2 * sizeof(void *));
  if (dst == NULL)
    goto fail;

  memset(dst, 0, RING_SIZE * 2 * sizeof(void *));
  cur_dst = dst;

  printf("enqueue 1 obj\n");
  ret = util_ring_sp_enqueue_bulk(r, cur_src, 1, NULL);
  cur_src += 1;
  if (ret == 0)
    goto fail;

  printf("enqueue 2 objs\n");
  ret = util_ring_sp_enqueue_bulk(r, cur_src, 2, NULL);
  cur_src += 2;
  if (ret == 0)
    goto fail;

  printf("enqueue MAX_BULK objs\n");
  ret = util_ring_sp_enqueue_bulk(r, cur_src, MAX_BULK, NULL);
  cur_src += MAX_BULK;
  if (ret == 0)
    goto fail;

  printf("dequeue 1 obj\n");
  ret = util_ring_sc_dequeue_bulk(r, cur_dst, 1, NULL);
  cur_dst += 1;
  if (ret == 0)
    goto fail;

  printf("dequeue 2 objs\n");
  ret = util_ring_sc_dequeue_bulk(r, cur_dst, 2, NULL);
  cur_dst += 2;
  if (ret == 0)
    goto fail;

  printf("dequeue MAX_BULK objs\n");
  ret = util_ring_sc_dequeue_bulk(r, cur_dst, MAX_BULK, NULL);
  cur_dst += MAX_BULK;
  if (ret == 0)
    goto fail;

  /* check data */
  if (memcmp(src, dst, cur_dst - dst)) {
    fprintf(stdout, "src %p, %lu", src, cur_src - src);
    fprintf(stdout, "dst %p  %lu", dst, cur_dst - dst);
    printf("data after dequeue is not the same\n");
    goto fail;
  }
  cur_src = src;
  cur_dst = dst;

  printf("enqueue 1 obj\n");
  ret = util_ring_mp_enqueue_bulk(r, cur_src, 1, NULL);
  cur_src += 1;
  if (ret == 0)
    goto fail;

  printf("enqueue 2 objs\n");
  ret = util_ring_mp_enqueue_bulk(r, cur_src, 2, NULL);
  cur_src += 2;
  if (ret == 0)
    goto fail;

  printf("enqueue MAX_BULK objs\n");
  ret = util_ring_mp_enqueue_bulk(r, cur_src, MAX_BULK, NULL);
  cur_src += MAX_BULK;
  if (ret == 0)
    goto fail;

  printf("dequeue 1 obj\n");
  ret = util_ring_mc_dequeue_bulk(r, cur_dst, 1, NULL);
  cur_dst += 1;
  if (ret == 0)
    goto fail;

  printf("dequeue 2 objs\n");
  ret = util_ring_mc_dequeue_bulk(r, cur_dst, 2, NULL);
  cur_dst += 2;
  if (ret == 0)
    goto fail;

  printf("dequeue MAX_BULK objs\n");
  ret = util_ring_mc_dequeue_bulk(r, cur_dst, MAX_BULK, NULL);
  cur_dst += MAX_BULK;
  if (ret == 0)
    goto fail;

  /* check data */
  if (memcmp(src, dst, cur_dst - dst)) {
    fprintf(stdout, "src  %p, %lu", src, cur_src - src);
    fprintf(stdout, "dst  %p, %lu", dst, cur_dst - dst);
    printf("data after dequeue is not the same\n");
    goto fail;
  }
  cur_src = src;
  cur_dst = dst;

  printf("fill and empty the ring\n");
  for (i = 0; i < RING_SIZE / MAX_BULK; i++) {
    ret = util_ring_mp_enqueue_bulk(r, cur_src, MAX_BULK, NULL);
    cur_src += MAX_BULK;
    if (ret == 0)
      goto fail;
    ret = util_ring_mc_dequeue_bulk(r, cur_dst, MAX_BULK, NULL);
    cur_dst += MAX_BULK;
    if (ret == 0)
      goto fail;
  }

  /* check data */
  if (memcmp(src, dst, cur_dst - dst)) {
    fprintf(stdout, "src  %p, %lu", src, cur_src - src);
    fprintf(stdout, "dst  %p, %lu", dst, cur_dst - dst);
    printf("data after dequeue is not the same\n");
    goto fail;
  }

  free(src);
  free(dst);
  free(r);
  return 0;

fail:
  free(src);
  free(dst);
  free(r);
  return -1;
}

int main() {
  int rt;
  rt = test_ring_basic();
  assert(rt == 0);
  return 0;
}
