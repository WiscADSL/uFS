/*
 * Standalone utility module to dump metadata from the journal
 * NOTE: copied and modified offlineCheckpointer.
 * There is a lot of reusable code.
 * TODO: create fsp spdk utilities in a namespace for common code
 * and use it in both places.
 */

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cassert>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>

#include "BlkDevSpdk.h"
#include "FsProc_FsInternal.h"
#include "FsProc_Journal.h"
#include "cxxopts.hpp"
#include "param.h"
#include "spdk/env.h"
#include "spdk/nvme.h"
#include "spdlog/spdlog.h"
#include "util.h"

/* spdk startup functions */

static struct ctrlr_entry *g_controllers = NULL;
static struct ns_entry *g_namespaces = NULL;
static struct spdk_nvme_qpair *g_qpair = NULL;

static void register_ns(struct spdk_nvme_ctrlr *ctrlr,
                        struct spdk_nvme_ns *ns) {
  if (!spdk_nvme_ns_is_active(ns)) {
    SPDLOG_WARN("nvme ns is not active");
    return;
  }

  auto entry = (struct ns_entry *)malloc(sizeof(struct ns_entry));
  if (entry == NULL) {
    SPDLOG_ERROR("Failed to allocate ns_entry");
    exit(1);
  }

  entry->ctrlr = ctrlr;
  entry->ns = ns;
  entry->next = g_namespaces;
  g_namespaces = entry;
}

static void attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                      struct spdk_nvme_ctrlr *ctrlr,
                      const struct spdk_nvme_ctrlr_opts *opts) {
  auto entry = (struct ctrlr_entry *)malloc(sizeof(struct ctrlr_entry));
  if (entry == NULL) {
    SPDLOG_ERROR("Failed to allocate ctrlr_entry");
    exit(1);
  }

  auto *cdata = spdk_nvme_ctrlr_get_data(ctrlr);
  snprintf(entry->name, sizeof(entry->name), "%-20.20s (%-20.20s)", cdata->mn,
           cdata->sn);
  entry->ctrlr = ctrlr;
  entry->next = g_controllers;
  g_controllers = entry;

  int num_ns = spdk_nvme_ctrlr_get_num_ns(ctrlr);
  SPDLOG_INFO("using controller {} with {} namespaces", entry->name, num_ns);
  for (int nsid = 1; nsid <= num_ns; nsid++) {
    auto ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
    if (ns == NULL) continue;
    register_ns(ctrlr, ns);
  }
}

static void cleanup(void) {
  struct ns_entry *ns_entry = g_namespaces;
  struct ctrlr_entry *ctrlr_entry = g_controllers;

  while (ns_entry != NULL) {
    struct ns_entry *next = ns_entry->next;
    free(ns_entry);
    ns_entry = next;
  }

  while (ctrlr_entry != NULL) {
    struct ctrlr_entry *next = ctrlr_entry->next;
    spdk_nvme_detach(ctrlr_entry->ctrlr);
    free(ctrlr_entry);
    ctrlr_entry = next;
  }

  // TODO any other spdk functions to call for cleanup?
}

static int initSpdkEnv() {
  struct spdk_env_opts opts;

  spdk_env_opts_init(&opts);
  opts.name = "FsProcDumpMetadata";
  opts.shm_id = 9;  // TODO what is this shm_id for?

  if (spdk_env_init(&opts) < 0) {
    SPDLOG_ERROR("failed to initialize spdk env");
    return 1;
  }

  auto probe_cb = [](auto ctx, auto trid, auto opts) { return true; };
  int rc = spdk_nvme_probe(NULL, NULL, probe_cb, attach_cb, NULL);
  if (rc != 0) {
    SPDLOG_ERROR("spdk_nvme_probe() failed, rc={}", rc);
    return 1;
  }

  // We choose the first namespace
  auto ns_entry = g_namespaces;
  if (ns_entry == NULL) {
    SPDLOG_ERROR("No namespace found");
    return 1;
  }

  g_qpair = spdk_nvme_ctrlr_alloc_io_qpair(ns_entry->ctrlr, NULL, 0);
  if (g_qpair == NULL) {
    SPDLOG_ERROR("failed to alloc io qpair");
    return 1;
  }

  SPDLOG_DEBUG("SPDK environment initialized successfully");
  return 0;
}

/* FsProcDumpMetadata declaration + definition */
class FsProcDumpMetadata {
 public:
  FsProcDumpMetadata(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair);
  ~FsProcDumpMetadata();
  void dumpMetadata(std::string &outdir);
  static void onIOComplete(void *arg, const struct spdk_nvme_cpl *completion);

 private:
  struct spdk_nvme_ns *ns;
  struct spdk_nvme_qpair *qpair;
  volatile int total_issued;
  volatile int total_completed;
  volatile int total_failed;
  size_t dev_mem_blocks;
  char *dev_mem;
  void readNBlocks(uint64_t starting_block, size_t num_blocks, void *buf,
                   bool blocking);
  void writeNBlocks(uint64_t starting_block, size_t num_blocks, void *buf,
                    bool blocking);
  void readNSectors(uint64_t starting_sector, size_t num_sectors, void *buf,
                    bool blocking);
  void writeNSectors(uint64_t starting_sector, size_t num_sectors, void *buf,
                     bool blocking);
  void blockingProcessCompletions();
  void dumpBlocks(uint64_t start_block, size_t num_blocks, int fd);
};

FsProcDumpMetadata::FsProcDumpMetadata(struct spdk_nvme_ns *ns,
                                       struct spdk_nvme_qpair *qpair)
    : ns(ns),
      qpair(qpair),
      total_issued(0),
      total_completed(0),
      total_failed(0),
      dev_mem_blocks(10),
      dev_mem(NULL) {
  uint64_t num_bytes_to_alloc = dev_mem_blocks * BSIZE;
  dev_mem = (char *)spdk_dma_zmalloc(num_bytes_to_alloc, BSIZE, NULL);
  if (dev_mem == NULL)
    throw std::runtime_error("Failed to alloc device memory");
}

FsProcDumpMetadata::~FsProcDumpMetadata() {
  if (dev_mem != NULL) spdk_dma_free(dev_mem);

  dev_mem = NULL;
}

inline void FsProcDumpMetadata::readNBlocks(uint64_t starting_block,
                                            size_t num_blocks, void *buf,
                                            bool blocking) {
  readNSectors(starting_block * 8, num_blocks * 8, buf, blocking);
}

void FsProcDumpMetadata::readNSectors(uint64_t starting_sector,
                                      size_t num_sectors, void *buf,
                                      bool blocking) {
  total_issued = total_issued + 1;
  int rc = spdk_nvme_ns_cmd_read(ns, qpair, buf, starting_sector, num_sectors,
                                 onIOComplete, this, 0);
  if (rc != 0) {
    SPDLOG_ERROR("Failed to issue read i/o cmd");
    total_completed = total_completed + 1;
    total_failed = total_failed + 1;
    return;
  }
  if (blocking) blockingProcessCompletions();
}

inline void FsProcDumpMetadata::writeNBlocks(uint64_t starting_block,
                                             size_t num_blocks, void *buf,
                                             bool blocking) {
  writeNSectors(starting_block * 8, num_blocks * 8, buf, blocking);
}

//#define offline_dryrun 1
void FsProcDumpMetadata::writeNSectors(uint64_t starting_sector,
                                       size_t num_sectors, void *buf,
                                       bool blocking) {
  total_issued = total_issued + 1;
  /* For now, no writes - just shallow check...
  if (offline_dryrun) {
    total_completed++;
    return;
  }*/

  int rc = spdk_nvme_ns_cmd_write(ns, qpair, buf, starting_sector, num_sectors,
                                  onIOComplete, this, 0);
  if (rc != 0) {
    SPDLOG_ERROR("Failed to issue write i/o cmd");
    total_completed = total_completed + 1;
    total_failed = total_failed + 1;
    return;
  }

  if (blocking) blockingProcessCompletions();
}

void FsProcDumpMetadata::onIOComplete(void *arg,
                                      const struct spdk_nvme_cpl *completion) {
  auto chkptr = (FsProcDumpMetadata *)arg;
  chkptr->total_completed = chkptr->total_completed + 1;
  if (spdk_nvme_cpl_is_error(completion)) {
    chkptr->total_failed = chkptr->total_failed + 1;
    return;
  }
}

void FsProcDumpMetadata::blockingProcessCompletions() {
  do {
    // TODO capture return code
    spdk_nvme_qpair_process_completions(qpair, 0);
  } while (total_issued != total_completed);
}

static int safe_create(std::string &filename) {
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC,
                S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR);
  if (fd < 0) {
    // TODO log errno
    std::string msg = "Failed to open file " + filename;
    throw std::runtime_error(msg);
  }

  return fd;
}

static void safe_close(int fd) {
  int rc = close(fd);
  if (rc != 0) {
    throw std::runtime_error("Failed to close file");
  }
}

void FsProcDumpMetadata::dumpBlocks(uint64_t start_block, size_t num_blocks,
                                    int fd) {
  uint64_t st = start_block;
  while (num_blocks > 0) {
    size_t n = (num_blocks < dev_mem_blocks) ? num_blocks : dev_mem_blocks;
    readNBlocks(st, n, dev_mem, /*blocking*/ true);
    ssize_t rc = write(fd, dev_mem, n * BSIZE);
    if (static_cast<size_t>(rc) != (n * BSIZE)) {
      throw std::runtime_error("Failed to dump blocks");
    }

    num_blocks -= n;
    st += n;
  }
}

void FsProcDumpMetadata::dumpMetadata(std::string &outdir) {
  std::string sb_name = outdir + "/superblock";
  int sb_fd = safe_create(sb_name);
  dumpBlocks(SB_BLOCK_NO, 1, sb_fd);
  safe_close(sb_fd);

  std::string imap_name = outdir + "/imap";
  int imap_fd = safe_create(imap_name);
  dumpBlocks(get_imap_start_block(), get_dev_imap_num_blocks(), imap_fd);
  safe_close(imap_fd);

  std::string inodes_name = outdir + "/inodes";
  int inodes_fd = safe_create(inodes_name);
  dumpBlocks(get_inode_start_block(), get_dev_inode_num_blocks(), inodes_fd);
  safe_close(inodes_fd);

  inodes_fd = open(inodes_name.c_str(), O_RDONLY);
  if (inodes_fd < 0) {
    throw std::runtime_error("open inodes error");
  }

  std::string inodes_json_name = outdir + "/inodes.json";
  std::ofstream dumpf;
  dumpf.open(inodes_json_name, std::ios::app);
  if (dumpf.is_open()) {
    cfs_dinode curInode;
    ssize_t ret = 0;
    while (true) {
      ret = read(inodes_fd, &curInode, sizeof(cfs_dinode));
      if (ret != sizeof(cfs_dinode)) break;
      cfs_dinode *dinode = &curInode;
      dumpf << from_cfsdinode_to_str(dinode);
      dumpf << std::endl;
    }
    dumpf.close();
  }
  safe_close(inodes_fd);

  for (uint64_t i = 0; i < NMAX_FSP_WORKER; i++) {
    std::ostringstream ss;
    ss << outdir << "/bmap_w" << std::setw(2) << std::setfill('0') << i;
    std::string bmap_name = ss.str();
    int bmap_fd = safe_create(bmap_name);
    auto bmap_start = get_bmap_start_block_for_worker(i);
    auto nblocks = get_dev_bmap_num_blocks_for_worker(i);
    dumpBlocks(bmap_start, nblocks, bmap_fd);
    safe_close(bmap_fd);
  }
}

static void dumpMetadata(std::string &outdir) {
  auto fspdm = new FsProcDumpMetadata(g_namespaces->ns, g_qpair);
  fspdm->dumpMetadata(outdir);
  delete fspdm;
}

int main(int argc, char **argv) {
  cxxopts::Options options(argv[0], "Dumps FSP on-disk metadata");
  auto opts = options.add_options();
  // TODO : have options for dumping specific metadata
  // TODO : have options for dumping only inodes that have bit set in imap
  opts("o,outdir", "Output Directory",
       cxxopts::value<std::string>()->default_value("/tmp"));
  opts("h,help", "Print usage");

  auto result = options.parse(argc, argv);
  if (result.count("help")) {
    std::cout << options.help() << std::endl;
    return 0;
  }

  std::string outdir = result["outdir"].as<std::string>();
  int rc = 0;
  rc = initSpdkEnv();
  if (rc != 0) {
    goto end;
  }

  rc = mkdir(outdir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  if (rc != 0) {
    SPDLOG_ERROR("Failed to mkdir {}", outdir);
    goto end;
  }

  dumpMetadata(outdir);
end:
  // TODO use RAII so even when there is an error, cleanup is called..
  cleanup();
  return rc;
}
