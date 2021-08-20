#include <cassert>
#include <cstdint>
#include <iostream>
#include <sstream>
#include <string>
#include <tuple>
#include <unordered_map>

#include "spdlog/spdlog.h"

#include "FsProc_Journal.h"
#include "util.h"

// basic definitions for InodeLogEntry
InodeLogEntry::InodeLogEntry(uint64_t inode, uint64_t syncID,
                             InMemInode *minode)
    : inode(inode),
      syncID(syncID),
      optfield_bitarr(0),
      extent_seq_no(0),
      minode(minode) {}

InodeLogEntry::InodeLogEntry(uint8_t *buf, size_t buf_size) {
  if (buf == NULL || buf_size < 8) throw std::runtime_error("invalid buffer");

  uint8_t *ptr = buf;
  // first entry is always the total size of the inode entry
  uint64_t *uint64_buf_header = (uint64_t *)buf;
  if (uint64_buf_header[0] > buf_size)
    throw std::runtime_error("invalid buffer - size too small");

  inode = uint64_buf_header[1];
  syncID = uint64_buf_header[2];

  ptr += 24;
  // the next are 4 uint32_t entries
  uint32_t *uint32_buf_entries = (uint32_t *)ptr;
  size_t i = 0;
  optfield_bitarr = uint32_buf_entries[i++];
  mode = uint32_buf_entries[i++];
  uid = uint32_buf_entries[i++];
  gid = uint32_buf_entries[i++];
  block_count = uint32_buf_entries[i++];
  bitmap_op = uint32_buf_entries[i++];

  ptr += 24;
  // the next 3 are struct timeval
  atime = *((struct timeval *)ptr);
  ptr += sizeof(struct timeval);

  mtime = *((struct timeval *)ptr);
  ptr += sizeof(struct timeval);

  ctime = *((struct timeval *)ptr);
  ptr += sizeof(struct timeval);

  dentry_count = *((uint16_t *)ptr);
  ptr += 2;

  nlink = *((uint8_t *)ptr);
  ptr += 1;

  size = *((uint64_t *)ptr);
  ptr += 8;

  // populate the extent maps
  uint8_t *ext_iter = ptr;
  size_t n_ext_add = *((uint64_t *)ext_iter);
  ext_iter += 8;
  ext_add.clear();
  while (n_ext_add > 0) {
    n_ext_add -= 1;
    uint64_t block_no = *((uint64_t *)ext_iter);
    ext_iter += 8;
    struct ExtMapVal val = *((struct ExtMapVal *)ext_iter);
    ext_iter += sizeof(struct ExtMapVal);
    ext_add[block_no] = val;
  }

  size_t n_ext_del = *((uint64_t *)ext_iter);
  ext_iter += 8;
  ext_del.clear();
  while (n_ext_del > 0) {
    n_ext_del -= 1;
    uint64_t block_no = *((uint64_t *)ext_iter);
    ext_iter += 8;
    struct ExtMapVal val = *((struct ExtMapVal *)ext_iter);
    ext_iter += sizeof(struct ExtMapVal);
    ext_del[block_no] = val;
  }

  // populate the depends on map
  uint64_t *uint64_buf_entries = (uint64_t *)ext_iter;
  i = 0;
  size_t ndepends_on = uint64_buf_entries[i++];
  depends_on.clear();
  while (ndepends_on > 0) {
    ndepends_on -= 1;
    uint64_t k = uint64_buf_entries[i++];
    uint64_t v = uint64_buf_entries[i++];
    depends_on[k] = v;
  }

  // NOTE: minode is required for on_successful_journal_write. This constructor
  // is used to deserialize log entries during startup.
  minode = nullptr;
  // TODO : validation...
}

// assumes that .calcSize() has been called and buf has that much space
size_t InodeLogEntry::serialize(uint8_t *buf) {
  // TODO create a struct for the private members and memcpy the entire thing
  uint64_t *uint64_header_fields = (uint64_t *)buf;
  uint64_header_fields[1] = inode;
  uint64_header_fields[2] = syncID;

  uint32_t *uint32_fields = (uint32_t *)(&(uint64_header_fields[3]));
  // TODO disassemble and see if the optimized code avoids the increment and
  // uses hard coded literals
  off_t i = 0;
  uint32_fields[i++] = optfield_bitarr;
  uint32_fields[i++] = mode;
  uint32_fields[i++] = uid;
  uint32_fields[i++] = gid;
  uint32_fields[i++] = block_count;
  uint32_fields[i++] = bitmap_op;

  struct timeval *timeval_fields = (struct timeval *)(&uint32_fields[i]);
  i = 0;
  timeval_fields[i++] = atime;
  timeval_fields[i++] = mtime;
  timeval_fields[i++] = ctime;

  uint8_t *ptr = (uint8_t *)(&timeval_fields[i]);
  *((uint16_t *)ptr) = dentry_count;
  ptr += 2;

  *((uint8_t *)ptr) = nlink;
  ptr += 1;

  *((uint64_t *)ptr) = size;
  ptr += 8;

  size_t n_ext_add = ext_add.size();
  uint8_t *ext_iter = ptr;

  *((uint64_t *)ext_iter) = (uint64_t)n_ext_add;
  ext_iter += 8;
  for (const auto &[block_no, val] : ext_add) {
    *((uint64_t *)ext_iter) = block_no;
    ext_iter += 8;
    *((struct ExtMapVal *)ext_iter) = val;
    ext_iter += sizeof(struct ExtMapVal);
  }

  size_t n_ext_del = ext_del.size();
  *((uint64_t *)ext_iter) = (uint64_t)n_ext_del;
  ext_iter += 8;
  for (const auto &[block_no, val] : ext_del) {
    *((uint64_t *)ext_iter) = block_no;
    ext_iter += 8;
    *((struct ExtMapVal *)ext_iter) = val;
    ext_iter += sizeof(struct ExtMapVal);
  }

  size_t n_depends_on = depends_on.size();
  uint64_t *depends_on_map = (uint64_t *)ext_iter;
  depends_on_map[0] = (uint64_t)n_depends_on;
  i = 1;
  for (auto const &iter : depends_on) {
    depends_on_map[i++] = (uint64_t)iter.first;
    depends_on_map[i++] = (uint64_t)iter.second;
  }

  // returns total number of bytes used
  size_t entry_size = ((uint8_t *)(&(depends_on_map[i]))) - buf;
  uint64_header_fields[0] = (uint64_t)entry_size;
  return entry_size;
}

std::string InodeLogEntry::as_json_str() {
#define jkey(k) "\"" #k "\":" /* simple quoting of k */
#define cjkey(k) "," jkey(k)  /* precedes quoted k with comma */
  std::stringstream s;
  s << "{ " << jkey(inode) << inode << cjkey(syncID) << syncID
    << cjkey(optfield_bitarr) << optfield_bitarr;

  if (optfield_bitarr & mode_IDX) s << cjkey(mode) << mode;

  if (optfield_bitarr & uid_IDX) s << cjkey(uid) << uid;

  if (optfield_bitarr & gid_IDX) s << cjkey(gid) << gid;

  if (optfield_bitarr & size_IDX) s << cjkey(size) << size;

  if (optfield_bitarr & block_count_IDX) s << cjkey(block_count) << block_count;

  if (optfield_bitarr & bitmap_op_IDX) s << cjkey(bitmap_op) << bitmap_op;

  if (optfield_bitarr & atime_IDX) {
    s << cjkey(atime) << "{" << jkey(tv_sec) << atime.tv_sec << cjkey(tv_usec)
      << atime.tv_usec << "}";
  }
  if (optfield_bitarr & mtime_IDX) {
    s << cjkey(mtime) << "{" << jkey(tv_sec) << mtime.tv_sec << cjkey(tv_usec)
      << mtime.tv_usec << "}";
  }

  if (optfield_bitarr & ctime_IDX) {
    s << cjkey(ctime) << "{" << jkey(tv_sec) << ctime.tv_sec << cjkey(tv_usec)
      << ctime.tv_usec << "}";
  }

  if (optfield_bitarr & dentry_count_IDX) {
    s << cjkey(dentry_count) << dentry_count;
  }

  if (optfield_bitarr & nlink_IDX) {
    s << cjkey(nlink) << (uint32_t)nlink;
  }

  using ExtMap = std::unordered_map<uint64_t, struct ExtMapVal>;
  auto serialize_map = [&s](const ExtMap &m) {
    s << "[";
    for (const auto &[block_no, val] : m) {
      s << "{";
      s << jkey(block_no) << block_no << cjkey(seq_no) << val.seq_no
        << cjkey(i_block_offset) << val.i_block_offset << cjkey(num_blocks)
        << val.num_blocks << cjkey(bmap_modified) << val.bmap_modified;
      s << "},";
    }

    if (!m.empty()) s.seekp(-1, s.cur); /* remove last comma */
    s << "]";
  };

  s << cjkey(ext_add);
  serialize_map(ext_add);
  s << cjkey(ext_del);
  serialize_map(ext_del);

  {
    s << cjkey(depends_on) << "[";
    for (auto const &iter : depends_on) {
      s << "{" << jkey(inode) << iter.first << cjkey(syncID) << iter.second
        << "},";
    }
    if (!depends_on.empty()) s.seekp(-1, s.cur); /* remove last comma */
    s << "]";
  }

  s << "}";
#undef cjkey
#undef jkey
  return s.str();
  // ext add
  // ext del
  // depends_on
}

void InodeLogEntry::display(void) {
  using namespace std;
  cout << "InodeLogEntry( " << inode << ", " << syncID << ")" << endl;

#define displayIfPresent(f) \
  if (optfield_bitarr & f##_IDX) cout << #f ": " << f << endl;

  displayIfPresent(mode);
  displayIfPresent(uid);
  displayIfPresent(gid);
  // displayIfPresent(atime);
  // displayIfPresent(mtime);
  // displayIfPresent(ctime);
  displayIfPresent(size);

#undef displayIfPresent
  cout << "ext_add: " << endl;
  for (auto const &iter : ext_add) {
    cout << "[" << iter.first << "] : " << iter.first << ": {"
         << iter.second.seq_no << ", " << iter.second.i_block_offset << ", "
         << iter.second.num_blocks << ", " << iter.second.bmap_modified << "}"
         << endl;
  }

  cout << "ext_del: " << endl;
  for (auto const &iter : ext_del) {
    cout << "[" << iter.first << "] : " << iter.first << ": {"
         << iter.second.seq_no << ", " << iter.second.i_block_offset << ", "
         << iter.second.num_blocks << ", " << iter.second.bmap_modified << "}"
         << endl;
  }

  cout << "depends_on: " << endl;
  for (auto const &iter : depends_on) {
    cout << iter.first << ": " << iter.second << endl;
  }
}

void InodeLogEntry::applyChangesTo(
    cfs_dinode *dst, std::unordered_map<uint64_t, bool> &blocks_add_or_del) {
  dst->syncID = syncID;
  uint32_t flags = optfield_bitarr;
  if (flags & mode_IDX) dst->type = (mode_t)mode;
  if (flags & uid_IDX) dst->i_uid = (uid_t)uid;
  if (flags & gid_IDX) dst->i_gid = (gid_t)gid;
  if (flags & atime_IDX) dst->atime = atime;
  if (flags & ctime_IDX) dst->ctime = ctime;
  if (flags & mtime_IDX) dst->mtime = mtime;
  if (flags & size_IDX) dst->size = size;
  if (flags & block_count_IDX) dst->i_block_count = block_count;
  if (flags & dentry_count_IDX) dst->i_dentry_count = dentry_count;
  if (flags & nlink_IDX) dst->nlink = nlink;

  std::unordered_map<uint64_t, uint32_t> block_to_ts;
  uint32_t inside_extent_idx, extent_max_block_num;
  for (const auto &[block_no, val] : ext_add) {
    block_to_ts[block_no] = val.seq_no;
    int extents_idx = getCurrentExtentArrIdx(
        val.i_block_offset, inside_extent_idx, extent_max_block_num);
    dst->ext_array[extents_idx].num_blocks = val.num_blocks;
    dst->ext_array[extents_idx].block_no = block_no - get_data_start_block();
    dst->ext_array[extents_idx].i_block_offset = val.i_block_offset;
    // we need to count this towards to blocks added/deleted only if bmap is
    // modified so that the caller can update stable maps.
    if (val.bmap_modified) blocks_add_or_del[block_no] = true;
  }

  for (const auto &[block_no, val] : ext_del) {
    auto search = block_to_ts.find(block_no);
    // There may be cases where we add and then delete or vice versa. If this
    // entry timestamp is greater than what we saw previously, apply it.
    if ((search == block_to_ts.end()) || (val.seq_no > search->second)) {
      int extents_idx = getCurrentExtentArrIdx(
          val.i_block_offset, inside_extent_idx, extent_max_block_num);

      // FIXME: when we allow partial truncation, we must set num blocks rather
      // than straightaway setting it to 0.
      // FIXME: should block_no be reset to 0? Do we rely on if (block_no != 0)
      // anywhere in the code to see if an extent is valid?
      dst->ext_array[extents_idx].num_blocks = 0;
      dst->ext_array[extents_idx].block_no = block_no - get_data_start_block();
      dst->ext_array[extents_idx].i_block_offset = 0;
      // we need to insert into blocks added/deleted only if bmap is modified so
      // that the caller can update stable maps.
      if (val.bmap_modified) blocks_add_or_del[block_no] = false;
    }
  }
}

// basic definitions for JournalEntry

JournalEntry::JournalEntry()
    : cb(nullptr),
      cb_arg(nullptr),
      state(JournalEntryState::PREPARE),
      mgr(nullptr) {}

void JournalEntry::ParseJournalEntryHeader(uint8_t *buf, size_t buf_size,
                                           JournalEntry &je) {
  assert(je.ile_vec.empty());
  uint64_t header_size = je.calcBodySize();
  assert(buf_size >= header_size);

  uint64_t *uint64_header_fields = (uint64_t *)buf;
  if (uint64_header_fields[0] != JBODY_MAGIC) {
    je.state = JournalEntryState::DESERIALIZED_ERROR_MAGIC;
    return;
  }

  je.ustime_serialized = uint64_header_fields[1];
  // [2] and [3] are for size and number of inode log entries
  if (uint64_header_fields[2] > JournalManager::kMaxJournalBodySize) {
    je.state = JournalEntryState::DESERIALIZED_ERROR_ENTRY_SIZE;
    return;
  }

  je.start_block = uint64_header_fields[4];
  je.nblocks = uint64_header_fields[5];
  je.state = JournalEntryState::DESERIALIZED_PARTIAL;
}

JournalEntry::JournalEntry(uint8_t *buf, size_t buf_size)
    : cb(nullptr),
      cb_arg(nullptr),
      state(JournalEntryState::DESERIALIZED_PARTIAL),
      mgr(nullptr) {
  if (buf_size < calcBodySize()) throw std::runtime_error("buffer too small");

  ParseJournalEntryHeader(buf, buf_size, *this);
  switch (state) {
    case JournalEntryState::DESERIALIZED_PARTIAL:
      return;
    case JournalEntryState::DESERIALIZED_ERROR_MAGIC:
      throw std::runtime_error("JBODY_MAGIC mismatch");
    case JournalEntryState::DESERIALIZED_ERROR_ENTRY_SIZE:
      throw std::runtime_error("Invalid journal entry size");
    default:
      throw std::runtime_error("Unknown deserialization state");
  }

  // TODO - checksum valid header so that we aren't reading garbage?
  // The magic takes care of this to an extent, but we may have a byte
  // corruption that causes us to read some other blocks...
}

std::string JournalEntry::as_json_str() {
#define jkey(k) "\"" #k "\":"
#define cjkey(k) "," jkey(k) /* precedes quoted k with comma */
  std::stringstream s;
  s << "{" << jkey(start_block) << start_block << cjkey(nblocks) << nblocks
    << cjkey(ustime) << ustime_serialized << cjkey(num_inode_log_entries)
    << ile_vec.size() << cjkey(inode_log_entries) << "[";

  for (size_t i = 0; i < ile_vec.size(); i++) {
    s << (ile_vec[i])->as_json_str() << ",";
  }

  if (!ile_vec.empty()) s.seekp(-1, s.cur); /* remove last comma */
  s << "]"
    << "}";
#undef cjkey
#undef jkey
  return s.str();
}

// Deserialization is done in two steps. First the header, then the body.
void JournalEntry::deserializeBody(uint8_t *buf, size_t buf_size) {
  assert(ile_vec.empty());
  if (buf_size < calcBodySize()) throw std::runtime_error("buffer too small");

  if (state != JournalEntryState::DESERIALIZED_PARTIAL)
    throw std::runtime_error("Requires partially deserialized Jentry");

  uint64_t *uint64_header_fields = (uint64_t *)buf;
  if ((uint64_header_fields[0] != JBODY_MAGIC) ||
      (uint64_header_fields[1] != ustime_serialized))
    throw std::runtime_error(
        "Requires same buffer used for partial deserialization");

  uint64_t je_size = uint64_header_fields[2];
  if (buf_size < je_size) throw std::runtime_error("buffer too small");

  uint64_t num_ile = uint64_header_fields[3];
  uint64_t remaining_buf_size = je_size - calcBodySize();
  char *ptr = (char *)buf;
  ptr += calcBodySize();
  for (uint64_t i = 0; i < num_ile && remaining_buf_size > 0; i++) {
    auto ile = new InodeLogEntry((uint8_t *)ptr, remaining_buf_size);
    ile_vec.push_back(ile);

    size_t ile_size = ile->calcSize();
    remaining_buf_size -= ile_size;
    ptr += ile_size;
  }
  state = JournalEntryState::DESERIALIZED;
}

bool JournalEntry::isValidCommitBlock(uint8_t *commit_buf, size_t buf_size) {
  // TODO give reasons for being invalid?
  if (commit_buf == NULL || buf_size < 64) return false;
  uint64_t *ptr = (uint64_t *)commit_buf;
  if ((ptr[0] != JCOMMIT_MAGIC) || (ptr[1] != ustime_serialized) ||
      (ptr[2] != ile_vec.size()) || (ptr[3] != start_block) ||
      (ptr[4] != nblocks))
    return false;

  return true;
}

void JournalEntry::logCommitBlockMismatches(uint8_t *commit_buf,
                                            size_t buf_size) {
  if (commit_buf == NULL || buf_size < 64) {
    SPDLOG_INFO("commit_buf null or buf_size < 64");
    return;
  }

  uint64_t *ptr = (uint64_t *)commit_buf;
  if (ptr[0] != JCOMMIT_MAGIC) SPDLOG_INFO("JCOMMIT_MAGIC mismatch");

  if (ptr[1] != ustime_serialized) SPDLOG_INFO("ustime_serialized mismatch");

  if (ptr[2] != ile_vec.size()) SPDLOG_INFO("ile_vec.size() mismatch");

  if (ptr[3] != start_block) SPDLOG_INFO("start_block mismatch");

  if (ptr[4] != nblocks) SPDLOG_INFO("nblocks mismatch");
}
JournalEntry::~JournalEntry() {
  // TODO: clean up state like vector?
  switch (state) {
    case JournalEntryState::DESERIALIZED:
    case JournalEntryState::DESERIALIZED_PARTIAL:
    case JournalEntryState::DESERIALIZED_ERROR_MAGIC:
    case JournalEntryState::DESERIALIZED_ERROR_ENTRY_SIZE:
      break;
    default:
      return;
  }

  // deserialized paths have to deallocate newly created log entries
  // TODO using unique_ptr would be better
  for (InodeLogEntry *ile : ile_vec) delete ile;
}

// assumes that .calcBodySize() has been called and buf has that much space
size_t JournalEntry::serializeBody(uint8_t *buf) {
  uint64_t *uint64_header_fields = (uint64_t *)buf;
  ustime_serialized = (uint64_t)tap_ustime();
  uint64_header_fields[0] = JBODY_MAGIC;
  uint64_header_fields[1] = ustime_serialized;
  // uint64_header_fields[2] is for the size of this journal entry
  uint64_header_fields[3] = ile_vec.size();
  uint64_header_fields[4] = start_block;
  uint64_header_fields[5] = nblocks;

  uint8_t *ile_buf = (uint8_t *)&(uint64_header_fields[6]);
  for (InodeLogEntry *ile : ile_vec) {
    size_t nbytes = ile->serialize(ile_buf);
    ile_buf += nbytes;
  }

  size_t entry_size = ile_buf - buf;
  uint64_header_fields[2] = (uint64_t)entry_size;
  return entry_size;
}
