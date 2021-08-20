#include "FsProc_KnowParaLoadMng.h"
#include "util.h"

namespace fsp_lm {

void LmMemLessRoutingTable::UpdateItems(
    const std::map<pid_t, std::map<int, std::map<int, float>>> &items) {
  for (const auto &[pid, tid_map] : items) {
    for (const auto &[tid, dst_map] : tid_map) {
      auto tau_id = AssembleTwo32B(pid, tid);
      auto it = src_routing_map.find(tau_id);
      if (it != src_routing_map.end()) {
        for (auto [old_dst, _pct] : it->second) {
          dst_handling_taus[old_dst].erase(tau_id);
        }
        it->second.clear();
      }
      src_routing_map[tau_id] = dst_map;
      for (auto [new_dst, _pct] : dst_map) {
        dst_handling_taus[new_dst].emplace(tau_id);
      }
    }
  }
  auto it = dst_handling_taus.begin();
  while (it != dst_handling_taus.end()) {
    if (it->second.empty()) {
      dst_handling_taus.erase(it++);
    } else {
      ++it;
    }
  }
}

void LmMemLessRoutingTable::GetWidHandling(
    int dst_wid, std::map<pid_t, std::set<int>> &tids) {
  auto it = dst_handling_taus.find(dst_wid);
  if (it != dst_handling_taus.end()) {
    pid_t pid;
    int tid;
    for (auto tau : it->second) {
      DessembleOne64B(tau, pid, tid);
      tids[pid].emplace(tid);
    }
  }
}

void LmMemLessRoutingTable::Print(std::ostream &out) {
  out << "=======" << std::endl;
  pid_t pid;
  int tid;
  for (const auto &[tau_id, dst_map] : src_routing_map) {
    DessembleOne64B(tau_id, pid, tid);
    for (auto [dst, pct] : dst_map) {
      out << " pid:" << pid << " tid:" << tid << " dst:" << dst
          << " pct:" << pct << std::endl;
    }
  }
  out << "---" << std::endl;
  for (const auto &[dst, tau_set] : dst_handling_taus) {
    out << " dst:" << dst;
    if (!tau_set.empty()) out << std::endl;
    for (auto tau_id : tau_set) {
      DessembleOne64B(tau_id, pid, tid);
      out << "    pid:" << pid << " tid:" << tid << std::endl;
    }
    if (tau_set.empty()) out << std::endl;
  }
  out << "=======" << std::endl;
}

void LmMemLessRoutingTable::PrintGottenTids(
    int dst_wid, const std::map<pid_t, std::set<int>> &tids,
    std::ostream &out) {
  out << "[GetResult] dst_wid:" << dst_wid << " num:" << tids.size()
      << std::endl;
  for (auto &[pid, tid_set] : tids) {
    out << " pid:" << pid << " ---";
    for (auto tid : tid_set) {
      out << " tid:" << tid;
    }
    out << std::endl;
  }
}

}  // namespace fsp_lm
