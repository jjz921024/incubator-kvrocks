#pragma once

#include <event2/bufferevent.h>

#include <atomic>
#include <condition_variable>
#include <list>
#include <mutex>
#include <set>
#include <vector>

#include "replication.h"

struct WaitingNode {
  uint64_t log_pos;
  std::condition_variable cond;
  int waiters;
};

struct WaitingNodeComparator {
  bool operator()(struct WaitingNode* left, struct WaitingNode* right) const {
    if (left->log_pos == right->log_pos) return false;

    return left->log_pos < right->log_pos;
  }
};

class WaitingNodeManager {
 public:
  WaitingNodeManager();
  ~WaitingNodeManager();
  bool InsertWaitingNode(uint64_t log_file_pos);
  void ClearWaitingNodes(uint64_t ack_log_file_pos);
  WaitingNode* FindWaitingNode(uint64_t log_file_pos);
  int SignalWaitingNodesUpTo(uint64_t log_file_pos);
  int SignalWaitingNodesAll();

 private:
  std::set<WaitingNode*, WaitingNodeComparator> waiting_node_list_;
};

struct AckInfo {
  int server_id = 0;
  uint64_t log_pos = 0;
  void Reset() {
    server_id = 0;
    log_pos = 0;
  }
  void Set(int id, uint64_t pos) {
    server_id = id;
    log_pos = pos;
  }
};

class AckContainer {
 public:
  AckContainer() = default;
  AckContainer(AckContainer&&) = delete;
  AckContainer& operator=(AckContainer&&) = delete;
  AckContainer(std::vector<AckInfo> ack_array, AckInfo greatest_return_ack)
      : ack_array_(std::move(ack_array)), greatest_return_ack_(greatest_return_ack) {}

  ~AckContainer() = default;
  bool Resize(uint32_t size, const AckInfo** ackinfo);
  void Clear();
  void RemoveAll(uint64_t log_file_pos);
  const AckInfo* Insert(int server_id, uint64_t log_file_pos);

 private:
  AckContainer(AckContainer const& container);
  AckContainer& operator=(const AckContainer& container);
  std::vector<AckInfo> ack_array_;
  AckInfo greatest_return_ack_;
  uint32_t empty_slot_ = 0;
};

class ReplSemiSyncMaster {
 public:
  ReplSemiSyncMaster(const ReplSemiSyncMaster&) = delete;
  ReplSemiSyncMaster& operator=(const ReplSemiSyncMaster&) = delete;
  static ReplSemiSyncMaster& GetInstance() {
    static ReplSemiSyncMaster instance;
    return instance;
  }
  ~ReplSemiSyncMaster();
  
  bool IsOn() const { return state_.load(); }
  bool IsSemiSyncEnabled() const { return semi_sync_enable_.load(); }
  void SetSemiSyncEnabled(bool enable) { semi_sync_enable_.store(enable); }
  void SetAutoFailBack(bool enable) { semi_sync_auto_fail_back_.store(enable); };
  bool IsAutoFailBack() const { return semi_sync_auto_fail_back_.load(); }

  int StartSemiSyncMaster(int wait_slave_count);
  int CloseSemiSyncMaster();
  void AddSlave(FeedSlaveThread* slave_thread_ptr);
  void RemoveSlave(FeedSlaveThread* slave_thread_ptr);
  bool CommitTrx(uint64_t trx_wait_binlog_pos);
  void HandleAck(int server_id, uint64_t log_file_pos);
  bool SetWaitSlaveCount(uint count);

 private:
  ReplSemiSyncMaster() {}
  
  std::list<FeedSlaveThread*> slave_threads_;

  WaitingNodeManager* node_manager_ = nullptr;
  AckContainer ack_container_;

  std::mutex lock_binlog_;
  
  // master已经commit的最大的seq, 每次write db时更新
  std::atomic<uint64_t> max_handle_sequence_ = {0};
  // 最大的slave ack的seq, 每次ack时更新
  std::atomic<uint64_t> wait_file_pos_ = {0};
  
  // 半同步是否正在运行
  std::atomic<bool> state_ = {false};
  // 是否开启半同步功能, 从节点上即使enable也不会运行   
  std::atomic<bool> semi_sync_enable_ = {false};
  std::atomic<bool> semi_sync_auto_fail_back_ = {false};

  uint wait_slave_count_ = 1;

  void reportReplyBinlog(uint64_t log_file_pos);
  bool setWaitSlaveCount(uint count);
  void switchOff();
};

// semisync_master