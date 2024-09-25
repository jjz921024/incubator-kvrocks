#include "semi_sync_master.h"

#define SEMI_SYNC_WAIT_TIMEOUT 10

WaitingNodeManager::WaitingNodeManager() = default;

WaitingNodeManager::~WaitingNodeManager() {
  for (auto it : waiting_node_list_) {
    delete it;
  }
  waiting_node_list_.clear();
}

bool WaitingNodeManager::InsertWaitingNode(uint64_t log_file_pos) {
  auto* ins_node = new WaitingNode();
  ins_node->log_pos = log_file_pos;

  bool is_insert = false;
  if (waiting_node_list_.empty()) {
    is_insert = true;
  } else {
    auto& last_node = *(--waiting_node_list_.end());
    if (last_node->log_pos < log_file_pos) {
      is_insert = true;
    } else {
      auto iter = waiting_node_list_.find(ins_node);
      if (iter == waiting_node_list_.end()) is_insert = true;
    }
  }
  if (is_insert) {
    waiting_node_list_.emplace(ins_node);
    return true;
  } else {
    LOG(WARNING) << "[semisync] Unknown error to write the same sequence data (" << log_file_pos << ")";
    delete ins_node;
    return false;
  }
}

void WaitingNodeManager::ClearWaitingNodes(uint64_t ack_log_file_pos) {
  auto iter = waiting_node_list_.begin();
  while (iter != waiting_node_list_.end()) {
    auto& item = *iter;
    if (item->log_pos > ack_log_file_pos) break;

    if (item->waiters != 0) {
      iter++;
      continue;
    }
    waiting_node_list_.erase(iter++);
  }
}

// Return the first item whose sequence is greater then or equal to log_file_pos
WaitingNode* WaitingNodeManager::FindWaitingNode(uint64_t log_file_pos) {
  auto iter = waiting_node_list_.begin();
  while (iter != waiting_node_list_.end()) {
    auto& item = *iter;
    if (item->log_pos >= log_file_pos) return item;
    iter++;
  }
  return waiting_node_list_.empty() ? nullptr : *waiting_node_list_.begin();
}

int WaitingNodeManager::SignalWaitingNodesUpTo(uint64_t log_file_pos) {
  int ret_num = 0;
  auto iter = waiting_node_list_.begin();
  while (iter != waiting_node_list_.end()) {
    auto& item = *iter;
    if (item->log_pos > log_file_pos) break;

    item->cond.notify_all();
    iter++;
    ++ret_num;
  }

  return ret_num;
}

int WaitingNodeManager::SignalWaitingNodesAll() {
  int ret_num = 0;
  auto iter = waiting_node_list_.begin();
  while (iter != waiting_node_list_.end()) {
    (*iter)->cond.notify_all();
    iter++;
    ++ret_num;
  }

  return ret_num;
}

bool AckContainer::Resize(uint32_t size, const AckInfo** ackinfo) {
  if (size == 0) return false;
  if (size - 1 == ack_array_.size()) return true;

  std::vector<AckInfo> old_ack_array;
  old_ack_array.swap(ack_array_);
  ack_array_.resize(size - 1);
  for (auto& info : old_ack_array) {
    if (info.server_id == 0) continue;
    auto* ack = Insert(info.server_id, info.log_pos);
    if (ack) {
      *ackinfo = ack;
    }
  }

  return true;
}

void AckContainer::Clear() {
  for (auto& item : ack_array_) {
    item.Reset();
  }
}

void AckContainer::RemoveAll(uint64_t log_file_pos) {
  for (std::size_t i = 0; i < ack_array_.size(); i++) {
    auto& info = ack_array_[i];
    if (info.log_pos == log_file_pos) {
      info.Reset();
      empty_slot_ = i;
    }
  }
}

const AckInfo* AckContainer::Insert(int server_id, uint64_t log_file_pos) {
  if (log_file_pos < greatest_return_ack_.log_pos) {
    return nullptr;
  }

  empty_slot_ = ack_array_.size();
  for (std::size_t i = 0; i < ack_array_.size(); i++) {
    auto& info = ack_array_[i];
    if (info.server_id == 0) {
      empty_slot_ = i;
    }
    if (info.server_id == server_id) {
      if (info.log_pos < log_file_pos) {
        info.log_pos = log_file_pos;
      }
      return nullptr;
    }
  }

  AckInfo* ret_ack = nullptr;
  bool to_insert = false;
  if (empty_slot_ == ack_array_.size()) {
    uint64_t min_seq = log_file_pos;
    for (auto& info : ack_array_) {
      if (info.server_id != 0 && info.log_pos < min_seq) {
        min_seq = info.log_pos;
        ret_ack = &info;
      }
    }
    if (ret_ack != nullptr) {
      greatest_return_ack_.Set(ret_ack->server_id, ret_ack->log_pos);
    } else {
      greatest_return_ack_.Set(server_id, log_file_pos);
    }
    ret_ack = &greatest_return_ack_;
    RemoveAll(greatest_return_ack_.log_pos);

    if (log_file_pos > greatest_return_ack_.log_pos) {
      to_insert = true;
    }
  } else {
    to_insert = true;
  }

  if (to_insert) ack_array_[empty_slot_].Set(server_id, log_file_pos);

  return ret_ack;
}

ReplSemiSyncMaster::~ReplSemiSyncMaster() {
  delete node_manager_;
  LOG(INFO) << "[semisync] release ReplSemiSyncMaster";
}

// 在主节点上开启半同步功能
// TODO: 在启动时会被调用吗
int ReplSemiSyncMaster::StartSemiSyncMaster(int wait_slave_count) {
  if (!IsSemiSyncEnabled()) {
    LOG(WARNING) << "[semisync] Can't start semi sync beacuse of disable";
    return -1;
  }

  std::lock_guard<std::mutex> lock(lock_binlog_);
  if (node_manager_ == nullptr) node_manager_ = new WaitingNodeManager();

  if (!setWaitSlaveCount(wait_slave_count)) {
    LOG(ERROR) << "[semisync] Fail to set wait for slave count";
    return -1;
  }

  // 判断slave节点数是否满足
  state_.store(slave_threads_.size() >= wait_slave_count_);
  return 0;
}

int ReplSemiSyncMaster::CloseSemiSyncMaster() {
  std::lock_guard<std::mutex> lock(lock_binlog_);
  switchOff();
  if (node_manager_) {
    delete node_manager_;
    node_manager_ = nullptr;
  }
  semi_sync_enable_.store(false);
  ack_container_.Clear();
  slave_threads_.clear();
  return 0;
}

void ReplSemiSyncMaster::AddSlave(FeedSlaveThread* slave_thread_ptr) {
  std::lock_guard<std::mutex> lock(lock_binlog_);
  if (slave_thread_ptr == nullptr && slave_thread_ptr->GetConn() == nullptr) {
    LOG(ERROR) << "[semisync] Failed to add slave as semi sync one";
    return;
  }
  slave_threads_.emplace_back(slave_thread_ptr);
}

void ReplSemiSyncMaster::RemoveSlave(FeedSlaveThread* slave_thread_ptr) {
  std::lock_guard<std::mutex> lock(lock_binlog_);
  if (slave_thread_ptr == nullptr && slave_thread_ptr->GetConn() == nullptr) {
    LOG(ERROR) << "[semisync] Fail to remove semi sync slave";
    return;
  }
  slave_threads_.remove(slave_thread_ptr);

  if (!IsSemiSyncEnabled() || !IsOn()) return;

  if (slave_threads_.size() < wait_slave_count_) {
    LOG(WARNING) << "[semisync] Slave count less than expected, switch off semi sync";
    switchOff();
  }
}

bool ReplSemiSyncMaster::CommitTrx(uint64_t trx_wait_binlog_pos) {
  std::unique_lock<std::mutex> lock(lock_binlog_);

  if (!IsSemiSyncEnabled() || !IsOn()) return false;

  if (trx_wait_binlog_pos <= wait_file_pos_.load()) {
    return false;
  }

  bool insert_result = node_manager_->InsertWaitingNode(trx_wait_binlog_pos);
  if (!insert_result) {
    LOG(ERROR) << "[semisync] Fail to insert log sequence to wait list";
  }
  auto trx_node = node_manager_->FindWaitingNode(trx_wait_binlog_pos);
  if (trx_node == nullptr) {
    LOG(ERROR) << "[semisync] Data in wait list is lost";
    return false;
  }

  // TODO: 不阻塞线程
  trx_node->waiters++;
  auto s = trx_node->cond.wait_for(lock, std::chrono::seconds(SEMI_SYNC_WAIT_TIMEOUT));
  trx_node->waiters--;
  if (std::cv_status::timeout == s) {
    LOG(ERROR) << "[semisync] Semi sync waits 10s, switch all the slaves to async";
    switchOff();
  }

  if (max_handle_sequence_.load() < trx_wait_binlog_pos) {
    max_handle_sequence_.store(trx_wait_binlog_pos);
  }

  if (trx_node->waiters == 0) {
    node_manager_->ClearWaitingNodes(trx_wait_binlog_pos);
  }

  return true;
}

void ReplSemiSyncMaster::HandleAck(int server_id, uint64_t log_file_pos) {
  std::lock_guard<std::mutex> lock(lock_binlog_);
  // TODO: 重构
  if (wait_slave_count_ == 1) {
    reportReplyBinlog(log_file_pos);
  } else {
    auto* ack = ack_container_.Insert(server_id, log_file_pos);
    if (ack != nullptr) {
      reportReplyBinlog(ack->log_pos);
    }
  }
}

bool ReplSemiSyncMaster::SetWaitSlaveCount(uint count) {
  std::lock_guard<std::mutex> lock(lock_binlog_);
  return setWaitSlaveCount(count);
}

bool ReplSemiSyncMaster::setWaitSlaveCount(uint count) {
  std::ostringstream log;
  log << "[semisync] Try to set slave count: " << count;
  if (count == 0) {
    count = slave_threads_.size() / 2 + 1;
    log << " ,auto adjust to: " << count;
  }
  LOG(INFO) << log.str();

  const AckInfo* ackinfo = nullptr;
  bool result = ack_container_.Resize(count, &ackinfo);
  if (result) {
    if (ackinfo != nullptr) {
      reportReplyBinlog(ackinfo->log_pos);
    }
    wait_slave_count_ = count;
  }

  LOG(INFO) << "[semisync] Finish setting slave count";
  return result;
}

void ReplSemiSyncMaster::reportReplyBinlog(uint64_t log_file_pos) {
  if (!IsSemiSyncEnabled()) return;

  // 若主从同步状态为off时,
  // 每次从节点ack后，根据seq判断是否可以恢复
  if (!IsOn() && log_file_pos > max_handle_sequence_.load()) {
    LOG(INFO) << "[semisync] try to switch on semi sync";
    state_.store(true);
  }

  node_manager_->SignalWaitingNodesUpTo(log_file_pos);
  if (log_file_pos > wait_file_pos_.load()) wait_file_pos_.store(log_file_pos);
}

// 仅用于暂停半同步状态, 但不释放所需资源
void ReplSemiSyncMaster::switchOff() {
  state_.store(false);
  wait_file_pos_.store(0);
  max_handle_sequence_.store(0);
  if (node_manager_ != nullptr) {
    node_manager_->SignalWaitingNodesAll();
  }
}

// semisync_master.cc