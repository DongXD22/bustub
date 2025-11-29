//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames
 *  the LRUReplacer will be required to store
 */
void bustub::LRUKNode::Insert(size_t timestamp) {
  if (history_.size() == k_) {
    history_.pop_front();
  }
  history_.push_back(timestamp);
  if (history_.size() == k_) {
    bkward_kth_ = history_.front();
  }
}

auto bustub::LRUKNode::operator<(const LRUKNode &other) const -> bool {
  if (bkward_kth_ == other.bkward_kth_) {
    return history_.front() > other.history_.front();
  }
  return bkward_kth_ > other.bkward_kth_;
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : replacer_size_(num_frames), k_(k), node_heap_(Cmp{&node_store_}) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance
 * and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references
 * is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance,
 * then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame
 * should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return the frame ID if a frame is successfully evicted,
 * or `std::nullopt` if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> guard(latch_);

#if HEAP == 1

  if (node_heap_.empty()) {
    return std::nullopt;
  }
  frame_id_t to_evict_id = *node_heap_.begin();

  node_store_.erase(to_evict_id);
  node_heap_.erase(node_heap_.begin());
  curr_size_--;

#else
  frame_id_t to_evict_id = INT32_MAX;
  LRUKNode *to_evict_node = nullptr;

  for (auto &x : node_store_) {
    if (x.second.is_evictable_ && (to_evict_node == nullptr || *to_evict_node < x.second)) {
      to_evict_id = x.first;
      to_evict_node = &x.second;
    }
  }

  if (to_evict_id == INT32_MIN) return std::nullopt;

#endif

  return to_evict_id;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Invalid frame ID");
  }
  std::lock_guard<std::mutex> guard(latch_);

  current_timestamp_++;

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    it = node_store_.emplace(frame_id, LRUKNode(k_, frame_id)).first;
  }

  if (!it->second.is_evictable_) {
    it->second.Insert(current_timestamp_);
    return;
  }

#if HEAP
  auto iter = node_heap_.find(frame_id);
  if (iter != node_heap_.end()) {
    node_heap_.erase(iter);
  }
#endif

  it->second.Insert(current_timestamp_);

#if HEAP
  node_heap_.insert(frame_id);
#endif
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Invalid frame ID");
  }

  std::lock_guard<std::mutex> guard(latch_);

  auto it = node_store_.find(frame_id);

  if (it == node_store_.end() || it->second.is_evictable_ == set_evictable) {
    return;
  }

  curr_size_ += set_evictable ? 1 : -1;
  it->second.is_evictable_ = set_evictable;

#if HEAP
  if (set_evictable) {
    node_heap_.insert(frame_id);
  } else {
    auto iter = node_heap_.find(frame_id);
    if (iter != node_heap_.end()) {
      node_heap_.erase(iter);
    }
  }
#endif
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }

  if (!it->second.is_evictable_) {
    throw Exception(ExceptionType::INVALID, "Called Remove on a non-evictable frame.");
  }

#if HEAP
  auto iter = node_heap_.find(frame_id);
  if (iter != node_heap_.end()) {
    node_heap_.erase(iter);
  }
#endif

  node_store_.erase(it);
  curr_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(latch_);
  return curr_size_;
}

}  // namespace bustub
