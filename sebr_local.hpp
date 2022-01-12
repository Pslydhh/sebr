// Copyright 2020 Pslydhh. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <iostream>
#include <mutex>
#include <optional>
#include <random>
#include <stack>
#include <thread>
#include <unordered_map>
#include <vector>

namespace sebr {
class Blocking {
public:
  std::mutex mtx;
  std::condition_variable cv;
  bool flag;

public:
  Blocking() : mtx(), cv(), flag(false) {}

  void unpark() {
    std::unique_lock<std::mutex> control(mtx);
    if (!flag) {
      flag = true;
      cv.notify_one();
    }
  }

  void park() {
    std::unique_lock<std::mutex> control(mtx);
    while (!flag) {
      cv.wait(control);
    }
    flag = false;
  }
};

class Base {
public:
  virtual void reclaim() {}
  virtual ~Base() {}

  void static reclaim(Base *obj) {
    obj->reclaim();
    delete obj;
  }
};

class ReclaimBridge : public Base {
public:
  virtual void reclaim() {}
  virtual ~ReclaimBridge() {}
};

template <typename T> class RecSingleNode : public ReclaimBridge {
public:
  RecSingleNode(T *node) : node(node) {}
  RecSingleNode() : node(nullptr) {}

  void reclaim() { delete node; }

private:
  T *node;
};

class RecWithEpoch {
  Base *recObj;
  uint64_t epoch;

public:
  RecWithEpoch(Base *recObj, int64_t epoch) : recObj(recObj), epoch(epoch) {}
  uint64_t getEpoch() { return epoch; }
  Base *getRecObj() { return recObj; }
};

template <typename T> class Next {
public:
  Next() : sentinel(nullptr), next(static_cast<T *>(this)) {}
  Next(T *sentinel) : sentinel(sentinel), next(nullptr) {}

  void pin() {
    T *senti_next;
    do {
      senti_next = sentinel->next.load();
      this->next.store(senti_next);
    } while (!com_exch_next(sentinel, senti_next, static_cast<T *>(this)));
  }

  static bool com_exch_next(T *handle, T *old_next, T *new_next) {
    return handle->next.compare_exchange_strong(old_next, new_next);
  }

  static bool com_exch_prev(T *handle, T *old_prev, T *new_prev) {
    return handle->prev.compare_exchange_strong(old_prev, new_prev);
  }

  static bool is_tagged(T *addr) {
    return reinterpret_cast<uintptr_t>(addr->next.load()) &
           static_cast<uintptr_t>(1);
  }

  static bool is_not_tagged(T *addr) { return !is_tagged(addr); }

  static T *tagged_address(T *plain_addr) {
    return reinterpret_cast<T *>(reinterpret_cast<uintptr_t>(plain_addr) |
                                 static_cast<uintptr_t>(1));
  }

  static T *untagged_address(T *tagged_addr) {
    return reinterpret_cast<T *>(reinterpret_cast<uintptr_t>(tagged_addr) &
                                 static_cast<uintptr_t>(~1));
  }

  T *sentinel;
  std::atomic<T *> next;
};

template <typename T> class NextWithUnpin : public Next<T> {
public:
  NextWithUnpin() : Next<T>(), prev(static_cast<T *>(this)) {}
  NextWithUnpin(T *sentinel) : Next<T>(sentinel), prev(nullptr) {}

  typename std::enable_if<(alignof(std::max_align_t) & 1) == 0, void>::type
  unpin(std::function<void()> f) {
    T *next_value;
    do {
      next_value = this->next.load();
    } while (!Next<T>::com_exch_next(static_cast<T *>(this), next_value,
                                     Next<T>::tagged_address(next_value)));

    bool r = Next<T>::is_tagged(static_cast<T *>(this));
    assert(r);

    // Unlink all tagged node.
    // This is a COMMON method drive every thread to clean the 'next' chain.
  UNLINK_TAGGED_NODE:
    T *prev = this->sentinel;
    T *next = prev->next.load();
    while (next != this->sentinel) {
      if (Next<T>::is_tagged(next)) {
        if (Next<T>::is_tagged(prev)) {
          goto UNLINK_TAGGED_NODE;
        }

        T *after_tagged_node = Next<T>::untagged_address(next->next.load());
        while (Next<T>::is_tagged(after_tagged_node)) {
          after_tagged_node =
              Next<T>::untagged_address(after_tagged_node->next.load());
        }

        if (Next<T>::is_tagged(prev)) {
          goto UNLINK_TAGGED_NODE;
        }
        Next<T>::com_exch_next(prev, next, after_tagged_node);
        goto UNLINK_TAGGED_NODE;
      }
      prev = next;
      next = Next<T>::untagged_address(prev->next.load());
    }
    // while next == sentinel => There is no unlink threadHandle in the group at
    // this moment.

    // =====must call control's methods before link to 'prev'
    f();
    // =====

    // And link to the 'prev' chain.
    T *senti_prev;
    do {
      senti_prev = this->sentinel->prev.load();
      this->prev.store(senti_prev);
    } while (!Next<T>::com_exch_prev(this->sentinel, senti_prev,
                                     static_cast<T *>(this)));
  }

  std::atomic<T *> prev;
};

template <typename T> class ConcurrentBridge;
class ThreadHandle;
class PackedHandle {
public:
  template <typename T> PackedHandle(ConcurrentBridge<T> *bridge);
  PackedHandle(ThreadHandle *owed);

  template <typename T, typename... Args> void retire(Args &&... args);

  ~PackedHandle();

private:
  ThreadHandle *owed;
};

class ConcurrencyControl {
public:
  ConcurrencyControl(int32_t flag = 0) : flag(flag), blocking() {}

  std::atomic<int32_t> flag;
  Blocking blocking;
};

class ThreadHandle : public NextWithUnpin<ThreadHandle> {
public:
  ThreadHandle(ThreadHandle *sentinel, std::atomic<int64_t> *global_epoch_ptr,
               int32_t bytes_gc_threshold, int32_t bytes_epoch_threshold)
      : NextWithUnpin(sentinel), epoch(-1), global_epoch_ptr(global_epoch_ptr),
        heap_tabs(), bytes_accumulate(0),
        bytes_gc_threshold(bytes_gc_threshold),
        bytes_epoch_threshold(bytes_epoch_threshold), epoch_add_lastbytes(0),
        control() {
    this->pin();
  }

  ThreadHandle()
      : NextWithUnpin(), epoch(-1), global_epoch_ptr(nullptr), heap_tabs(),
        bytes_accumulate(0), bytes_gc_threshold(0), bytes_epoch_threshold(0),
        epoch_add_lastbytes(0), control() {}

  void unbind(std::function<void()> f) { this->unpin(f); }

  ThreadHandle *lock_guard() {
    epoch.store(global_epoch_ptr->load());
    return this;
  }

  ThreadHandle *unguard() {
    epoch.store(LEAVE);
    reclaim(bytes_gc_threshold);
    return this;
  }

  void try_increase_epoch(std::atomic<int64_t> *globalEpoch) {
    if (!((++touch_times) % 256)) {
      globalEpoch->fetch_add(1);
    }
  }

  template <typename T, typename... Args> void retire(Args &&... args) {
    int64_t epoch = global_epoch_ptr->load();
    try_increase_epoch(global_epoch_ptr);
    heap_tabs.emplace_back(new T(std::forward<Args>(args)...), epoch);
  }

  void clean() {
    int32_t rec_num = 0;
    for (auto &recObj : heap_tabs) {
      // reclaim unused memory.
      auto rec = recObj.getRecObj();
      Base::reclaim(rec);
      ++rec_num;
    }
    heap_tabs.erase(heap_tabs.begin(), heap_tabs.begin() + rec_num);
  }

  void reclaim(int64_t threshold) {
    if (!(touch_times % 512)) {
      if (heap_tabs.empty())
        return;

      uint64_t min_epoch = global_epoch_ptr->load();
      if (heap_tabs[0].getEpoch() == min_epoch) {
        return;
      }

      ThreadHandle *handle = sentinel->next.load();
      while (handle != sentinel) {
        uint64_t th_epoch = handle->epoch.load();
        min_epoch = std::min(min_epoch, th_epoch);
        handle = ThreadHandle::untagged_address(handle->next.load());
      }

      int32_t rec_num = 0;
      for (RecWithEpoch &recObj : heap_tabs) {
        if (recObj.getEpoch() >= min_epoch)
          break;
        // reclaim unused memory.
        auto rec = recObj.getRecObj();
        Base::reclaim(rec);
        ++rec_num;
      }
      heap_tabs.erase(heap_tabs.begin(), heap_tabs.begin() + rec_num);
    }
  }

private:
  // local epoch for this thread.
  std::atomic<int64_t> epoch;
  // global epoch for this threads group.
  std::atomic<int64_t> *global_epoch_ptr;
  std::vector<RecWithEpoch> heap_tabs;
  int64_t bytes_accumulate;
  int32_t bytes_gc_threshold;
  int32_t bytes_epoch_threshold;
  int64_t epoch_add_lastbytes;
  uint64_t touch_times = 0;
  constexpr static int64_t LEAVE = -1;

public:
  ConcurrencyControl control;
};

class IdAllocator {
public:
  IdAllocator() : upper_bound(0), freed(), lock() {}
  size_t allocate() {
    std::lock_guard<std::mutex> guard(lock);
    if (freed.empty()) {
      return upper_bound++;
    } else {
      size_t id = freed.top();
      freed.pop();
      return id;
    }
  }

  void deallocate(size_t id) {
    std::lock_guard<std::mutex> guard(lock);
    freed.push(id);
  }

private:
  uint32_t upper_bound;
  std::stack<uint32_t, std::vector<uint32_t>> freed;

  // TODO: use concurrent_stack or concurrent_queue
  std::mutex lock;
};

template <typename T> class ThreadGroup {
  class ThreadHandleAggregate {
  public:
    ThreadHandleAggregate() : handles_vector() {}

    ThreadHandle *get_thread_handle(ThreadGroup<T> *group,
                                    ThreadHandle *sentinel,
                                    std::atomic<int64_t> *global_epoch,
                                    int32_t bytes_gc_threshold,
                                    int32_t bytes_epoch_threshold) {
      while (handles_vector.size() <= group->id) {
        auto handle = reinterpret_cast<ThreadHandle *>(
            new std::uint8_t[sizeof(ThreadHandle)]);
        new (&handle->control) ConcurrencyControl(-2);
        handles_vector.push_back(handle);
      }

      auto h = handles_vector[group->id];
      if (h->control.flag.load() < 0) {
        group->handle_total.fetch_add(1);
        new (h) ThreadHandle(sentinel, global_epoch, bytes_gc_threshold,
                             bytes_epoch_threshold);
      }

      return h;
    }

    ~ThreadHandleAggregate() {
      for (auto iter = handles_vector.begin(); iter != handles_vector.end();
           ++iter) {
        auto handle = *iter;
        auto &control = handle->control;

        int32_t flag = 0;
        if (control.flag.load() == -2) {
          (&handle->control)->~ConcurrencyControl();
          delete[] reinterpret_cast<std::uint8_t *>(handle);
        } else if (control.flag.load() == flag &&
                   control.flag.compare_exchange_strong(flag, 1)) {
          handle->unbind([&control]() -> void {
            control.flag.store(-1);
            control.blocking.unpark();
          });
        } else {
          do {
            control.blocking.park();
          } while (control.flag.load() > 0);

          // delete handle;
          handle->~ThreadHandle();
          delete[] reinterpret_cast<std::uint8_t *>(handle);
        }
      }
    }

    std::vector<ThreadHandle *> handles_vector;
  };

public:
  ThreadGroup(int32_t bytes_gc_threshold, int32_t bytes_epoch_threshold)
      : id(id_allocator.allocate()), sentinel(), global_epoch(0),
        bytes_gc_threshold(bytes_gc_threshold),
        bytes_epoch_threshold(bytes_epoch_threshold), handle_total(0) {}

  ~ThreadGroup() { deallocate(); }

  ThreadHandle *bind() {
    thread_local ThreadHandleAggregate aggregate;
    ThreadHandle *handle =
        aggregate.get_thread_handle(this, &sentinel, &global_epoch,
                                    bytes_gc_threshold, bytes_epoch_threshold);
    return handle;
  }

  void deallocate() { id_allocator.deallocate(id); }

public:
  static IdAllocator id_allocator;
  const uint32_t id;
  ThreadHandle sentinel;
  std::atomic<int64_t> global_epoch;
  const int32_t bytes_gc_threshold;
  const int32_t bytes_epoch_threshold;
  std::atomic<int32_t> handle_total;
};

template <typename T> IdAllocator ThreadGroup<T>::id_allocator;

template <typename T> class ConcurrentBridge {
  friend class PackedHandle;

public:
  ConcurrentBridge(int32_t bytes_gc_threshold = 8192,
                   int32_t bytes_epoch_threshold = 1024)
      : group(new ThreadGroup<T>(bytes_gc_threshold, bytes_epoch_threshold)) {}

  ThreadHandle *bind() { return group->bind(); }

  ~ConcurrentBridge() {
    auto handle_num = group->handle_total.load();

    auto sentinel = &group->sentinel;
    auto next = sentinel->next.load();
    while (next != sentinel) {
      auto temp_obj = ThreadHandle::untagged_address(next->next.load());
      auto &control = next->control;
      int32_t flag = 0;
      if (control.flag.load() == flag &&
          control.flag.compare_exchange_strong(flag, 1)) {
        next->clean();
        --handle_num;
      } else {
        do {
          control.blocking.park();
        } while (control.flag.load() > 0);
      }
      next = temp_obj;
    }

    ThreadHandle *prev;
    for (;;) {
      auto num = handle_num;
      prev = sentinel->prev.load();
      while (prev != sentinel) {
        --num;
        prev = prev->prev.load();
      }
      if (num == 0) {
        break;
      } else {
        std::this_thread::yield();
      }
    }

    next = sentinel->next.load();
    while (next != sentinel) {
      auto &control = next->control;
      control.flag.store(-1);
      control.blocking.unpark();
      next = next->next.load();
    }

    prev = sentinel->prev.load();
    while (prev != sentinel) {
      auto temp_obj = prev->prev.load();
      prev->clean();
      prev->~ThreadHandle();
      delete[] reinterpret_cast<std::uint8_t *>(prev);
      prev = temp_obj;
    }

    delete group;
  }

private:
  ThreadGroup<T> *group;
};

template <typename T>
inline PackedHandle::PackedHandle(ConcurrentBridge<T> *bridge)
    : owed(bridge->group->bind()) {
  owed->lock_guard();
}

inline PackedHandle::PackedHandle(ThreadHandle *owed) : owed(owed) {
  owed->lock_guard();
}

template <typename T, typename... Args>
inline void PackedHandle::retire(Args &&... args) {
  owed->retire<T>(std::forward<Args>(args)...);
}

inline PackedHandle::~PackedHandle() { owed->unguard(); }

using Pin = PackedHandle;
} // namespace sebr
