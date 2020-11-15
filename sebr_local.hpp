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

#include <algorithm>
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <mutex>
#include <optional>
#include <random>
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

    void static reclaim(Base* obj) {
        obj->reclaim();
        delete obj;
    }
};

template <typename T>
class ReclaimBridge : public Base {
public:
    virtual void reclaim() {}
    virtual ~ReclaimBridge() {}
};

template <typename T>
class RecSingleNode : public ReclaimBridge<RecSingleNode<T>> {
public:
    RecSingleNode(T* node) : node(node) {}
    RecSingleNode() : node(nullptr) {}

    void reclaim() { delete node; }

private:
    T* node;
};

class RecWithEpoch {
    Base* recObj;
    unsigned long epoch;
    long bytes_rec;

public:
    RecWithEpoch(Base* recObj, long epoch, long bytes_rec)
            : recObj(recObj), epoch(epoch), bytes_rec(bytes_rec) {}
    unsigned long getEpoch() { return epoch; }
    Base* getRecObj() { return recObj; }
    long getBytesForRec() { return bytes_rec; }
};

template <typename T>
class Next {
public:
    Next() : sentinel(nullptr), next(static_cast<T*>(this)) {}
    Next(T* sentinel) : sentinel(sentinel), next(nullptr) {}

    void pin() {
        T* senti_next;
        do {
            senti_next = sentinel->next.load();
            this->next.store(senti_next);
        } while (!com_exch_next(sentinel, senti_next, static_cast<T*>(this)));
    }

    static bool com_exch_next(T* handle, T* old_next, T* new_next) {
        return handle->next.compare_exchange_strong(old_next, new_next);
    }

    static bool com_exch_prev(T* handle, T* old_prev, T* new_prev) {
        return handle->prev.compare_exchange_strong(old_prev, new_prev);
    }

    static bool is_tagged(T* addr) {
        return reinterpret_cast<uintptr_t>(addr->next.load()) & static_cast<uintptr_t>(1);
    }

    static bool is_not_tagged(T* addr) { return !is_tagged(addr); }

    static T* tagged_address(T* plain_addr) {
        return reinterpret_cast<T*>(reinterpret_cast<uintptr_t>(plain_addr) | static_cast<uintptr_t>(1));
    }

    static T* untagged_address(T* tagged_addr) {
        return reinterpret_cast<T*>(reinterpret_cast<uintptr_t>(tagged_addr) & static_cast<uintptr_t>(~1));
    }

    T* sentinel;
    std::atomic<T*> next;
};

template <typename T>
class NextWithUnpin : public Next<T> {
public:
    NextWithUnpin() : Next<T>(), prev(static_cast<T*>(this)) {}
    NextWithUnpin(T* sentinel) : Next<T>(sentinel), prev(nullptr) {}

    typename std::enable_if<(alignof(std::max_align_t) & 1) == 0, void>::type unpin(
            std::function<void()> f) {
        T* next_value;
        do {
            next_value = this->next.load();
        } while (!Next<T>::com_exch_next(static_cast<T*>(this), next_value,
                                         Next<T>::tagged_address(next_value)));

        bool r = Next<T>::is_tagged(static_cast<T*>(this));
        assert(r);

        // Unlink all tagged node.
        // This is a COMMON method drive every thread to clean the 'next' chain.
    UNLINK_TAGGED_NODE:
        T* prev = this->sentinel;
        T* next = prev->next.load();
        while (next != this->sentinel) {
            if (Next<T>::is_tagged(next)) {
                if (Next<T>::is_tagged(prev)) {
                    goto UNLINK_TAGGED_NODE;
                }

                T* after_tagged_node = Next<T>::untagged_address(next->next.load());
                while (Next<T>::is_tagged(after_tagged_node)) {
                    after_tagged_node = Next<T>::untagged_address(after_tagged_node->next.load());
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
        // while next == sentinel => There is no unlink threadHandle in the group at this moment.

        // =====must call control's methods before link to 'prev'
        f();
        // =====

        // And link to the 'prev' chain.
        T* senti_prev;
        do {
            senti_prev = this->sentinel->prev.load();
            this->prev.store(senti_prev);
        } while (!Next<T>::com_exch_prev(this->sentinel, senti_prev, static_cast<T*>(this)));
    }

    std::atomic<T*> prev;
};

template <typename T>
class ConcurrentBridge;
class ThreadHandle;
class PackedHandle {
public:
    template <typename T>
    PackedHandle(ConcurrentBridge<T>* bridge);

    template <typename T, typename... Args>
    void retire(long bytes, Args&&... args);

    ~PackedHandle();

private:
    ThreadHandle* owed;
};

class ConcurrencyControl {
public:
    ConcurrencyControl() : flag(0), blocking() {}

    std::atomic<int> flag;
    Blocking blocking;
};

class ThreadHandle : public NextWithUnpin<ThreadHandle> {
public:
    ThreadHandle(ThreadHandle* sentinel, std::atomic<long>* global_epoch_ptr,
                 int bytes_gc_threshold, int bytes_epoch_threshold, ConcurrencyControl* control)
            : NextWithUnpin(sentinel),
              epoch(-1),
              global_epoch_ptr(global_epoch_ptr),
              heap_tabs(),
              bytes_accumulate(0),
              bytes_gc_threshold(bytes_gc_threshold),
              bytes_epoch_threshold(bytes_epoch_threshold),
              epoch_add_lastbytes(0),
              control(control) {
        this->pin();
    }

    ThreadHandle()
            : NextWithUnpin(),
              epoch(-1),
              global_epoch_ptr(nullptr),
              heap_tabs(),
              bytes_accumulate(0),
              bytes_gc_threshold(0),
              bytes_epoch_threshold(0),
              epoch_add_lastbytes(0),
              control(nullptr) {}

    void unbind(std::function<void()> f) { this->unpin(f); }

    ThreadHandle* lock_guard() {
        epoch.store(global_epoch_ptr->load());
        return this;
    }

    ThreadHandle* unguard() {
        epoch.store(LEAVE);
        reclaim(bytes_gc_threshold);
        return this;
    }

    void try_increase_epoch(long bytes, std::atomic<long>* globalEpoch) {
        if ((bytes_accumulate += bytes) - epoch_add_lastbytes > bytes_epoch_threshold) {
            globalEpoch->fetch_add(1);
            epoch_add_lastbytes = bytes_accumulate;
        }
    }

    template <typename T, typename... Args>
    void retire(long bytes, Args&&... args) {
        long epoch = global_epoch_ptr->load();
        try_increase_epoch(bytes += sizeof(T), global_epoch_ptr);
        heap_tabs.emplace_back(new T(std::forward<Args>(args)...), epoch, bytes);
    }

    void clean() {
        int rec_num = 0;
        for (auto& recObj : heap_tabs) {
            // reclaim unused memory.
            long bytes_to_reclaim = recObj.getBytesForRec();
            auto rec = recObj.getRecObj();
            Base::reclaim(rec);
            bytes_accumulate -= bytes_to_reclaim;
            ++rec_num;
        }
        heap_tabs.erase(heap_tabs.begin(), heap_tabs.begin() + rec_num);
    }

    void reclaim(long threshold) {
        if (bytes_accumulate > threshold) {
            if (heap_tabs.empty()) return;

            unsigned long min_epoch = global_epoch_ptr->load();
            if (heap_tabs[0].getEpoch() == min_epoch) {
                return;
            }

            ThreadHandle* handle = sentinel->next.load();
            while (handle != sentinel) {
                unsigned long th_epoch = handle->epoch.load();
                min_epoch = std::min(min_epoch, th_epoch);
                handle = ThreadHandle::untagged_address(handle->next.load());
            }

            int rec_num = 0;
            for (RecWithEpoch& recObj : heap_tabs) {
                if (recObj.getEpoch() >= min_epoch) break;
                // reclaim unused memory.
                long bytes_to_reclaim = recObj.getBytesForRec();
                auto rec = recObj.getRecObj();
                Base::reclaim(rec);
                bytes_accumulate -= bytes_to_reclaim;
                ++rec_num;
            }
            heap_tabs.erase(heap_tabs.begin(), heap_tabs.begin() + rec_num);
        }
    }

private:
    // local epoch for this thread.
    std::atomic<long> epoch;
    // global epoch for this threads group.
    std::atomic<long>* global_epoch_ptr;
    std::vector<RecWithEpoch> heap_tabs;
    long bytes_accumulate;
    int bytes_gc_threshold;
    int bytes_epoch_threshold;
    long epoch_add_lastbytes;
    constexpr static long LEAVE = -1;

public:
    ConcurrencyControl* control;
};

template <typename T>
class ThreadGroup {
    class ThreadHandleAggregate {
        class HandleWithControl {
        public:
            HandleWithControl(ThreadHandle* handle, ConcurrencyControl* control)
                    : handle(handle), control(control) {}

            ThreadHandle* handle;
            ConcurrencyControl* control;
        };

    public:
        ThreadHandleAggregate()
                : handles_table() {}

        ThreadHandle* get_thread_handle(ThreadGroup<T>* group, ThreadHandle* sentinel, std::atomic<long>* global_epoch,
                                        int bytes_gc_threshold, int bytes_epoch_threshold) {
            auto iter = handles_table.find(group);
            int flag = 0;
            if (iter != handles_table.end() && (flag = iter->second.control->flag.load()) == 0) {
                return iter->second.handle;
            } else {
                // Dirty reading
                if (flag != 0) {
                    assert(flag < 0);
                    // this means the last data struct object but 
                    // have the same address, so we must 
                    // clean it.f just delete control.
                    iter->second.control->blocking.park();
                    delete iter->second.control;
                }

                auto control = new ConcurrencyControl();
                auto handle = new ThreadHandle(sentinel, global_epoch, bytes_gc_threshold,
                                               bytes_epoch_threshold, control);
                handles_table.insert({group, HandleWithControl(handle, control)});
                group->handle_total.fetch_add(1);
                return handle;
            }
        }

        ~ThreadHandleAggregate() {
            for (auto iter = handles_table.begin(); iter != handles_table.end(); ++iter) {
                auto handle = iter->second.handle;
                auto control = iter->second.control;
                int flag = 0;
                if (control->flag.load() == flag &&
                    control->flag.compare_exchange_strong(flag, 1)) {
                    handle->unbind([control]() -> void {
                        control->flag.store(-1);
                        control->blocking.unpark();
                    });
                } else {
                    do {
                        control->blocking.park();
                    } while (control->flag.load() > 0);

                    delete control;
                }
            }
        }

        std::unordered_map<ThreadGroup<T>*, HandleWithControl> handles_table;
    };

public:
    ThreadGroup(int bytes_gc_threshold, int bytes_epoch_threshold)
            : sentinel(),
              global_epoch(0),
              bytes_gc_threshold(bytes_gc_threshold),
              bytes_epoch_threshold(bytes_epoch_threshold),
              handle_total(0) {}

    ThreadGroup()
            : sentinel(),
              global_epoch(0),
              bytes_gc_threshold(0),
              bytes_epoch_threshold(0),
              handle_total(0) {}

    ThreadHandle* bind() {
        thread_local ThreadHandleAggregate aggregate;
        ThreadHandle* handle = aggregate.get_thread_handle(this, &sentinel, &global_epoch, bytes_gc_threshold, bytes_epoch_threshold);
        return handle;
    }

public:
    ThreadHandle sentinel;
    std::atomic<long> global_epoch;
    const int bytes_gc_threshold;
    const int bytes_epoch_threshold;
    std::atomic<int> handle_total;
};

template <typename T>
class ConcurrentBridge {
    friend class PackedHandle;
public:
    ConcurrentBridge(int bytes_gc_threshold = 8192, int bytes_epoch_threshold = 1024)
            : group(new ThreadGroup<T>(bytes_gc_threshold, bytes_epoch_threshold)) {}

    ~ConcurrentBridge() {
        auto handle_num = group->handle_total.load();
        std::vector<ThreadHandle*> handles_capture;

        auto sentinel = &group->sentinel;
        auto next = sentinel->next.load();
        while (next != sentinel) {
            auto temp_obj = ThreadHandle::untagged_address(next->next.load());
            auto control = next->control;
            int flag = 0;
            if (control->flag.load() == flag && control->flag.compare_exchange_strong(flag, 1)) {
                next->clean();
                handles_capture.emplace_back(next);
                --handle_num;
                control->flag.store(-1);
                control->blocking.unpark();
            } else {
                do {
                    control->blocking.park();
                } while (control->flag.load() > 0);
            }
            next = temp_obj;
        }

        ThreadHandle* prev;
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

        prev = sentinel->prev.load();
        while (prev != sentinel) {
            auto temp_obj = prev->prev.load();
            prev->clean();
            delete prev->control;
            delete prev;
            prev = temp_obj;
        }

        for (auto handle : handles_capture) {
            delete handle;
        }

        delete group;
    }

private:
    ThreadGroup<T>* group;
};

template <typename T>
PackedHandle::PackedHandle(ConcurrentBridge<T>* bridge) : owed(bridge->group->bind()) {
    owed->lock_guard();
}

template <typename T, typename... Args>
void PackedHandle::retire(long bytes, Args&&... args) {
    owed->retire<T>(bytes, std::forward<Args>(args)...);
}

PackedHandle::~PackedHandle() {
    owed->unguard();
}

using Pin = PackedHandle;
} // namespace sebr
