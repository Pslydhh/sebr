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
    
    void park(int miliseconds) {
        bool* local = &flag;
        std::unique_lock<std::mutex> control(mtx);
        cv.wait_for(control, std::chrono::milliseconds(miliseconds), [local]{return *local == true;});
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

public:
    RecWithEpoch(Base* recObj) : recObj(recObj) {}
    Base* getRecObj() { return recObj; }
};

class ArrayOfRecVector {
public:
    ArrayOfRecVector() : stack_tabs(), bytes(0) {}
    std::vector<RecWithEpoch> stack_tabs;
    long bytes;

    long reclaim() {
        for (RecWithEpoch& recObj : stack_tabs) {
            auto rec = recObj.getRecObj();
            Base::reclaim(rec);
        }
        return bytes;
    }
};

static constexpr int N_SHIFT = 10;
static constexpr int N_VALUE = 1 << N_SHIFT;
static constexpr int N_BITS = N_VALUE - 1;
class ThreadHandle;
class LinkedNodeOfArrayRec {
public:
    LinkedNodeOfArrayRec(long begin_index) : arrays(), next(nullptr), begin_index(begin_index) {}
    LinkedNodeOfArrayRec* enable_next(std::atomic<LinkedNodeOfArrayRec*>& tail,
                                        ThreadHandle* threadHandle, std::atomic<long>& epoch);

    ArrayOfRecVector arrays[N_VALUE];
    std::atomic<LinkedNodeOfArrayRec*> next;
    const long begin_index;
};

class RecBlocks {
public:
    RecBlocks(ThreadHandle* sentinel, int bytes_gc_threshold, int bytes_epoch_threshold)
            : head(new LinkedNodeOfArrayRec(0)),
              tail(head),
              head_index(0),
              tail_index(0),
              control_GC(-1),
              sentinel(sentinel),
              bytes_gc_threshold(bytes_gc_threshold),
              bytes_epoch_threshold(bytes_epoch_threshold),
              threshold((static_cast<unsigned int> (bytes_gc_threshold) >> 1) / bytes_epoch_threshold) {}


    // putRecVector only called by the thread of handle.
    int putRecVectorWaitFree(std::vector<RecWithEpoch>& stack_tabs, long bytes, int entries, ThreadHandle* handle) {
        auto size = stack_tabs.size();

        auto tail_ptr = tail.load();
        const long old = tail_index.fetch_add(entries);
        long distance = old - tail_ptr->begin_index;
        
        for (;;) {
            assert(distance >= 0);
            if (distance < N_VALUE) {
                tail_ptr->arrays[distance].stack_tabs.swap(stack_tabs);
                tail_ptr->arrays[distance].bytes = bytes;
                break;
            } else {
                //assert(distance == N); NO
                tail_ptr = tail_ptr->enable_next(tail, handle, tail_index);
                distance = old - tail_ptr->begin_index;
            }
        }

        const long new_index = old + entries;
        while ((new_index - tail_ptr->begin_index) >= N_VALUE) {
            tail_ptr = tail_ptr->enable_next(tail, handle, tail_index);
        }

        return size;
    }

    // putRecVector only called by the thread of handle.
    /*
    int putRecVectorLockFree(std::vector<RecWithEpoch>& stack_tabs, long bytes, ThreadHandle* handle) {
        auto size = stack_tabs.size();

        for (;;) {
            auto tail_ptr = tail.load();
            long old = tail_index.load();
            long distance = old - tail_ptr->begin_index;

            assert(distance >= 0);
            if (distance < N_BITS) {
                if (tail_index.compare_exchange_strong(old, old + 1)) {
                    tail_ptr->arrays[distance].stack_tabs.swap(stack_tabs);
                    tail_ptr->arrays[distance].bytes = bytes;
                    return size;
                }
            } else if (distance == N_BITS) {
                if (tail_index.compare_exchange_strong(old, old + 1)) {
                    tail_ptr->arrays[distance].stack_tabs.swap(stack_tabs);
                    tail_ptr->arrays[distance].bytes = bytes;
                    tail_ptr->enable_next(tail, handle);
                    return size;
                }
            } else {
                //assert(distance == N); NO
                tail_ptr->enable_next(tail, handle);
            }
        }
    }
    */
    
    long reclaimAloneArrays(long reclaim_end) {
        long bytes = 0;
        long head_id = static_cast<unsigned long>(head_index.load()) >> N_SHIFT;
        long end_id = static_cast<unsigned long>(reclaim_end) >> N_SHIFT;
        long start = head_index.load() & N_BITS;
        long end = reclaim_end & N_BITS;

        long distance = end_id - head_id;
        if (distance == 0) {
            for (; start < end;) {
                bytes += head->arrays[start++].reclaim();
            }
        } else {
            for (; start < N_VALUE; ++start) {
                bytes += head->arrays[start].reclaim();
            }
            auto local = head->next.load();
            delete head;
            head = local;
            --distance;

            while (distance > 0) {
                for (int i = 0; i < N_VALUE; ++i) {
                    bytes += head->arrays[i].reclaim();
                }
                auto local = head->next.load();
                delete head;
                head = local;
                --distance;
            }

            for (int i = 0; i < end; i++) {
                bytes += head->arrays[i].reclaim();
            }
        }
        head_index.store(reclaim_end);
        return bytes;
    }

    bool is_need_guard();
    long reclaim_compulsive();
    long reclaim();
    void allReclaim();

    LinkedNodeOfArrayRec* head;
    std::atomic<LinkedNodeOfArrayRec*> tail;
    std::atomic<long> head_index;
    std::atomic<long> tail_index;
    std::atomic<long> control_GC;
    ThreadHandle* sentinel;
    int bytes_gc_threshold;
    int bytes_epoch_threshold;
    int threshold;
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
        return reinterpret_cast<T*>(reinterpret_cast<uintptr_t>(plain_addr) |
                                    static_cast<uintptr_t>(1));
    }

    static T* untagged_address(T* tagged_addr) {
        return reinterpret_cast<T*>(reinterpret_cast<uintptr_t>(tagged_addr) &
                                    static_cast<uintptr_t>(~1));
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
        // This is a COMMON method means every thread should clean the group.
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

        // =====must call control's methods before link to 'prev'
        f();
        // =====

        // while next == sentinel => There is no unlink threadHandle in the group.
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
    ThreadHandle(ThreadHandle* sentinel, RecBlocks* recBlocks,
                 int bytes_epoch_threshold, ConcurrencyControl* control)
            : NextWithUnpin(sentinel),
              epoch(-1),
              recBlocks(recBlocks),
              heap_tabs(),
              bytes_accumulate(0),
              gc_enable_lastbytes(0),
              bytes_epoch_threshold(bytes_epoch_threshold),
              epoch_add_lastbytes(0),
              control(control) {
        this->pin();
    }

    ThreadHandle()
            : NextWithUnpin(),
              epoch(-1),
              recBlocks(),
              heap_tabs(),
              bytes_accumulate(0),
              gc_enable_lastbytes(0),
              bytes_epoch_threshold(0),
              epoch_add_lastbytes(0),
              control(nullptr) {}

    void unbind(std::function<void()> f) { this->unpin(f); }

    ThreadHandle* lock_guard() {
        epoch.store((*recBlocks)->tail_index.load());
        return this;
    }

    ThreadHandle* unguard() {
        epoch.store(LEAVE);
        reclaim();
        return this;
    }

    void emit_to_queue(long bytes, ThreadHandle* handle) {
        long bytes_ = (bytes_accumulate += bytes) - epoch_add_lastbytes;
        if (bytes_ > bytes_epoch_threshold) {
            (*recBlocks)->putRecVectorWaitFree(heap_tabs, bytes_, bytes_ / bytes_epoch_threshold, this);
            epoch_add_lastbytes = bytes_accumulate;
        }
    }

    template <typename T, typename... Args>
    void retire(long bytes, Args&&... args) {
        heap_tabs.emplace_back(new T(std::forward<Args>(args)...));
        emit_to_queue(bytes += sizeof(T), this);
    }

    void clean() {
        for (auto& recObj : heap_tabs) {
            auto rec = recObj.getRecObj();
            Base::reclaim(rec);
        }
        heap_tabs.clear();
    }

    void reclaim() {
        long gc_result = (*recBlocks)->reclaim_compulsive();
    }

    long get_epoch() { return epoch.load(); }

private:
    // local epoch for this thread.
    std::atomic<long> epoch;
    std::optional<RecBlocks*> recBlocks;
    std::vector<RecWithEpoch> heap_tabs;
    long bytes_accumulate;
    long gc_enable_lastbytes;
    int bytes_epoch_threshold;
    long epoch_add_lastbytes;
    constexpr static long LEAVE = -1;

public:
    ConcurrencyControl* control;
};

bool RecBlocks::is_need_guard() {
    long head_index_now = head_index.load();
    long tail_index_now = tail_index.load();
    return (tail_index_now - head_index_now) > threshold;
}

long RecBlocks::reclaim_compulsive() {
    long reclaim_bytes = 0;
    for (;;) {
        long gc = control_GC.load();
        if (gc >= 0 || !control_GC.compare_exchange_strong(gc, -gc)) {

            if (is_need_guard()) {
                std::this_thread::yield();
                continue;
            }
            return reclaim_bytes;
        }

        const unsigned long min_epoch = tail_index.load();
        if (head_index.load() == min_epoch) {
            control_GC.store(gc);
            return reclaim_bytes;
        }

        unsigned long reclaim_end = min_epoch;
        auto* handle = sentinel->next.load();
        while (handle != sentinel) {
            unsigned long th_epoch = handle->get_epoch();
            reclaim_end = std::min(reclaim_end, th_epoch);
            handle = ThreadHandle::untagged_address(handle->next.load());
        }

        if (head_index.load() < reclaim_end) {
            reclaim_bytes += reclaimAloneArrays(reclaim_end);
            control_GC.store(gc - 1);

            if (is_need_guard()) {
                std::this_thread::yield();
                continue;
            }
            return reclaim_bytes;
        } else {
            control_GC.store(gc);

            if (is_need_guard()) {
                std::this_thread::yield();
                continue;
            }
            return reclaim_bytes;
        }
    }
}

long RecBlocks::reclaim() {
    long gc = control_GC.load();
    if (gc >= 0 || !control_GC.compare_exchange_strong(gc, -gc)) {
        return -1;
    }

    const unsigned long min_epoch = tail_index.load();
    if (head_index.load() == min_epoch) {
        control_GC.store(gc);
        return 0;
    }

    unsigned long reclaim_end = min_epoch;
    auto* handle = sentinel->next.load();
    while (handle != sentinel) {
        unsigned long th_epoch = handle->get_epoch();
        reclaim_end = std::min(reclaim_end, th_epoch);
        handle = ThreadHandle::untagged_address(handle->next.load());
    }

    if (head_index.load() < reclaim_end) {
        long bytes = reclaimAloneArrays(reclaim_end);
        control_GC.store(gc - 1);
        return bytes;
    } else {
        control_GC.store(gc);
        return 0;
    }
}

void RecBlocks::allReclaim() {
    long gc = control_GC.load();
    if (gc < 0 && control_GC.compare_exchange_strong(gc, -gc)) {
        long reclaim_end = tail_index.load();

        if (head_index.load() < reclaim_end) {
            reclaimAloneArrays(reclaim_end);
        }

        delete tail.load();
        control_GC.store(gc - 1);
    }
}

LinkedNodeOfArrayRec* LinkedNodeOfArrayRec::enable_next(std::atomic<LinkedNodeOfArrayRec*>& tail,
                                       ThreadHandle* threadHandle, std::atomic<long>& epoch) {
    LinkedNodeOfArrayRec* old = next.load();
    auto this_alias = this;
    if (old == nullptr) {
        auto new_next = new LinkedNodeOfArrayRec(begin_index + N_VALUE);
        if (!next.compare_exchange_strong(old, new_next)) {
            if (this == tail.load() && tail.compare_exchange_strong(this_alias, old)) {
                //threadHandle->retire_local<RecSingleNode<LinkedNodeOfArrayRec>>(epoch.load(), this);
            }
            delete new_next;
            return old;
        } else {
            if (this == tail.load() && tail.compare_exchange_strong(this_alias, new_next)) {
                //threadHandle->retire_local<RecSingleNode<LinkedNodeOfArrayRec>>(epoch.load(), this);
            }
            return new_next;
        }
    } else {
        if (this == tail.load() && tail.compare_exchange_strong(this_alias, old)) {
            //threadHandle->retire_local<RecSingleNode<LinkedNodeOfArrayRec>>(epoch.load(), this);
        }
        return old;
    }
}

class ReclaimChain : public Next<ReclaimChain> {
public:
    static ReclaimChain& getReclaimChain() {
        static ReclaimChain chain;
        return chain;
    }

    ReclaimChain() : Next<ReclaimChain>(), control(new ConcurrencyControl()), thread_gc() {
        ReclaimChain* const sentinel = this;
        ConcurrencyControl* ctrl = control;
        
        std::cout << "GC thread start!" << std::endl;
        std::thread* thread = new std::thread([sentinel, ctrl]() -> void {
            for(;;) {
                ReclaimChain* prev = sentinel;
                ReclaimChain* curr = prev->next.load();
                if (curr == sentinel) {
                    goto SLEEP;
                }
                
                if ((curr->control->flag.load() & 4) != 0) {
                    if (prev->next.compare_exchange_strong(curr, curr->next.load())) {
                        delete curr->control;
                        delete curr;
                    }
                    continue;
                } else if (curr->control->flag.load() == 0){
                    int flag = 0;
                    if (curr->control->flag.compare_exchange_strong(flag, 2)) {
                        curr->reclaim();
                        flag = curr->control->flag.fetch_xor(2);
                        if ((flag & 1) != 0) {
                            curr->control->blocking.unpark();
                        }
                    }
                }
                
                for (;;) {
                    prev = curr;
                    curr = prev->next.load();
                    if (curr == sentinel) {
                        goto SLEEP;
                    }
                    
                    if ((curr->control->flag.load() & 4) != 0) {
                        prev->next.store(curr->next.load());
                        delete curr->control;
                        delete curr;
                        curr = prev;
                        continue;
                    } else if (curr->control->flag.load() == 0){
                        int flag = 0;
                        if (curr->control->flag.compare_exchange_strong(flag, 2)) {
                            curr->reclaim();
                            flag = curr->control->flag.fetch_xor(2);
                            if ((flag & 1) != 0) {
                                curr->control->blocking.unpark();
                            }
                        }
                    }
                }
                
                SLEEP:
                if (ctrl->flag.load() == 1) {
                    ++ctrl->flag;
                    continue;
                }
                if (ctrl->flag.load() == 2) {
                    return;
                }
                ctrl->blocking.park(1000);
            }
        });
        thread_gc.emplace(thread, control);
    }
    
    ReclaimChain(ReclaimChain* sentinel, ConcurrencyControl* control) :
                                        Next<ReclaimChain>(sentinel), control(control), thread_gc() {}
    virtual ~ReclaimChain() { }
    
    ConcurrencyControl* control;
    class GcThread {
    public:
        GcThread(std::thread* gc_thread, ConcurrencyControl* control) : gc_thread(gc_thread), control(control) { }
        ~GcThread() {
            std::cout << "\nGC thread over!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << std::endl;
            control->flag.store(1);
            control->blocking.unpark();
            gc_thread->join();
            delete control;
            delete gc_thread;
        }

        ConcurrencyControl* control;
        std::thread* gc_thread;
    };

    std::optional<GcThread> thread_gc;

    virtual long reclaim() { return 0; }
};

template <typename T>
class ThreadGroup : public ReclaimChain {
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

        ThreadHandle* get_thread_handle(ThreadGroup<T>* group, ThreadHandle* sentinel, RecBlocks* recBlocks, int bytes_epoch_threshold) {
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
                auto handle = new ThreadHandle(sentinel, recBlocks, bytes_epoch_threshold, control);
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
    ThreadGroup(ReclaimChain* sentinel2, ConcurrencyControl* control, int bytes_gc_threshold, int bytes_epoch_threshold)
            : ReclaimChain(sentinel2, control),
              sentinel(),
              recBlocks{std::in_place, &sentinel, bytes_gc_threshold, bytes_epoch_threshold},
              bytes_epoch_threshold(bytes_epoch_threshold),
              handle_total(0) {
                  this->pin();
              }

    ThreadGroup()
            : ReclaimChain(),
              sentinel(),
              recBlocks{},
              bytes_epoch_threshold(0),
              handle_total(0) {}

    long reclaim() { return (*recBlocks).reclaim(); }
    void allReclaim() { (*recBlocks).allReclaim(); }

    ThreadHandle* bind() {
        thread_local ThreadHandleAggregate aggregate;
        ThreadHandle* handle = aggregate.get_thread_handle(this, &sentinel, &*recBlocks, bytes_epoch_threshold);
        return handle;
    }

public:
    ThreadHandle sentinel;
    std::optional<RecBlocks> recBlocks;
    const int bytes_epoch_threshold;
    std::atomic<int> handle_total;
};

template <typename T>
class ConcurrentBridge {
    friend class PackedHandle;
public:
    ConcurrentBridge(int bytes_gc_threshold = 1024 * 1024, int bytes_epoch_threshold = 64 * 1024)
            : /*control(new ConcurrencyControl()),*/ group(new ThreadGroup<T>(&ReclaimChain::getReclaimChain(), new ConcurrencyControl(), bytes_gc_threshold, bytes_epoch_threshold)) { }

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

        ConcurrencyControl* control = group->control;
        //int flag = control->flag.load();
        //assert((flag & 1) == 0);
        int prev_flag = control->flag.fetch_or(1);
        if (prev_flag == 0) {
            group->allReclaim();
        } else {
            assert((prev_flag & 2) != 0);
            do {
                control->blocking.park();
            } while((control->flag.load() & 2) != 0);
            group->allReclaim();
        }
        control->flag.fetch_or(4);

        //prev = sentinel->prev.load();
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
    }

private:
    //ConcurrencyControl* control;
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
