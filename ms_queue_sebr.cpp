#include <cassert>
#include <atomic>
#include <functional>
#include <iostream>
#include <thread>
#include <vector>
#include "sebr_local.hpp"

template <typename T>
class ms_queue : sebr::ConcurrentBridge<ms_queue<T>>
{
struct Node {
    Node() : data(), next(nullptr) {}
    Node(const T& data) : data(data), next(nullptr) {}
    T data;
    std::atomic<Node*> next;
};

class RecLockFreeNode : public sebr::ReclaimBridge<RecLockFreeNode> {
    Node* node;
public:
    RecLockFreeNode(Node* node) : node(node) { }

    void reclaim() {
        delete node;
    }
};

public:
    ms_queue() : sebr::ConcurrentBridge<ms_queue<T>>(), Head(new Node()), Tail(Head.load()) { }

    ~ms_queue() {
        Node* end = Tail.load();
        Node* node = Head.load();
        for(;;) {
            Node* next = node->next.load();
            delete node;
            if (node == end) {
                return ;
            }
            node = next;
        }
    }

    void push(const T& data) {
        Node* node = new Node(data);
        Node* tail = nullptr;
        sebr::Pin pin(this);
        for (;;) {
            tail = Tail.load();
            Node* next = tail->next.load();
            if (tail == Tail.load()) {
                if (next == nullptr) {
                    if (tail->next.compare_exchange_strong(next, node)) {
                        break;
                    }
                } else {
                    Tail.compare_exchange_strong(tail, next);
                }
            }
        }
        Tail.compare_exchange_strong(tail, node);
    }

    bool pop(T* ptr) {
        Node* head = nullptr;
        Node* next = nullptr;
        sebr::Pin pin(this);
        for (;;) {
            head = Head.load();
            Node* tail = Tail.load();
            next = head->next.load();
            if (head == Head.load()) {
                if (head == tail) {
                    if (next == nullptr) {
                        return false;
                    }
                    Tail.compare_exchange_strong(tail, next);
                } else {
                    *ptr = next->data;
                    if (Head.compare_exchange_strong(head, next)) {
                        break;
                    }
                }
            }
        }
        pin.retire<RecLockFreeNode> (sizeof(Node), head);
        return true;
    }

private:
    std::atomic<Node*> Head;
    std::atomic<Node*> Tail;
};

long n_const;
long nthreads_const;

void test_scalable_queue(int count, int num) {
    ms_queue<int> queue;
    std::vector<std::thread> threads;

    {
        auto beginTime = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < num; ++i) {
            threads.emplace_back([&queue, count, num] () -> void {
                for (int j = 0; j < (count / num); ++j) {
                    queue.push(53211);
                    int value;
                    queue.pop(&value);
                    assert(value == 53211);
                }
            });
        }
        for (std::thread& th : threads) th.join();
        threads.clear();
        auto endTime = std::chrono::high_resolution_clock::now();
        auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
        std::cout << "push/pop elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;
    }
}

int main(int argc, char* argv[]) {
    int times = atoi(argv[1]);
    n_const = atoi(argv[2]);
    nthreads_const = atoi(argv[3]);
    for (int i = 0; i < times; ++i) {
        std::thread thread([]() -> void {
            test_scalable_queue(n_const, nthreads_const);
        });
        thread.join();
    }

    return 0;
}
