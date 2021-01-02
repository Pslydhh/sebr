#include "concurrent_hash_map.hpp"
#include "concurrent_ms_queue.hpp"
#include <set>

static int n_const = 10000000;
static int nthreads_const = 8;

ConcurrentHashMap<std::string, std::string>* global;
void test1() {
    std::cout << "Test ConcurrentHashMap  with GC" << std::endl;
    auto conMap = new ConcurrentHashMap<std::string, std::string>();

    std::vector<std::thread> threads;
    std::default_random_engine dre(time(0));
    std::uniform_int_distribution<uint64_t> di(0, 20000000000000);
    int n = n_const;
    uint64_t* keys = new uint64_t[n];
    std::cout << "insert " << n << " key[0~20000000000000]/value randomly" << std::endl
              << std::endl;

    auto beginTime = std::chrono::high_resolution_clock::now();
    {
        std::set<uint64_t> s;
        for (int i = 0; i < n; ++i) s.insert(di(dre));
        while (s.size() < (unsigned int)n) {
            s.insert(di(dre));
        }
        auto ite = s.begin();
        for (int i = 0; i < n; ++i) {
            keys[i] = *ite++;
        }
    }
    auto endTime = std::chrono::high_resolution_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "init " << n << " randoms with set Elapsed time is " << elapsedTime.count()
              << " milliseconds" << std::endl;

    int32_t nthreads = nthreads_const;
    std::cout << "threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                conMap->insertAbsent("doris" + std::to_string(keys[i * pro + j]), "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (std::thread& th : threads) th.join();
    assert(n_const == conMap->size());

    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "insert elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    threads.clear();

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value;
                bool r;
                r = conMap->erase("doris" + std::to_string(keys[i * consume + j]), &value);
                assert(r);
                assert(value == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (std::thread& th : threads) th.join();
    assert(0 == conMap->size());

    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "remove elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    threads.clear();

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value;
                bool r;
                r = conMap->find("doris" + std::to_string(keys[i * consume + j]), &value);
                assert(!r);
                //assert(value == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (std::thread& th : threads) th.join();
    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "get elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    threads.clear();

    ConcurrentHashMap<std::string, std::string> conMap2;
    std::cout << "2 only insert threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap2, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                conMap2.insertAbsent("doris" + std::to_string(keys[i * pro + j]), "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (std::thread& th : threads) th.join();

    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "insert elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    global = conMap;
    delete[] keys;
    //std::this_thread::sleep_for(std::chrono::seconds(1000));
}

void test2() {
    std::cout << "Test ConcurrentHashMap  with GC" << std::endl;
    ConcurrentHashMap<long, std::string> conMap;
    std::vector<std::thread> threads;
    std::default_random_engine dre(time(0));
    std::uniform_int_distribution<uint64_t> di(0, 20000000000000);

    int n = n_const;
    uint64_t* keys = new uint64_t[n];
    std::cout << "insert " << n << " key[0~20000000000000]/value randomly" << std::endl
              << std::endl;

    auto beginTime = std::chrono::high_resolution_clock::now();
    {
        std::set<uint64_t> s;
        for (int i = 0; i < n; ++i) s.insert(di(dre));
        while (s.size() < (unsigned int)n) {
            s.insert(di(dre));
        }
        auto ite = s.begin();
        for (int i = 0; i < n; ++i) {
            keys[i] = *ite++;
        }
    }
    auto endTime = std::chrono::high_resolution_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "init " << n << " randoms with set Elapsed time is " << elapsedTime.count()
              << " milliseconds" << std::endl;

    int32_t nthreads = nthreads_const;
    std::cout << "threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                conMap.insertAbsent(keys[i * pro + j], "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (std::thread& th : threads) th.join();

    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "insert elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    threads.clear();

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value;
                bool r;
                r = conMap.find(keys[i * consume + j], &value);
                assert(r);
                assert(value == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (std::thread& th : threads) th.join();
    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "get elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    threads.clear();

    delete[] keys;
}

void test3() {
    std::cout << "Test ConcurrentHashMap  with GC" << std::endl;
    ConcurrentHashMap<std::string, std::string> conMap;
    std::vector<std::thread> threads;
    std::default_random_engine dre(time(0));
    std::uniform_int_distribution<uint64_t> di(0, 20000000000000);
    int n = n_const;
    uint64_t* keys = new uint64_t[n];
    std::cout << "insert " << n << " key[0~20000000000000]/value randomly" << std::endl
              << std::endl;

    auto beginTime = std::chrono::high_resolution_clock::now();
    {
        std::set<uint64_t> s;
        for (int i = 0; i < n; ++i) s.insert(di(dre));
        while (s.size() < (unsigned int)n) {
            s.insert(di(dre));
        }
        auto ite = s.begin();
        for (int i = 0; i < n; ++i) {
            keys[i] = *ite++;
        }
    }
    auto endTime = std::chrono::high_resolution_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "init " << n << " randoms with set Elapsed time is " << elapsedTime.count()
              << " milliseconds" << std::endl;

    int32_t nthreads = nthreads_const;
    std::cout << "threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                conMap.insertAbsent("doris" + std::to_string(keys[i * pro + j]), "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (std::thread& th : threads) th.join();

    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "insert elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    threads.clear();

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value;
                bool r;
                r = conMap.erase("doris" + std::to_string(keys[i * consume + j]), &value);
                assert(r);
                assert(value == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    threads.emplace_back([&conMap] {
        auto iterator = conMap.begin();
        int nums = 0;
        while (iterator != conMap.end()) {
            ++nums;
            //std::cout << "key: " << iterator.key() << "\nval: " << iterator.val() << std::endl;
            ++iterator;
        }
        std::cout << nums << " iterators elapsed"
                  << " milliseconds" << std::endl;
    });

    for (std::thread& th : threads) th.join();
    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "remove elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    delete[] keys;
}

void test4() {
    std::cout << "Test ConcurrentHashMap  with GC" << std::endl;
    ConcurrentHashMap<std::string, std::string> conMap;
    std::vector<std::thread> threads;
    std::default_random_engine dre(time(0));
    std::uniform_int_distribution<uint64_t> di(0, 20000000000000);
    int n = n_const;
    uint64_t* keys = new uint64_t[n];
    std::cout << "insert " << n << " key[0~20000000000000]/value randomly" << std::endl
              << std::endl;

    auto beginTime = std::chrono::high_resolution_clock::now();
    {
        std::set<uint64_t> s;
        for (int i = 0; i < n; ++i) s.insert(di(dre));
        while (s.size() < (unsigned int)n) {
            s.insert(di(dre));
        }
        auto ite = s.begin();
        for (int i = 0; i < n; ++i) {
            keys[i] = *ite++;
        }
    }
    auto endTime = std::chrono::high_resolution_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "init " << n << " randoms with set Elapsed time is " << elapsedTime.count()
              << " milliseconds" << std::endl;

    int32_t nthreads = nthreads_const;
    std::cout << "threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                conMap.insertAbsent("doris" + std::to_string(keys[i * pro + j]), "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (std::thread& th : threads) th.join();

    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "insert elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    std::cout << "threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    threads.clear();
    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                bool r = conMap.insertAbsent("doris" + std::to_string(keys[i * pro + j]), "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
                assert(!r);
            }
        });
    }

    for (std::thread& th : threads) th.join();

    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "insert duplications elapsed time is " << elapsedTime.count() << " milliseconds"
              << std::endl;

    threads.clear();
    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value;
                bool r;
                r = conMap.erase("doris" + std::to_string(keys[i * consume + j]), &value);
                assert(r);
                assert(value == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    threads.emplace_back([&conMap] {
        auto iterator = conMap.begin();
        int nums = 0;
        while (iterator != conMap.end()) {
            ++nums;
            //std::cout << "key: " << iterator.key() << "\nval: " << iterator.val() << std::endl;
            ++iterator;
        }
        std::cout << nums << " iterators elapsed"
                  << " milliseconds" << std::endl;
    });

    for (std::thread& th : threads) th.join();
    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "remove elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    delete[] keys;
}

void test5() {
    std::cout << "Test ConcurrentHashMap  with GC" << std::endl;
    ConcurrentHashMap<std::string, std::string> conMap;
    std::vector<std::thread> threads;
    std::default_random_engine dre(time(0));
    std::uniform_int_distribution<uint64_t> di(0, 20000000000000);
    int n = n_const;
    uint64_t* keys = new uint64_t[n];
    std::cout << "insert " << n << " key[0~20000000000000]/value randomly" << std::endl
              << std::endl;

    auto beginTime = std::chrono::high_resolution_clock::now();
    {
        std::set<uint64_t> s;
        for (int i = 0; i < n; ++i) s.insert(di(dre));
        while (s.size() < (unsigned int)n) {
            s.insert(di(dre));
        }
        auto ite = s.begin();
        for (int i = 0; i < n; ++i) {
            keys[i] = *ite++;
        }
    }
    auto endTime = std::chrono::high_resolution_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "init " << n << " randoms with set Elapsed time is " << elapsedTime.count()
              << " milliseconds" << std::endl;

    int32_t nthreads = nthreads_const;
    std::cout << "threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                conMap.insertAbsent("doris" + std::to_string(keys[i * pro + j]), "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (std::thread& th : threads) th.join();

    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "insert elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    std::cout << "threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    threads.clear();
    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                std::string value("23412");
                bool r = conMap.insert("doris" + std::to_string(keys[i * pro + j]), &value);
                assert(r);
                assert(value == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value;
                bool r;
                r = conMap.find("doris" + std::to_string(keys[i * consume + j]), &value);
                assert(r && (value == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW" || value == "23412"));
            }
        });
    }

    for (std::thread& th : threads) th.join();

    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "insert duplications elapsed time is " << elapsedTime.count() << " milliseconds"
              << std::endl;

    threads.clear();
    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value;
                bool r;
                r = conMap.erase("doris" + std::to_string(keys[i * consume + j]), &value);
                assert(r);
                assert(value == "23412");
            }
        });
    }

    threads.emplace_back([&conMap] {
        auto iterator = conMap.begin();
        int nums = 0;
        while (iterator != conMap.end()) {
            ++nums;
            //std::cout << "key: " << iterator.key() << "\nval: " << iterator.val() << std::endl;
            ++iterator;
        }
        std::cout << nums << " iterators elapsed"
                  << " milliseconds" << std::endl;
    });

    for (std::thread& th : threads) th.join();
    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "remove elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    delete[] keys;
}

void test6() {
    std::cout << "Test ConcurrentHashMap  with GC" << std::endl;
    ConcurrentHashMap<std::string, std::string> conMap;
    std::vector<std::thread> threads;
    std::default_random_engine dre(time(0));
    std::uniform_int_distribution<uint64_t> di(0, 20000000000000);
    int n = n_const;
    uint64_t* keys = new uint64_t[n];
    std::cout << "insert " << n << " key[0~20000000000000]/value randomly" << std::endl
              << std::endl;

    auto beginTime = std::chrono::high_resolution_clock::now();
    {
        std::set<uint64_t> s;
        for (int i = 0; i < n; ++i) s.insert(di(dre));
        while (s.size() < (unsigned int)n) {
            s.insert(di(dre));
        }
        auto ite = s.begin();
        for (int i = 0; i < n; ++i) {
            keys[i] = *ite++;
        }
    }
    auto endTime = std::chrono::high_resolution_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "init " << n << " randoms with set Elapsed time is " << elapsedTime.count()
              << " milliseconds" << std::endl;

    int32_t nthreads = nthreads_const;
    std::cout << "threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                conMap.insertAbsent("doris" + std::to_string(keys[i * pro + j]), "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (std::thread& th : threads) th.join();

    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "insert elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    threads.clear();
    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value("DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
                bool r;
                r = conMap.eraseEqual("pdoris" + std::to_string(keys[i * consume + j]), value);
                assert(!r);
            }
        });
    }

    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value("23412");
                bool r;
                r = conMap.eraseEqual("doris" + std::to_string(keys[i * consume + j]), value);
                assert(!r);
            }
        });
    }

    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value("DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
                bool r;
                r = conMap.eraseEqual("doris" + std::to_string(keys[i * consume + j]), value);
                assert(r);
            }
        });
    }

    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value;
                bool r;
                r = conMap.find("doris" + std::to_string(keys[i * consume + j]), &value);
                assert(!r || value == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    threads.emplace_back([&conMap] {
        auto iterator = conMap.begin();
        int nums = 0;
        while (iterator != conMap.end()) {
            ++nums;
            //std::cout << "key: " << iterator.key() << "\nval: " << iterator.val() << std::endl;
            ++iterator;
        }
        std::cout << nums << " iterators elapsed"
                  << " milliseconds" << std::endl;
    });

    for (std::thread& th : threads) th.join();
    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "remove elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    delete[] keys;
}

void test7() {
    std::cout << "Test ConcurrentHashMap  with GC" << std::endl;
    ConcurrentHashMap<std::string, std::string> conMap;
    std::vector<std::thread> threads;
    std::default_random_engine dre(time(0));
    std::uniform_int_distribution<uint64_t> di(0, 20000000000000);
    int n = n_const;
    uint64_t* keys = new uint64_t[n];
    std::cout << "insert " << n << " key[0~20000000000000]/value randomly" << std::endl
              << std::endl;

    auto beginTime = std::chrono::high_resolution_clock::now();
    {
        std::set<uint64_t> s;
        for (int i = 0; i < n; ++i) s.insert(di(dre));
        while (s.size() < (unsigned int)n) {
            s.insert(di(dre));
        }
        auto ite = s.begin();
        for (int i = 0; i < n; ++i) {
            keys[i] = *ite++;
        }
    }
    auto endTime = std::chrono::high_resolution_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "init " << n << " randoms with set Elapsed time is " << elapsedTime.count()
              << " milliseconds" << std::endl;

    int32_t nthreads = nthreads_const;
    std::cout << "threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                conMap.insertAbsent("doris" + std::to_string(keys[i * pro + j]), "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (std::thread& th : threads) th.join();

    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "insert elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    std::cout << "threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    threads.clear();
    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                bool r = conMap.insertAbsent("doris" + std::to_string(keys[i * pro + j]), "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
                assert(!r);
            }
        });
    }

    for (std::thread& th : threads) th.join();

    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "insert duplications elapsed time is " << elapsedTime.count() << " milliseconds"
              << std::endl;

    threads.clear();

    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value;
                bool r;
                r = conMap.erase("adoris" + std::to_string(keys[i * consume + j]), &value);
                assert(!r);
            }
        });
    }

    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value;
                bool r;
                r = conMap.erase("doris" + std::to_string(keys[i * consume + j]), &value);
                assert(r);
                assert(value == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value;
                bool r;
                r = conMap.find("doris" + std::to_string(keys[i * consume + j]), &value);
                assert(!r || value == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    threads.emplace_back([&conMap] {
        auto iterator = conMap.begin();
        int nums = 0;
        while (iterator != conMap.end()) {
            ++nums;
            //std::cout << "key: " << iterator.key() << "\nval: " << iterator.val() << std::endl;
            ++iterator;
        }
        std::cout << nums << " iterators elapsed"
                  << " milliseconds" << std::endl;
    });

    for (std::thread& th : threads) th.join();
    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "remove elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    delete[] keys;
}

void test8() {
    std::cout << "Test ConcurrentHashMap  with GC" << std::endl;
    ConcurrentHashMap<std::string, std::string> conMap;
    std::vector<std::thread> threads;
    std::default_random_engine dre(time(0));
    std::uniform_int_distribution<uint64_t> di(0, 20000000000000);
    int n = n_const;
    uint64_t* keys = new uint64_t[n];
    std::cout << "insert " << n << " key[0~20000000000000]/value randomly" << std::endl
              << std::endl;

    auto beginTime = std::chrono::high_resolution_clock::now();
    {
        std::set<uint64_t> s;
        for (int i = 0; i < n; ++i) s.insert(di(dre));
        while (s.size() < (unsigned int)n) {
            s.insert(di(dre));
        }
        auto ite = s.begin();
        for (int i = 0; i < n; ++i) {
            keys[i] = *ite++;
        }
    }
    auto endTime = std::chrono::high_resolution_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "init " << n << " randoms with set Elapsed time is " << elapsedTime.count()
              << " milliseconds" << std::endl;

    int32_t nthreads = nthreads_const;
    std::cout << "threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                conMap.insertAbsent("doris" + std::to_string(keys[i * pro + j]), "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (std::thread& th : threads) th.join();

    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "insert elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    std::cout << "threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    threads.clear();
    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                std::string value("23412");
                bool r;
                r = conMap.insertAbsent("doris" + std::to_string(keys[i * pro + j]), value);
            }
        });
    }

    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                std::string value("23412");
                bool r;
                r = conMap.insert("doris" + std::to_string(keys[i * pro + j]), &value);
                assert(!r || (value == "23412" || value == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW"));
            }
        });
    }

    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value;
                bool r;
                r = conMap.erase("doris" + std::to_string(keys[i * consume + j]), &value);
                assert(!r || (value == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW" || value == "23412"));
            }
        });
    }

    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value("DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
                bool r;
                r = conMap.eraseEqual("doris" + std::to_string(keys[i * consume + j]), value);
            }
        });
    }

    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value("23412");
                bool r;
                r = conMap.eraseEqual("kdoris" + std::to_string(keys[i * consume + j]), value);
                assert(!r);
            }
        });
    }

    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value("23413");
                bool r;
                r = conMap.eraseEqual("doris" + std::to_string(keys[i * consume + j]), value);
                assert(!r);
            }
        });
    }

    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value;
                bool r;
                r = conMap.find("doris" + std::to_string(keys[i * consume + j]), &value);
                assert(!r || (value == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW" || value == "23412"));
            }
        });
    }

    threads.emplace_back([&conMap] {
        auto iterator = conMap.begin();
        int nums = 0;
        while (iterator != conMap.end()) {
            ++nums;
            //std::cout << "key: " << iterator.key() << "\nval: " << iterator.val() << std::endl;
            ++iterator;
        }
        std::cout << nums << " iterators elapsed"
                  << " milliseconds" << std::endl;
    });

    for (std::thread& th : threads) th.join();
    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "remove elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    delete[] keys;
}

// test for park/unpark.
void test9() {
    std::cout << "Test ConcurrentHashMap  with GC" << std::endl;
    ConcurrentHashMap<std::string, std::string> conMap;
    std::vector<std::thread> threads;
    std::default_random_engine dre(time(0));
    std::uniform_int_distribution<uint64_t> di(0, 20000000000000);
    int n = n_const;
    uint64_t* keys = new uint64_t[n];
    std::cout << "insert " << n << " key[0~20000000000000]/value randomly" << std::endl
              << std::endl;

    auto beginTime = std::chrono::high_resolution_clock::now();
    {
        std::set<uint64_t> s;
        for (int i = 0; i < n; ++i) s.insert(di(dre));
        while (s.size() < (unsigned int)n) {
            s.insert(di(dre));
        }
        auto ite = s.begin();
        for (int i = 0; i < n; ++i) {
            keys[i] = *ite++;
        }
    }
    auto endTime = std::chrono::high_resolution_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "init " << n << " randoms with set Elapsed time is " << elapsedTime.count()
              << " milliseconds" << std::endl;

    int32_t nthreads = nthreads_const;
    std::cout << "threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                conMap.insertAbsent("doris" + std::to_string(keys[i * pro + j]), "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (int k = 0; k < 4; ++k) {
        for (int j = 0, consume = nthreads; j < consume; ++j) {
            threads.emplace_back([&conMap, n, keys, consume, j] {
                for (int i = 0; i < n / consume; ++i) {
                    std::string value;
                    bool r;
                    r = conMap.find("doris" + std::to_string(keys[i * consume + j]), &value);
                    assert(!r || value == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
                }
            });
        }
    }

    for (std::thread& th : threads) th.join();
    assert(n_const == conMap.size());

    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "insert elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    threads.clear();
    delete[] keys;
}

void test10() {
    std::cout << "Test ConcurrentHashMap  with GC" << std::endl;
    auto conMap = new ConcurrentHashMap<long, std::string>();
    std::vector<std::thread> threads;
    std::default_random_engine dre(time(0));
    std::uniform_int_distribution<uint64_t> di(0, 20000000000000);

    int n = n_const;
    uint64_t* keys = new uint64_t[n];
    std::cout << "insert " << n << " key[0~20000000000000]/value randomly" << std::endl
              << std::endl;

    auto beginTime = std::chrono::high_resolution_clock::now();
    {
        std::set<uint64_t> s;
        for (int i = 0; i < n; ++i) s.insert(di(dre));
        while (s.size() < (unsigned int)n) {
            s.insert(di(dre));
        }
        auto ite = s.begin();
        for (int i = 0; i < n; ++i) {
            keys[i] = *ite++;
        }
    }
    auto endTime = std::chrono::high_resolution_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "init " << n << " randoms with set Elapsed time is " << elapsedTime.count()
              << " milliseconds" << std::endl;

    int32_t nthreads = nthreads_const;
    std::cout << "threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                conMap->insertAbsent(keys[i * pro + j], "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (std::thread& th : threads) th.join();

    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "insert elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    threads.clear();

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value;
                bool r;
                r = conMap->find(keys[i * consume + j], &value);
                assert(r);
                assert(value == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));
            if (j == 0) {
                std::cout << "BeG delete conMap(emit TSAN WARNING)" << std::endl;
                //delete conMap;
                std::cout << "HAs delete conMap" << std::endl;
            }
        });
    }

    for (std::thread& th : threads) th.join();
    delete conMap;
    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "get elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    threads.clear();

    delete[] keys;
}

void test11() {
    std::cout << "Test ConcurrentHashMap  with GC" << std::endl;
    constexpr static int NUM_MAP = 64;
    ConcurrentHashMap<long, std::string>* conMap[NUM_MAP];
    for (int i = 0; i < NUM_MAP; ++i) {
        conMap[i] = new ConcurrentHashMap<long, std::string>();
    }
    std::vector<std::thread> threads;
    std::default_random_engine dre(time(0));
    std::uniform_int_distribution<uint64_t> di(0, 20000000000000);

    int n = n_const;
    uint64_t* keys = new uint64_t[n];
    std::cout << "insert " << n << " key[0~20000000000000]/value randomly" << std::endl
              << std::endl;

    auto beginTime = std::chrono::high_resolution_clock::now();
    {
        std::set<uint64_t> s;
        for (int i = 0; i < n; ++i) s.insert(di(dre));
        while (s.size() < (unsigned int)n) {
            s.insert(di(dre));
        }
        auto ite = s.begin();
        for (int i = 0; i < n; ++i) {
            keys[i] = *ite++;
        }
    }
    auto endTime = std::chrono::high_resolution_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "init " << n << " randoms with set Elapsed time is " << elapsedTime.count()
              << " milliseconds" << std::endl;

    int32_t nthreads = nthreads_const;
    std::cout << "threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                for (int k = 0; k < NUM_MAP; ++k) {
                    conMap[k]->insertAbsent(keys[i * pro + j], "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
                }
            }
        });
    }

    for (std::thread& th : threads) th.join();

    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "insert elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    threads.clear();

    delete[] keys;
    for (int i = 0; i < NUM_MAP; ++i) {
        delete conMap[i];
    }
}

void test12() {
    std::cout << "Test ConcurrentHashMap  with GC" << std::endl;
    ConcurrentHashMap<long, std::string> conMap;
    std::vector<std::thread> threads;
    std::default_random_engine dre(time(0));
    std::uniform_int_distribution<uint64_t> di(0, 20000000000000);

    int n = n_const;
    uint64_t* keys = new uint64_t[n];
    std::cout << "insert " << n << " key[0~20000000000000]/value randomly" << std::endl
              << std::endl;

    auto beginTime = std::chrono::high_resolution_clock::now();
    {
        std::set<uint64_t> s;
        for (int i = 0; i < n; ++i) s.insert(di(dre));
        while (s.size() < (unsigned int)n) {
            s.insert(di(dre));
        }
        auto ite = s.begin();
        for (int i = 0; i < n; ++i) {
            keys[i] = *ite++;
        }
    }
    auto endTime = std::chrono::high_resolution_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "init " << n << " randoms with set Elapsed time is " << elapsedTime.count()
              << " milliseconds" << std::endl;

    int32_t nthreads = nthreads_const;
    std::cout << "threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                conMap.insertAbsent(keys[i * pro + j], "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (std::thread& th : threads) th.join();

    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "insert elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    threads.clear();

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value;
                auto r = conMap.find_reference(keys[i * consume + j]);
                assert(r.is_data());
                assert(r.val() == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (std::thread& th : threads) th.join();
    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "get elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    threads.clear();

    delete[] keys;
}

void test13() {
    std::cout << "Test ConcurrentHashMap  with GC" << std::endl;
    ConcurrentHashMap<std::string, std::string> conMap;
    std::vector<std::thread> threads;
    std::default_random_engine dre(time(0));
    std::uniform_int_distribution<uint64_t> di(0, 20000000000000);
    int n = n_const;
    uint64_t* keys = new uint64_t[n];
    std::cout << "insert " << n << " key[0~20000000000000]/value randomly" << std::endl
              << std::endl;

    auto beginTime = std::chrono::high_resolution_clock::now();
    {
        std::set<uint64_t> s;
        for (int i = 0; i < n; ++i) s.insert(di(dre));
        while (s.size() < (unsigned int)n) {
            s.insert(di(dre));
        }
        auto ite = s.begin();
        for (int i = 0; i < n; ++i) {
            keys[i] = *ite++;
        }
    }
    auto endTime = std::chrono::high_resolution_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "init " << n << " randoms with set Elapsed time is " << elapsedTime.count()
              << " milliseconds" << std::endl;

    int32_t nthreads = nthreads_const;
    std::cout << "threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                conMap.insertAbsent("doris" + std::to_string(keys[i * pro + j]), "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (std::thread& th : threads) th.join();

    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "insert elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    std::cout << "threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    threads.clear();
    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value;
                //bool r;
                auto r = conMap.find_reference("doris" + std::to_string(keys[i * consume + j]));
                assert(!r.is_data() || (r.val() == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW" || r.val() == "23412"));
            }
        });
    }

    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                std::string value("23412");
                bool r;
                r = conMap.insertAbsent("doris" + std::to_string(keys[i * pro + j]), value);
            }
        });
    }

    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                std::string value("23412");
                bool r;
                r = conMap.insert("doris" + std::to_string(keys[i * pro + j]), &value);
                assert(!r || (value == "23412" || value == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW"));
            }
        });
    }

    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value;
                bool r;
                r = conMap.erase("doris" + std::to_string(keys[i * consume + j]), &value);
                assert(!r || (value == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW" || value == "23412"));
            }
        });
    }

    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value("DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
                bool r;
                r = conMap.eraseEqual("doris" + std::to_string(keys[i * consume + j]), value);
            }
        });
    }

    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value("23412");
                bool r;
                r = conMap.eraseEqual("kdoris" + std::to_string(keys[i * consume + j]), value);
                assert(!r);
            }
        });
    }

    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value("23413");
                bool r;
                r = conMap.eraseEqual("doris" + std::to_string(keys[i * consume + j]), value);
                assert(!r);
            }
        });
    }

    threads.emplace_back([&conMap] {
        auto iterator = conMap.begin();
        int nums = 0;
        while (iterator != conMap.end()) {
            ++nums;
            //std::cout << "key: " << iterator.key() << "\nval: " << iterator.val() << std::endl;
            ++iterator;
        }
        std::cout << nums << " iterators elapsed"
                  << " milliseconds" << std::endl;
    });

    for (std::thread& th : threads) th.join();
    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "remove elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    delete[] keys;
}

void test14() {
    std::cout << "Test ConcurrentHashMap  with GC" << std::endl;
    ConcurrentHashMap<long, std::string> conMap;
    std::vector<std::thread> threads;
    std::default_random_engine dre(time(0));
    std::uniform_int_distribution<uint64_t> di(0, 20000000000000);

    int n = n_const;
    uint64_t* keys = new uint64_t[n];
    std::cout << "insert " << n << " key[0~20000000000000]/value randomly" << std::endl
              << std::endl;

    auto beginTime = std::chrono::high_resolution_clock::now();
    {
        std::set<uint64_t> s;
        for (int i = 0; i < n; ++i) s.insert(di(dre));
        while (s.size() < (unsigned int)n) {
            s.insert(di(dre));
        }
        auto ite = s.begin();
        for (int i = 0; i < n; ++i) {
            keys[i] = *ite++;
        }
    }
    auto endTime = std::chrono::high_resolution_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "init " << n << " randoms with set Elapsed time is " << elapsedTime.count()
              << " milliseconds" << std::endl;

    int32_t nthreads = nthreads_const;
    std::cout << "threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, pro = nthreads; j < pro; ++j) {
        threads.emplace_back([&conMap, n, keys, pro, j] {
            for (int i = 0; i < n / pro; ++i) {
                conMap.insertAbsent(keys[i * pro + j], "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (std::thread& th : threads) th.join();

    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "insert elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    threads.clear();

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value;
                bool r;
                r = conMap.find(keys[i * consume + j], &value);
                assert(r);
                assert(value == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (std::thread& th : threads) th.join();
    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "copy et elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    threads.clear();

    beginTime = std::chrono::high_resolution_clock::now();
    for (int j = 0, consume = nthreads; j < consume; ++j) {
        threads.emplace_back([&conMap, n, keys, consume, j] {
            for (int i = 0; i < n / consume; ++i) {
                std::string value;
                auto r = conMap.find_reference(keys[i * consume + j]);
                assert(r.is_data());
                assert(r.val() == "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
            }
        });
    }

    for (std::thread& th : threads) th.join();
    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "reference get elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    threads.clear();

    delete[] keys;
}

void test_scalable_hashtable(int i) {
    std::cout << "\n\nIterator " << i << " test!" << std::endl;
    std::cout << "test1\n";
    test1();

    std::cout << "test2\n";
    test2();
    std::cout << "test3\n";
    test3();
    std::cout << "test4\n";
    test4();
    std::cout << "test5\n";
    test5();
    std::cout << "test6\n";
    test6();
    std::cout << "test7\n";
    test7();
    std::cout << "test8\n";
    test8();
    std::cout << "test9\n";
    test9();
    std::cout << "test10\n";
    test10();
    //std::cout << "test11\n";
    //test11();
    std::cout << "test12\n";
    test12();
    std::cout << "test13\n";
    test13();
    std::cout << "test14\n";
    test14();

    std::cout << "test over\n";
    std::cout << "\n\n";
}

void test_scalable_queue_no_pin(int count, int num) {
    ms_queue<int> queue, queue2;
    std::vector<std::thread> threads;

    {
        auto beginTime = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < num; ++i) {
            threads.emplace_back([&queue, &queue2, count, num] () -> void {
                ThreadHandle* handle = queue.bind();
                ThreadHandle* handle2 = queue2.bind();
                for (int j = 0; j < (count / num); ++j) {
                    queue.push2(53211, handle);
                    queue2.push2(53211, handle2);
                }
            });
        }
        for (std::thread& th : threads) th.join();
        threads.clear();
        auto endTime = std::chrono::high_resolution_clock::now();
        auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
        std::cout << "non pin local push elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;
    }
}

void test_scalable_queue(int count, int num) {
    ms_queue<int> queue, queue2;
    std::vector<std::thread> threads;

    {
        auto beginTime = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < num; ++i) {
            threads.emplace_back([&queue, &queue2, count, num] () -> void {
                ThreadHandle* handle = queue.bind();
                ThreadHandle* handle2 = queue2.bind();
                for (int j = 0; j < (count / num); ++j) {
                    queue.push(53211, handle);
                    queue2.push(53211, handle2);
                }
            });
        }
        for (std::thread& th : threads) th.join();
        threads.clear();
        auto endTime = std::chrono::high_resolution_clock::now();
        auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
        std::cout << "local push elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;
    }

    /*
    {
        auto beginTime = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < num; ++i) {
            threads.emplace_back([&queue, &queue2, count, num] () -> void {
                ThreadHandle* handle = queue.bind();
                ThreadHandle* handle2 = queue2.bind();
                for (int j = 0; j < (count / num) - 10; ++j) {
                    int value;
                    queue.pop(&value, handle);
                    assert(value = 53211);
                    queue2.pop(&value, handle2);
                    assert(value = 53211);
                }
            });
        }
        for (std::thread& th : threads) th.join();
        threads.clear();
        auto endTime = std::chrono::high_resolution_clock::now();
        auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
        std::cout << "local pop elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;
    }
    */

    {
        auto beginTime = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < num; ++i) {
            threads.emplace_back([&queue, &queue2, count, num] () -> void {
                for (int j = 0; j < (count / num); ++j) {
                    queue.push(53211);
                    queue2.push(53211);
                }
            });
        }
        for (std::thread& th : threads) th.join();
        threads.clear();
        auto endTime = std::chrono::high_resolution_clock::now();
        auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
        std::cout << "push elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;
    }
    
    /*
    {
        auto beginTime = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < num; ++i) {
            threads.emplace_back([&queue, &queue2, count, num] () -> void {
                for (int j = 0; j < (count / num) - 10; ++j) {
                    int value;
                    queue.pop(&value);
                    assert(value = 53211);
                    queue2.pop(&value);
                    assert(value = 53211);
                }
            });
        }
        for (std::thread& th : threads) th.join();
        threads.clear();
        auto endTime = std::chrono::high_resolution_clock::now();
        auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
        std::cout << "pop elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;
    }
    */
}

void test_scalable_queue2(int count, int num) {
    ms_queue<int> queue;
    std::vector<std::thread> threads;

    {
        auto beginTime = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < num; ++i) {
            threads.emplace_back([&queue, count, num] () -> void {
                ThreadHandle* handle = queue.bind();
                for (int j = 0; j < (count / num); ++j) {
                    queue.push(53211, handle);
                    int value;
                    queue.pop(&value, handle);
                    assert(value == 53211);
                }
            });
        }
        for (std::thread& th : threads) th.join();
        threads.clear();
        auto endTime = std::chrono::high_resolution_clock::now();
        auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
        std::cout << "local push/pop elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;
    }

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

void test_russian_dolls() {
    std::cout << "Test ConcurrentHashMap with GC" << std::endl;
    ConcurrentHashMap<std::string, std::string> conMap;
    std::vector<std::thread> threads;
    std::default_random_engine dre(time(0));
    std::uniform_int_distribution<uint64_t> di(0, 20000000000000);
    int n = n_const;
    uint64_t* keys = new uint64_t[n];
    std::cout << "insert " << n << " key[0~20000000000000]/value randomly" << std::endl
              << std::endl;

    auto beginTime = std::chrono::high_resolution_clock::now();
    {
        std::set<uint64_t> s;
        for (int i = 0; i < n; ++i) s.insert(di(dre));
        while (s.size() < (unsigned int)n) {
            s.insert(di(dre));
        }
        auto ite = s.begin();
        for (int i = 0; i < n; ++i) {
            keys[i] = *ite++;
        }
    }
    auto endTime = std::chrono::high_resolution_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "init " << n << " randoms with set Elapsed time is " << elapsedTime.count()
              << " milliseconds" << std::endl;

    int32_t nthreads = nthreads_const - 1;
    std::cout << "threads: " << nthreads << std::endl;
    std::cout << "num of k/v(s): " << n << std::endl;
    std::cout << "print first 3 key: " << std::endl;
    std::cout << keys[0] << std::endl << std::endl;

    int num_per_thread = n / nthreads_const;
    std::function<void(void)> task = [&conMap, keys, num_per_thread, &nthreads, &task] () -> void {
        std::cout << "nthreads: " << nthreads << std::endl;
        std::vector<std::thread> threads_local;
        auto local_thread_index = nthreads;
        if (nthreads > 0) {
            --nthreads;
            threads_local.emplace_back(task);
        }

        for (int i = 0; i < num_per_thread; ++i) {
            conMap.insertAbsent("doris" + std::to_string(keys[local_thread_index * num_per_thread + i]), "DORISDF343j43ljjj#$LJLJJFOJFOEFJOEFJOEJFOEJFOEJFOEFJOW");
        }

        for (auto& thread : threads_local) {
            thread.join();
        }
    };

    beginTime = std::chrono::high_resolution_clock::now();

    threads.emplace_back(task);

    for (std::thread& th : threads) th.join();

    endTime = std::chrono::high_resolution_clock::now();
    elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime);
    std::cout << "insert elapsed time is " << elapsedTime.count() << " milliseconds" << std::endl;

    threads.clear();
    delete[] keys;
}

int main(int argc, char* argv[]) {
    int times = atoi(argv[1]);
    n_const = atoi(argv[2]);
    nthreads_const = atoi(argv[3]);
    for (int i = 0; i < times; ++i) {
        std::thread thread([i]() -> void { 
            test_scalable_hashtable(i);
            test_scalable_queue(n_const, nthreads_const);
            test_scalable_queue_no_pin(n_const, nthreads_const);
            test_scalable_queue(n_const, nthreads_const);
            test_scalable_queue2(n_const, nthreads_const);
            test_russian_dolls();
        });
        thread.join();
        delete global;
    }

    return 0;
}
