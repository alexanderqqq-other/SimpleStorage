#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <string>
#include <unordered_set>
#include <iostream>
#include <chrono>
#include <map>
#include <shared_mutex>
#include "../src/skiplist.h"
#include "test_utils.h"
#include "../src/types.h"

template<typename SL>
void runMultiThreadedInsertAndFind(SL& skiplist, const std::string& tag) {
    const int num_threads = 8;
    const int keys_per_thread = 100000;
    std::vector<std::thread> threads;

    auto start_time = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < keys_per_thread; ++j) {
                std::string key = "thread_" + std::to_string(i) + "_key_" + std::to_string(j);
                if constexpr (std::is_same_v<SL, std::map<std::string, int>>) {
                    static std::shared_mutex m;
                    {
                        std::lock_guard lock(m);
                        skiplist.emplace(key, i * keys_per_thread + j);
                    }
                }
                else {
                    skiplist.insert(std::make_pair(key, i * keys_per_thread + j));
                }
            }
            });
    }
    for (auto& t : threads) t.join();
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    std::cout << tag << " Insert duration: " << duration << " ms" << std::endl;

    for (int i = 0; i < num_threads; ++i) {
        for (int j = 0; j < keys_per_thread; ++j) {
            std::string key = "thread_" + std::to_string(i) + "_key_" + std::to_string(j);
            auto it = skiplist.find(key);
            ASSERT_TRUE(it != skiplist.end());
            if constexpr (std::is_same_v<SL, std::map<std::string, int>>) {
                ASSERT_EQ(it->second, i * keys_per_thread + j);
            }
            else {
                ASSERT_EQ((*it).second, i * keys_per_thread + j);
            }
        }
    }


}

template<typename SL>
void runParallelFind(SL& skiplist, const std::string& tag) {
    const int N = 500000;
    if constexpr (std::is_same_v<SL, std::map<std::string, int>>) {
        static std::shared_mutex m;
        for (int i = 0; i < N; ++i) {
            std::lock_guard lock(m);
            skiplist.emplace("key_" + std::to_string(i), i);
        }
    }
    else {
        for (int i = 0; i < N; ++i) {
            skiplist.insert(std::make_pair("key_" + std::to_string(i), i));
        }
    }

    auto start_time = std::chrono::high_resolution_clock::now();

    const int num_threads = 8;
    std::vector<std::thread> threads;
    std::atomic<int> found_count{ 0 };

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < N; ++i) {
                if constexpr (std::is_same_v<SL, std::map<std::string, int>>) {
                    auto it = skiplist.end();
                    {
                        static std::shared_mutex m;
                        std::shared_lock lock(m);
                        it = skiplist.find("key_" + std::to_string(i));
                    }
                    if (it != skiplist.end() && it->second == i) {
                        found_count.fetch_add(1, std::memory_order_relaxed);
                    }
                }
                else {
                    auto it = skiplist.find("key_" + std::to_string(i));
                    if (it != skiplist.end() && (*it).second == i) {
                        found_count.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
            });
    }
    for (auto& t : threads) t.join();

    ASSERT_EQ(found_count, num_threads * N);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    std::cout << tag << " ParallelFind duration: " << duration << " ms" << std::endl;
}

template<typename SL>
void runParallelInsertWithDuplicateKeys(SL& skiplist, const std::string& tag) {
    constexpr int num_threads = 8;
    constexpr int keys_per_thread = 100000;
    std::vector<std::thread> threads;

    auto start_time = std::chrono::high_resolution_clock::now();

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t] {
            for (int k = 0; k < keys_per_thread; ++k) {
                std::string key = "dup_key_" + std::to_string(k);
                if constexpr (std::is_same_v<SL, std::map<std::string, int>>) {
                    static std::shared_mutex m;
                    {
                        std::lock_guard lock(m);
                        skiplist[key] = t * keys_per_thread + k;
                    }
                }
                else {
                    skiplist.insert(std::make_pair(key, t * keys_per_thread + k));
                }
            }
            });
    }
    for (auto& t : threads) t.join();

    std::unordered_set<std::string> seen;
    seen.reserve(keys_per_thread);

    for (auto it = skiplist.begin(); it != skiplist.end(); ++it) {
        const std::string& key = (*it).first;
        bool inserted = seen.insert(key).second;
        ASSERT_TRUE(inserted) << "Duplicate key found in iteration: " << key;
    }

    ASSERT_EQ(seen.size(), static_cast<std::size_t>(keys_per_thread));
    ASSERT_EQ(skiplist.size(), static_cast<std::size_t>(keys_per_thread));

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    std::cout << tag << " ParallelInsertWithDuplicateKeys duration: " << duration << " ms" << std::endl;
}

TEST(SkipListPerformanceTest, MultiThreadedInsertAndFind) {
    {
        std::map<std::string, int> std_map;
        runMultiThreadedInsertAndFind(std_map, "std::map");
    }
    {
        SkipList<std::string, int> skiplist;
        runMultiThreadedInsertAndFind(skiplist, "SkipList");
    }
}

TEST(SkipListPerformanceTest, ParallelFind) {
    {
        std::map<std::string, int> std_map;
        runParallelFind(std_map, "std::map");
    }
    {
        SkipList<std::string, int> skiplist;
        runParallelFind(skiplist, "SkipList");
    }
}

TEST(SkipListPerformanceTest, ParallelInsertWithDuplicateKeys) {
    {
        std::map<std::string, int> std_map;
        runParallelInsertWithDuplicateKeys(std_map, "std::map");
    }
    {
        SkipList<std::string, int> skiplist;
        runParallelInsertWithDuplicateKeys(skiplist, "SkipList");
    }
}

TEST(SkipListTest, LowerBound) {
    SkipList<std::string, int> skiplist;

    skiplist.insert(std::make_pair("apple", 1));
    skiplist.insert(std::make_pair("banana", 2));
    skiplist.insert(std::make_pair("cherry", 3));
    skiplist.insert(std::make_pair("date", 4));
    skiplist.insert(std::make_pair("fig", 5));
    skiplist.insert(std::make_pair("grape", 6));
    skiplist.insert(std::make_pair("kiwi", 7));
    skiplist.insert(std::make_pair("lemon", 8));
    skiplist.insert(std::make_pair("mango", 9));
    skiplist.insert(std::make_pair("orange", 10));
    skiplist.insert(std::make_pair("peach", 11));
    skiplist.insert(std::make_pair("pear", 12));
    skiplist.insert(std::make_pair("plum", 13));
    skiplist.insert(std::make_pair("quince", 14));
    skiplist.insert(std::make_pair("raspberry", 15));
    skiplist.insert(std::make_pair("strawberry", 16));
    skiplist.insert(std::make_pair("watermelon", 17));

    auto it = skiplist.lower_bound("apple");
    ASSERT_NE(it, skiplist.end());
    ASSERT_EQ((*it).first, "apple");

    it = skiplist.lower_bound("lemon");
    ASSERT_NE(it, skiplist.end());
    ASSERT_EQ((*it).first, "lemon");

    it = skiplist.lower_bound("watermelon");
    ASSERT_NE(it, skiplist.end());
    ASSERT_EQ((*it).first, "watermelon");

    it = skiplist.lower_bound("blueberry");
    ASSERT_NE(it, skiplist.end());
    ASSERT_EQ((*it).first, "cherry");

    it = skiplist.lower_bound("grapefruit");
    ASSERT_NE(it, skiplist.end());
    ASSERT_EQ((*it).first, "kiwi");

    it = skiplist.lower_bound("pineapple");
    ASSERT_NE(it, skiplist.end());
    ASSERT_EQ((*it).first, "plum");

    it = skiplist.lower_bound("zucchini");
    ASSERT_EQ(it, skiplist.end());

    skiplist.insert(std::make_pair("lemon", 88));
    it = skiplist.lower_bound("lemon");
    ASSERT_NE(it, skiplist.end());
    ASSERT_EQ((*it).first, "lemon");
    ASSERT_EQ((*it).second, 88);

    it = skiplist.lower_bound("lime");
    ASSERT_NE(it, skiplist.end());
    ASSERT_EQ((*it).first, "mango");
}