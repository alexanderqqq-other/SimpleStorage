#include <gtest/gtest.h>
#include "../src/simplestorage.h"
#include "test_utils.h"
#include <filesystem>
#include <latch>
#include <thread>
#include <vector>

using namespace std;

class SimpleStorageMTTest : public ::testing::Test {
protected:
    std::filesystem::path temp_dir;
    Config config;

    void SetUp() override {
        temp_dir = std::filesystem::temp_directory_path() / "mt_test_db";
        std::filesystem::remove_all(temp_dir);
        config.memtable_size_bytes = 8 * 1024 * 1024; // 8 MB to trigger flushes
        config.l0_max_files = 3;                 // low threshold for merges
        config.block_size = 64 * 1024;
    }
    void TearDown() override {
        std::filesystem::remove_all(temp_dir);
    }
};

TEST_F(SimpleStorageMTTest, ConcurrentReadWriteRemove) {
    const int num_threads = 1;
    const int ops_per_thread = 30000;

    {
        auto db = std::make_shared<SimpleStorage>(temp_dir, config);
        std::latch start_latch(num_threads);
        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([db, t, &start_latch]() {
                start_latch.count_down();
                start_latch.wait(); // All threads wait here until latch reaches 0
                for (uint64_t i = 0; i < ops_per_thread; ++i) {
                    std::string key = pseudo_random_string(i*t) + "_" + to_string(t) + "_" + std::to_string(i);
					auto entry = pseudo_random_value(i * t);
					std::visit([&db, &key, i](auto&& arg) mutable {
						using T = std::decay_t<decltype(arg)>;
							db->put(key, arg);
						}, entry.value);
                    if (i % 3 == 0) {
                        auto val = db->get(key);
						auto origin = pseudo_random_value(i * t);
                        ASSERT_TRUE(val.has_value()) << key;
                        EXPECT_EQ(origin.type, val.value().type) << key;
						EXPECT_EQ(origin.value, val.value().value) << key;
                    }
                    if (i % 5 == 0) {
                        db->removeAsync(key);
                    } 
                    if (i % 13 == 0) {
						db->remove(key);
                    }
                }
                });
        }

        for (auto& th : threads) {
            th.join();
        }

        db->flush();
        db->waitAllAsync();
    }

    {
        auto db = std::make_shared<SimpleStorage>(temp_dir, config);
        for (int t = 0; t < num_threads; ++t) {
            for (uint64_t i = 0; i < ops_per_thread; i += 23) {
                std::string key =  pseudo_random_string(i * t) + "_" + to_string(t) + "_" + std::to_string(i);
                auto val = db->get(key);
                if (i % 5 == 0 || i % 13 == 0) {
                    EXPECT_FALSE(val.has_value()) << key;
                }
                else {
                    ASSERT_TRUE(val.has_value()) << key;
                    auto origin = pseudo_random_value(i * t);
                    ASSERT_TRUE(val.has_value()) << key << i << t;
                    EXPECT_EQ(origin.type, val.value().type);
                    EXPECT_EQ(origin.value, val.value().value) << key << i << t;
                }
            }
        }
    }
}
