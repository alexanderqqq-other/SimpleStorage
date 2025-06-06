#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <random>
#include "../src/sstfile.h"
#include "../src/types.h"
#include "../src/utils.h"
#include "test_utils.h"

namespace fs = std::filesystem;

namespace {
    const fs::path TMP_SST_PATH = fs::temp_directory_path() / "sst_testfile.vsst";
    const fs::path temp_dir = fs::temp_directory_path() / "sstfile_test_dir1";
    const fs::path temp_dir2 = fs::temp_directory_path() / "sstfile_test_dir2";
    constexpr int BLOCK_SIZE = 32 * 1024; // 32 KB
}

class SSTFileTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Ensure the temporary directory exists
        std::error_code ec;
        fs::remove_all(TMP_SST_PATH, ec);
        fs::remove_all(temp_dir, ec);
        fs::remove_all(temp_dir2, ec);
        fs::create_directories(temp_dir);
        fs::create_directories(temp_dir2);
    }
    void TearDown() override {
        // Clean up file after each test
        std::error_code ec;
        fs::remove(TMP_SST_PATH, ec);
        fs::remove_all(temp_dir, ec);
        fs::remove_all(temp_dir2, ec);    }
};

TEST_F(SSTFileTest, WriteAndReadBack_MixedTypes) {
    std::vector<std::pair<std::string, TestEntry>> items = {
        {"a", TestEntry{Entry{ValueType::STRING, std::string("abc")}, 0}},
        {"b", TestEntry{Entry{ValueType::UINT64, uint64_t(42)}, 0}},
        {"c", TestEntry{Entry{ValueType::DOUBLE, 3.14}, 0}},
        {"d", TestEntry{Entry{ValueType::BLOB, std::vector<uint8_t>{1,2,3,4}}, 0}},
        {"e", TestEntry{Entry{ValueType::U8STRING, std::u8string(u8"Тест")}, 0}}
    };
    std::sort(items.begin(), items.end(), [](auto& a, auto& b) { return a.first < b.first; });
    {
        auto file = SSTFile::writeAndCreate(TMP_SST_PATH, BLOCK_SIZE, 0, true, items.begin(), items.end());

        auto val = file->get("a");
        ASSERT_TRUE(val.has_value());
        EXPECT_EQ(val->type, ValueType::STRING);
        EXPECT_EQ(std::get<std::string>(val->value), "abc");
        val = file->get("b");
        ASSERT_TRUE(val.has_value());
        EXPECT_EQ(val->type, ValueType::UINT64);
        EXPECT_EQ(std::get<uint64_t>(val->value), 42u);
        val = file->get("c");
        ASSERT_TRUE(val.has_value());
        EXPECT_EQ(val->type, ValueType::DOUBLE);
        EXPECT_DOUBLE_EQ(std::get<double>(val->value), 3.14);
        val = file->get("d");
        ASSERT_TRUE(val.has_value());
        EXPECT_EQ(val->type, ValueType::BLOB);
        EXPECT_EQ(std::get<std::vector<uint8_t>>(val->value), std::vector<uint8_t>({ 1,2,3,4 }));
        val = file->get("e");
        ASSERT_TRUE(val.has_value());
        EXPECT_EQ(val->type, ValueType::U8STRING);
        EXPECT_EQ(std::get<std::u8string>(val->value), std::u8string(u8"Тест"));
        auto missing = file->get("nokey");
        EXPECT_FALSE(missing.has_value());
        EXPECT_EQ(file->minKey(), "a");
        EXPECT_EQ(file->maxKey(), "e");
    }
    ASSERT_TRUE(fs::exists(TMP_SST_PATH));
    {
        auto file = SSTFile::readAndCreate(TMP_SST_PATH);
        auto val = file->get("a");
        ASSERT_TRUE(val.has_value());
        EXPECT_EQ(val->type, ValueType::STRING);
        EXPECT_EQ(std::get<std::string>(val->value), "abc");
        val = file->get("b");
        ASSERT_TRUE(val.has_value());
        EXPECT_EQ(val->type, ValueType::UINT64);
        EXPECT_EQ(std::get<uint64_t>(val->value), 42u);
        val = file->get("c");
        ASSERT_TRUE(val.has_value());
        EXPECT_EQ(val->type, ValueType::DOUBLE);
        EXPECT_DOUBLE_EQ(std::get<double>(val->value), 3.14);
        val = file->get("d");
        ASSERT_TRUE(val.has_value());
        EXPECT_EQ(val->type, ValueType::BLOB);
        EXPECT_EQ(std::get<std::vector<uint8_t>>(val->value), std::vector<uint8_t>({ 1,2,3,4 }));
        val = file->get("e");
        ASSERT_TRUE(val.has_value());
        EXPECT_EQ(val->type, ValueType::U8STRING);
        EXPECT_EQ(std::get<std::u8string>(val->value), std::u8string(u8"Тест"));
        auto missing = file->get("nokey");
        EXPECT_FALSE(missing.has_value());
        EXPECT_EQ(file->minKey(), "a");
        EXPECT_EQ(file->maxKey(), "e");
    }
    SUCCEED();
}

TEST_F(SSTFileTest, ExpiredEntry_ReturnsRemoved) {
    // Use expiration_ms = 1 to simulate deletion
    std::vector<std::pair<std::string, TestEntry>> items = {
        {"foo", TestEntry{Entry{ValueType::UINT32, uint32_t(1)}, 1}}
    };
    {
        auto file = SSTFile::writeAndCreate(TMP_SST_PATH, BLOCK_SIZE, 0, true, items.begin(), items.end());
        auto v = file->get("foo");
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(v->type, ValueType::REMOVED);
    }
    ASSERT_TRUE(fs::exists(TMP_SST_PATH));
    {
        auto file = SSTFile::readAndCreate(TMP_SST_PATH);
        auto v = file->get("foo");
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(v->type, ValueType::REMOVED);
    }
    SUCCEED();
}

TEST_F(SSTFileTest, KeysWithPrefix) {
    std::vector<std::pair<std::string, TestEntry>> items = {
        {"a1", TestEntry{Entry{ValueType::UINT8, uint8_t(1)}, 0}},
        {"a2", TestEntry{Entry{ValueType::UINT8, uint8_t(2)}, 0}},
        {"a3", TestEntry{Entry{ValueType::UINT8, uint8_t(3)}, 0}},
        {"b1", TestEntry{Entry{ValueType::UINT8, uint8_t(4)}, 0}}
    };
    std::sort(items.begin(), items.end(), [](auto& a, auto& b) { return a.first < b.first; });
    {
        auto file = SSTFile::writeAndCreate(TMP_SST_PATH, BLOCK_SIZE, 0, true, items.begin(), items.end());
        auto keys = file->keysWithPrefix("a", 10);

        EXPECT_EQ(keys.size(), 3);
        EXPECT_TRUE(std::find(keys.begin(), keys.end(), "a1") != keys.end());
        EXPECT_TRUE(std::find(keys.begin(), keys.end(), "a2") != keys.end());
        EXPECT_TRUE(std::find(keys.begin(), keys.end(), "a3") != keys.end());
    }
    ASSERT_TRUE(fs::exists(TMP_SST_PATH));
    {
        auto file = SSTFile::readAndCreate(TMP_SST_PATH);
        auto keys = file->keysWithPrefix("a", 10);

        EXPECT_EQ(keys.size(), 3);
        EXPECT_TRUE(std::find(keys.begin(), keys.end(), "a1") != keys.end());
        EXPECT_TRUE(std::find(keys.begin(), keys.end(), "a2") != keys.end());
        EXPECT_TRUE(std::find(keys.begin(), keys.end(), "a3") != keys.end());
    }

    SUCCEED();
}

TEST_F(SSTFileTest, EmptyFile_NoEntries) {
    std::vector<std::pair<std::string, TestEntry>> empty;
    {
        auto file = SSTFile::writeAndCreate(TMP_SST_PATH, BLOCK_SIZE, 0, true, empty.begin(), empty.end());
        EXPECT_FALSE(file);
    }
    EXPECT_FALSE(fs::exists(TMP_SST_PATH));
    SUCCEED();
}

TEST_F(SSTFileTest, LargeDataSet_MultiBlock) {


    std::vector<std::pair<std::string, TestEntry>> items = generateBigData();

    auto do_test = [&](const std::unique_ptr<SSTFile>& file) {
        auto prefix_keys = file->keysWithPrefix("pref_", PREFIX_SERIES + 10);
        EXPECT_EQ(prefix_keys.size(), PREFIX_SERIES);

        for (int i = 0; i < PREFIX_SERIES; ++i) {
            std::string key = "pref_" + std::to_string(i / 100) + "_" + std::string(1 + (i % 5), char('a' + (i % 26)));
            EXPECT_TRUE(std::find(prefix_keys.begin(), prefix_keys.end(), key) != prefix_keys.end());
        }

        {
            auto v = file->get("test_control_uint32");
            ASSERT_TRUE(v.has_value());
            EXPECT_EQ(v->type, ValueType::UINT32);
            EXPECT_EQ(std::get<uint32_t>(v->value), 424242u);
        }
        {
            auto v = file->get("test_control_str");
            ASSERT_TRUE(v.has_value());
            EXPECT_EQ(v->type, ValueType::STRING);
            EXPECT_EQ(std::get<std::string>(v->value), "control_test");
        }
        {
            auto v = file->get("test_control_blob");
            ASSERT_TRUE(v.has_value());
            EXPECT_EQ(v->type, ValueType::BLOB);
            EXPECT_EQ(std::get<std::vector<uint8_t>>(v->value), ref_blob);
        }
        {
            auto v = file->get("test_control_double");
            ASSERT_TRUE(v.has_value());
            EXPECT_EQ(v->type, ValueType::DOUBLE);
            EXPECT_DOUBLE_EQ(std::get<double>(v->value), 123456.789);
        }
        {
            auto v = file->get("test_control_u8str");
            ASSERT_TRUE(v.has_value());
            EXPECT_EQ(v->type, ValueType::U8STRING);
            EXPECT_EQ(std::get<std::u8string>(v->value), std::u8string(u8"Юникод"));
        }
        auto batch2_prefix = file->keysWithPrefix("batch2_", BIG_BATCH2 + 5);
        EXPECT_EQ(batch2_prefix.size(), BIG_BATCH2);
        for (int i = 0; i < BIG_BATCH2; ++i) {
            std::string key = "batch2_" + std::to_string(i) + "_ZZZZZZZZZZZ";
            EXPECT_TRUE(std::find(batch2_prefix.begin(), batch2_prefix.end(), key) != batch2_prefix.end());
        }
        batch2_prefix = file->keysWithPrefix("batch2_", 3);
        EXPECT_EQ(batch2_prefix.size(), 3);
        std::string key = "batch2_" + std::to_string(0) + "_ZZZZZZZZZZZ";
        EXPECT_TRUE(std::find(batch2_prefix.begin(), batch2_prefix.end(), key) != batch2_prefix.end());

        key = "batch2_" + std::to_string(100) + "_ZZZZZZZZZZZ";
        EXPECT_TRUE(std::find(batch2_prefix.begin(), batch2_prefix.end(), key) != batch2_prefix.end());

        key = "batch2_" + std::to_string(101) + "_ZZZZZZZZZZZ";
        EXPECT_TRUE(std::find(batch2_prefix.begin(), batch2_prefix.end(), key) != batch2_prefix.end());

        {
            auto v = file->get("batch2_0_ZZZZZZZZZZZ");
            ASSERT_TRUE(v.has_value());
            EXPECT_EQ(v->type, ValueType::UINT64);
            EXPECT_EQ(std::get<uint64_t>(v->value), 0u);
        }
        {
            auto v = file->get("batch2_123_ZZZZZZZZZZZ");
            ASSERT_TRUE(v.has_value());
            EXPECT_EQ(v->type, ValueType::UINT64);
            EXPECT_EQ(std::get<std::uint64_t>(v->value), 40999959);
        }
        {
            auto v = file->get("batch2_154_ZZZZZZZZZZZ");
            ASSERT_TRUE(v.has_value());
            EXPECT_EQ(v->type, ValueType::STRING);
            EXPECT_EQ(std::get<std::string>(v->value), "b2_154");
        }
        };
    {
        auto file = SSTFile::writeAndCreate(TMP_SST_PATH, BIG_BLOCK_SIZE, 0, true, items.begin(), items.end());
        do_test(file);

        auto v = file->get("test_remove_blob");
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(v->type, ValueType::BLOB);
        EXPECT_EQ(std::get<std::vector<uint8_t>>(v->value), ref_blob);
        auto test_prefix = file->keysWithPrefix("test_", 50);
        EXPECT_EQ(test_prefix.size(), 6);
        EXPECT_EQ(test_prefix[0], "test_control_blob");
        EXPECT_EQ(test_prefix[1], "test_control_double");
        EXPECT_EQ(test_prefix[2], "test_control_str");
        EXPECT_EQ(test_prefix[3], "test_control_u8str");
        EXPECT_EQ(test_prefix[4], "test_control_uint32");
        EXPECT_EQ(test_prefix[5], "test_remove_blob");

        bool removed = file->remove("test_remove_blob");
        EXPECT_TRUE(removed);
        auto removed_val = file->get("test_remove_blob");
        ASSERT_TRUE(removed_val.has_value());
        EXPECT_EQ(removed_val->type, ValueType::REMOVED);
        EXPECT_EQ(file->status("test_remove_blob"), EntryStatus::REMOVED);
        test_prefix = file->keysWithPrefix("test_", 50);
        EXPECT_EQ(test_prefix.size(), 5);
        auto it = std::find(test_prefix.begin(), test_prefix.end(), "test_remove_blob");
        EXPECT_EQ(it, test_prefix.end()) << "test_remove_blob should not be in the prefix list after removal";
    }
    {
        auto file = SSTFile::readAndCreate(TMP_SST_PATH);
        do_test(file);
        auto removed_val = file->get("test_remove_blob");
        ASSERT_TRUE(removed_val.has_value());
        EXPECT_EQ(removed_val->type, ValueType::REMOVED);
        EXPECT_EQ(file->status("test_remove_blob"), EntryStatus::REMOVED);
        auto test_prefix = file->keysWithPrefix("test_", 50);
        EXPECT_EQ(test_prefix.size(), 5);
        auto it = std::find(test_prefix.begin(), test_prefix.end(), "test_remove_blob");
        EXPECT_EQ(it, test_prefix.end()) << "test_remove_blob should not be in the prefix list after removal";
    }
    SUCCEED();
}

TEST_F(SSTFileTest, Shrink_RemovesDeletedEntries) {
    std::vector<std::pair<std::string, TestEntry>> items = {
        {"a", TestEntry{Entry{ValueType::STRING, std::string("one")}, 0}},
        {"b", TestEntry{Entry{ValueType::STRING, std::string("two")}, 0}},
        {"c", TestEntry{Entry{ValueType::STRING, std::string("three")}, 0}}
    };
    std::sort(items.begin(), items.end(), [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

    auto orig_path = temp_dir / "shrink_src.vsst";
    auto file = SSTFile::writeAndCreate(orig_path, BLOCK_SIZE, 0, true, items.begin(), items.end());

    ASSERT_TRUE(file->remove("b"));
    auto removed = file->get("b");
    ASSERT_TRUE(removed.has_value());
    EXPECT_EQ(removed->type, ValueType::REMOVED);

    auto orig_size = fs::file_size(file->path());

    auto shrunk = file->shrink(BLOCK_SIZE);

    EXPECT_TRUE(fs::exists(shrunk->path()));
    EXPECT_LT(fs::file_size(shrunk->path()), orig_size);

    EXPECT_FALSE(shrunk->get("b").has_value());

    auto v = shrunk->get("a");
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(std::get<std::string>(v->value), "one");
    v = shrunk->get("c");
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(std::get<std::string>(v->value), "three");
}



TEST_F(SSTFileTest, RemoveEntry_WorksAsExpected) {
    std::vector<std::pair<std::string, TestEntry>> items = {
        {"keep1", TestEntry{Entry{ValueType::STRING, std::string("first")}, 0}},
        {"remove_me", TestEntry{Entry{ValueType::UINT64, uint64_t(999)}, 0}},
        {"keep2", TestEntry{Entry{ValueType::DOUBLE, 2.71}, 0}}
    };
    std::sort(items.begin(), items.end(), [](auto& a, auto& b) { return a.first < b.first; });
    {
        auto file = SSTFile::writeAndCreate(TMP_SST_PATH, BLOCK_SIZE, 0, true, items.begin(), items.end());

        auto v1 = file->get("keep1");
        ASSERT_TRUE(v1.has_value());
        EXPECT_EQ(v1->type, ValueType::STRING);
        auto v2 = file->get("remove_me");
        ASSERT_TRUE(v2.has_value());
        EXPECT_EQ(v2->type, ValueType::UINT64);
        auto v3 = file->get("keep2");
        ASSERT_TRUE(v3.has_value());
        EXPECT_EQ(v3->type, ValueType::DOUBLE);

        bool removed = file->remove("remove_me");
        EXPECT_TRUE(removed);
        removed = file->remove("not_found");
        EXPECT_FALSE(removed);

        auto removed_val = file->get("remove_me");
        ASSERT_TRUE(removed_val.has_value());
        EXPECT_EQ(removed_val->type, ValueType::REMOVED);
        EXPECT_EQ(file->status("remove_me"), EntryStatus::REMOVED);
        auto still_v1 = file->get("keep1");
        ASSERT_TRUE(still_v1.has_value());
        EXPECT_EQ(still_v1->type, ValueType::STRING);
        EXPECT_EQ(file->status("keep1"), EntryStatus::EXISTS);
        auto still_v3 = file->get("keep2");
        ASSERT_TRUE(still_v3.has_value());
        EXPECT_EQ(still_v3->type, ValueType::DOUBLE);
        EXPECT_EQ(file->status("not_found"), EntryStatus::NOT_FOUND);

    }
    {
        auto file = SSTFile::readAndCreate(TMP_SST_PATH);
        auto removed_val = file->get("remove_me");
        ASSERT_TRUE(removed_val.has_value());
        EXPECT_EQ(removed_val->type, ValueType::REMOVED);
        EXPECT_EQ(file->status("remove_me"), EntryStatus::REMOVED);
        auto still_v1 = file->get("keep1");
        ASSERT_TRUE(still_v1.has_value());
        EXPECT_EQ(still_v1->type, ValueType::STRING);
        EXPECT_EQ(file->status("keep1"), EntryStatus::EXISTS);
        auto still_v3 = file->get("keep2");
        ASSERT_TRUE(still_v3.has_value());
        EXPECT_EQ(still_v3->type, ValueType::DOUBLE);
        EXPECT_EQ(file->status("not_found"), EntryStatus::NOT_FOUND);
    }
}

TEST_F(SSTFileTest, CorruptionTest) {
    constexpr size_t CORRUPT_ITER = 100;
    constexpr size_t FILE_SIZE = 32 * 1024;
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int> dist(0, 255);

    for (size_t i = 0; i < CORRUPT_ITER; ++i) {
        // Generate random binary data
        std::vector<uint8_t> random_data(FILE_SIZE);
        for (size_t j = 0; j < FILE_SIZE; ++j)
            random_data[j] = static_cast<uint8_t>(dist(rng));

        // Write random data to temp file
        {
            std::ofstream ofs(TMP_SST_PATH, std::ios::binary);
            ofs.write("VSST", 4); // Write a fake header
            uint32_t count = dist(rng);
            Utils::serializeLE(count, random_data); // Write fake count    
            ofs.write(reinterpret_cast<const char*>(random_data.data()), random_data.size());
            std::vector<uint8_t> footer(8, 0); // Fake footer

        }

        // Try to open as SSTFile and get keys, must NOT crash
        try {
            auto file = SSTFile::readAndCreate(TMP_SST_PATH);
            // Try to read keys; it's ok if garbage or exceptions
            auto v1 = file->get("test_1");
            auto v2 = file->get("test_2");
            EXPECT_TRUE(v2.has_value() || !v2.has_value());
            // No expectations on value: just make sure it does not crash
        }
        catch (const std::exception& ex) {
            // Exception is OK, just continue
        }
        catch (...) {
            // Any non-std exception is also OK for this smoke test
        }
    }

    // Clean up after the test
    std::error_code ec;
    fs::remove(TMP_SST_PATH, ec);

    SUCCEED();
}


TEST_F(SSTFileTest, Iterator_MultiBlock) {
    constexpr int BLOCK_SIZE_SMALL = 1024;

    const int NUM_ENTRIES = 1000;
    std::vector<std::pair<std::string, TestEntry>> items;
    items.reserve(NUM_ENTRIES);
    for (int i = 0; i < NUM_ENTRIES; ++i) {
        std::ostringstream oss;
        oss << "key_" << std::setw(3) << std::setfill('0') << i;
        std::string key = oss.str();

        Entry e;
        e.type = ValueType::UINT32;
        e.value = static_cast<uint32_t>(i);
        items.push_back({ key, TestEntry{ e, Utils::getNow() + i + 100000ull } });
    }
    std::sort(items.begin(), items.end(), [](auto& a, auto& b) {
        return a.first < b.first;
        });

    {
        auto file = SSTFile::writeAndCreate(TMP_SST_PATH,
            BLOCK_SIZE_SMALL,
            /*seq_num=*/0,
            true,
            items.begin(),
            items.end());

        std::vector<std::pair<std::string, DataBlock::DataBlockEntry>> collected;
        for (auto it = file->begin(); it != file->end(); ++it) {
            collected.push_back(*it);
        }

        ASSERT_EQ(collected.size(), items.size());

        for (size_t i = 0; i < items.size(); ++i) {
            EXPECT_EQ(collected[i].second.expiration_ms, items[i].second.expiration_ms);
            EXPECT_EQ(collected[i].first, items[i].first);
            const Entry& got = collected[i].second.entry;
            const Entry& want = items[i].second.entry;
            EXPECT_EQ(got.type, want.type);
            EXPECT_EQ(std::get<uint32_t>(got.value),
                std::get<uint32_t>(want.value));
        }
    }

    {
        auto file = SSTFile::readAndCreate(TMP_SST_PATH);

        std::vector<std::pair<std::string, DataBlock::DataBlockEntry>> collected2;
        for (auto it = file->begin(); it != file->end(); ++it) {
            collected2.push_back(*it);
        }

        ASSERT_EQ(collected2.size(), items.size());
        for (size_t i = 0; i < items.size(); ++i) {
            EXPECT_EQ(collected2[i].first, items[i].first);
            const Entry& got = collected2[i].second.entry;
            const Entry& want = items[i].second.entry;
            EXPECT_EQ(got.type, want.type);
            EXPECT_EQ(std::get<uint32_t>(got.value),
                std::get<uint32_t>(want.value));
        }
    }

    SUCCEED();
}

TEST_F(SSTFileTest, Merge_WithDuplicatesAndRemoved) {
    // Prepare file 1 (lower seqNum)
    uint64_t seq1 = 10;
    std::vector<std::pair<std::string, TestEntry>> data1 = {
        {"a", TestEntry{Entry{ValueType::STRING, std::string("abc")}, 0}},
        {"b", TestEntry{Entry{ValueType::UINT64, uint64_t(42)}, 0}},
        {"c", TestEntry{Entry{ValueType::DOUBLE, 3.14}, 0}},
        {"d", TestEntry{Entry{ValueType::BLOB, std::vector<uint8_t>{1,2,3,4}}, 0}},
        {"dup",      {{ValueType::UINT32, uint32_t(111)}, 0}},
        {"to_remove",{{ValueType::REMOVED, {}}, 0}},           // REMOVED
        {"e", TestEntry{Entry{ValueType::U8STRING, std::u8string(u8"Тест")}, 0}}
    };

    // Prepare file 2 (higher seqNum)
    uint64_t seq2 = 99;
        std::vector<std::pair<std::string, TestEntry>> data2 = {
        {"aaa_123", TestEntry{Entry{ValueType::STRING, std::string("abc1")}, 0}},
        {"bbb_123", TestEntry{Entry{ValueType::REMOVED, uint64_t(43)}, 0}},
        {"ccc_123", TestEntry{Entry{ValueType::DOUBLE, 3.15}, 0}},
        {"ddd_123", TestEntry{Entry{ValueType::BLOB, std::vector<uint8_t>{1,2,3,5}}, 0}},
        {"dup",      {{ValueType::UINT32, uint32_t(112)}, 0}},
        {"to_remove",{{ValueType::REMOVED, {}}, 0}},           // REMOVED
        {"eee_123", TestEntry{Entry{ValueType::U8STRING, std::u8string(u8"Тест_2")}, 0}}
        };

    // Save as SST files (with tiny block size to force many DataBlocks)
    auto sst1_path = temp_dir / "sst1->vsst";
    auto sst2_path = temp_dir / "sst2->vsst";
    uint32_t block_size = 64; // tiny block size
    std::sort(data1.begin(), data1.end(), [](auto& a, auto& b) { return a.first < b.first; });
    std::sort(data2.begin(), data2.end(), [](auto& a, auto& b) { return a.first < b.first; });
    auto sst1 = SSTFile::writeAndCreate(sst1_path, block_size, seq1, true, data1.begin(), data1.end());
    std::vector<std::filesystem::path> dst_files;
    dst_files.push_back(SSTFile::writeAndCreate(sst2_path, block_size, seq2, true, data2.begin(), data2.end())->path());

    // Merge in one file->

    auto test_f = [&](const std::vector<std::unique_ptr<SSTFile>> & merged) {
        // Read all output keys/values into a map, assert uniqueness
        std::map<std::string, Entry> merged_entries;
        for (const auto& mf : merged) {
            for (auto it = mf->begin(); it != mf->end(); ++it) {
                auto [key, entry] = *it;
                if (entry.entry.type != ValueType::REMOVED) {
                    ASSERT_TRUE(merged_entries.emplace(key, entry.entry).second)
                        << "Duplicate key in merged SST output: " << key;
                }
            }
        }

        std::vector<std::string> expected_keys = {
            "a", "b", "c", "d", "dup", "e",
            "aaa_123", "ccc_123", "ddd_123", "eee_123"
        };
        for (const auto& key : expected_keys) {
            ASSERT_TRUE(merged_entries.count(key)) << "Missing key: " << key;
        }

        EXPECT_EQ(merged_entries.count("to_remove"), 0u);
        EXPECT_EQ(merged_entries.count("bbb_123"), 0u);

        EXPECT_EQ(merged_entries["a"].type, ValueType::STRING);
        EXPECT_EQ(std::get<std::string>(merged_entries["a"].value), "abc");

        EXPECT_EQ(merged_entries["b"].type, ValueType::UINT64);
        EXPECT_EQ(std::get<uint64_t>(merged_entries["b"].value), 42);

        EXPECT_EQ(merged_entries["c"].type, ValueType::DOUBLE);
        EXPECT_DOUBLE_EQ(std::get<double>(merged_entries["c"].value), 3.14);

        EXPECT_EQ(merged_entries["d"].type, ValueType::BLOB);
        EXPECT_EQ(std::get<std::vector<uint8_t>>(merged_entries["d"].value), (std::vector<uint8_t>{1, 2, 3, 4}));

        EXPECT_EQ(merged_entries["dup"].type, ValueType::UINT32);
        EXPECT_EQ(std::get<uint32_t>(merged_entries["dup"].value), 112);

        EXPECT_EQ(merged_entries["e"].type, ValueType::U8STRING);
        EXPECT_EQ(std::get<std::u8string>(merged_entries["e"].value), u8"Тест");

        EXPECT_EQ(merged_entries["aaa_123"].type, ValueType::STRING);
        EXPECT_EQ(std::get<std::string>(merged_entries["aaa_123"].value), "abc1");

        EXPECT_EQ(merged_entries["ccc_123"].type, ValueType::DOUBLE);
        EXPECT_DOUBLE_EQ(std::get<double>(merged_entries["ccc_123"].value), 3.15);

        EXPECT_EQ(merged_entries["ddd_123"].type, ValueType::BLOB);
        EXPECT_EQ(std::get<std::vector<uint8_t>>(merged_entries["ddd_123"].value), (std::vector<uint8_t>{1, 2, 3, 5}));

        EXPECT_EQ(merged_entries["eee_123"].type, ValueType::U8STRING);
        EXPECT_EQ(std::get<std::u8string>(merged_entries["eee_123"].value), u8"Тест_2");
    };

    // Test merged file
    {
        //merge in one file
        auto merged = SSTFile::merge(sst1_path, dst_files, temp_dir, 1024, block_size, false);
        ASSERT_TRUE(merged.size() == 1);
        test_f(merged);
        EXPECT_EQ(merged[0]->minKey(), "a");
        EXPECT_EQ(merged[0]->maxKey(), "eee_123");

        std::vector<std::unique_ptr<SSTFile>>  readed_files;
        readed_files.push_back(SSTFile::readAndCreate(merged.front()->path()));
        test_f(readed_files);
        EXPECT_EQ(readed_files[0]->minKey(), "a");
        EXPECT_EQ(readed_files[0]->maxKey(), "eee_123");
    }
    {   //merge in two files
        auto merged = SSTFile::merge(sst1_path, dst_files, temp_dir2, 397, block_size, false);
        ASSERT_TRUE(merged.size() == 2);
        test_f(merged);
        EXPECT_EQ(merged[0]->minKey(), "a");
        EXPECT_EQ(merged[1]->maxKey(), "eee_123");
        std::vector<std::unique_ptr<SSTFile>>  readed_files;
        for (const auto& mf : merged) {
            readed_files.push_back(SSTFile::readAndCreate(mf->path()));
        }
        test_f(readed_files);
        EXPECT_EQ(readed_files[0]->minKey(), "a");
        EXPECT_EQ(readed_files[1]->maxKey(), "eee_123");

    }
}


TEST_F(SSTFileTest, Merge_WithMultiple) {
    // Prepare file 1 (lower seqNum)
    uint64_t seq1 = 10;
    std::vector<std::pair<std::string, TestEntry>> data1 = {
        {"a", TestEntry{Entry{ValueType::STRING, std::string("abc")}, 0}},
        {"b", TestEntry{Entry{ValueType::UINT64, uint64_t(42)}, 0}},
        {"c", TestEntry{Entry{ValueType::DOUBLE, 3.14}, 0}},
        {"d", TestEntry{Entry{ValueType::BLOB, std::vector<uint8_t>{1,2,3,4}}, 0}},
        {"dup",      {{ValueType::UINT32, uint32_t(111)}, 0}},
        {"to_remove",{{ValueType::REMOVED, {}}, 0}},           // REMOVED
        {"e", TestEntry{Entry{ValueType::U8STRING, std::u8string(u8"Тест")}, 0}},
        {"xdup", {{ValueType::UINT32, uint32_t(200)}, 0}} // Duplicate key with different value
    };

    // Prepare file 2 (higher seqNum)
    uint64_t seq2 = 99;
    std::vector<std::pair<std::string, TestEntry>> data2 = {
    {"aaa_123", TestEntry{Entry{ValueType::STRING, std::string("abc1")}, 0}},
    {"bbb_123", TestEntry{Entry{ValueType::REMOVED, uint64_t(43)}, 0}},
    {"ccc_123", TestEntry{Entry{ValueType::DOUBLE, 3.15}, 0}},
    {"ddd_123", TestEntry{Entry{ValueType::BLOB, std::vector<uint8_t>{1,2,3,5}}, 0}},
    {"dup",      {{ValueType::UINT32, uint32_t(112)}, 0}},
    {"eee_123", TestEntry{Entry{ValueType::U8STRING, std::u8string(u8"Тест_2")}, 0}},
    };

    uint64_t seq3 = 0;
    std::vector<std::pair<std::string, TestEntry>> data3 = {
    {"fff_123", TestEntry{Entry{ValueType::STRING, std::string("abc1")}, 0}},
    {"ggg_123", TestEntry{Entry{ValueType::REMOVED, uint64_t(43)}, 0}},
    {"hhh_123", TestEntry{Entry{ValueType::DOUBLE, 3.15}, 0}},
    {"kkk_123", TestEntry{Entry{ValueType::BLOB, std::vector<uint8_t>{1,2,3,5}}, 0}},
    {"to_remove",{{ValueType::REMOVED, {}}, 0}},           // REMOVED
    {"xdup", {{ValueType::UINT32, uint32_t(201)}, 0}} 
    };

    // Save as SST files (with tiny block size to force many DataBlocks)
    auto sst1_path = temp_dir / "sst1->vsst";
    auto sst2_path = temp_dir / "sst2->vsst";
    auto sst3_path = temp_dir / "sst3.vsst";
    uint32_t block_size = 64; // tiny block size
    std::sort(data1.begin(), data1.end(), [](auto& a, auto& b) { return a.first < b.first; });
    std::sort(data2.begin(), data2.end(), [](auto& a, auto& b) { return a.first < b.first; });
    std::sort(data3.begin(), data3.end(), [](auto& a, auto& b) { return a.first < b.first; });
    auto sst1 = SSTFile::writeAndCreate(sst1_path, block_size, seq1, true, data1.begin(), data1.end());
    std::vector<std::filesystem::path> dst_files;
    dst_files.push_back(SSTFile::writeAndCreate(sst2_path, block_size, seq2, true, data2.begin(), data2.end())->path());
    dst_files.push_back(SSTFile::writeAndCreate(sst3_path, block_size, seq3, true, data3.begin(), data3.end())->path());

    // Merge in one file->

    auto test_f = [&](const std::vector<std::unique_ptr<SSTFile>> & merged) {
        // Read all output keys/values into a map, assert uniqueness
        std::map<std::string, Entry> merged_entries;
        for (const auto& mf : merged) {
            for (auto it = mf->begin(); it != mf->end(); ++it) {
                const auto& [key, entry] = *it;
                if (entry.entry.type != ValueType::REMOVED) {
                    ASSERT_TRUE(merged_entries.emplace(key, entry.entry).second)
                        << "Duplicate key in merged SST output: " << key;
                }
            }
        }

        std::vector<std::string> expected_keys = {
            "a", "b", "c", "d", "dup", "e",
            "aaa_123", "ccc_123", "ddd_123", "eee_123",
            "fff_123", "hhh_123", "xdup"
        };
        for (const auto& key : expected_keys) {
            ASSERT_TRUE(merged_entries.count(key)) << "Missing key: " << key;
        }

        EXPECT_EQ(merged_entries.count("to_remove"), 0u);
        EXPECT_EQ(merged_entries.count("bbb_123"), 0u);

        EXPECT_EQ(merged_entries["a"].type, ValueType::STRING);
        EXPECT_EQ(std::get<std::string>(merged_entries["a"].value), "abc");

        EXPECT_EQ(merged_entries["b"].type, ValueType::UINT64);
        EXPECT_EQ(std::get<uint64_t>(merged_entries["b"].value), 42);

        EXPECT_EQ(merged_entries["c"].type, ValueType::DOUBLE);
        EXPECT_DOUBLE_EQ(std::get<double>(merged_entries["c"].value), 3.14);

        EXPECT_EQ(merged_entries["d"].type, ValueType::BLOB);
        EXPECT_EQ(std::get<std::vector<uint8_t>>(merged_entries["d"].value), (std::vector<uint8_t>{1, 2, 3, 4}));

        EXPECT_EQ(merged_entries["dup"].type, ValueType::UINT32);
        EXPECT_EQ(std::get<uint32_t>(merged_entries["dup"].value), 112);

        EXPECT_EQ(merged_entries["e"].type, ValueType::U8STRING);
        EXPECT_EQ(std::get<std::u8string>(merged_entries["e"].value), u8"Тест");

        EXPECT_EQ(merged_entries["aaa_123"].type, ValueType::STRING);
        EXPECT_EQ(std::get<std::string>(merged_entries["aaa_123"].value), "abc1");

        EXPECT_EQ(merged_entries["ccc_123"].type, ValueType::DOUBLE);
        EXPECT_DOUBLE_EQ(std::get<double>(merged_entries["ccc_123"].value), 3.15);

        EXPECT_EQ(merged_entries["ddd_123"].type, ValueType::BLOB);
        EXPECT_EQ(std::get<std::vector<uint8_t>>(merged_entries["ddd_123"].value), (std::vector<uint8_t>{1, 2, 3, 5}));

        EXPECT_EQ(merged_entries["eee_123"].type, ValueType::U8STRING);
        EXPECT_EQ(std::get<std::u8string>(merged_entries["eee_123"].value), u8"Тест_2");

        EXPECT_EQ(merged_entries["fff_123"].type, ValueType::STRING);
        EXPECT_EQ(std::get<std::string>(merged_entries["aaa_123"].value), "abc1");

        EXPECT_EQ(merged_entries["hhh_123"].type, ValueType::DOUBLE);
        EXPECT_DOUBLE_EQ(std::get<double>(merged_entries["ccc_123"].value), 3.15);

        EXPECT_EQ(merged_entries["kkk_123"].type, ValueType::BLOB);
        EXPECT_EQ(std::get<std::vector<uint8_t>>(merged_entries["ddd_123"].value), (std::vector<uint8_t>{1, 2, 3, 5}));

        EXPECT_EQ(merged_entries["xdup"].type, ValueType::UINT32);
        EXPECT_EQ(std::get<uint32_t>(merged_entries["xdup"].value), 200);
        };

    // Test merged file
    {
        //merge in one file
        auto merged = SSTFile::merge(sst1_path, dst_files, temp_dir, 1024, block_size, false);
        ASSERT_TRUE(merged.size() == 1);
        test_f(merged);
        EXPECT_EQ(merged.front()->minKey(), "a");
        EXPECT_EQ(merged.back()->maxKey(), "xdup");

        std::vector<std::unique_ptr<SSTFile>>  readed_files;
        readed_files.push_back(SSTFile::readAndCreate(merged.front()->path()));
        test_f(readed_files);
        EXPECT_EQ(merged.front()->minKey(), "a");
        EXPECT_EQ(merged.back()->maxKey(), "xdup");
    }
    {   //merge in three files
        auto merged = SSTFile::merge(sst1_path, dst_files, temp_dir2, 397, block_size, false);
        ASSERT_TRUE(merged.size() == 2);
        test_f(merged);
        EXPECT_EQ(merged.front()->minKey(), "a");
        EXPECT_EQ(merged.back()->maxKey(), "xdup");
        std::vector<std::unique_ptr<SSTFile>>  readed_files;
        for (const auto& mf : merged) {
            readed_files.push_back(SSTFile::readAndCreate(mf->path()));
        }
        test_f(readed_files);
        EXPECT_EQ(merged.front()->minKey(), "a");
        EXPECT_EQ(merged.back()->maxKey(), "xdup");;

    }
}
