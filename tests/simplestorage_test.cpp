#include <gtest/gtest.h>
#include "../src/simplestorage.h"
#include <filesystem>
#include <fstream>

using namespace std;

class SimpleStorageTest : public ::testing::Test {
protected:
    filesystem::path temp_dir;
    Config config;

    void SetUp() override {
        temp_dir = filesystem::temp_directory_path() / "test_db";
        std::filesystem::remove_all(temp_dir);
    }

    void TearDown() override {
        std::filesystem::remove_all(temp_dir);
    }
};

TEST_F(SimpleStorageTest, PutAndGet_UInt32) {
    auto db = std::make_shared<SimpleStorage>(temp_dir, config);

    std::string key = "my_key";
    uint32_t value = 12345;
    db->put(key, value);

    auto result = db->get(key);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->type, ValueType::UINT32);
    EXPECT_EQ(std::get<uint32_t>(result->value), value);
}

TEST_F(SimpleStorageTest, ExistsAndDelete) {
    auto db = std::make_shared<SimpleStorage>(temp_dir, config);

    std::string key = "test_key";
    uint64_t value = 123456789;
    db->put(key, value);

    EXPECT_TRUE(db->exists(key));
    db->remove(key);
    EXPECT_FALSE(db->exists(key));
    EXPECT_FALSE(db->get(key).has_value());
}

TEST_F(SimpleStorageTest, PutAndGet_String) {
    auto db = std::make_shared<SimpleStorage>(temp_dir, config);

    std::string key = "test key";
    std::u8string value = u8"Значение с Unicode 👋";
    db->put(key, value);

    auto result = db->get(key);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->type, ValueType::U8STRING);
    EXPECT_EQ(std::get<std::u8string>(result->value), value);
}

TEST_F(SimpleStorageTest, PrefixSearch) {
    auto db = std::make_shared<SimpleStorage>(temp_dir, config);

    db->put("foo:1", 1);
    db->put("foo:2", 2);
    db->put("bar:1", 100);

    auto keys = db->keysWithPrefix("foo:");
    EXPECT_EQ(keys.size(), 2);
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "foo:1") != keys.end());
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "foo:2") != keys.end());
}

TEST_F(SimpleStorageTest, FlushAndCompact_Smoke) {
    auto db = std::make_shared<SimpleStorage>(temp_dir, config);

    db->put("key1", 42);
    db->flush();
    db->shrink();
    SUCCEED();
}


// -----------------------------------------------------------------------------
// New test to exercise large-volume writes, flushes, and merges.
// This will insert tens of megabytes of data, force multiple memtable flushes,
// trigger Level-0 SST file creation, and ensure merges occur in the background.
// -----------------------------------------------------------------------------
TEST_F(SimpleStorageTest, LargeVolume_Merge) {
    // Adjust the config to force small memtable and small L0 threshold,
    // so that many SST files are created and merged.
    Config localConfig;
    localConfig.memtable_size_bytes = 4 * 1024 * 1024;  // 1 MB memtable
    localConfig.l0_max_files = 3;                       // Merge after 3 SST files
    localConfig.block_size = 256 * 1024;                  // 256 KB data block size
    // Generate a value of ~1 KB, so ~1024 entries fill ~1 MB.
    std::string value(1024, 'x');

    // Insert enough entries to produce ~30 MB of data.
    // Each entry is ~1 KB key + 1 KB value + overhead, so 30,000 entries ~ 30 MB.
    const size_t num_entries = 30000;

    {

        auto db = std::make_shared<SimpleStorage>(temp_dir, localConfig);
        db->put("to_remove", 123);
        db->put("to_remove_async", 123);
        for (size_t i = 0; i < num_entries; ++i) {
            db->put("key_" + std::to_string(i), value);
        }
        // Ensure any remaining entries in memtable are flushed.
        db->flush();
        db->remove("to_remove");
        db->removeAsync("to_remove_async");
        for (size_t i = 0; i < num_entries; i += 17) {
            auto key = "key_" + std::to_string(i);
            auto v = db->get(key);
            ASSERT_TRUE(v.has_value()) << "Missing key: " << key;
            EXPECT_EQ(std::get<std::string>(v->value), value);
        }
        EXPECT_FALSE(db->get("nonexistent_key").has_value());

        // Allow background merge tasks to complete.
        db->waitAllAsync();

        //// Verify a few sample keys are retrievable with correct values.
        for (size_t i = 0; i < num_entries; i+=17) {
            auto key = "key_" + std::to_string(i);
            auto v = db->get(key);
            ASSERT_TRUE(v.has_value()) << "Missing key: " << key;
            EXPECT_EQ(std::get<std::string>(v->value), value);
        }
        EXPECT_FALSE(db->get("nonexistent_key").has_value());
        EXPECT_FALSE(db->get("to_remove").has_value());
        EXPECT_FALSE(db->get("to_remove_async").has_value());
    }

    {
        auto db = std::make_shared<SimpleStorage>(temp_dir, config);
        for (size_t i = 0; i < num_entries; i+=17) {
            auto key = "key_" + std::to_string(i);
            auto v = db->get(key);
            ASSERT_TRUE(v.has_value()) << "Missing key: " << key;
            EXPECT_EQ(std::get<std::string>(v->value), value);
        }
        EXPECT_FALSE(db->get("nonexistent_key").has_value());
        EXPECT_FALSE(db->get("to_remove").has_value());
        EXPECT_FALSE(db->get("to_remove_async").has_value());
    }
}

TEST_F(SimpleStorageTest, ForEachKeyWithPrefix_Basic) {
    auto db = std::make_shared<SimpleStorage>(temp_dir, config);

    db->put("foo:1", 1);
    db->put("foo:2", 2);
    db->put("foo:3", 3);
    db->put("bar:1", 10);

    std::vector<std::string> keys;
    db->forEachKeyWithPrefix("foo:", [&](const std::string& k){
        keys.push_back(k);
        return true;
    });
    ASSERT_EQ(keys.size(), 3u);
    EXPECT_EQ(keys[0], "foo:1");
    EXPECT_EQ(keys[1], "foo:2");
    EXPECT_EQ(keys[2], "foo:3");

    std::vector<std::string> stop;
    db->forEachKeyWithPrefix("foo:", [&](const std::string& k){
        stop.push_back(k);
        return false;
    });
    ASSERT_EQ(stop.size(), 1u);
    EXPECT_EQ(stop[0], "foo:1");
}
