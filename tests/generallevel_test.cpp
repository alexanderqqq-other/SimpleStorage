#include <gtest/gtest.h>
#include "../src/generallevel.h"
#include "../src/sstfile.h"
#include "test_utils.h"
#include <filesystem>

namespace fs = std::filesystem;

class GeneralLevelTest : public ::testing::Test {
protected:
    fs::path dir;
    void SetUp() override {
        dir = fs::temp_directory_path() / "generallevel_test";
        fs::remove_all(dir);
        fs::create_directories(dir);
    }
    void TearDown() override {
        fs::remove_all(dir);
    }
};

TEST_F(GeneralLevelTest, KeyBetweenRanges_NoSSTReturned) {
    std::vector<std::pair<std::string, TestEntry>> items1 = {
        {"aaa", TestEntry{Entry{ValueType::UINT32, uint32_t(1)}, 0}},
        {"aac", TestEntry{Entry{ValueType::UINT32, uint32_t(2)}, 0}}
    };
    auto sst1 = SSTFile::writeAndCreate(dir / "first.vsst", 4096, 1, true,
        items1.begin(), items1.end());

    std::vector<std::pair<std::string, TestEntry>> items2 = {
        {"ddd", TestEntry{Entry{ValueType::UINT32, uint32_t(3)}, 0}},
        {"ddf", TestEntry{Entry{ValueType::UINT32, uint32_t(4)}, 0}}
    };
    auto sst2 = SSTFile::writeAndCreate(dir / "second.vsst", 4096, 2, true,
        items2.begin(), items2.end());

    // Construct the level after files are created so it loads them
    GeneralLevel level(dir, 1 << 20, 10, true);

    // Valid key from first file
    auto val = level.get("aaa");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(std::get<uint32_t>(val->value), 1u);

    // Key lying between file1.maxKey() and file2.minKey()
    EXPECT_FALSE(level.get("bbb").has_value());
}

TEST_F(GeneralLevelTest, KeyBeforeFirst_NoSSTReturned) {
    std::vector<std::pair<std::string, TestEntry>> items1 = {
        {"aaa", TestEntry{Entry{ValueType::UINT32, uint32_t(1)}, 0}},
    };
    auto sst1 = SSTFile::writeAndCreate(dir / "first.vsst", 4096, 1, true,
        items1.begin(), items1.end());

    GeneralLevel level(dir, 1 << 20, 10, true);

    EXPECT_FALSE(level.get("000").has_value());
}

TEST_F(GeneralLevelTest, MergeAllRemoved_NoNewFiles) {
    std::vector<std::pair<std::string, TestEntry>> removed = {
        {"a", TestEntry{Entry{ValueType::REMOVED, {}}, 0}},
        {"b", TestEntry{Entry{ValueType::REMOVED, {}}, 0}},
    };

    GeneralLevel level(dir, 1 << 20, 10, true);

    auto src_path = dir / "src.vsst";
    auto sst_src = SSTFile::writeAndCreate(src_path, 4096, 1, true, removed.begin(), removed.end());
    ASSERT_TRUE(sst_src);

    auto res = level.mergeToTmp(src_path, 4096);
    EXPECT_TRUE(res.new_files.empty());
}

TEST_F(GeneralLevelTest, ForEachKeyWithPrefix_Basic) {
    std::vector<std::pair<std::string, TestEntry>> items1 = {
        {"aa1", TestEntry{Entry{ValueType::UINT32, uint32_t(1)}, 0}},
        {"aa2", TestEntry{Entry{ValueType::UINT32, uint32_t(2)}, 0}}
    };
    auto sst1 = SSTFile::writeAndCreate(dir / "first.vsst", 4096, 1, true,
        items1.begin(), items1.end());

    std::vector<std::pair<std::string, TestEntry>> items2 = {
        {"ab1", TestEntry{Entry{ValueType::UINT32, uint32_t(3)}, 0}}
    };
    auto sst2 = SSTFile::writeAndCreate(dir / "second.vsst", 4096, 2, true,
        items2.begin(), items2.end());

    GeneralLevel level(dir, 1 << 20, 10, true);

    std::vector<std::string> keys;
    level.forEachKeyWithPrefix("a", [&](const std::string& k) {
        keys.push_back(k);
        return true;
    });
    ASSERT_EQ(keys.size(), 3u);
    EXPECT_EQ(keys[0], "aa1");
    EXPECT_EQ(keys[1], "aa2");
    EXPECT_EQ(keys[2], "ab1");

    std::vector<std::string> stop;
    level.forEachKeyWithPrefix("a", [&](const std::string& k) {
        stop.push_back(k);
        return false;
    });
    ASSERT_EQ(stop.size(), 1u);
    EXPECT_EQ(stop[0], "aa1");
}
