#include "../src/levelzero.h"
#include "../src/sstfile.h"
#include "test_utils.h"
#include <filesystem>
#include <gtest/gtest.h>

namespace fs = std::filesystem;

namespace {
    constexpr int BLOCK_SIZE = 32 * 1024;
}

class LevelZeroTest : public ::testing::Test {
protected:
    fs::path dir;
    void SetUp() override {
        dir = fs::temp_directory_path() / "levelzero_test_dir";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir);
    }
    void TearDown() override {
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    template <typename Vec> std::unique_ptr<SSTFile> createSST(uint64_t seq, const Vec& items) {
        fs::path tmp = dir / ("tmp_" + std::to_string(seq) + ".vsst");
        std::vector<std::pair<std::string, TestEntry>> sorted(items.begin(),
            items.end());
        std::sort(sorted.begin(), sorted.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });
        return SSTFile::writeAndCreate(tmp, BLOCK_SIZE, seq, true, sorted.begin(),
            sorted.end());
    }
};

TEST_F(LevelZeroTest, AddAndGetLatest) {
    std::vector<std::pair<std::string, TestEntry>> v1 = {
        {"key", TestEntry{Entry{ValueType::UINT32, uint32_t(1)}, 0}} };
    std::vector<std::pair<std::string, TestEntry>> v2 = {
        {"key", TestEntry{Entry{ValueType::UINT32, uint32_t(2)}, 0}} };
    LevelZero lz(dir, 10);
    auto sst1 = createSST(1, v1);
    auto sst2 = createSST(2, v2);
    std::vector<std::unique_ptr<SSTFile>>  vec;
    vec.push_back(std::move(sst1));
    vec.push_back(std::move(sst2));
    lz.addSST(std::move(vec));

    auto val = lz.get("key");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(val->type, ValueType::UINT32);
    EXPECT_EQ(std::get<uint32_t>(val->value), 2u);

    // Reopen from disk and ensure persistence
    LevelZero lz2(dir, 10);
    val = lz2.get("key");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(std::get<uint32_t>(val->value), 2u);
}

TEST_F(LevelZeroTest, KeysWithPrefixAndRemoveSST) {
    std::vector<std::pair<std::string, TestEntry>> v1 = {
        {"foo1", TestEntry{Entry{ValueType::UINT32, uint32_t(1)}, 0}},
        {"bar1", TestEntry{Entry{ValueType::UINT32, uint32_t(1)}, 0}} };
    std::vector<std::pair<std::string, TestEntry>> v2 = {
        {"foo2", TestEntry{Entry{ValueType::UINT32, uint32_t(2)}, 0}} };
    std::vector<std::pair<std::string, TestEntry>> v3 = {
        {"foo3", TestEntry{Entry{ValueType::UINT32, uint32_t(3)}, 0}} };
    LevelZero lz(dir, 10);
    auto sst1 = createSST(1, v1);
    auto sst2 = createSST(2, v2);
    auto sst3 = createSST(3, v3);
    std::vector<std::unique_ptr<SSTFile>>  vec;
    vec.push_back(std::move(sst1));
    vec.push_back(std::move(sst2));
    vec.push_back(std::move(sst3));
    lz.addSST(std::move(vec));

    auto keys = lz.keysWithPrefix("foo", 10);
    ASSERT_EQ(keys.size(), 3u);
    EXPECT_EQ(keys[0], "foo3");
    EXPECT_EQ(keys[1], "foo2");
    EXPECT_EQ(keys[2], "foo1");

    fs::path p2 = dir / ("L0_" + std::to_string(2) + ".vsst");
    lz.removeSSTs({ p2 });
    keys = lz.keysWithPrefix("foo", 10);
    ASSERT_EQ(keys.size(), 2u);
    EXPECT_EQ(keys[0], "foo3");
    EXPECT_EQ(keys[1], "foo1");
}

TEST_F(LevelZeroTest, FilelistToMergeAndRemove) {
    LevelZero lz(dir, 2);
    std::vector<std::pair<std::string, TestEntry>> v = {
        {"k", TestEntry{Entry{ValueType::UINT32, uint32_t(1)}, 0}} };
    auto s1 = createSST(1, v);
    auto s2 = createSST(2, v);
    auto s3 = createSST(3, v);
    std::vector<std::unique_ptr<SSTFile>>  vec2;
    vec2.push_back(std::move(s1));
    vec2.push_back(std::move(s2));
    vec2.push_back(std::move(s3));
    lz.addSST(std::move(vec2));

    auto to_merge = lz.filelistToMerge(3);
    ASSERT_EQ(to_merge.size(), 3u);
    EXPECT_TRUE(fs::exists(to_merge[0]));

    EXPECT_THROW(lz.mergeToTmp(dir / "tmp", BLOCK_SIZE), std::logic_error);

    lz.removeSSTs({ to_merge[0] });
    to_merge = lz.filelistToMerge(3);
    ASSERT_EQ(to_merge.size(), 2u);
}

TEST_F(LevelZeroTest, RemoveDoesNotAffectHigherSeq) {
    LevelZero lz(dir, 10);
    std::vector<std::pair<std::string, TestEntry>> v1 = {
        {"a", {Entry{ValueType::UINT32, uint32_t(1)}, 0}} };
    std::vector<std::pair<std::string, TestEntry>> v2 = {
        {"b", {Entry{ValueType::UINT32, uint32_t(2)}, 0}} };
    auto s1 = createSST(1, v1);
    auto s2 = createSST(2, v2);
    std::vector<std::unique_ptr<SSTFile>>  vec3;
    vec3.push_back(std::move(s1));
    vec3.push_back(std::move(s2));
    lz.addSST(std::move(vec3));


    EXPECT_FALSE(lz.remove("a", 0));
    EXPECT_TRUE(lz.remove("a", 1));
    auto val = lz.get("a");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(val->type, ValueType::REMOVED);


    EXPECT_FALSE(lz.remove("missing", 2));
}