//// filelevel_test.cpp
//#include <gtest/gtest.h>
//#include <filesystem>
//#include <vector>
//#include <memory>
//#include "../src/levelzero.h"
//#include "../src/generallevel.h"
//#include "../src/sstfile.h"
//#include "../src/types.h"
//#include "test_utils.h"
//namespace fs = std::filesystem;
//
//// Helper: create SSTFile with test entries
//inline SSTFile createTestSST(const fs::path& path, const std::vector<std::pair<std::string, Entry>>& items, size_t block_size = 16 * 1024) {
//    struct TestEntry { Entry entry; uint64_t expiration_ms; };
//    std::vector<std::pair<std::string, TestEntry>> data;
//    for (const auto& [k, e] : items)
//        data.push_back({ k, {e, std::numeric_limits<uint64_t>::max()} });
//    std::sort(data.begin(), data.end(), [](auto& a, auto& b) { return a.first < b.first; });
//    return SSTFile::writeAndCreate(path, block_size,0, data.begin(), data.end());
//}
//inline SSTFile createTestSST(const fs::path& path, const std::vector<std::pair<std::string, TestEntry>>& items, size_t block_size = 16 * 1024) {
//    return SSTFile::writeAndCreate(path, block_size,1, items.begin(), items.end());
//}
//
//template <typename T>
//class FileLevelTest : public ::testing::Test {
//protected:
//    fs::path temp_dir ;
//
//    void SetUp() override {
//        temp_dir = fs::temp_directory_path() / ("filelevel_test_" + std::string(typeid(T).name()));
//        fs::remove_all(temp_dir);
//        fs::create_directory(temp_dir);
//    }
//    void TearDown() override {
//        fs::remove_all(temp_dir);
//    }
//
//    std::unique_ptr<IFileLevel> makeLevel(std::filesystem::path test_path = {}, size_t param = 8) {
//        if (test_path.empty()) {
//            test_path = temp_dir;
//        }
//        if constexpr (std::is_same_v<T, LevelZero>) {
//            return std::make_unique<LevelZero>(test_path, param);
//        }
//        else if constexpr (std::is_same_v<T, GeneralLevel>) {
//            return std::make_unique<GeneralLevel>(test_path, param * 1024 * 1024); // maxFileSize
//        }
//    }
//
//    fs::path sstPath(int i) {
//        return temp_dir / ("testfile_" + std::to_string(i) + ".vsst");
//    }
//};
//
//using FileLevelTypes = ::testing::Types<LevelZero, GeneralLevel>;
//TYPED_TEST_SUITE(FileLevelTest, FileLevelTypes);
//
//TYPED_TEST(FileLevelTest, AddAndGetWorks) {
//    auto level = this->makeLevel();
//    auto sst = createTestSST(this->sstPath(1), {
//        {"foo", Entry{ValueType::UINT32, uint32_t(42)}},
//        {"bar", Entry{ValueType::STRING, std::string("abc")}}
//        });
//    std::vector<SSTFile> sst_files;
//    sst_files.push_back(std::move(sst));
//    level->addSST(std::move(sst_files));
//
//    auto foo = level->get("foo");
//    ASSERT_TRUE(foo.has_value());
//    EXPECT_EQ(foo->type, ValueType::UINT32);
//    EXPECT_EQ(std::get<uint32_t>(foo->value), 42u);
//
//    auto bar = level->get("bar");
//    ASSERT_TRUE(bar.has_value());
//    EXPECT_EQ(bar->type, ValueType::STRING);
//    EXPECT_EQ(std::get<std::string>(bar->value), "abc");
//
//    EXPECT_FALSE(level->get("missing").has_value());
//}
//
//TYPED_TEST(FileLevelTest, RemoveKeyAndStatus) {
//    auto level = this->makeLevel();
//    auto sst = createTestSST(this->sstPath(2), {
//        {"alpha", Entry{ValueType::UINT32, uint32_t(1)}},
//        {"beta", Entry{ValueType::UINT32, uint32_t(2)}}
//        });
//    std::vector<SSTFile> sst_files;
//    sst_files.push_back(std::move(sst));
//    level->addSST(std::move(sst_files));
//
//    ASSERT_TRUE(level->remove("beta"));
//    auto res = level->get("beta");
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(res->type, ValueType::REMOVED);
//
//    EXPECT_EQ(level->status("beta"), EntryStatus::REMOVED);
//    EXPECT_EQ(level->status("alpha"), EntryStatus::EXISTS);
//    EXPECT_EQ(level->status("nope"), EntryStatus::NOT_FOUND);
//}
//
//TYPED_TEST(FileLevelTest, KeysWithPrefixFindsAll) {
//    auto level = this->makeLevel();
//    auto sst = createTestSST(this->sstPath(3), {
//        {"x1", Entry{ValueType::UINT32, uint32_t(10)}},
//        {"x2", Entry{ValueType::UINT32, uint32_t(20)}},
//        {"y1", Entry{ValueType::UINT32, uint32_t(30)}}
//        });
//    std::vector<SSTFile> sst_files;
//    sst_files.push_back(std::move(sst));
//    level->addSST(std::move(sst_files));
//
//    auto keys = level->keysWithPrefix("x", 10);
//    EXPECT_EQ(keys.size(), 2);
//    EXPECT_NE(std::find(keys.begin(), keys.end(), "x1"), keys.end());
//    EXPECT_NE(std::find(keys.begin(), keys.end(), "x2"), keys.end());
//    EXPECT_EQ(std::find(keys.begin(), keys.end(), "y1"), keys.end());
//}
//
//TYPED_TEST(FileLevelTest, ShadowingLatestValueReturned) {
//    auto level = this->makeLevel();
//    auto sst1 = createTestSST(this->sstPath(4), {
//        {"dup", Entry{ValueType::UINT32, uint32_t(1)}},
//        });
//    auto sst2 = createTestSST(this->sstPath(5), {
//        {"dup", Entry{ValueType::UINT32, uint32_t(99)}},
//        });
//    std::vector<SSTFile> sst_files;
//    sst_files.push_back(std::move(sst1));
//    level->addSST(std::move(sst_files));
//    sst_files.clear();
//    sst_files.push_back(std::move(sst2));
//    level->addSST(std::move(sst_files));
//
//    auto v = level->get("dup");
//    ASSERT_TRUE(v.has_value());
//    // For LevelZero, latest file shadows previous; for GeneralLevel, files don't overlap, but addSST is still allowed by interface
//    // For LevelZero, will be 99; for GeneralLevel, depends on addSST implementation (may keep both, or only latest)
//    if (std::is_same_v<TypeParam, LevelZero>)
//        EXPECT_TRUE(std::get<uint32_t>(v->value) == 99);
//    else
//        // GeneralLevel accept only non-overlapping files, so it will keep both
//        EXPECT_TRUE(std::get<uint32_t>(v->value) == 1 || std::get<uint32_t>(v->value) == 99);
//}
////
////TYPED_TEST(FileLevelTest, MergeToClearsFilesIfNeeded) {
////
////    std::vector<std::pair<std::string, TestEntry>> items1 = generateBigData();
////    std::vector<std::pair<std::string, TestEntry>> items2 = generateBigData2();
////
////    //int duplicate_count = 0;
////    //auto it1 = items1.begin();
////    //auto end1 = items1.end();
////    //for (const auto& [k, v] : items2) {
////    //    it1 = std::lower_bound(it1, end1, k, [](const auto& item, const auto& key) { return item.first < key; });
////    //    if (it1 != end1 && it1->first == k) {
////    //        duplicate_count++;
////    //    }
////    //}
////
////    auto level = this->makeLevel(this->temp_dir, 1);
////    auto next_level = std::make_unique<GeneralLevel>(this->temp_dir, 8 * 1024 * 1024);
////
////    std::vector<SSTFile> sst_files1;
////    sst_files1.push_back(createTestSST(this->sstPath(6), items1));
////    level->addSST(std::move(sst_files1));
////    auto v = next_level->get("control_will_overwrite");
////    ASSERT_TRUE(v.has_value());
////    EXPECT_EQ(v->type, ValueType::STRING);
////    EXPECT_EQ(std::get<std::string>(v->value), std::string("overwritten"));
////
////    std::vector<SSTFile> sst_files2;
////    sst_files2.push_back(createTestSST(this->sstPath(7), items2));
////
////    
////    next_level->addSST(std::move(sst_files2));
////    v = level->get("control_will_overwrite");
////    ASSERT_TRUE(v.has_value());
////    EXPECT_EQ(v->type, ValueType::DOUBLE);
////    EXPECT_EQ(std::get<uint32_t>(v->value), 123456.789);
////
////
////    auto out = level->merge_to(next_level.get());
////    EXPECT_EQ(out.size(), 1);
////    v = out[0].get("control_will_overwrite");
////    ASSERT_TRUE(v.has_value());
////    EXPECT_EQ(v->type, ValueType::STRING);
////    EXPECT_EQ(std::get<std::string>(v->value), std::string("overwritten"));
////    next_level->addSST(std::move(out));
////    level->clearMerged();
////    {
////        auto v = next_level->get("test_control_uint32");
////        ASSERT_TRUE(v.has_value());
////        EXPECT_EQ(v->type, ValueType::UINT32);
////        EXPECT_EQ(std::get<uint32_t>(v->value), 424242u);
////    }
////    {
////        auto v = next_level->get("test_control_str");
////        ASSERT_TRUE(v.has_value());
////        EXPECT_EQ(v->type, ValueType::STRING);
////        EXPECT_EQ(std::get<std::string>(v->value), "control_test");
////    }
////    {
////        auto v = next_level->get("test_control_blob");
////        ASSERT_TRUE(v.has_value());
////        EXPECT_EQ(v->type, ValueType::BLOB);
////        EXPECT_EQ(std::get<std::vector<uint8_t>>(v->value), ref_blob);
////    }
////    {
////        auto v = next_level->get("test_control_double");
////        ASSERT_TRUE(v.has_value());
////        EXPECT_EQ(v->type, ValueType::DOUBLE);
////        EXPECT_DOUBLE_EQ(std::get<double>(v->value), 123456.789);
////    }
////    {
////        auto v = next_level->get("test_control_u8str");
////        ASSERT_TRUE(v.has_value());
////        EXPECT_EQ(v->type, ValueType::U8STRING);
////        EXPECT_EQ(std::get<std::u8string>(v->value), std::u8string(u8"Юникод"));
////    }
////    EXPECT_TRUE(out.empty() || out.size() > 0);
////}
