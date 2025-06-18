#include <gtest/gtest.h>
#include "../src/sstfile.h"
#include "../src/utils.h"
#include <cstring>
#include <vector>
#include <random>

// ---------- DataBlock Tests ----------

TEST(DataBlockTest, InsertAndRetrieveUint32) {
    // Test inserting and retrieving a uint32 value
    DataBlockBuilder builder(4096);
    Entry entry{ ValueType::UINT32, uint32_t(42) };
    ASSERT_TRUE(builder.addEntry("foo", entry, 0));
    auto block_data = builder.build();

    DataBlock block(block_data);
    auto result = block.get("foo");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->type, ValueType::UINT32);
    EXPECT_EQ(std::get<uint32_t>(result->value), 42u);

    auto not_found = block.get("bar");
    EXPECT_FALSE(not_found.has_value());
}

TEST(DataBlockTest, InsertAndRetrieveString) {
    // Test inserting and retrieving a string value
    DataBlockBuilder builder(4096);
    Entry entry{ ValueType::STRING, std::string("hello") };
    ASSERT_TRUE(builder.addEntry("str_key", entry, 0));
    auto block_data = builder.build();

    DataBlock block(block_data);
    auto result = block.get("str_key");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->type, ValueType::STRING);
    EXPECT_EQ(std::get<std::string>(result->value), "hello");
}

TEST(DataBlockTest, InsertMultipleKeys_BinarySearch) {
    // Test binary search over multiple sorted keys
    DataBlockBuilder builder(4096);
    builder.addEntry("afff", Entry{ ValueType::UINT32, uint32_t(1) }, 0);
    builder.addEntry("azzz", Entry{ ValueType::STRING, "abc" }, 0);
    builder.addEntry("bbbbb", Entry{ ValueType::UINT32, uint32_t(2) }, 0);
    builder.addEntry("cff", Entry{ ValueType::STRING, "ffffffffffff"}, 0);
    builder.addEntry("xxxx", Entry{ ValueType::UINT32, uint32_t(3) }, 0);
    auto block_data = builder.build();

    DataBlock block(block_data);

    auto result = block.get("bbbbb");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(std::get<uint32_t>(result->value), 2u);

    result = block.get("azzz");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(std::get<std::string>(result->value), std::string("abc"));

    result = block.get("cff");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(std::get<std::string>(result->value), std::string("ffffffffffff"));

    EXPECT_FALSE(block.get("aa").has_value());
    EXPECT_FALSE(block.get("zz").has_value());
}

TEST(DataBlockTest, RetrieveExpiredReturnsRemoved) {
    // Test that retrieving expired (deleted) key returns REMOVED
    DataBlockBuilder builder(4096);
    Entry entry{ ValueType::UINT32, uint32_t(99) };
    // Use 1 as expiration_ms value to simulate deletion
    ASSERT_TRUE(builder.addEntry("dead", entry, 1));
    auto block_data = builder.build();

    DataBlock block(block_data);
    auto result = block.get("dead");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->type, ValueType::REMOVED);
}

TEST(DataBlockTest, InsertAllTypesAndRetrieveMixedOrder) {
    DataBlockBuilder builder(8192);
    std::vector<std::pair<std::string, Entry>> entries = {
        {"u8a", Entry{ValueType::UINT8, uint8_t(123)}},
        {"i8a", Entry{ValueType::INT8, int8_t(-10)}},
        {"u16", Entry{ValueType::UINT16, uint16_t(65530)}},
        {"i16", Entry{ValueType::INT16, int16_t(-32000)}},
        {"f32", Entry{ValueType::FLOAT, float(3.14f)}},
        {"dbl", Entry{ValueType::DOUBLE, double(2.718)}},
        {"u64", Entry{ValueType::UINT64, uint64_t(1ull << 40)}},
        {"str1", Entry{ValueType::STRING, std::string("abc")}},
        {"blob", Entry{ValueType::BLOB, std::vector<uint8_t>{1,2,3,4,5}}},
        {"i64", Entry{ValueType::INT64, int64_t(-123456789)}},
        {"str2", Entry{ValueType::STRING, std::string("xyz")}},
        {"u32", Entry{ValueType::UINT32, uint32_t(9999)}},
        {"i32", Entry{ValueType::INT32, int32_t(-50000)}},
        {"u8b", Entry{ValueType::UINT8, uint8_t(200)}},
        {"i8b", Entry{ValueType::INT8, int8_t(42)}},
        {"blob2", Entry{ValueType::BLOB, std::vector<uint8_t>{6,7,8}}},
        {"f64", Entry{ValueType::DOUBLE, double(-1.234)}},
        {"utf8", Entry{ValueType::U8STRING, std::u8string(u8"Привет")}}
    };

    std::sort(entries.begin(), entries.end(),
        [](const auto& a, const auto& b) { return a.first < b.first; });

    for (const auto& [k, v] : entries) {
        ASSERT_TRUE(builder.addEntry(k, v, 0));
    }

    auto block_data = builder.build();
    DataBlock block(block_data);

    // Retrieve and check all entries
    for (const auto& [k, v] : entries) {
        auto result = block.get(k);
        ASSERT_TRUE(result.has_value()) << "Key not found: " << k;
        EXPECT_EQ(result->type, v.type);

        // Type-specific value check
        switch (v.type) {
        case ValueType::UINT8:
            EXPECT_EQ(std::get<uint8_t>(result->value), std::get<uint8_t>(v.value)); break;
        case ValueType::INT8:
            EXPECT_EQ(std::get<int8_t>(result->value), std::get<int8_t>(v.value)); break;
        case ValueType::UINT16:
            EXPECT_EQ(std::get<uint16_t>(result->value), std::get<uint16_t>(v.value)); break;
        case ValueType::INT16:
            EXPECT_EQ(std::get<int16_t>(result->value), std::get<int16_t>(v.value)); break;
        case ValueType::UINT32:
            EXPECT_EQ(std::get<uint32_t>(result->value), std::get<uint32_t>(v.value)); break;
        case ValueType::INT32:
            EXPECT_EQ(std::get<int32_t>(result->value), std::get<int32_t>(v.value)); break;
        case ValueType::UINT64:
            EXPECT_EQ(std::get<uint64_t>(result->value), std::get<uint64_t>(v.value)); break;
        case ValueType::INT64:
            EXPECT_EQ(std::get<int64_t>(result->value), std::get<int64_t>(v.value)); break;
        case ValueType::FLOAT:
            EXPECT_FLOAT_EQ(std::get<float>(result->value), std::get<float>(v.value)); break;
        case ValueType::DOUBLE:
            EXPECT_DOUBLE_EQ(std::get<double>(result->value), std::get<double>(v.value)); break;
        case ValueType::STRING:
            EXPECT_EQ(std::get<std::string>(result->value), std::get<std::string>(v.value)); break;
        case ValueType::U8STRING:
            EXPECT_EQ(std::get<std::u8string>(result->value), std::get<std::u8string>(v.value)); break;
        case ValueType::BLOB:
            EXPECT_EQ(std::get<std::vector<uint8_t>>(result->value), std::get<std::vector<uint8_t>>(v.value)); break;
        default:
            FAIL() << "Unsupported ValueType";
        }
    }
}

// ---------- IndexBlock Tests ----------

TEST(IndexBlockTest, AddKeysAndParseRaw) {
    // Test IndexBlockBuilder serialization and field offsets
    std::vector<std::pair<std::string, sst::indexblock::OffsetFieldType>> keys
        = {
        {"aaa", sst::indexblock::OffsetFieldType(100)},
        {"bbb", sst::indexblock::OffsetFieldType(500)},
        {"ccc", sst::indexblock::OffsetFieldType(1000)},
        {"ddd", sst::indexblock::OffsetFieldType(1500)},
        {"eee", sst::indexblock::OffsetFieldType(2000)} };

    IndexBlockBuilder builder;
    for (const auto& [key, offset] : keys) {
        builder.addKey(key, offset);
    }

    auto raw_data = builder.build();
    ASSERT_FALSE(raw_data.empty());

    // Use Utils::deserializeLE for all fields
    size_t pos = 0;

    // Read total size (sst::datablock::CountFieldType, little-endian, at the end)
    sst::datablock::CountFieldType total_size = Utils::deserializeLE<sst::datablock::CountFieldType>(&raw_data[raw_data.size() - sizeof(sst::datablock::CountFieldType)]);
    EXPECT_EQ(total_size, raw_data.size() - sizeof(sst::datablock::CountFieldType));
    size_t end = raw_data.size() - sizeof(sst::datablock::CountFieldType);

    pos = 0;
    size_t key_idx = 0;
    while (pos < end) {
        auto key_len = Utils::deserializeLE<sst::indexblock::IndexKeyLengthFieldType>(&raw_data[pos]);
        pos += sizeof(sst::indexblock::IndexKeyLengthFieldType);
        std::string key = Utils::deserializeLE<std::string>(&raw_data[pos], key_len);
        EXPECT_EQ(key, keys[key_idx].first);
        pos += key_len;
        auto offset = Utils::deserializeLE<sst::indexblock::OffsetFieldType>(&raw_data[pos]);
        EXPECT_EQ(offset, keys[key_idx].second);
        pos += sizeof(sst::indexblock::OffsetFieldType);

        ++key_idx;
    }
    EXPECT_EQ(key_idx, keys.size());
    EXPECT_EQ(pos, end);
}

TEST(DataBlockTest, CorruptionTest) {
    // This test tries to parse random/corrupted data as DataBlock
    std::mt19937 rng(static_cast<unsigned>(std::chrono::system_clock::now().time_since_epoch().count()));
    std::uniform_int_distribution<unsigned> dist(0, 255);

    for (int i = 0; i < 100; ++i) {
        std::vector<uint8_t> random_data(10 * 1024); // 10 KB
        for (auto& b : random_data)
            b = static_cast<uint8_t>(dist(rng));

        try {
            DataBlock block(random_data);
            // Try to call get on a random key
            // It may throw or return a value (possibly garbage)
            auto result = block.get("a_corrupt_key");
            result = block.get("h_corrupt_key");
            result = block.get("r_corrupt_key");
            result = block.get("z_corrupt_key");

            EXPECT_TRUE(result.has_value() || !result.has_value());
            // No assertion: just making sure no crash
        }
        catch (const std::exception& ex) {
            // Exception is expected and OK for corrupted input
        }
        catch (...) {
            // Any other exception should be caught too
        }
    }
    SUCCEED();

}

TEST(DataBlockTest, KeysWithPrefix_Status_Remove) {
    DataBlockBuilder builder(4096);
    builder.addEntry("noprefix", Entry{ ValueType::UINT32, uint32_t(444) }, 0);
    builder.addEntry("pre_ab", Entry{ ValueType::UINT32, uint32_t(555) }, 0);
    builder.addEntry("pre_abc", Entry{ ValueType::UINT32, uint32_t(111) }, 0);
    builder.addEntry("pre_abd", Entry{ ValueType::UINT32, uint32_t(222) }, 0);
    builder.addEntry("pre_xyz", Entry{ ValueType::UINT32, uint32_t(333) }, 0);

    auto block_data = builder.build();
    DataBlock block(block_data);

    auto keys = block.keysWithPrefix("pre_a", 10);
    EXPECT_EQ(keys.size(), 3u);
    EXPECT_NE(std::find(keys.begin(), keys.end(), "pre_abc"), keys.end());
    EXPECT_NE(std::find(keys.begin(), keys.end(), "pre_abd"), keys.end());
    EXPECT_NE(std::find(keys.begin(), keys.end(), "pre_ab"), keys.end());

    auto keys2 = block.keysWithPrefix("pre_", 2);
    EXPECT_EQ(keys2.size(), 2u);

    EXPECT_EQ(block.status("pre_abc"), EntryStatus::EXISTS);
    EXPECT_EQ(block.status("pre_xyz"), EntryStatus::EXISTS);
    EXPECT_EQ(block.status("noprefix"), EntryStatus::EXISTS);
    EXPECT_EQ(block.status("notfound"), EntryStatus::NOT_FOUND);

    EXPECT_TRUE(block.remove("pre_abc"));
    EXPECT_EQ(block.status("pre_abc"), EntryStatus::REMOVED);

    auto res = block.get("pre_abc");
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->type, ValueType::REMOVED);

    EXPECT_FALSE(block.remove("notfound"));
    EXPECT_TRUE(block.remove("pre_abc"));
}

TEST(DataBlockTest, ForEachKeyWithPrefix_Basic) {
    DataBlockBuilder builder(4096);
    builder.addEntry("pre_a", Entry{ ValueType::UINT32, uint32_t(1) }, 0);
    builder.addEntry("pre_b", Entry{ ValueType::UINT32, uint32_t(2) }, 0);
    builder.addEntry("pre_c", Entry{ ValueType::UINT32, uint32_t(3) }, 0);
    builder.addEntry("zzz", Entry{ ValueType::UINT32, uint32_t(4) }, 0);
    auto data = builder.build();
    DataBlock block(data);

    std::vector<std::string> keys;
    block.forEachKeyWithPrefix("pre_", [&](const std::string& k) {
        keys.push_back(k);
        return true;
    });
    ASSERT_EQ(keys.size(), 3u);
    EXPECT_EQ(keys[0], "pre_a");
    EXPECT_EQ(keys[1], "pre_b");
    EXPECT_EQ(keys[2], "pre_c");

    std::vector<std::string> first_only;
    block.forEachKeyWithPrefix("pre_", [&](const std::string& k) {
        first_only.push_back(k);
        return false; // stop after first
    });
    ASSERT_EQ(first_only.size(), 1u);
    EXPECT_EQ(first_only[0], "pre_a");
}
