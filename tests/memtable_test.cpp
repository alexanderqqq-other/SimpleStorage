#include <gtest/gtest.h>
#include "../src/memtable.h"
#include "../src/types.h"
#include <thread>
using namespace std;

class MemTableTest : public ::testing::Test {
protected:
    size_t memtable_size = 1024; 
    MemTable* memtable;

    void SetUp() override {
        memtable = new MemTable(memtable_size);
    }
    void TearDown() override {
        delete memtable;
    }
};

TEST_F(MemTableTest, PutAndGetUint32) {
    Entry entry{ ValueType::UINT32, uint32_t(42) };
    memtable->put("test_key", entry, std::numeric_limits<uint64_t>::max());

    auto result = memtable->get("test_key");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->type, ValueType::UINT32);
    EXPECT_EQ(std::get<uint32_t>(result->value), 42u);
}

TEST_F(MemTableTest, PutAndGetString) {
    Entry entry{ ValueType::STRING, std::string("hello world") };
    memtable->put("str_key", entry, std::numeric_limits<uint64_t>::max());

    auto result = memtable->get("str_key");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->type, ValueType::STRING);
    EXPECT_EQ(std::get<std::string>(result->value), "hello world");
}

TEST_F(MemTableTest, RemoveKey) {
    Entry entry{ ValueType::UINT32, uint32_t(100) };
    memtable->put("key_to_remove", entry, std::numeric_limits<uint64_t>::max());
    memtable->remove("key_to_remove");

    auto result = memtable->get("key_to_remove");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->type, ValueType::REMOVED);
}

TEST_F(MemTableTest, FullMemTable) {
    // Fill memtable until full() returns true
    int i = 0;
    while (!memtable->full()) {
        string key = "key" + std::to_string(i++);
        Entry entry{ ValueType::UINT32, uint32_t(i) };
        memtable->put(key, entry, std::numeric_limits<uint64_t>::max());
    }
    EXPECT_TRUE(memtable->full());
}

TEST_F(MemTableTest, FullMemTable2) {
    // Fill memtable until full() returns true
    int i = 0;
    std::string key = "long_key_for_test123456789098765431";
    std::vector<uint8_t> blob(1024 - key.size() - 1, 0xFF);
    Entry entry{ ValueType::BLOB, blob };
    memtable->put(key, entry, std::numeric_limits<uint64_t>::max());
    EXPECT_TRUE(memtable->full());
}

TEST_F(MemTableTest, KeysWithPrefix_Basic) {
    memtable->put("abc1", Entry{ ValueType::UINT32, uint32_t(1) }, std::numeric_limits<uint64_t>::max());
    memtable->put("abc2", Entry{ ValueType::UINT32, uint32_t(2) }, std::numeric_limits<uint64_t>::max());
    memtable->put("abb1", Entry{ ValueType::UINT32, uint32_t(5) }, std::numeric_limits<uint64_t>::max());
    memtable->put("bca1", Entry{ ValueType::UINT32, uint32_t(5) }, std::numeric_limits<uint64_t>::max());
    memtable->put("abd1", Entry{ ValueType::UINT32, uint32_t(4) }, std::numeric_limits<uint64_t>::max());
    memtable->put("abc3", Entry{ ValueType::UINT32, uint32_t(5) }, std::numeric_limits<uint64_t>::max());

    auto keys = memtable->keysWithPrefix("abc", 10);
    EXPECT_EQ(keys.size(), 3);
    EXPECT_NE(std::find(keys.begin(), keys.end(), "abc1"), keys.end());
    EXPECT_NE(std::find(keys.begin(), keys.end(), "abc2"), keys.end());
    EXPECT_NE(std::find(keys.begin(), keys.end(), "abc3"), keys.end());
    auto keys2 = memtable->keysWithPrefix("abc", 2);
    EXPECT_EQ(keys2.size(), 2);

    for (const auto& key : keys) {
        EXPECT_TRUE(key.rfind("abc", 0) == 0); 
    }
}

TEST_F(MemTableTest, KeysWithPrefix_RemovedAndExpired) {
    memtable->put("abc1", Entry{ ValueType::UINT32, uint32_t(1) }, std::numeric_limits<uint64_t>::max());
    memtable->put("abc5", Entry{ ValueType::UINT32, uint32_t(2) }, std::numeric_limits<uint64_t>::max());
    memtable->put("abc2", Entry{ ValueType::UINT32, uint32_t(3) }, 5); 
    memtable->put("abc3", Entry{ ValueType::UINT32, uint32_t(2) }, std::numeric_limits<uint64_t>::max());
    memtable->put("abc4", Entry{ ValueType::UINT32, uint32_t(3) }, 1);
    memtable->put("abc1", Entry{ ValueType::UINT32, uint32_t(2) }, 1);


    memtable->remove("abc3");

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    auto keys = memtable->keysWithPrefix("abc", 10);
    ASSERT_TRUE(keys.size() == 1);
    EXPECT_EQ(keys[0], "abc5");
}
