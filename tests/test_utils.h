#include "../src/types.h"
#include <vector>
constexpr int BIG_BLOCK_SIZE = 16 * 1024;
constexpr int BIG_BATCH1 = 800;
constexpr int PREFIX_SERIES = 300;
constexpr int BIG_BATCH2 = 900;
constexpr size_t BIG_BLOB_SIZE = 8192;
std::vector<uint8_t> ref_blob = { 11,22,33,44,55,66,77,88,99 };

struct TestEntry {
    Entry entry;
    uint64_t expiration_ms = std::numeric_limits<uint64_t>::max();
};

uint64_t mix(uint64_t x) {
    // Simple 64-bit mix, inspired by SplitMix64
    x += 0x9e3779b97f4a7c15;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9;
    x = (x ^ (x >> 27)) * 0x94d049bb133111eb;
    x = x ^ (x >> 31);
    return x;
}

std::string pseudo_random_string(uint64_t i, int length = 10) {
    const char charset[] = "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789";
    const int charset_size = sizeof(charset) - 1; // exclude null terminator

    std::string result;
    uint64_t hash = mix(static_cast<uint64_t>(i));
    for (int j = 0; j < length; ++j) {
        // Further mix for each character
        uint64_t h = mix(hash + j * 0x123456789ABCDEF);
        result += charset[h % charset_size];
    }
    return result;
}

std::u8string pseudo_unicode_string(uint64_t i) {
    const int length = 10;
    const char charset[] = "АБВГДЕЁЖЗИКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдеёжзиклмнопрстуфхцчшщъыьэюя";
    const int charset_size = sizeof(charset) - 1; // exclude null terminator

    std::u8string result;
    uint64_t hash = mix(static_cast<uint64_t>(i));
    for (int j = 0; j < length; ++j) {
        // Further mix for each character
        uint64_t h = mix(hash + j * 0x123456789ABCDEF);
        result += charset[h % charset_size];
    }
    return result;
}


Entry pseudo_random_value(uint64_t i) {
    uint64_t hash = mix(static_cast<uint64_t>(i));
    ValueType type = static_cast<ValueType>(i % (static_cast<uint64_t>(ValueType::BLOB) + 1));
    Value val;
    switch (type) {
    case ValueType::UINT8:
        val = static_cast<uint8_t>(hash % std::numeric_limits<uint8_t>::max());
        break;
    case ValueType::INT8:
        val = static_cast<int8_t>(hash % std::numeric_limits<int8_t>::max());
        break;
    case ValueType::UINT16:
        val = static_cast<uint16_t>(hash % std::numeric_limits<uint16_t>::max());
        break;
    case ValueType::INT16:
        val = static_cast<int16_t>(hash % std::numeric_limits<int16_t>::max());
        break;
    case ValueType::UINT32:
        val = static_cast<uint32_t>(hash % std::numeric_limits<uint32_t>::max());
        break;
    case ValueType::INT32:
        val = static_cast<int32_t>(hash % std::numeric_limits<int32_t>::max());
        break;
    case ValueType::UINT64:
        val = static_cast<uint64_t>(hash % std::numeric_limits<uint64_t>::max());
        break;
    case ValueType::INT64:
        val = static_cast<int64_t>(hash % std::numeric_limits<int64_t>::max());
        break;
    case ValueType::FLOAT:
        val = static_cast<float>(hash % 10000) / 100.0f; // Example float value
        break;
    case ValueType::DOUBLE:
        val = static_cast<double>(hash % 100000) / 1000.0; // Example double value
        break;
    case ValueType::STRING:
        val = pseudo_random_string(hash);
        break;
    case ValueType::U8STRING:
        val = pseudo_unicode_string(hash);
        break;
    case ValueType::BLOB:
    {
        std::vector<uint8_t> blob(2048 + hash % BIG_BLOB_SIZE, static_cast<uint8_t>(hash % 256));
        val = blob;
    }
        break;
    case ValueType::REMOVED:
        break;
    default:
        break;
    }
    return Entry{ type, val };
}


inline std::vector<std::pair<std::string, TestEntry>> generateBigData() {
    std::vector<std::pair<std::string, TestEntry>> items;

    for (int i = 0; i < BIG_BATCH1; ++i) {
        std::string key = pseudo_random_string(i) + std::to_string(i) + "_AAAAAAAAAAA";
        if (i % 4 == 0) {
            items.emplace_back(key, TestEntry{ Entry{ValueType::UINT32, uint32_t(i * 100)}, 0 });
        }
        else if (i % 4 == 1) {
            items.emplace_back(key, TestEntry{ Entry{ValueType::STRING, std::string("s_" + std::to_string(i))}, 0 });
        }
        else if (i % 4 == 2) {
            std::vector<uint8_t> blob(BIG_BLOB_SIZE, static_cast<uint8_t>(i % 256));
            items.emplace_back(key, TestEntry{ Entry{ValueType::BLOB, blob}, 0 });
        }
        else {
            items.emplace_back(key, TestEntry{ Entry{ValueType::DOUBLE, double(i) * 1.5}, 0 });
        }
    }
    std::vector<uint8_t> blob(512);
    for (int i = 0; i < PREFIX_SERIES; ++i) {
        std::string key = "pref_" + std::to_string(i / 100) + "_" + std::string(1 + (i % 5), char('a' + (i % 26)));
        items.emplace_back(key, TestEntry{ Entry{ValueType::BLOB, blob}, 0 });
    }

    items.emplace_back("test_control_uint32", TestEntry{ Entry{ValueType::UINT32, uint32_t(424242)}, 0 });
    items.emplace_back("test_control_str", TestEntry{ Entry{ValueType::STRING, std::string("control_test")}, 0 });
    items.emplace_back("test_control_blob", TestEntry{ Entry{ValueType::BLOB, ref_blob}, 0 });
    items.emplace_back("test_control_double", TestEntry{ Entry{ValueType::DOUBLE, 123456.789}, 0 });
    items.emplace_back("control_will_overwrite", TestEntry{ Entry{ValueType::STRING, std::string("overwritten")}, 0 });
    items.emplace_back("test_control_u8str", TestEntry{ Entry{ValueType::U8STRING, std::u8string(u8"Юникод")}, 0 });
    items.emplace_back("test_remove_blob", TestEntry{ Entry{ValueType::BLOB, ref_blob}, 0 });

    for (int i = 0; i < BIG_BATCH2; ++i) {
        std::string key = "batch2_" + std::to_string(i) + "_ZZZZZZZZZZZ";
        if (i % 3 == 0) {
            items.emplace_back(key, TestEntry{ Entry{ValueType::UINT64, uint64_t(i * 333333)}, 0 });
        }
        else if (i % 3 == 1) {
            items.emplace_back(key, TestEntry{ Entry{ValueType::STRING, std::string("b2_" + std::to_string(i))}, 0 });
        }
        else {
            std::vector<uint8_t> blob(BIG_BLOB_SIZE, static_cast<uint8_t>((i * 7) % 256));
            items.emplace_back(key, TestEntry{ Entry{ValueType::BLOB, blob}, 0 });
        }
    }

    std::sort(items.begin(), items.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
    return items;
}



inline std::vector<std::pair<std::string, TestEntry>> generateBigData2() {
    std::vector<std::pair<std::string, TestEntry>> items;

    for (int i = 0; i < BIG_BATCH1; ++i) {
        int val = i + 5123;
        std::string key = pseudo_random_string(val) + std::to_string(val) + "_AAAAAAAAAAA";
        if (i % 4 == 0) {
            items.emplace_back(key, TestEntry{ Entry{ValueType::UINT32, uint32_t(i * 100)}, 0 });
        }
        else if (i % 4 == 1) {
            items.emplace_back(key, TestEntry{ Entry{ValueType::STRING, std::string("s_" + std::to_string(i))}, 0 });
        }
        else if (i % 4 == 2) {
            std::vector<uint8_t> blob(BIG_BLOB_SIZE, static_cast<uint8_t>(i % 256));
            items.emplace_back(key, TestEntry{ Entry{ValueType::BLOB, blob}, 0 });
        }
        else {
            items.emplace_back(key, TestEntry{ Entry{ValueType::DOUBLE, double(i) * 1.5}, 0 });
        }
    }
    std::vector<uint8_t> blob(512);
    for (int i = 0; i < PREFIX_SERIES; ++i) {
        std::string key = "pref_" + std::to_string(i / 100) + "_" + std::string(1 + (i % 5), char('a' + (i % 26)));
        items.emplace_back(key, TestEntry{ Entry{ValueType::BLOB, blob}, 0 });
    }

    items.emplace_back("test2_control_uint32", TestEntry{ Entry{ValueType::UINT32, uint32_t(424242)}, 0 });
    items.emplace_back("test2_control_str", TestEntry{ Entry{ValueType::STRING, std::string("control_test")}, 0 });
    std::vector<uint8_t> ref_blob = { 11,22,33,44,55,66,77,88,99 };
    items.emplace_back("test2_control_blob", TestEntry{ Entry{ValueType::BLOB, ref_blob}, 0 });
    items.emplace_back("test2_control_double", TestEntry{ Entry{ValueType::DOUBLE, 123456.789}, 0 });
    items.emplace_back("control_will_overwrite", TestEntry{ Entry{ValueType::DOUBLE, 123456.789}, 0 });
    items.emplace_back("test2_control_u8str", TestEntry{ Entry{ValueType::U8STRING, std::u8string(u8"Юникод")}, 0 });
    items.emplace_back("test2_remove_blob", TestEntry{ Entry{ValueType::BLOB, ref_blob}, 0 });

    for (int i = 0; i < BIG_BATCH2; ++i) {
        std::string key = "batch2_" + std::to_string(i) + "_ZZZZZZZZZZZ";
        if (i % 3 == 0) {
            items.emplace_back(key, TestEntry{ Entry{ValueType::UINT64, uint64_t(i * 333333)}, 0 });
        }
        else if (i % 3 == 1) {
            items.emplace_back(key, TestEntry{ Entry{ValueType::STRING, std::string("b2_" + std::to_string(i))}, 0 });
        }
        else {
            std::vector<uint8_t> blob(BIG_BLOB_SIZE, static_cast<uint8_t>((i * 7) % 256));
            items.emplace_back(key, TestEntry{ Entry{ValueType::BLOB, blob}, 0 });
        }
    }

    std::sort(items.begin(), items.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
    return items;
}