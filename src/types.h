#pragma once
#include "constants.h"
#include <variant>
#include <string>
#include <vector>
#include <cstdint>
#include <concepts>

template <typename T>
concept SupportedInteger =
std::is_same_v<T, int8_t> ||
std::is_same_v<T, uint8_t> ||
std::is_same_v<T, int16_t> ||
std::is_same_v<T, uint16_t> ||
std::is_same_v<T, int32_t> ||
std::is_same_v<T, uint32_t> ||
std::is_same_v<T, int64_t> ||
std::is_same_v<T, uint64_t>;

//On the most platforms float and double are 4 and 8 bytes respectively.
//To avoid errors on some rare platforms we support only IEEE 754 compliant types. Use BLOB for other types.
template <typename T>
concept SupportedReal =
(std::is_same_v<T, float> && sizeof(float) == 4 && std::numeric_limits<T>::is_iec559) ||
(std::is_same_v<T, double> && sizeof(double) == 8 && std::numeric_limits<T>::is_iec559);

// Concept for string and blob
template <typename T>
concept SupportedBlob =
std::is_same_v<T, std::string> ||
std::is_same_v<T, std::u8string> ||
std::is_same_v<T, std::vector<uint8_t>>;

template <typename T>
concept SupportedTrivial = SupportedInteger<T> || SupportedReal<T>;


template <typename T>
concept AllSupportedTypes = SupportedTrivial<T> || SupportedBlob<T>;


enum class ValueType : sst::datablock::ValueTypeFieldType {
    UINT8 = 0,
    INT8,
    UINT16,
    INT16,
    UINT32,
    INT32,
    UINT64,
    INT64,
    FLOAT,
    DOUBLE,
    STRING,
    U8STRING,
    BLOB,
    REMOVED = std::numeric_limits<sst::datablock::ValueTypeFieldType>::max(),
};

template <class... T>
struct always_false : std::false_type {};

 template<typename T>
 constexpr ValueType valueTypeFromType() {
     if constexpr (std::is_same_v<T, uint8_t>)
         return ValueType::UINT8;
     else if constexpr (std::is_same_v<T, int8_t>)
         return ValueType::INT8;
     else if constexpr (std::is_same_v<T, uint16_t>)
         return ValueType::UINT16;
     else if constexpr (std::is_same_v<T, int16_t>)
         return ValueType::INT16;
     else if constexpr (std::is_same_v<T, uint32_t>)
         return ValueType::UINT32;
     else if constexpr (std::is_same_v<T, int32_t>)
         return ValueType::INT32;
     else if constexpr (std::is_same_v<T, uint64_t>)
         return ValueType::UINT64;
     else if constexpr (std::is_same_v<T, int64_t>)
         return ValueType::INT64;
     else if constexpr (std::is_same_v<T, float>)
         return ValueType::FLOAT;
     else if constexpr (std::is_same_v<T, double>)
         return ValueType::DOUBLE;
     else if constexpr (std::is_same_v<T, std::string>)
         return ValueType::STRING;
     else if constexpr (std::is_same_v<T, std::u8string>)
         return ValueType::U8STRING;
     else if constexpr (std::is_same_v<T, std::vector<uint8_t>>)
         return ValueType::BLOB;
     else
         static_assert(always_false<T>::value, "Unsupported type for ValueType::from");
    }

using Value = std::variant<
    uint8_t,
    int8_t,
    uint16_t,
    int16_t,
    uint32_t,
    int32_t,
    uint64_t,
    int64_t,
    float,
    double,
    std::string,
    std::u8string,
    std::vector<uint8_t>
>;

enum class EntryStatus {
    EXISTS,
    NOT_FOUND,
    REMOVED,
};

struct Entry {
    ValueType type;
    Value value;
};


struct Config {
    size_t memtable_size_bytes = 64 * 1024 * 1024; //64 MB
    size_t l0_max_files = 4; 
    size_t block_size = 128 * 1024;
};
