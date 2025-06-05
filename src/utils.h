#pragma once
#include <cstdint>
#include <chrono>
#include "types.h"
namespace Utils {
    uint64_t getNow();
    bool isExpired(uint64_t timestamp);


    template <SupportedTrivial T>
    void serializeLE(T value, std::vector<uint8_t>& buffer) {
        if constexpr (std::is_floating_point_v<T>) {
            using UIntType =
                std::conditional_t<sizeof(T) == sizeof(float), uint32_t, uint64_t>;
            for (size_t i = 0; i < sizeof(T); ++i) {
                buffer.push_back(static_cast<uint8_t>(
                    (std::bit_cast<UIntType>(value) >> (8 * i)) & 0xFF));
            }
        }
        else {
            using UIntType = std::make_unsigned_t<T>;
            for (size_t i = 0; i < sizeof(T); ++i) {
                buffer.push_back(static_cast<uint8_t>(
                    (std::bit_cast<UIntType>(value) >> (8 * i)) & 0xFF));
            }
        }
    }

    template <SupportedBlob T>
    void serializeLE(T value, std::vector<uint8_t>& buffer) {
        buffer.insert(buffer.end(), value.begin(), value.end());
    }

    template <SupportedTrivial T>
    T deserializeLE(const uint8_t* buffer) {
        if constexpr (std::is_floating_point_v<T>) {
            using UIntType =
                std::conditional_t<sizeof(T) == sizeof(float), uint32_t, uint64_t>;
            UIntType value = 0;
            for (size_t i = 0; i < sizeof(T); ++i)
                value |= static_cast<UIntType>(buffer[i]) << (8 * i);
            return std::bit_cast<T>(value);
        }
        else {
            using UIntType = std::make_unsigned_t<T>;
            UIntType value = 0;
            for (size_t i = 0; i < sizeof(T); ++i)
                value |= static_cast<UIntType>(buffer[i]) << (8 * i);
            return std::bit_cast<T>(value);
        }
    }

    template <SupportedBlob T>
    T deserializeLE(const uint8_t* buffer, sst::datablock::CountFieldType size) {
        return T(buffer, buffer + size);
    }

    template <typename T>
    size_t onDiskSize(const T& value) {
        if constexpr (SupportedInteger<T> || SupportedReal<T>) {
            return sizeof(T);
        }
        else if constexpr (SupportedBlob<T>) {
            return value.size() * sizeof(typename T::value_type) + sst::datablock::VALUE_LEN_SIZE;
        }
        else {
            static_assert(always_false<T>::value, "Unsupported type for value_size");
        }
    }

    inline sst::datablock::ValueLengthFieldType onDiskSize(const Value& v) {
        return std::visit([](const auto& val) { return onDiskSize(val); }, v);
    }

    template <typename T>
    inline uint32_t onDiskEntrySize(const std::string& key, const T& value) {
        return key.size() + onDiskSize(value) + sst::datablock::MIN_ENTRY_SIZE +
            sst::datablock::OFFSET_ENTRY_SIZE;
    }

}