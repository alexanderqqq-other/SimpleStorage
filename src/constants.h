#pragma once
#include <cstdint>

namespace sst {
    constexpr uint64_t MAX_L_LAST_SST_FILE_SIZE = 2ull * 1024 * 1024 * 1024 - 1; // 2 GB - 1 byte
    constexpr uint64_t MIN_MEMTABLE_SIZE = 4ull * 1024 * 1014;
    constexpr uint64_t MAX_MEMTABLE_SIZE = MAX_L_LAST_SST_FILE_SIZE;
    constexpr uint64_t MIN_L0_NUM_FILES = 2; // Minimum number of files in Level 0
    constexpr uint64_t MIN_BLOCK_SIZE = 2048; // Minimum block size in bytes
    constexpr uint64_t MAX_BLOCK_SIZE = 2 * 1024 * 1024; // Maximum block size in bytes

    namespace header {
        // Signature size in SST header (uint32_t)
        constexpr char SST_SIGNATURE[] = "VSSF";
        constexpr size_t SST_SIGNATURE_SIZE = sizeof(SST_SIGNATURE) - 1; // Exclude null terminator
        constexpr uint8_t SST_VERSION = 1; 
        constexpr uint64_t SST_SEQUENCE_SIZE = sizeof(uint64_t); // Sequence number size in SST header (uint64_t)
        // Version in SST header (uint8_t)
        constexpr size_t SST_VERSION_SIZE = 1;
        // Header total size (sum of all header fields)
        constexpr size_t SST_HEADER_SIZE = SST_SIGNATURE_SIZE + SST_VERSION_SIZE + SST_SEQUENCE_SIZE;
    }

    namespace datablock {
        // Key length in DataBlock
        using KeyLengthFieldType = uint16_t;
        using ExpirationFieldType = uint64_t;
        using ValueTypeFieldType = uint8_t;
        using ValueLengthFieldType = uint32_t;
        using CountFieldType = uint32_t;
        using OffsetEntryFieldType = uint32_t;

        constexpr size_t KEY_LEN_SIZE = sizeof(KeyLengthFieldType);
        constexpr size_t EXPIRATION_SIZE = sizeof(ExpirationFieldType);
        constexpr size_t VALUE_TYPE_SIZE = sizeof(ValueTypeFieldType);
        constexpr size_t VALUE_LEN_SIZE = sizeof(ValueLengthFieldType); //optional, only for blob-like values
        constexpr size_t OFFSET_ENTRY_SIZE = sizeof(OffsetEntryFieldType);
        constexpr size_t DATABLOCK_COUNT_SIZE = sizeof(CountFieldType);
        constexpr size_t MIN_ENTRY_SIZE = KEY_LEN_SIZE + EXPIRATION_SIZE + VALUE_TYPE_SIZE;
        constexpr size_t MAX_KEY_LENGTH = 1024;
        // Expiration special values (for not set and for deleted)
        constexpr uint64_t EXPIRATION_NOT_SET = 0ull;
        constexpr uint64_t EXPIRATION_DELETED = 1ull;
    }
    namespace indexblock {
        using IndexKeyLengthFieldType = datablock::KeyLengthFieldType;
        using OffsetFieldType = uint64_t;
        using CountFieldType = uint32_t;
        constexpr size_t INDEX_KEY_LEN = sizeof(IndexKeyLengthFieldType);
        constexpr size_t BLOCK_OFFSET_SIZE = sizeof(OffsetFieldType);
        constexpr size_t INDEX_BLOCK_COUNT_SIZE = sizeof(CountFieldType);
    }
}