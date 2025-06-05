#pragma once
#include "types.h"
#include <vector>
#include <string>
#include <optional>

class DataBlock {
public:
    struct DataBlockEntry {
        Entry entry;
        uint64_t expiration_ms;
    };

    DataBlock() = default;
    DataBlock(std::vector<uint8_t> data);
    std::optional<Entry> get(const std::string& key) const;
    std::pair<std::string, DataBlockEntry> get(sst::datablock::CountFieldType offsetIdx) const;
    std::vector<std::string> keysWithPrefix(const std::string& prefix, unsigned int max_results) const;
    bool remove(const std::string& key);
    EntryStatus status(const std::string& key) const;

    sst::datablock::CountFieldType count() const noexcept {
        return count_;
    }
    
    const std::vector<uint8_t>& data() const noexcept {
        return data_;
    }
private:
    uint64_t posByOffset(sst::datablock::CountFieldType offsetIdx) const;
    ValueType parseValueType(uint64_t entry_start_pos, sst::datablock::KeyLengthFieldType key_size) const;
    std::string parseKey(uint64_t entry_start_pos) const;
    Value parseValue(uint64_t entry_start_pos, sst::datablock::KeyLengthFieldType key_size, ValueType type) const;
    std::optional<sst::datablock::CountFieldType> lowerBoundOffset(const std::string& key) const;

    std::vector<uint8_t> data_;
    sst::datablock::CountFieldType count_ = 0;  // Number of entries in the block
    sst::datablock::OffsetEntryFieldType offset_table_pos_ = 0;  // Position of the offset table in the data
    uint32_t max_entry_ptr_ = 0;
};

class DataBlockBuilder {
public:
    DataBlockBuilder(uint32_t max_block_size);
    bool addEntry(const std::string& key, const Entry& entry, uint64_t ttl);
    bool empty() const noexcept;
    uint64_t size() const noexcept;
    std::vector<uint8_t> build();

private:
    uint32_t max_block_size_;
    std::vector<uint32_t> offset_table_;
    std::vector<uint8_t> raw_data_;
    uint32_t count_ = 0;  // Number of entries in the block
};