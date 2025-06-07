#pragma once
#include "constants.h"
#include "types.h"
#include "datablock.h"
#include "utils.h"
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

class SSTFile;
class IndexBlockBuilder {
public:
    IndexBlockBuilder() = default;

    void addKey(const std::string& key, sst::indexblock::OffsetFieldType offset);
    std::vector<uint8_t> build();
    uint64_t size() const noexcept {
        return raw_data_.size() + sizeof(sst::indexblock::CountFieldType);
    }

private:
    std::vector<uint8_t> raw_data_;
};

class SSTBuilder {
public:
    SSTBuilder(const std::filesystem::path& path, uint32_t max_datablock_size, uint64_t seq_num);
    uint64_t currentSize();
    void addEntry(const std::string& key, const Entry& entry, uint64_t expiration_ms);
    void addDatablock(const std::string& min_key, const std::vector<uint8_t>& data,
        const std::string& max_key);
    std::unique_ptr<SSTFile> finalize();

private:
    void writeHeader(uint64_t seq_num);
    IndexBlockBuilder index_block_builder_;
    DataBlockBuilder data_block_builder_;
    std::vector<std::pair<std::string, sst::indexblock::OffsetFieldType>> inmemory_index_block_;
    std::ofstream ofs_;
    std::filesystem::path path_;
    uint64_t seq_num_;
    std::string last_key_; // Used to track the last key added to the SST
};
