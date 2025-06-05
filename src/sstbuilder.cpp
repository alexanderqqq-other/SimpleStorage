#include "sstbuilder.h"
#include "utils.h"
#include "sstfile.h"
namespace iblock = sst::indexblock;

void IndexBlockBuilder::addKey(const std::string& key, iblock::OffsetFieldType offset) {
    raw_data_.reserve(raw_data_.size() + key.size()
        + sizeof(iblock::IndexKeyLengthFieldType)
        + sizeof(iblock::OffsetFieldType));
    auto key_size = static_cast<iblock::IndexKeyLengthFieldType>(key.size());
    Utils::serializeLE(key_size, raw_data_);
    raw_data_.insert(raw_data_.end(), key.begin(), key.end());
    Utils::serializeLE(offset, raw_data_);
}
std::vector<uint8_t> IndexBlockBuilder::build() {
    Utils::serializeLE(static_cast<iblock::CountFieldType>(raw_data_.size()), raw_data_);
    std::vector<uint8_t> ret;
    ret.swap(raw_data_);
    return ret;
}



SSTBuilder::SSTBuilder(const std::filesystem::path& path, uint32_t max_datablock_size, uint64_t seq_num)
    : index_block_builder_(), data_block_builder_(max_datablock_size),
    ofs_(path, std::ios::binary), path_(path), seq_num_(seq_num) {
    if (!ofs_) {
        throw std::runtime_error("Failed to open SST file for writing: " +
            path.string());
    }
}
uint64_t SSTBuilder::currentSize() {
    return static_cast<uint64_t>(ofs_.tellp()) + data_block_builder_.size() + index_block_builder_.size();
}

void SSTBuilder::writeHeader(uint64_t seq_num) {
    ofs_.write(sst::header::SST_SIGNATURE, sst::header::SST_SIGNATURE_SIZE);
    ofs_ << sst::header::SST_VERSION;
    std::vector<uint8_t> sequence_buf;
    Utils::serializeLE(seq_num, sequence_buf);
    ofs_.write(reinterpret_cast<const char*>(sequence_buf.data()), sequence_buf.size());
}

void SSTBuilder::addEntry(const std::string& key, const Entry& entry, uint64_t expiration_ms) {
    last_key_ = key;
    if (inmemory_index_block_.empty()) {
        writeHeader(seq_num_);
        index_block_builder_.addKey(key, ofs_.tellp());
        inmemory_index_block_.push_back({ key, ofs_.tellp() });
    }
    if (!data_block_builder_.addEntry(key, entry, expiration_ms)) {
        auto datablock_data = data_block_builder_.build();
        ofs_.write(reinterpret_cast<const char*>(datablock_data.data()),
            datablock_data.size());
        index_block_builder_.addKey(key, ofs_.tellp());
        inmemory_index_block_.push_back({ key, ofs_.tellp() });

        // add current value to new datablock
        if (!data_block_builder_.addEntry(key, entry, expiration_ms)) {
            throw std::runtime_error("Failed to add entry even after flushing DataBlock (entry too large?)");
        }
    }
}

SSTFile SSTBuilder::finalize() {
    if (!data_block_builder_.empty()) {
        auto datablock_data = data_block_builder_.build();
        ofs_.write(reinterpret_cast<const char*>(datablock_data.data()),
            datablock_data.size());
    }
    auto indexblock_data = index_block_builder_.build();
    size_t index_block_offset = ofs_.tellp();
    ofs_.write(reinterpret_cast<const char*>(indexblock_data.data()),
        indexblock_data.size());
    ofs_.close();
    return SSTFile(path_, index_block_offset, seq_num_, last_key_, inmemory_index_block_);
}