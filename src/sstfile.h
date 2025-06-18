#pragma once
#include <bit>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <functional>

#include "constants.h"
#include "types.h"
#include "datablock.h"
#include "sstbuilder.h"
#include "utils.h"

template <typename T>
concept SSTEntryConcept = requires(T t) {
    { t.entry } -> std::convertible_to<Entry>;
    { t.expiration_ms } -> std::convertible_to<uint64_t>;
};

template <typename T>
concept SSTPairConcept = requires(T t) {
    { t.first } -> std::convertible_to<const std::string&>;
    { t.second } -> SSTEntryConcept;
};

template <typename It>
concept SSTInputIterator =
std::input_iterator<It> && SSTPairConcept<std::iter_value_t<It>>;

class SSTFile {
public:
    class iterator {
    public:
        // Standard iterator typedefs:
        using iterator_category = std::input_iterator_tag;
        using value_type = std::pair<std::string, DataBlock::DataBlockEntry>;
        using difference_type = std::ptrdiff_t;
        using pointer = value_type*;
        using reference = value_type&;

        // Default‐constructed iterator is “end.”
        iterator() noexcept : sst_file_(nullptr), block_idx_(0), inner_idx_(0) {}
        // Construct a “begin” iterator (loads the first DataBlock, if any)
        explicit iterator(const SSTFile* sst) noexcept : sst_file_(sst), block_idx_(0), inner_idx_(0) {
            if (sst_file_->index_block_.empty()) {
                sst_file_ = nullptr;
                return;
            }
            loadCurrentBlock();
        }

        // Prefix ++
        iterator& operator++() {
            if (!sst_file_) {
                return *this; // already end
            }

            ++inner_idx_;
            // If still inside the same DataBlock:
            if (inner_idx_ < static_cast<size_t>(current_block_.count())) {
                return *this;
            }

            // else: move on to the next block
            ++block_idx_;
            if (block_idx_ >= sst_file_->index_block_.size()) {
                // no more blocks → become end
                sst_file_ = nullptr;
                return *this;
            }

            // otherwise, load the next DataBlock
            loadCurrentBlock();
            inner_idx_ = 0;
            return *this;
        }

        iterator operator++(int) {
            iterator tmp = *this;
            ++(*this);
            return tmp;
        }


        //return value_type for simplicity
        value_type operator*() const {
            return current_block_.get(static_cast<sst::datablock::CountFieldType>(inner_idx_));
        }

        bool operator==(const iterator& other) const noexcept {
            if (sst_file_ == nullptr && other.sst_file_ == nullptr) {
                return true;
            }
            return (sst_file_ == other.sst_file_) &&
                (block_idx_ == other.block_idx_) &&
                (inner_idx_ == other.inner_idx_);
        }

        bool operator!=(const iterator& other) const noexcept {
            return !(*this == other);
        }

    private:
        const SSTFile* sst_file_;
        size_t block_idx_;
        DataBlock current_block_;
        size_t inner_idx_;
        void loadCurrentBlock() {
            // Fetch offset of the block
            const auto& idx_vec = sst_file_->index_block_;
            auto offset = idx_vec[block_idx_].second;
            auto block_size = sst_file_->getDatablockSize(idx_vec.begin() + block_idx_);
            auto raw_bytes = sst_file_->readDatablock(offset, block_size);
            current_block_ = DataBlock(std::move(raw_bytes));
        }
    };

    iterator begin() const noexcept {
        return iterator(this);
    }
    iterator end() const noexcept {
        return iterator(); // default‐constructed = “end”
    }

    SSTFile(SSTFile&&) = delete;
    SSTFile& operator=(SSTFile&&) = delete;
    SSTFile(const SSTFile&) = delete;
    SSTFile& operator=(const SSTFile&) = delete;
    std::vector<std::string> keysWithPrefix(const std::string& prefix,
        unsigned int max_results) const;
    bool forEachKeyWithPrefix(const std::string& prefix,
        const std::function<bool(const std::string&)>& callback) const;

    std::optional<Entry> get(const std::string& key) const;
    bool remove(const std::string& key);
    EntryStatus status(const std::string& key) const;
    void rename(const std::filesystem::path& new_path);
    const std::filesystem::path& path() const noexcept {
        return path_;
    }
    const uint64_t& seqNum() const noexcept {
        return seq_num_;
    }
    std::string minKey() const;
    std::string maxKey() const;
    static std::unique_ptr<SSTFile> readAndCreate(const std::filesystem::path& sst_path);
    std::unique_ptr<SSTFile> shrink(uint32_t datablock_size) const;
    void clearCache() noexcept;
    static std::vector<std::unique_ptr<SSTFile>>  merge(
        const std::filesystem::path& sst1_path,
        const std::vector<std::filesystem::path>&,
        const std::filesystem::path& out_dir,
        uint64_t max_file_size,
        uint32_t datablock_size,
        bool keep_removed);

    template <SSTInputIterator InputIt>
    static std::unique_ptr<SSTFile> writeAndCreate(const std::filesystem::path& sst_path, int max_datablock_size, uint64_t seq_num,
        bool keep_removed, InputIt begin, InputIt end) {
        if (begin == end) {
            return nullptr;
        }

        SSTBuilder builder(sst_path, max_datablock_size, seq_num);
        for (auto it = begin; it != end; ++it) {
            const auto& val = *it;
            if (keep_removed || (val.second.entry.type != ValueType::REMOVED && !Utils::isExpired(val.second.expiration_ms))) {
                builder.addEntry(val.first, val.second.entry, val.second.expiration_ms);
            }
        }
        return builder.finalize();
    }


protected:
private:
    SSTFile(const std::filesystem::path& path, sst::indexblock::OffsetFieldType file_size,
        uint64_t seq_num, const std::string max_key,
        std::vector<std::pair<std::string, sst::indexblock::OffsetFieldType>> index_block);

    std::vector<uint8_t> readDatablock(sst::indexblock::OffsetFieldType block_offset, sst::indexblock::OffsetFieldType block_size) const;
    static std::vector<uint8_t> readDatablock(const std::filesystem::path path, sst::indexblock::OffsetFieldType block_offset, sst::indexblock::OffsetFieldType block_size);
    void writeDatablock(const DataBlock& block, sst::indexblock::OffsetFieldType offsetIndex) const;
    auto findDBlockOffset(const std::string& min_key) const;

    mutable std::ifstream ifs_;
    void openIfNeeded() const;

    std::filesystem::path path_;
    std::vector<std::pair<std::string, sst::indexblock::OffsetFieldType>> index_block_;
    sst::indexblock::OffsetFieldType index_block_offset_;
    uint64_t seq_num_;
    std::string max_key_;

    mutable std::mutex cache_mutex_;
    mutable std::unordered_map<sst::indexblock::OffsetFieldType, std::vector<uint8_t>> datablock_cache_; // Cache for datablocks by their offset
    static int max_cached_files_;
    sst::indexblock::OffsetFieldType getDatablockSize(decltype(index_block_)::const_iterator it) const;

    friend class SSTBuilder;
};
