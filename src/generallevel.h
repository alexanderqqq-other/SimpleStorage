#pragma once

#include "ilevel.h"

#include <filesystem>
#include <map>
#include <list>
#include <unordered_map>
// Implementation for Level 1 and higher. Key ranges do not overlap.
class GeneralLevel : public IFileLevel {
public:
    GeneralLevel(const std::filesystem::path& path, size_t max_file_size, size_t max_num_files, bool is_last);
    ~GeneralLevel() override = default;
    std::optional<Entry> get(const std::string& key) const override;
    bool remove(const std::string& key, uint64_t max_seq_num) override;
    EntryStatus status(const std::string& key) const override;
    std::vector<std::string> keysWithPrefix(const std::string& prefix, unsigned int max_results) const override;
    MergeResult mergeToTmp(const std::filesystem::path&, size_t datablock_size) const override;
    std::vector<std::filesystem::path> filelistToMerge(uint64_t max_seq_num) const override;
    void addSST(std::vector<std::unique_ptr<SSTFile>>  sst) override;
    void removeSSTs(const std::vector<std::filesystem::path>& sst_paths) override;
    void clearCache() noexcept override;
    uint64_t maxSeqNum() const override {
        return seq_num_map_.empty() ? 0 : (*seq_num_map_.rbegin()->second)->seqNum();
    }
    MergeResult shrink(uint32_t datablock_size);
    size_t count() const override;

private:

    std::filesystem::path path_;
    size_t max_file_size_;
    uint64_t max_file_index_ = 0; // Used to generate unique file names
    size_t max_num_files_; // Maximum number of SST files allowed in this level
    bool is_last_;

    mutable std::list<std::unique_ptr<SSTFile>> lru_sst_files_; // Least Recently Used cache for SST files
    std::map<std::string, decltype(lru_sst_files_)::iterator> sst_file_map_; // Maps keys to SST files
    std::map<uint64_t, decltype(lru_sst_files_)::iterator> seq_num_map_; // Maps sequence numbers to SST files
    std::unordered_map<std::string, decltype(lru_sst_files_)::iterator> file_path_map_; // Maps by filepath


    auto findSST(const std::string& key) -> decltype(lru_sst_files_)::iterator;
    auto findSST(const std::string& key) const -> decltype(lru_sst_files_)::const_iterator;

};
